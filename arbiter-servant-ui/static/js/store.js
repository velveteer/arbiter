/**
 * Alpine.js global store: queues, selected queue, SSE state, theme.
 */
document.addEventListener('alpine:init', () => {
  Alpine.store('app', {
    queues: [],
    selectedQueue: '',
    initialized: false,
    connected: false,
    sseDisabled: false,
    eventSource: null,
    events: [],
    maxEvents: 200,
    _eventBuffer: [],
    _flushScheduled: false,
    _hasConnected: false,
    _refreshInterval: null,
    _pollInterval: null,
    streaming: false,
    theme: document.documentElement.getAttribute('data-bs-theme') || 'dark',

    async init() {
      try {
        const data = await ArbiterAPI.listQueues();
        this.queues = data.queues || [];
        const params = new URLSearchParams(location.search);
        const urlQueue = params.get('queue');
        if (urlQueue && this.queues.includes(urlQueue)) {
          this.selectQueue(urlQueue);
        } else if (this.queues.length > 0) {
          this.selectQueue(this.queues[0]);
        }
      } catch (e) {
        console.error('Failed to load queues:', e);
      }
      this.initialized = true;
      this.connectSSE();

      // Sync tab → hash
      document.addEventListener('shown.bs.tab', (e) => {
        const target = e.target.getAttribute('data-bs-target');
        if (target) {
          this._updateUrl(target.replace('#tab-', ''));
        }
      });
    },

    selectQueue(queue) {
      this.selectedQueue = queue;
      clearFiltersFromUrl();
      this._updateUrl();
      window.dispatchEvent(new CustomEvent('queue-changed', { detail: queue }));
    },

    _updateUrl(newHash) {
      const url = new URL(location.href);
      if (this.selectedQueue) {
        url.searchParams.set('queue', this.selectedQueue);
      } else {
        url.searchParams.delete('queue');
      }
      if (newHash !== undefined) {
        url.hash = newHash;
      }
      history.replaceState(null, '', url);
    },

    toggleTheme() {
      this.theme = this.theme === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-bs-theme', this.theme);
      localStorage.setItem('arbiter-theme', this.theme);
    },

    connectSSE() {
      if (this.eventSource) {
        this.eventSource.close();
      }
      this.eventSource = ArbiterAPI.connectSSE(
        (event) => {
          this.connected = true;
          try {
            const data = JSON.parse(event.data);
            if (data.event === 'disabled') {
              // SSE disabled on the server — close and don't reconnect
              this.eventSource.close();
              this.eventSource = null;
              this.connected = false;
              this.sseDisabled = true;
              return;
            }
            if (data.event === 'connected') {
              // Reconnect (not first connect) — refetch all tabs
              if (this._hasConnected) {
                window.dispatchEvent(new CustomEvent('sse-reconnect'));
              }
              this._hasConnected = true;
              this.streaming = false;
              this._startPolling();
              this._startRefreshTimer();
              return;
            }
            this._eventBuffer.push({
              ...data,
              receivedAt: new Date().toISOString(),
            });
            this._scheduleFlush();
          } catch (e) {
            // Non-JSON event (keep-alive)
          }
        },
        () => {
          this.connected = false;
        }
      );
    },

    _startPolling() {
      if (this._pollInterval) return;
      this._pollInterval = setInterval(() => {
        window.dispatchEvent(new CustomEvent('poll-tick'));
      }, 5000);
    },

    _stopPolling() {
      if (this._pollInterval) {
        clearInterval(this._pollInterval);
        this._pollInterval = null;
      }
    },

    _startRefreshTimer() {
      if (this._refreshInterval) return;
      this._refreshInterval = setInterval(() => {
        window.dispatchEvent(new CustomEvent('sse-refresh'));
      }, 30000);
    },

    _scheduleFlush() {
      if (this._flushScheduled) return;
      this._flushScheduled = true;
      setTimeout(() => {
        this._flushScheduled = false;
        if (this._eventBuffer.length === 0) return;
        if (!this.streaming) {
          this.streaming = true;
          this._stopPolling();
        }
        const batch = this._eventBuffer.splice(0);
        this.events = [...batch].reverse().concat(this.events).slice(0, this.maxEvents);
        window.dispatchEvent(new CustomEvent('sse-event', { detail: batch }));
      }, 250);
    },
  });
});
