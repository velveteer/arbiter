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
    _eventSeq: 0,
    _eventBuffer: [],
    _flushScheduled: false,
    _hasConnected: false,
    _refreshInterval: null,
    _pollInterval: null,
    _sseHandshakeTimer: null,
    theme: document.documentElement.getAttribute('data-bs-theme') || 'dark',

    async init() {
      try {
        const data = await ArbiterAPI.listQueues();
        this.queues = (data && data.queues) || [];
        const params = new URLSearchParams(location.search);
        const urlQueue = params.get('queue');
        if (urlQueue && this.queues.includes(urlQueue)) {
          this.selectQueue(urlQueue);
        } else {
          if (urlQueue) showToast(`Queue "${urlQueue}" not found`, 'warning');
          if (this.queues.length > 0) this.selectQueue(this.queues[0]);
        }
      } catch (e) {
        console.error('Failed to load queues:', e);
        showToast('Failed to load queues: ' + e.message);
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
      window.dispatchEvent(new CustomEvent(ARB_EVENTS.queueChanged, { detail: queue }));
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
      if (this._sseHandshakeTimer) clearTimeout(this._sseHandshakeTimer);
      this._sseHandshakeTimer = setTimeout(() => {
        if (!this._hasConnected && !this.sseDisabled) {
          this._startPolling();
          this._startRefreshTimer();
        }
      }, ARB_TIMING.sseHandshakeMs);
      this.eventSource = ArbiterAPI.connectSSE(
        (event) => {
          this.connected = true;
          try {
            const data = JSON.parse(event.data);
            if (data.event === 'disabled') {
              if (this._sseHandshakeTimer) { clearTimeout(this._sseHandshakeTimer); this._sseHandshakeTimer = null; }
              this.eventSource.close();
              this.eventSource = null;
              this.connected = false;
              this.sseDisabled = true;
              this._startPolling();
              this._startRefreshTimer();
              return;
            }
            if (data.event === 'connected') {
              if (this._sseHandshakeTimer) { clearTimeout(this._sseHandshakeTimer); this._sseHandshakeTimer = null; }
              // Reconnect (not first connect) — refetch all tabs
              if (this._hasConnected) {
                window.dispatchEvent(new CustomEvent(ARB_EVENTS.sseReconnect));
              }
              this._hasConnected = true;
              this._stopPolling();
              this._startRefreshTimer();
              return;
            }
            this._eventBuffer.push({
              ...data,
              receivedAt: new Date().toISOString(),
              _seq: ++this._eventSeq,
            });
            this._scheduleFlush();
          } catch (e) {
            // Non-JSON event (keep-alive)
          }
        },
        () => {
          this.connected = false;
          this._startPolling();
          this._startRefreshTimer();
        }
      );
    },

    _startPolling() {
      if (this._pollInterval) return;
      this._pollInterval = setInterval(() => {
        window.dispatchEvent(new CustomEvent(ARB_EVENTS.pollTick));
      }, ARB_TIMING.pollMs);
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
        window.dispatchEvent(new CustomEvent(ARB_EVENTS.sseRefresh));
      }, ARB_TIMING.refreshMs);
    },

    _scheduleFlush() {
      if (this._flushScheduled) return;
      this._flushScheduled = true;
      setTimeout(() => {
        this._flushScheduled = false;
        if (this._eventBuffer.length === 0) return;
        const batch = this._eventBuffer.splice(0);
        this.events = [...batch].reverse().concat(this.events).slice(0, this.maxEvents);
        window.dispatchEvent(new CustomEvent(ARB_EVENTS.sseEvent, { detail: batch }));
      }, ARB_TIMING.flushMs);
    },
  });
});
