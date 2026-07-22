/**
 * Alpine.js global store: queues, selected queue, SSE state, theme.
 */
// Top-level views that aren't queue-scoped (each a nav destination after Queues).
const SYSTEM_VIEWS = ['events', 'ratelimits', 'concurrency', 'cron'];

document.addEventListener('alpine:init', () => {
  Alpine.store('app', {
    queues: [],
    selectedQueue: '',
    view: 'queues',
    initialized: false,
    initialLoads: 0,
    showLoader: false,
    _loaderTimer: null,
    _deepLinkPending: false,
    detailReady: false,
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

    // Count of in-flight first-time loads. The bar only renders once loads have
    // been pending past a threshold, so fast connections never flash it.
    beginInitialLoad() {
      this.initialLoads++;
      if (this.initialLoads === 1 && !this._loaderTimer) {
        this._loaderTimer = setTimeout(() => {
          this._loaderTimer = null;
          if (this.initialLoads > 0) this.showLoader = true;
        }, ARB_TIMING.loaderDelayMs);
      }
    },
    endInitialLoad() {
      this.initialLoads = Math.max(0, this.initialLoads - 1);
      if (this.initialLoads === 0) {
        if (this._loaderTimer) { clearTimeout(this._loaderTimer); this._loaderTimer = null; }
        this.showLoader = false;
      }
    },

    async init() {
      // Mount the detail one frame after the list unmounts. A same-flush
      // list-unmount plus detail-mount skips the last tab pane's Alpine init.
      Alpine.effect(() => {
        const show = this.view === 'queues' && !!this.selectedQueue;
        if (!show) {
          this.detailReady = false;
          return;
        }
        requestAnimationFrame(() => {
          if (this.view === 'queues' && this.selectedQueue) this.detailReady = true;
        });
      });
      // A system view is queue-independent, so resolve it up front. A deep-linked
      // queue is mounted only after listQueues confirms it exists, so a stale link
      // never mounts a detail view or fires sub-tab loads against a missing table.
      // While that validation is in flight, _deepLinkPending holds the queue list
      // back so it doesn't flash before the detail view takes over.
      const params = new URLSearchParams(location.search);
      const urlQueue = params.get('queue');
      const urlView = params.get('view');
      if (urlView && SYSTEM_VIEWS.includes(urlView)) {
        this.view = urlView;
      } else if (urlQueue) {
        this._deepLinkPending = true;
      }

      this.beginInitialLoad();
      try {
        const data = await ArbiterAPI.listQueues();
        this.queues = (data && data.queues) || [];
        if (urlQueue && this.view === 'queues') {
          if (this.queues.includes(urlQueue)) {
            this.selectedQueue = urlQueue;
          } else {
            showToast(`Queue "${urlQueue}" not found`, 'warning');
            this._updateUrl('');
          }
        }
      } catch (e) {
        console.error('Failed to load queues:', e);
        showToast('Failed to load queues: ' + e.message);
      } finally {
        this.endInitialLoad();
      }
      this._deepLinkPending = false;
      this.initialized = true;
      this.connectSSE();

      // Sync tab → hash
      document.addEventListener('shown.bs.tab', (e) => {
        const target = e.target.getAttribute('data-bs-target');
        if (target) {
          const tab = target.replace('#tab-', '');
          // Jobs/DLQ own the filter params and rewrite them on load; other sub-tabs
          // must clear them, else a stale filter desyncs the URL from the view.
          if (tab !== 'jobs' && tab !== 'dlq') clearFiltersFromUrl();
          this._updateUrl(tab);
        }
      });
    },

    // Drill into a queue (view + selection + queueChanged), writing the URL via `setUrl`.
    // forceReset dispatches queueChanged even when the queue is unchanged, so re-opening
    // the current queue still resets tab filters to match the freshly cleared URL.
    _drillInto(queue, setUrl, forceReset = false) {
      const changed = this.selectedQueue !== queue;
      dismissOpenModals();
      this.selectedQueue = queue;
      this.view = 'queues';
      setUrl();
      if (changed || forceReset) window.dispatchEvent(new CustomEvent(ARB_EVENTS.queueChanged, { detail: queue }));
    },

    // Drill into a queue's detail view (from the queue list or the quick-switcher).
    // A lateral switch from within a detail keeps the current sub-tab; drilling in from
    // the list resets to the default (Stats) by clearing the hash.
    openQueue(queue) {
      const wasInDetail = !!this.selectedQueue;
      this._drillInto(queue, () => { clearFiltersFromUrl(); this._updateUrl(wasInDetail ? undefined : ''); }, true);
    },

    // Drill into a queue's Jobs tab pre-filtered to a status (from a queue card).
    openQueueJobs(queue, status) {
      this._drillInto(queue, () => history.replaceState(null, '', queueJobsUrl(queue, status)));
    },

    // Switch to a top-level view: 'queues' (the queue area) or one of SYSTEM_VIEWS.
    setView(view) {
      dismissOpenModals();
      this.view = view;
      // Returning to the Queues section always lands on the list, so the nav
      // button is never a no-op while a queue is open.
      if (view === 'queues') this.selectedQueue = '';
      // Clear any sub-tab hash left over from a queue detail view.
      this._updateUrl('');
    },

    _updateUrl(newHash) {
      const url = new URL(location.href);
      url.searchParams.delete('view');
      url.searchParams.delete('queue');
      if (this.view === 'queues') {
        if (this.selectedQueue) url.searchParams.set('queue', this.selectedQueue);
      } else {
        url.searchParams.set('view', this.view);
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
