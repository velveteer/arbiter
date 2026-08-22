/**
 * Alpine.js global store: queues, selected queue, SSE state, theme.
 */
// Top-level views that aren't queue-scoped (each a nav destination after Queues).
const SYSTEM_VIEWS = ['events', 'ratelimits', 'concurrency', 'cron', 'workers'];

document.addEventListener('alpine:init', () => {
  Alpine.store('app', {
    queues: [],
    selectedQueue: '',
    view: 'queues',
    initialized: false,
    showLoader: false,
    _loaderClaims: new Set(),
    _loaderSeq: 0,
    _loaderTimer: null,
    _deepLinkPending: false,
    detailReady: false,
    connected: false,
    sseDisabled: false,
    eventSource: null,
    events: [],
    maxEventsPerQueue: 200,
    _eventSeq: 0,
    _eventBuffer: [],
    _flushScheduled: false,
    _hasConnected: false,
    _dropped: false,
    _sseRetryTimer: null,
    _sseRetryMs: 0,
    theme: document.documentElement.getAttribute('data-bs-theme') || 'dark',
    // Off unless switched on, so a reader opts into the stream rather than out.
    sseOff: localStorage.getItem('arb.eventsOff') !== '0',
    health: null,
    _healthInterval: null,
    _healthInFlight: false,

    // ok / down / unknown, the last only before the first probe answers.
    get healthState() {
      return this.health ? this.health.status : 'unknown';
    },

    get healthTitle() {
      if (this.health && this.health.reachable === false) return 'Cannot reach the server';
      return {
        ok: 'Server healthy',
        down: 'Server cannot reach its database',
        unknown: 'Checking server health',
      }[this.healthState];
    },

    // Runs on its own timer, so it reports even with live updates switched off.
    startHealthPolling() {
      if (this._healthInterval) return;
      this.loadHealth();
      this._healthInterval = setInterval(() => this.loadHealth(), ARB_TIMING.healthPollMs);
    },

    async loadHealth() {
      if (this._healthInFlight) return;
      this._healthInFlight = true;
      try {
        this.health = await ArbiterAPI.getHealth();
      } finally {
        this._healthInFlight = false;
      }
    },

    // connected / connecting / disconnected / polling / off, for the nav indicator.
    // A stream that has not answered yet is connecting. Only one that was live
    // and went away is disconnected.
    get sseState() {
      if (this.sseOff) return 'off';
      if (this.sseDisabled) return 'polling';
      if (this.connected) return 'connected';
      return this._dropped ? 'disconnected' : 'connecting';
    },

    get sseTitle() {
      return {
        off: 'Live updates off',
        polling: 'Live updates unavailable on this server',
        connected: 'Live updates connected',
        connecting: 'Connecting to live updates',
        disconnected: 'Reconnecting to live updates',
      }[this.sseState];
    },

    // Why the log is empty, which depends on whether a stream is feeding it.
    get eventsEmptyText() {
      return {
        off: 'Live updates are off. Switch them on to stream events.',
        polling: 'This server does not stream events.',
        connected: 'No events yet. They appear here as they happen.',
        connecting: 'Connecting to the event stream.',
        disconnected: 'Reconnecting. Events resume when the stream is back.',
      }[this.sseState];
    },

    toggleSSE() {
      if (this.sseDisabled) return;
      this.sseOff = !this.sseOff;
      localStorage.setItem('arb.eventsOff', this.sseOff ? '1' : '0');
      if (this.sseOff) {
        this.closeSSE();
      } else {
        this._sseRetryMs = 0;
        this.connectSSE();
      }
    },

    // Tears the stream down without the retry that a dropped connection gets.
    closeSSE() {
      if (this._sseRetryTimer) { clearTimeout(this._sseRetryTimer); this._sseRetryTimer = null; }
      if (this.eventSource) { this.eventSource.close(); this.eventSource = null; }
      this.connected = false;
      this._dropped = false;
    },

    // Names where the reader is, for the page heading and the browser tab. A
    // queue takes the name; otherwise the nav destination does.
    get pageTitle() {
      if (this.selectedQueue) return this.selectedQueue;
      return {
        queues: 'Queues',
        ratelimits: 'Rate Limits',
        concurrency: 'Concurrency',
        cron: 'Cron',
        workers: 'Workers',
        events: 'Events',
      }[this.view] || 'Queues';
    },

    // Outstanding first-time loads, one token per claimant. The bar only renders
    // once loads have been pending past a threshold, so fast connections never
    // flash it.
    claimLoader() {
      const token = ++this._loaderSeq;
      this._loaderClaims.add(token);
      if (this._loaderClaims.size === 1 && !this._loaderTimer) {
        this._loaderTimer = setTimeout(() => {
          this._loaderTimer = null;
          if (this._loaderClaims.size) this.showLoader = true;
        }, ARB_TIMING.loaderDelayMs);
      }
      return token;
    },
    // A token spends once, so releasing a claim twice cannot drop another view's.
    releaseLoader(token) {
      if (!this._loaderClaims.delete(token)) return;
      if (this._loaderClaims.size) return;
      if (this._loaderTimer) { clearTimeout(this._loaderTimer); this._loaderTimer = null; }
      this.showLoader = false;
    },

    async init() {
      this.startHealthPolling();
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

      const loaderToken = this.claimLoader();
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
        this.releaseLoader(loaderToken);
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

    // Drill into a named sub-tab of a queue's detail view (from a queue card badge).
    openQueueTab(queue, tab) {
      this._drillInto(queue, () => { clearFiltersFromUrl(); this._updateUrl(tab); }, true);
    },

    // Drill into a queue's Jobs tab pre-filtered to a status (from a queue card).
    openQueueJobs(queue, status) {
      this._drillInto(queue, () => history.replaceState(null, '', queueJobsUrl(queue, status)));
    },

    // Drill into a queue's Jobs tab showing one job (from the event log).
    openQueueJob(queue, jobId) {
      this._drillInto(queue, () => history.replaceState(null, '', queueJobUrl(queue, jobId)));
    },

    // Open a policy view focused on one gate prefix, from a job's Gates cell.
    openPolicy(view, prefix) {
      dismissOpenModals();
      this.view = view;
      this.selectedQueue = '';
      const url = new URL(location.href);
      url.searchParams.delete('queue');
      url.searchParams.set('view', view);
      url.searchParams.set('prefix', prefix);
      url.hash = '';
      history.replaceState(null, '', url);
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
      url.searchParams.delete('prefix');
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

    // The browser retries a stream that drops on its own. One the server refuses
    // closes for good, so re-arm it here and back off to a slow re-probe.
    _scheduleSSE() {
      if (this.sseOff || this._sseRetryTimer) return;
      this._sseRetryMs = Math.min(
        this._sseRetryMs ? this._sseRetryMs * 2 : ARB_TIMING.sseRetryMs,
        ARB_TIMING.sseRetryMaxMs
      );
      this._sseRetryTimer = setTimeout(() => {
        this._sseRetryTimer = null;
        this.connectSSE();
      }, this._sseRetryMs);
    },

    connectSSE() {
      if (this.sseOff) return;
      if (this.eventSource) {
        this.eventSource.close();
      }
      this.eventSource = ArbiterAPI.connectSSE(
        (event) => {
          this.connected = true;
          try {
            const data = JSON.parse(event.data);
            if (data.event === 'disabled') {
              this.eventSource.close();
              this.eventSource = null;
              this.connected = false;
              this.sseDisabled = true;
              this._scheduleSSE();
              return;
            }
            if (data.event === 'connected') {
              // Reconnect (not first connect) — refetch all tabs
              if (this._hasConnected) {
                window.dispatchEvent(new CustomEvent(ARB_EVENTS.sseReconnect));
              }
              this._hasConnected = true;
              this._dropped = false;
              this._sseRetryMs = 0;
              if (this._sseRetryTimer) { clearTimeout(this._sseRetryTimer); this._sseRetryTimer = null; }
              this.sseDisabled = false;
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
          this._dropped = true;
          if (!this.eventSource || this.eventSource.readyState === EventSource.CLOSED) this._scheduleSSE();
        }
      );
    },

    _scheduleFlush() {
      if (this._flushScheduled) return;
      this._flushScheduled = true;
      setTimeout(() => {
        this._flushScheduled = false;
        if (this._eventBuffer.length === 0) return;
        const batch = this._eventBuffer.splice(0);
        this.events = this._retainEvents([...batch].reverse().concat(this.events));
        window.dispatchEvent(new CustomEvent(ARB_EVENTS.sseEvent, { detail: batch }));
      }, ARB_TIMING.flushMs);
    },

    // Retention is per queue, so a busy queue does not evict a quiet one's tail
    // before it can be read. The list arrives newest first, so counting down it
    // keeps each queue's newest.
    _retainEvents(events) {
      const kept = {};
      return events.filter((e) => {
        const queue = e.table || '';
        kept[queue] = (kept[queue] || 0) + 1;
        return kept[queue] <= this.maxEventsPerQueue;
      });
    },
  });
});
