/**
 * Alpine component: stat cards (total/ready/in-flight/scheduled/backoff/suspended/oldest-ready)
 *
 * Refreshes on SSE events matching the selected queue instead of polling.
 * The 30s sse-refresh timer keeps time-dependent values (like oldest job age) fresh.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('statsTab', () => ({
    ...eventBusTab(),
    stats: null,
    loading: false,
    active: false,
    _statsDebounce: null,
    _loadErrored: false,

    init() {
      trackTabActive(this, '#tab-stats', {
        onShow: () => this.loadStats(),
      });
      const refreshTick = () => { if (this.active && !this.loading) this.loadStats(); };
      this._bindBus({
        queueChanged: () => { this.stats = null; if (this.active) this.loadStats(); },
        sseEvent: (e) => {
          if (!this.active) return;
          const queue = Alpine.store('app').selectedQueue;
          if (e.detail.some(evt => evt.table === queue)) this._debouncedLoadStats();
        },
        sseReconnect: () => { if (this.active) this.loadStats(); },
        sseRefresh: refreshTick,
        pollTick: refreshTick,
      });
    },

    destroy() {
      untrackTabActive(this);
      if (this._statsDebounce) { clearTimeout(this._statsDebounce); this._statsDebounce = null; }
      this._unbindBus();
    },

    _debouncedLoadStats() {
      if (this._statsDebounce) return;
      this._statsDebounce = setTimeout(() => {
        this._statsDebounce = null;
        this.loadStats();
      }, ARB_TIMING.statsDebounceMs);
    },

    async loadStats() {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      await guardedLoad(this, 'Failed to load stats', async (seq, isStale) => {
        const data = await ArbiterAPI.getStats(queue);
        if (isStale()) return;
        this.stats = data.stats;
      });
    },
  }));
});
