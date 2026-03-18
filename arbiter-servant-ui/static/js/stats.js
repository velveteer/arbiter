/**
 * Alpine component: stat cards (total/visible/in-flight/oldest)
 *
 * Refreshes on SSE events matching the selected queue instead of polling.
 * The 30s sse-refresh timer keeps time-dependent values (like oldest job age) fresh.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('statsTab', () => ({
    stats: null,
    loading: false,
    active: false,
    _statsDebounce: null,

    init() {
      trackTabActive(this, '#tab-stats', {
        onShow: () => this.loadStats(),
      });

      window.addEventListener('queue-changed', () => {
        this.stats = null;
        if (this.active) {
          this.loadStats();
        }
      });

      window.addEventListener('sse-event', (e) => {
        if (!this.active) return;
        const queue = Alpine.store('app').selectedQueue;
        const hasRelevant = e.detail.some(evt => evt.table === queue);
        if (hasRelevant) {
          this._debouncedLoadStats();
        }
      });
      window.addEventListener('sse-reconnect', () => {
        if (this.active) this.loadStats();
      });
      window.addEventListener('sse-refresh', () => {
        if (this.active && !this.loading) this.loadStats();
      });
      window.addEventListener('poll-tick', () => {
        if (this.active && !this.loading) this.loadStats();
      });
    },

    _debouncedLoadStats() {
      if (this._statsDebounce) return;
      this._statsDebounce = setTimeout(() => {
        this._statsDebounce = null;
        this.loadStats();
      }, 500);
    },

    async loadStats() {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      this.loading = true;
      try {
        const data = await ArbiterAPI.getStats(queue);
        this.stats = data.stats;
      } catch (e) {
        console.error('Failed to load stats:', e);
      } finally {
        this.loading = false;
      }
    },
  }));
});
