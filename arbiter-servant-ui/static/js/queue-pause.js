/**
 * Alpine component: shows the selected queue's pause state and a toggle button.
 * Refreshes off the global event bus (poll-tick / sse-refresh / sse-reconnect),
 * so its cadence tracks whatever the rest of the UI is doing.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('queuePauseToggle', () => ({
    paused: false,
    pausedAt: null,
    pausedAgeStr: '',
    busy: false,

    init() {
      this._onQueueChanged = () => {
        this.paused = false;
        this.pausedAt = null;
        if (Alpine.store('app').selectedQueue) this.refresh();
      };
      this._onRefresh = () => this.refresh();
      window.addEventListener('queue-changed', this._onQueueChanged);
      window.addEventListener('poll-tick', this._onRefresh);
      window.addEventListener('sse-refresh', this._onRefresh);
      window.addEventListener('sse-reconnect', this._onRefresh);
      if (Alpine.store('app').selectedQueue) this.refresh();
    },

    destroy() {
      window.removeEventListener('queue-changed', this._onQueueChanged);
      window.removeEventListener('poll-tick', this._onRefresh);
      window.removeEventListener('sse-refresh', this._onRefresh);
      window.removeEventListener('sse-reconnect', this._onRefresh);
    },

    async refresh() {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      // Sequence guard: drop a stale response if a newer refresh (e.g. after a
      // queue change) started while this one was in flight.
      this._refreshSeq = (this._refreshSeq || 0) + 1;
      const seq = this._refreshSeq;
      try {
        const details = await ArbiterAPI.getQueueDetails(queue);
        if (seq !== this._refreshSeq) return;
        this.paused = details && !!details.paused;
        this.pausedAt = details && details.pausedAt || null;
      } catch (e) {
        if (seq !== this._refreshSeq) return;
        if (e.status === 404) {
          this.paused = false;
          this.pausedAt = null;
        } else {
          console.error('Failed to load queue details:', e);
        }
      }
      // Recompute every refresh so the relative age string ticks (formatAge reads
      // Date.now(), which Alpine cannot track on its own).
      this.pausedAgeStr = this.pausedAt ? formatAge(this.pausedAt) : '';
    },

    async toggle() {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue || this.busy) return;
      const action = this.paused ? 'Resume' : 'Pause';
      const detail = this.paused
        ? `Resume queue "${queue}"? Workers will start claiming jobs again.`
        : `Pause queue "${queue}"? All workers will stop claiming jobs.`;
      if (!confirm(detail)) return;
      this.busy = true;
      try {
        if (this.paused) {
          await ArbiterAPI.resumeQueue(queue);
        } else {
          await ArbiterAPI.pauseQueue(queue);
        }
        await this.refresh();
      } catch (e) {
        console.error(`Failed to ${action.toLowerCase()} queue:`, e);
        showToast(`Failed to ${action.toLowerCase()} queue: ` + e.message);
      } finally {
        this.busy = false;
      }
    },
  }));
});
