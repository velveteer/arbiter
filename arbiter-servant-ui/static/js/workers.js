/**
 * Alpine component: worker registry view with pause/resume controls.
 *
 * Polls every 30s while the Workers tab is visible.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('workersTab', () => ({
    ...pollingTab('loadWorkers', ARB_TIMING.workerPollMs),
    ...confirmArm(),
    workers: [],
    loading: false,
    loaded: false,
    _loadErrored: false,
    selectedWorker: null,
    liveOnly: localStorage.getItem('arb.workersLiveOnly') === 'true',

    init() {
      this.initPolling('#tab-workers', {
        onQueueChange: () => { this.disarm(); this.workers = []; },
      });
    },

    destroy() {
      this.teardownPolling();
    },

    async loadWorkers() {
      const queue = this.$store.app.selectedQueue;
      if (!queue) {
        this.workers = [];
        return;
      }
      await guardedLoad(this, 'Failed to load workers', async (seq, isStale) => {
        const data = await ArbiterAPI.listWorkers({ queue });
        if (isStale()) return;
        this.workers = data.workers || [];
      });
    },

    get displayWorkers() {
      if (!this.liveOnly) return this.workers;
      return this.workers.filter(w => w.health !== 'stale');
    },

    persistLiveOnly() {
      localStorage.setItem('arb.workersLiveOnly', this.liveOnly ? 'true' : 'false');
    },

    // Health is server-computed (DB clock): 'live' | 'stale' | 'draining'.
    // Independent of the paused flag, which is rendered as its own badge.
    healthLabel(w) {
      return w.health || 'live';
    },

    healthClass(w) {
      switch (w.health) {
        case 'live': return 'badge bg-success';
        case 'stale': return 'badge bg-danger';
        case 'draining': return 'badge bg-secondary';
        default: return 'badge bg-light text-dark';
      }
    },

    showDetail(worker) {
      this.selectedWorker = worker;
      showModal('workerDetailModal');
    },

    async togglePause(worker) {
      const id = worker.workerId;
      if (!id || this.busyRows[id]) return;
      if (!this.confirmArmed('toggle:' + id)) return;
      await this.withBusyRow(id, async () => {
        try {
          if (worker.paused) {
            await ArbiterAPI.resumeWorker(id);
          } else {
            await ArbiterAPI.pauseWorker(id);
          }
          await this.loadWorkers();
        } catch (e) {
          showToast(`Failed to toggle worker ${String(id).slice(0, 8)}: ${e.message}`);
        }
      });
    },
  }));
});
