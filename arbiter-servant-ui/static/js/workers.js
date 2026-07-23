/**
 * Alpine component: worker registry view with pause/resume controls.
 *
 * Polls every 30s while the Workers tab is visible.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('workersTab', () => ({
    ...pollingTab('loadWorkers', ARB_TIMING.workerPollMs),
    ...confirmArm(),
    ...typeToConfirm('pauseConfirm'),
    workers: [],
    loading: false,
    loaded: false,
    _loadErrored: false,
    selectedWorker: null,
    liveOnly: localStorage.getItem('arb.workersLiveOnly') === 'true',
    // How many trailing worker-id characters the pause confirmation asks for.
    workerConfirmLen: 6,
    pendingWorker: null,

    init() {
      this.initPolling('#tab-workers', {
        onQueueChange: () => { this.disarm(); this.resetConfirm(); this.pendingWorker = null; this.workers = []; },
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

    togglePause(worker) {
      const id = worker.workerId;
      if (!id || this.busyRows[id]) return;
      // Resume is low-risk, so it keeps the two-click arm. Pause opens a
      // type-to-confirm modal (type the worker id's trailing characters).
      if (worker.paused) {
        if (!this.confirmArmed('toggle:' + id)) return;
        this._applyPause(worker, false);
        return;
      }
      this.pendingWorker = worker;
      this.openConfirm(String(id).slice(-this.workerConfirmLen));
      showModal('workerPauseConfirmModal');
    },

    confirmPauseWorker() {
      if (!this.confirmValid() || !this.pendingWorker) return;
      const worker = this.pendingWorker;
      hideModal('workerPauseConfirmModal');
      this._applyPause(worker, true);
    },

    async _applyPause(worker, pause) {
      const id = worker.workerId;
      await this.withBusyRow(id, async () => {
        try {
          if (pause) await ArbiterAPI.pauseWorker(id);
          else await ArbiterAPI.resumeWorker(id);
          await this.loadWorkers();
        } catch (e) {
          showToast(`Failed to toggle worker ${String(id).slice(0, 8)}: ${e.message}`);
        }
      });
      this.pendingWorker = null;
      this.resetConfirm();
    },
  }));
});
