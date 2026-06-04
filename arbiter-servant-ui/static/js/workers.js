/**
 * Alpine component: worker registry view with pause/resume controls.
 *
 * Polls every 30s while the Workers tab is visible.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('workersTab', () => ({
    workers: [],
    loading: false,
    actionError: '',
    refreshInterval: null,
    busyRows: {},
    selectedWorker: null,
    liveOnly: localStorage.getItem('arb.workersLiveOnly') === 'true',

    init() {
      trackTabActive(this, '#tab-workers', {
        onShow: () => {
          this.loadWorkers();
          this.startPolling();
        },
        onHide: () => {
          this.stopPolling();
        },
      });

      this.$watch('$store.app.selectedQueue', () => {
        this.workers = [];
        if (this.active) this.loadWorkers();
      });

      this._visibilityHandler = () => {
        if (document.hidden) {
          this.stopPolling();
        } else if (this.active) {
          this.loadWorkers();
          this.startPolling();
        }
      };
      document.addEventListener('visibilitychange', this._visibilityHandler);
    },

    destroy() {
      this.stopPolling();
      if (this._visibilityHandler) {
        document.removeEventListener('visibilitychange', this._visibilityHandler);
        this._visibilityHandler = null;
      }
    },

    startPolling() {
      this.stopPolling();
      this.refreshInterval = setInterval(() => this.loadWorkers(), 30000);
    },

    stopPolling() {
      if (this.refreshInterval) {
        clearInterval(this.refreshInterval);
        this.refreshInterval = null;
      }
    },

    async loadWorkers() {
      const queue = this.$store.app.selectedQueue;
      if (!queue) {
        this.workers = [];
        return;
      }
      this.loading = true;
      this._loadSeq = (this._loadSeq || 0) + 1;
      const seq = this._loadSeq;
      try {
        const data = await ArbiterAPI.listWorkers({ queue });
        if (seq !== this._loadSeq) return;
        this.workers = data.workers || [];
      } catch (e) {
        if (seq !== this._loadSeq) return;
        console.error('Failed to load workers:', e);
      } finally {
        if (seq === this._loadSeq) this.loading = false;
      }
    },

    isBusy(id) {
      return !!this.busyRows[id];
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
      const modalEl = document.getElementById('workerDetailModal');
      if (modalEl && window.bootstrap) {
        bootstrap.Modal.getOrCreateInstance(modalEl).show();
      }
    },

    async togglePause(worker) {
      const id = worker.workerId;
      if (this.busyRows[id]) return;
      this.actionError = '';
      this.busyRows = { ...this.busyRows, [id]: true };
      try {
        if (worker.paused) {
          await ArbiterAPI.resumeWorker(id);
        } else {
          await ArbiterAPI.pauseWorker(id);
        }
        await this.loadWorkers();
      } catch (e) {
        this.actionError = `Failed to toggle worker ${id.slice(0, 8)}: ${e.message}`;
      } finally {
        const next = { ...this.busyRows };
        delete next[id];
        this.busyRows = next;
      }
    },
  }));
});
