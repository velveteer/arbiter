/**
 * Alpine component: the per-queue stat cards.
 *
 * The stats query aggregates over the whole queue table, so the refresh interval
 * is the only thing that schedules it, as it is for the job tables. A queue's
 * event stream does not reload it: at a busy queue's event rate that outpaced any
 * interval the reader picked.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('statsTab', () => ({
    ...eventBusTab(),
    ...tabActive(),
    ...pollSpinner(),
    ...refreshControl('loadStats', 'arb.statsRefresh', '30s'),
    loadNoun: 'stats',
    stats: null,
    kinds: [],
    _kindsQueue: '',
    ...loadState((s) => !s.stats),

    init() {
      this._watchPolling();
      trackTabActive(this, '#tab-stats', {
        onShow: () => { this.loadStats(); this._startTimer(); },
        onHide: () => this._stopTimer(),
      });
      this._bindBus({
        queueChanged: () => { this.stats = null; this.kinds = []; this._kindsQueue = ''; if (this.active) this.loadStats(); },
        sseReconnect: () => { if (this.active) this.loadStats(); },
      });
    },

    destroy() {
      untrackTabActive(this);
      this._stopTimer();
      this._stopWatchPolling();
      this._unbindBus();
      releaseInitialLoad(this);
    },

    // Card link href: this queue's Jobs tab filtered by status (empty = all).
    jobsUrl(status) {
      return queueJobsUrl(Alpine.store('app').selectedQueue, status);
    },

    goToJobs(e, status) {
      if (!plainNavClick(e)) return;
      window.dispatchEvent(new CustomEvent(ARB_EVENTS.filterJobs, { detail: status }));
    },

    // Declared kinds with their live depth, DLQ count and bar width, deepest first.
    kindRows() {
      const live = this.stats?.kindCounts || {};
      const dead = this.stats?.dlqKindCounts || {};
      const rows = this.kinds
        .map((kind) => ({ kind, depth: live[kind] || 0, dlq: dead[kind] || 0 }))
        .sort((a, b) => b.depth - a.depth || b.dlq - a.dlq || a.kind.localeCompare(b.kind));
      const top = rows[0]?.depth;
      return rows.map((r) => ({ ...r, barPct: top ? clampPct(r.depth / top) : 0 }));
    },

    kindUrl(kind, tab) {
      return queueJobsUrl(Alpine.store('app').selectedQueue, { kind }, tab);
    },

    goToKind(e, kind, tab) {
      if (!plainNavClick(e)) return;
      Alpine.store('app').openQueueJobs(Alpine.store('app').selectedQueue, { kind }, tab);
    },

    goToDLQ() {
      const btn = document.querySelector('[data-bs-target="#tab-dlq"]');
      if (btn) bootstrap.Tab.getOrCreateInstance(btn).show();
    },

    fmtAge: formatDurationSecs,
    fmtCount: formatCompact,

    zeroClass(n) {
      return n ? '' : 'is-zero';
    },

    async loadStats() {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      await guardedLoad(this, async (seq, isStale) => {
        const kindsWanted = this._kindsQueue !== queue;
        // The kinds only feed the By kind list, so their failure leaves the stats standing.
        const [data, kinds] = await Promise.all([
          ArbiterAPI.getStats(queue),
          kindsWanted ? ArbiterAPI.listKinds(queue).catch(() => null) : null,
        ]);
        if (isStale()) return;
        this.stats = data.stats;
        if (kindsWanted && kinds) {
          this.kinds = kinds;
          this._kindsQueue = queue;
        }
      }, {
        // Suppress a stats toast for a queue we've already navigated away from
        // mid-fetch: the in-flight load for the old queue is no longer relevant.
        suppressToast: () => Alpine.store('app').selectedQueue !== queue,
      });
    },
  }));
});
