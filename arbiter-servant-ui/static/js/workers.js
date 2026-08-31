/**
 * Alpine component: worker registry view with pause/resume controls. Backs the
 * per-queue Workers tab and the global Workers view (all queues, mounted via
 * workersTab({ global: true })).
 *
 * Polls every 30s while visible.
 */
// The pause-confirm and detail modals, stamped into both the per-queue Workers
// tab and the global Workers view. Bindings resolve against the workersTab scope.
const WORKER_MODALS_HTML = `
<div class="modal fade" id="workerPauseConfirmModal" tabindex="-1" aria-labelledby="workerPauseConfirmModalLabel">
  <div class="modal-dialog">
    <div class="modal-content">
      <div class="modal-header">
        <div class="modal-heading">
          <h2 class="modal-title" id="workerPauseConfirmModalLabel">Pause worker</h2>
          <p class="modal-subject" :title="pendingWorker?.workerId" x-text="pendingWorker?.workerId"></p>
        </div>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <p class="text-muted small mb-3">Pausing stops this worker pool from claiming new jobs until it is resumed. In-flight jobs finish, new ones wait.</p>
        <label class="form-label small">Type the last <span x-text="workerConfirmLen"></span> characters <code x-text="confirmTarget"></code> to confirm:</label>
        <input type="text" class="form-control form-control-sm" x-model="confirmText" autocomplete="off" spellcheck="false"
               @keydown.enter.prevent="confirmPauseWorker()" :placeholder="confirmTarget">
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary btn-sm" data-bs-dismiss="modal">Cancel</button>
        <button type="button" class="btn btn-warning btn-sm" @click="confirmPauseWorker()" :disabled="!confirmValid() || isBusy(pendingWorker?.workerId)">Pause worker</button>
      </div>
    </div>
  </div>
</div>

<div class="offcanvas offcanvas-end detail-drawer" tabindex="-1" id="workerDetailDrawer"
     data-bs-backdrop="false" data-bs-scroll="true" :aria-label="detailTitle">
  <div class="offcanvas-header" x-html="DETAIL_HEAD_HTML"></div>
  <div class="offcanvas-body" x-show="selectedWorker">
        <dl class="row">
          <dt class="col-sm-4">Worker ID</dt>
          <dd class="col-sm-8"><code class="small" x-text="selectedWorker?.workerId"></code></dd>
          <dt class="col-sm-4">Queue</dt>
          <dd class="col-sm-8" x-text="selectedWorker?.queueName"></dd>
          <dt class="col-sm-4">Host</dt>
          <dd class="col-sm-8" x-text="selectedWorker?.hostName ?? EMPTY"></dd>
          <dt class="col-sm-4">Threads</dt>
          <dd class="col-sm-8" x-text="selectedWorker?.workerCount ?? EMPTY"></dd>
          <dt class="col-sm-4">Started At</dt>
          <dd class="col-sm-8" x-text="formatTime(selectedWorker?.startedAt, EMPTY)"></dd>
          <dt class="col-sm-4">Last Heartbeat</dt>
          <dd class="col-sm-8">
            <span x-text="formatTime(selectedWorker?.lastHeartbeat, EMPTY)"></span>
            <span class="text-muted ms-2" x-text="selectedWorker ? '(' + formatAge(selectedWorker.lastHeartbeat) + ')' : ''"></span>
          </dd>
          <dt class="col-sm-4">Stale Threshold</dt>
          <dd class="col-sm-8"><span x-text="selectedWorker?.staleThresholdSecs"></span>s</dd>
          <dt class="col-sm-4">Health</dt>
          <dd class="col-sm-8">
            <span :class="selectedWorker ? healthClass(selectedWorker) : ''" x-text="selectedWorker ? healthLabel(selectedWorker) : ''"></span>
          </dd>
          <dt class="col-sm-4">Paused</dt>
          <dd class="col-sm-8" x-text="selectedWorker?.paused ? 'Yes' : 'No'"></dd>
          <dt class="col-sm-4">Metadata</dt>
          <dd class="col-sm-8">
            <div x-copyable="formatJson(selectedWorker?.metadata)"></div>
          </dd>
        </dl>
  </div>
</div>`;

// The worker table, stamped into both the per-queue Workers tab and the global
// Workers view. The Queue column is the only difference between them.
const WORKERS_TABLE_HTML = `
<div class="table-responsive" :aria-busy="loading">
  <table class="table table-striped table-hover table-sm sticky-head" style="table-layout: fixed; width: 100%;">
    <colgroup>
      <col style="width: 10%">
      <template x-if="global"><col style="width: 13%"></template>
      <col style="width: 20%">
      <col style="width: 6%">
      <col style="width: 14%">
      <template x-if="anyMetadata"><col style="width: 14%"></template>
      <col style="width: 10%">
      <col style="width: 13%">
    </colgroup>
    <thead>
      <tr>
        <th class="sortable" @click="toggleSort('workerId')" @keydown.enter.prevent="toggleSort('workerId')" @keydown.space.prevent="toggleSort('workerId')" tabindex="0" :aria-sort="ariaSort('workerId')">Worker ID<span class="sort-caret" x-text="sortIndicator('workerId')" aria-hidden="true"></span></th>
        <template x-if="global"><th class="sortable" @click="toggleSort('queue')" @keydown.enter.prevent="toggleSort('queue')" @keydown.space.prevent="toggleSort('queue')" tabindex="0" :aria-sort="ariaSort('queue')">Queue<span class="sort-caret" x-text="sortIndicator('queue')" aria-hidden="true"></span></th></template>
        <th class="sortable" @click="toggleSort('host')" @keydown.enter.prevent="toggleSort('host')" @keydown.space.prevent="toggleSort('host')" tabindex="0" :aria-sort="ariaSort('host')">Host<span class="sort-caret" x-text="sortIndicator('host')" aria-hidden="true"></span></th>
        <th class="sortable" @click="toggleSort('threads')" @keydown.enter.prevent="toggleSort('threads')" @keydown.space.prevent="toggleSort('threads')" tabindex="0" :aria-sort="ariaSort('threads')">Threads<span class="sort-caret" x-text="sortIndicator('threads')" aria-hidden="true"></span></th>
        <th class="sortable" @click="toggleSort('heartbeat')" @keydown.enter.prevent="toggleSort('heartbeat')" @keydown.space.prevent="toggleSort('heartbeat')" tabindex="0" :aria-sort="ariaSort('heartbeat')">Last heartbeat<span class="sort-caret" x-text="sortIndicator('heartbeat')" aria-hidden="true"></span></th>
        <template x-if="anyMetadata"><th>Metadata</th></template>
        <th class="sortable" @click="toggleSort('status')" @keydown.enter.prevent="toggleSort('status')" @keydown.space.prevent="toggleSort('status')" tabindex="0" :aria-sort="ariaSort('status')">Status<span class="sort-caret" x-text="sortIndicator('status')" aria-hidden="true"></span></th>
        <th class="cell-actions">Actions</th>
      </tr>
    </thead>
    <tbody>
      <template x-for="job in displayWorkers" :key="job.workerId">
        <tr class="detail-row" @click="rowClick($event, job)">
          <td class="text-truncate"><code class="small" :title="job.workerId" x-text="shortId(job.workerId)"></code></td>
          <template x-if="global">
            <td class="text-truncate">
              <a href="#" @click.prevent="$store.app.openQueueTab(job.queueName, 'workers')" x-text="job.queueName"
                :title="'Open ' + job.queueName"></a>
            </td>
          </template>
          <td class="text-truncate" :title="job.hostName ?? ''" x-text="job.hostName ?? EMPTY"></td>
          <td x-text="job.workerCount ?? EMPTY"></td>
          <td><span :title="formatTime(job.lastHeartbeat)" x-text="formatAge(job.lastHeartbeat)"></span></td>
          <template x-if="anyMetadata">
            <td class="text-truncate small font-monospace" :title="job.metadata ? JSON.stringify(job.metadata) : ''"
                x-text="job.metadata ? JSON.stringify(job.metadata) : EMPTY"></td>
          </template>
          <td>
            <span :class="healthClass(job)" x-text="healthLabel(job)"></span>
            <template x-if="job.paused"><span class="badge paused-badge ms-1">paused</span></template>
          </td>
          <td class="cell-actions">
            <div class="dropdown">
              <button class="btn btn-row-actions btn-sm" type="button" data-bs-toggle="dropdown" data-bs-auto-close="outside" aria-expanded="false" :disabled="isBusy(job.workerId)" title="Row actions" aria-label="Row actions">&#8942;</button>
              <ul class="dropdown-menu dropdown-menu-end" x-html="WORKER_ACTIONS_HTML"></ul>
            </div>
          </td>
        </tr>
      </template>
      <tr x-show="_loadErrored && displayWorkers.length === 0">
        <td :colspan="colCount()" class="text-danger text-center">Failed to load workers. <a href="#" @click.prevent="loadWorkers()">Retry</a></td>
      </tr>
      <tr x-show="!_loadErrored && loaded && displayWorkers.length === 0">
        <td :colspan="colCount()" class="text-muted text-center"
          x-text="liveOnly && workers.length > 0 ? 'No active workers.' : 'No workers registered.'"></td>
      </tr>
    </tbody>
    <tbody x-html="TABLE_SKELETON_HTML"></tbody>
  </table>
</div>`;

// Roll-up strip, stamped above the toolbar in the global Workers view.
const WORKERS_SUMMARY_HTML = `
<div class="queue-summary" :class="{ 'is-loading': !loaded, 'd-none': loaded && workers.length === 0 }">
  <div class="qs-skeleton" x-html="SUMMARY_SKELETON_HTML"></div>
  <div class="qs-item">
    <span class="qs-val" x-text="workers.length"></span>
    <span class="qs-lbl" x-text="pluralize(workers.length, 'worker')"></span>
  </div>
  <div class="qs-item">
    <span class="qs-val" x-text="threadCount"></span>
    <span class="qs-lbl" x-text="pluralize(threadCount, 'thread')"></span>
  </div>
  <div class="qs-item">
    <span class="qs-val" x-text="healthCounts.live"></span>
    <span class="qs-lbl">live</span>
  </div>
  <div class="qs-item">
    <span class="qs-val" :class="{ bad: healthCounts.stale > 0 }" x-text="healthCounts.stale"></span>
    <span class="qs-lbl">stale</span>
  </div>
  <div class="qs-item" x-show="healthCounts.draining > 0">
    <span class="qs-val" x-text="healthCounts.draining"></span>
    <span class="qs-lbl">draining</span>
  </div>
  <div class="qs-item">
    <span class="qs-val" :class="{ warn: healthCounts.paused > 0 }" x-text="healthCounts.paused"></span>
    <span class="qs-lbl">paused</span>
  </div>
</div>`;

// Row actions, stamped into both the row menu and the drawer header.
const WORKER_ACTIONS_HTML = `
<li x-show="!job._inDrawer"><a class="dropdown-item" href="#" @click.prevent="viewDetail(job); closeDropdown($el)">Detail</a></li>
<li><a class="dropdown-item" :href="jobsUrl(job)" @click="openJobs($event, job)">View held jobs</a></li>
<li>
  <a class="dropdown-item" href="#" :class="{ 'text-warning fw-semibold': job.paused && isArmed('toggle:' + job.workerId), 'disabled': job.shuttingDown }"
    @click.prevent="togglePause(job)"
    x-text="job.paused ? (isArmed('toggle:' + job.workerId) ? 'Click to confirm' : 'Resume') : 'Pause'"></a>
</li>`;

// Sort readers for the worker table.
const WORKER_SORT_KEYS = {
  workerId: (w) => String(w.workerId),
  queue: (w) => w.queueName || '',
  host: (w) => w.hostName || '',
  threads: (w) => w.workerCount ?? 0,
  heartbeat: (w) => Date.parse(w.lastHeartbeat || '') || -1,
  status: (w) => healthRank(w),
};

// Sort order for the global view: the workers worth looking at come first.
function healthRank(w) {
  if (w.health === 'stale') return 0;
  if (w.paused) return 1;
  if (w.health === 'draining') return 2;
  return 3;
}

document.addEventListener('alpine:init', () => {
  Alpine.data('workersTab', (opts = {}) => ({
    ...pollingTab('loadWorkers', ARB_TIMING.workerPollMs, 'arb.workersRefresh'),
    ...clientSort('workers', WORKER_SORT_KEYS, '', 'workerId'),
    ...confirmArm(),
    ...typeToConfirm('pauseConfirm'),
    ...rowDetail('displayWorkers', 'workerId', 'selectedWorker'),
    global: !!opts.global,
    detailActionsHtml: WORKER_ACTIONS_HTML,
    workers: [],
    ...loadState(),
    selectedWorker: null,
    liveOnly: localStorage.getItem('arb.workersLiveOnly') === 'true',
    // How many trailing worker-id characters the pause confirmation asks for.
    workerConfirmLen: 6,
    pendingWorker: null,

    init() {
      // Global mode is a top-level view mounted by x-if. The per-queue tab is
      // a Bootstrap tab scoped to the selected queue.
      if (this.global) {
        this.initPollingMounted();
      } else {
        this.initPolling('#tab-workers', {
          onQueueChange: () => { this.disarm(); this.resetConfirm(); this.pendingWorker = null; this.workers = []; },
        });
      }
    },

    destroy() {
      this.teardownPolling();
    },

    async loadWorkers() {
      const queue = this.global ? undefined : this.$store.app.selectedQueue;
      if (!this.global && !queue) {
        this.workers = [];
        return;
      }
      await guardedLoad(this, 'Failed to load workers', async (seq, isStale) => {
        const data = await ArbiterAPI.listWorkers({ queue });
        if (isStale()) return;
        this.workers = data.workers || [];
        this.resyncDetailSelection();
      });
    },

    get displayWorkers() {
      const rows = this.liveOnly ? this.workers.filter(w => w.health !== 'stale') : this.workers;
      // A chosen column wins. Until then the global view leads with the workers
      // worth looking at.
      if (this.sortBy) return this.sortRows(rows);
      if (!this.global) return rows;
      return rows.slice().sort((a, b) =>
        healthRank(a) - healthRank(b)
        || (a.queueName || '').localeCompare(b.queueName || '')
        || String(a.workerId).localeCompare(String(b.workerId)));
    },

    // Counts for the global header: the answer to "is anything wrong anywhere".
    // Metadata is optional per worker, so the column only earns its width when
    // some row fills it.
    get anyMetadata() {
      return this.displayWorkers.some((w) => w.metadata);
    },

    colCount() {
      return (this.global ? 8 : 7) - (this.anyMetadata ? 0 : 1);
    },

    shortId,

    // The jobs this worker currently holds: its queue's Jobs tab, filtered to its lease.
    jobsUrl(worker) {
      return worker ? queueJobsUrl(worker.queueName, { claimed_by: worker.workerId }) : '#';
    },

    openJobs(e, worker) {
      if (!worker || !plainNavClick(e)) return;
      Alpine.store('app').openQueueJobs(worker.queueName, { claimed_by: worker.workerId });
    },

    get threadCount() {
      return this.workers.reduce((n, w) => n + (w.workerCount || 0), 0);
    },

    get healthCounts() {
      return this.workers.reduce((acc, w) => {
        acc[w.health] = (acc[w.health] || 0) + 1;
        if (w.paused) acc.paused += 1;
        return acc;
      }, { live: 0, stale: 0, draining: 0, paused: 0 });
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
        case 'live': return 'badge bg-success-subtle text-success-emphasis';
        case 'stale': return 'badge bg-danger-subtle text-danger-emphasis';
        case 'draining': return 'badge bg-secondary-subtle text-secondary-emphasis';
        default: return 'badge bg-secondary-subtle text-secondary-emphasis';
      }
    },

    get detailTitle() {
      return this.selectedWorker ? 'Worker ' + String(this.selectedWorker.workerId).slice(0, 8) : 'Worker';
    },

    get detailStatus() {
      return this.selectedWorker ? this.healthLabel(this.selectedWorker) : '';
    },

    // Worker health has its own colour scale, not the job-status one.
    detailStatusClass() {
      return this.selectedWorker ? this.healthClass(this.selectedWorker) : '';
    },

    get detailRows() {
      const cur = this.selectedWorker;
      return cur ? [Object.assign({}, cur, { _id: cur.workerId, _inDrawer: true })] : [];
    },

    viewDetail(worker) {
      this.selectedWorker = worker;
      showDrawer('workerDetailDrawer');
    },

    togglePause(worker) {
      const id = worker.workerId;
      if (!id || this.busyRows[id] || worker.shuttingDown) return;
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
