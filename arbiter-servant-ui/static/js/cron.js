/**
 * Alpine component: cron schedule table with an override editor in the detail panel.
 *
 * Used in two places: the per-queue Cron tab (scoped to the selected queue)
 * and the global Cron view (all queues, mounted via cronTab({ global: true })).
 * Only polls while its view is active and the browser tab is visible.
 * The disable-schedule confirm modal renders once at page level, driven by the cronEdit store.
 */
// Display text for an overlap policy. The wire value stays as declared.
function overlapLabel(v) {
  return v === 'SkipOverlap' ? 'Skip overlap' : v === 'AllowOverlap' ? 'Allow overlap' : (v ?? EMPTY);
}

// Human-readable description of a cron expression, or '' if it can't be parsed.
function safeCronDescribe(expr) {
  try {
    return cronstrue.toString(expr);
  } catch {
    return '';
  }
}

// Alpine re-wraps `this` in a fresh proxy per evaluation, so a host cannot be
// identified by reference.
let cronHostSeq = 0;

// Sort readers for the schedule table.
const CRON_SORT_KEYS = {
  name: (s) => s.name,
  queue: (s) => s.queueName || '',
  overlap: (s) => s.overrideOverlap ?? s.defaultOverlap ?? '',
  timezone: (s) => s.overrideTimezone ?? s.defaultTimezone ?? '',
  enabled: (s) => (s.enabled ? 1 : 0),
  lastFired: (s) => Date.parse(s.lastManualRunAt || s.lastFiredAt || '') || -1,
  lastChecked: (s) => Date.parse(s.lastCheckedAt || '') || -1,
};

// Row actions, stamped into both the row menu and the drawer header.
const CRON_ACTIONS_HTML = `
<li x-show="!job._inDrawer"><a class="dropdown-item" href="#" @click.prevent="viewDetail(job); closeDropdown($el)">Detail</a></li>
<li x-show="!$store.cronEdit.editing"><a class="dropdown-item" href="#" :class="{ disabled: isBusy(job.name) }" @click.prevent="openEdit(job); closeDropdown($el)">Edit</a></li>
<li>
  <a class="dropdown-item" href="#" :class="{ 'fw-semibold': isArmed('run:' + job.name), disabled: isBusy(job.name) || !canRun(job) }"
    @click.prevent="runNow(job)" :title="runTitle(job)"
    x-text="isArmed('run:' + job.name) ? 'Confirm run' : 'Run now'"></a>
</li>`;

// The schedule table, stamped into both the per-queue Cron tab and the global Cron
// view. The Queue column is the only difference between them.
const CRON_TABLE_HTML = `
<div class="table-responsive">
  <table class="table table-hover table-sm sticky-head" style="table-layout: fixed; width: 100%;">
    <colgroup>
      <col style="width: 12%">
      <template x-if="global"><col style="width: 11%"></template>
      <col style="width: 16%">
      <col style="width: 10%">
      <col style="width: 10%">
      <col style="width: 6%">
      <col style="width: 14%">
      <col style="width: 12%">
      <col style="width: 10%">
    </colgroup>
    <thead>
      <tr>
        <th class="sortable" @click="toggleSort('name')" @keydown.enter.prevent="toggleSort('name')" @keydown.space.prevent="toggleSort('name')" tabindex="0" :aria-sort="ariaSort('name')">Name<span class="sort-caret" x-text="sortIndicator('name')" aria-hidden="true"></span></th>
        <template x-if="global"><th class="sortable" @click="toggleSort('queue')" @keydown.enter.prevent="toggleSort('queue')" @keydown.space.prevent="toggleSort('queue')" tabindex="0" :aria-sort="ariaSort('queue')">Queue<span class="sort-caret" x-text="sortIndicator('queue')" aria-hidden="true"></span></th></template>
        <th>Expression</th>
        <th class="sortable" @click="toggleSort('overlap')" @keydown.enter.prevent="toggleSort('overlap')" @keydown.space.prevent="toggleSort('overlap')" tabindex="0" :aria-sort="ariaSort('overlap')">Overlap policy<span class="sort-caret" x-text="sortIndicator('overlap')" aria-hidden="true"></span></th>
        <th class="sortable" @click="toggleSort('timezone')" @keydown.enter.prevent="toggleSort('timezone')" @keydown.space.prevent="toggleSort('timezone')" tabindex="0" :aria-sort="ariaSort('timezone')">Timezone<span class="sort-caret" x-text="sortIndicator('timezone')" aria-hidden="true"></span></th>
        <th class="sortable" @click="toggleSort('enabled')" @keydown.enter.prevent="toggleSort('enabled')" @keydown.space.prevent="toggleSort('enabled')" tabindex="0" :aria-sort="ariaSort('enabled')">Enabled<span class="sort-caret" x-text="sortIndicator('enabled')" aria-hidden="true"></span></th>
        <th class="sortable" @click="toggleSort('lastFired')" @keydown.enter.prevent="toggleSort('lastFired')" @keydown.space.prevent="toggleSort('lastFired')" tabindex="0" :aria-sort="ariaSort('lastFired')">Last fired<span class="sort-caret" x-text="sortIndicator('lastFired')" aria-hidden="true"></span></th>
        <th class="sortable" @click="toggleSort('lastChecked')" @keydown.enter.prevent="toggleSort('lastChecked')" @keydown.space.prevent="toggleSort('lastChecked')" tabindex="0" :aria-sort="ariaSort('lastChecked')">Last checked<span class="sort-caret" x-text="sortIndicator('lastChecked')" aria-hidden="true"></span></th>
        <th class="cell-actions">Actions</th>
      </tr>
    </thead>
    <tbody>
      <template x-for="job in displaySchedules" :key="job.name">
        <tr class="detail-row" @click="rowClick($event, job)">
          <td class="text-truncate" x-text="job.name" :title="job.name"></td>
          <template x-if="global">
            <td class="text-truncate">
              <a href="#" @click.prevent="$store.app.openQueue(job.queueName)" x-text="job.queueName" :title="'Open ' + job.queueName"></a>
            </td>
          </template>
          <td>
            <div class="d-flex flex-column gap-0">
              <div class="d-flex flex-wrap align-items-center gap-1">
                <span x-text="effectiveExpression(job)"></span>
                <span x-show="isOverridden(job, 'expression')" class="badge bg-info-subtle text-info-emphasis">override</span>
              </div>
              <small class="text-muted" x-text="describeExpression(job)"></small>
            </div>
          </td>
          <td>
            <span x-text="overlapLabel(effectiveOverlap(job))"></span>
            <span x-show="isOverridden(job, 'overlap')" class="badge bg-info-subtle text-info-emphasis ms-1">override</span>
          </td>
          <td class="text-truncate">
            <span x-text="effectiveTimezone(job) || 'UTC'"></span>
            <span x-show="isOverridden(job, 'timezone')" class="badge bg-info-subtle text-info-emphasis ms-1">override</span>
          </td>
          <td>
            <div class="form-check form-switch">
              <input class="form-check-input" type="checkbox" :checked="job.enabled" aria-label="Enabled"
                :disabled="isBusy(job.name)" @change="onToggleEnabled(job, $event)">
            </div>
          </td>
          <td>
            <div class="d-flex flex-column gap-0">
              <span class="text-nowrap" :title="formatTime(lastFired(job).at, '')" x-text="formatAge(lastFired(job).at, 'Never')"></span>
              <div class="d-flex flex-wrap align-items-center gap-1">
                <span x-show="lastFired(job).manual" class="badge bg-secondary-subtle text-secondary-emphasis"
                  title="Last fired by a manual run, not the schedule">manual</span>
                <span x-show="isRunPending(job)" class="badge bg-warning-subtle text-warning-emphasis"
                  title="A manual run is waiting for a worker pool to claim it">run pending</span>
              </div>
            </div>
          </td>
          <td class="text-nowrap" :title="formatTime(job.lastCheckedAt, '')" x-text="formatAge(job.lastCheckedAt, 'Never')"></td>
          <td class="cell-actions">
            <div class="dropdown">
              <button class="btn btn-row-actions btn-sm" type="button" data-bs-toggle="dropdown" data-bs-auto-close="outside" aria-expanded="false" :disabled="isBusy(job.name)" title="Row actions" aria-label="Row actions">&#8942;</button>
              <ul class="dropdown-menu dropdown-menu-end" x-html="CRON_ACTIONS_HTML"></ul>
            </div>
          </td>
        </tr>
      </template>
      <tr x-show="_loadErrored && schedules.length === 0">
        <td :colspan="colCount()" class="text-danger text-center">Failed to load cron schedules. <a href="#" @click.prevent="loadSchedules()">Retry</a></td>
      </tr>
      <tr x-show="!_loadErrored && loaded && schedules.length === 0">
        <td :colspan="colCount()" class="text-muted text-center">No cron schedules configured.</td>
      </tr>
    </tbody>
    <tbody x-html="TABLE_SKELETON_HTML"></tbody>
  </table>
</div>`;

// Detail drawer, stamped into both cron views.
const CRON_DRAWER_HTML = `
<div class="offcanvas offcanvas-end detail-drawer" tabindex="-1" id="cronDetailDrawer"
     data-bs-backdrop="false" data-bs-scroll="true" :aria-label="detailTitle">
  <div class="offcanvas-header" x-html="DETAIL_HEAD_HTML"></div>
  <div class="offcanvas-body" x-show="selectedSchedule && !$store.cronEdit.editing">
    <dl class="row">
      <dt class="col-sm-5">Queue</dt>
      <dd class="col-sm-7" x-text="selectedSchedule?.queueName ?? EMPTY"></dd>
      <dt class="col-sm-5">Expression</dt>
      <dd class="col-sm-7 font-monospace small" x-text="selectedSchedule ? effectiveExpression(selectedSchedule) : EMPTY"></dd>
      <dt class="col-sm-5">Runs</dt>
      <dd class="col-sm-7" x-text="selectedSchedule ? describeExpression(selectedSchedule) : EMPTY"></dd>
      <dt class="col-sm-5">Declared expression</dt>
      <dd class="col-sm-7 font-monospace small" x-text="selectedSchedule?.defaultExpression ?? EMPTY"></dd>
      <dt class="col-sm-5">Overlap policy</dt>
      <dd class="col-sm-7" x-text="selectedSchedule ? overlapLabel(effectiveOverlap(selectedSchedule)) : EMPTY"></dd>
      <dt class="col-sm-5">Timezone</dt>
      <dd class="col-sm-7" x-text="selectedSchedule ? (effectiveTimezone(selectedSchedule) || 'UTC') : EMPTY"></dd>
      <dt class="col-sm-5">Enabled</dt>
      <dd class="col-sm-7" x-text="selectedSchedule?.enabled ? 'Yes' : 'No'"></dd>
      <dt class="col-sm-5">Last fired</dt>
      <dd class="col-sm-7" x-text="selectedSchedule ? formatTime(lastFired(selectedSchedule).at, 'Never') : EMPTY"></dd>
      <dt class="col-sm-5">Last checked</dt>
      <dd class="col-sm-7" x-text="formatTime(selectedSchedule?.lastCheckedAt, 'Never')"></dd>
    </dl>
  </div>
  <template x-if="$store.cronEdit.editing">
    <div class="offcanvas-body drawer-edit arb-edit">
      <p class="edit-note">Overrides take effect on the next tick. Uncheck a field to revert it to the declared default.</p>
      <div class="edit-field">
        <div class="form-check form-switch">
          <input class="form-check-input" type="checkbox" id="cronExprOn" x-model="$store.cronEdit.edit.exprOn">
          <label class="form-check-label" for="cronExprOn">Expression</label>
        </div>
        <input type="text" class="form-control form-control-sm font-monospace" x-model="$store.cronEdit.edit.expr" :disabled="!$store.cronEdit.edit.exprOn"
          @keydown.enter.prevent="$store.cronEdit.saveEdit()" :placeholder="\`default \${$store.cronEdit.edit.defaultExpression}\`">
        <small class="edit-hint" x-text="$store.cronEdit.editDescribe()"></small>
      </div>
      <div class="edit-field">
        <div class="form-check form-switch">
          <input class="form-check-input" type="checkbox" id="cronOverlapOn" x-model="$store.cronEdit.edit.overlapOn">
          <label class="form-check-label" for="cronOverlapOn">Overlap policy</label>
        </div>
        <select class="form-select form-select-sm" x-model="$store.cronEdit.edit.overlap" :disabled="!$store.cronEdit.edit.overlapOn">
          <option value="SkipOverlap">Skip overlap</option>
          <option value="AllowOverlap">Allow overlap</option>
        </select>
        <small class="edit-hint" x-show="!$store.cronEdit.edit.overlapOn" x-text="\`default \${overlapLabel($store.cronEdit.edit.defaultOverlap)}\`"></small>
      </div>
      <div class="edit-field">
        <div class="form-check form-switch">
          <input class="form-check-input" type="checkbox" id="cronTzOn" x-model="$store.cronEdit.edit.tzOn">
          <label class="form-check-label" for="cronTzOn">Timezone</label>
        </div>
        <input type="text" class="form-control form-control-sm" list="cronTzOptions" x-model="$store.cronEdit.edit.tz"
          :disabled="!$store.cronEdit.edit.tzOn" autocomplete="off" spellcheck="false"
          @keydown.enter.prevent="$store.cronEdit.saveEdit()" placeholder="IANA zone, e.g. America/New_York">
        <datalist id="cronTzOptions">
          <template x-for="z in $store.cronEdit.tzList" :key="z"><option :value="z"></option></template>
        </datalist>
        <small class="edit-hint" x-show="!$store.cronEdit.edit.tzOn" x-text="\`default \${$store.cronEdit.edit.defaultTimezone}\`"></small>
      </div>
      <div class="alert alert-danger py-2 mt-3" x-show="$store.cronEdit.edit.error" x-text="$store.cronEdit.edit.error"></div>
      <div class="edit-actions">
        <button type="button" class="btn btn-outline-secondary btn-sm" @click="$store.cronEdit.cancelEdit()">Cancel</button>
        <button type="button" class="btn btn-primary btn-sm" @click="$store.cronEdit.saveEdit()" :disabled="$store.cronEdit.edit.saving">Save</button>
      </div>
    </div>
  </template>
</div>`;

document.addEventListener('alpine:init', () => {
  Alpine.store('cronEdit', {
    ...typeToConfirm('cronConfirm'),
    host: null,
    editing: false,
    tzList: [],
    edit: {
      prefix: '', queueName: '',
      exprOn: false, expr: '',
      overlapOn: false, overlap: 'SkipOverlap',
      tzOn: false, tz: '',
      orig: {},
      defaultExpression: '', defaultOverlap: '', defaultTimezone: '',
      saving: false, error: '',
    },

    // Suggestions only. The field accepts any zone name, and the server
    // rejects one it cannot resolve, so an engine without Intl.supportedValuesOf
    // just gets a shorter list rather than a smaller choice of zones.
    populateTimezones() {
      if (this.tzList.length > 0) return;
      if (typeof Intl.supportedValuesOf !== 'function') {
        this.tzList = ['UTC'];
        return;
      }
      const zones = Intl.supportedValuesOf('timeZone').slice();
      if (!zones.includes('UTC')) zones.push('UTC');
      this.tzList = zones.sort();
    },

    isHostBusy(name) {
      return !!(this.host && this.host.isBusy(name));
    },

    setHost(host) {
      this.host = host;
    },

    releaseHost(hostId) {
      if (this.host?.hostId !== hostId) return;
      this.host = null;
      this.editing = false;
    },

    // Open the override editor for a schedule, hosted by the active table.
    openEdit(host, s) {
      this.setHost(host);
      this.populateTimezones();
      const tz = s.overrideTimezone ?? (s.defaultTimezone || 'UTC');
      const values = {
        exprOn: s.overrideExpression !== null,
        expr: s.overrideExpression ?? '',
        overlapOn: s.overrideOverlap !== null,
        overlap: s.overrideOverlap ?? s.defaultOverlap,
        tzOn: s.overrideTimezone !== null,
        tz,
      };
      this.edit = {
        prefix: s.name,
        queueName: s.queueName,
        ...values,
        orig: values,
        defaultExpression: s.defaultExpression,
        defaultOverlap: s.defaultOverlap,
        defaultTimezone: s.defaultTimezone || 'UTC',
        saving: false,
        error: '',
      };
      // The panel is the editor's home, so open it on this schedule first.
      host.viewDetail(s);
      this.editing = true;
    },

    cancelEdit() {
      this.editing = false;
      this.edit.error = '';
    },

    // Live human-readable description of the expression being edited.
    editDescribe() {
      const expr = this.edit.exprOn ? this.edit.expr : this.edit.defaultExpression;
      return safeCronDescribe(expr);
    },

    async saveEdit() {
      const host = this.host;
      await saveOverrides(this.edit, {
        apiFn: (name, body) => ArbiterAPI.updateCronSchedule(name, body),
        close: () => { this.editing = false; },
        reload: () => host?.loadSchedules(),
        buildBody: (e) => {
          if (e.exprOn && !e.expr.trim()) return { error: 'Expression cannot be empty' };
          if (e.tzOn && !e.tz.trim()) return { error: 'Timezone cannot be empty' };
          // Sent as a value (override on) or null (revert), only when changed.
          const body = {};
          const put = (key, on, value, origOn, origValue) => {
            const next = on ? value : null;
            const prev = origOn ? origValue : null;
            if (next !== prev) body[key] = next;
          };
          put('overrideExpression', e.exprOn, e.expr.trim(), e.orig.exprOn, e.orig.expr);
          put('overrideOverlap', e.overlapOn, e.overlap, e.orig.overlapOn, e.orig.overlap);
          put('overrideTimezone', e.tzOn, e.tz.trim(), e.orig.tzOn, e.orig.tz);
          return { body };
        },
      });
    },

    // Checkbox change handler. Enabling applies immediately. Disabling is guarded
    // like pausing a queue: revert the switch and open the confirm modal.
    onToggleEnabled(host, schedule, ev) {
      this.setHost(host);
      const target = !schedule.enabled;
      if (target || this.confirmMode() === 'off') {
        host.applyEnabled(schedule.name, target);
        return;
      }
      if (ev) ev.target.checked = schedule.enabled;
      this.openConfirm(schedule.name);
      showModal('cronToggleModal');
    },

    confirmToggleEnabled() {
      if (!this.confirmValid() || this.isHostBusy(this.confirmTarget)) return;
      hideModal('cronToggleModal');
      this.host?.applyEnabled(this.confirmTarget, false);
    },
  });

  Alpine.data('cronTab', (opts = {}) => ({
    ...pollingTab('loadSchedules', ARB_TIMING.cronPollMs, 'arb.cronRefresh'),
    ...clientSort('schedules', CRON_SORT_KEYS, 'name', 'name'),
    ...confirmArm(),
    ...rowDetail('displaySchedules', 'name', 'selectedSchedule'),
    global: !!opts.global,
    hostId: ++cronHostSeq,
    schedules: [],
    selectedSchedule: null,
    detailActionsHtml: CRON_ACTIONS_HTML,
    ...loadState(),
    active: false,

    // Roll-up for the global header strip.
    get enabledCount() {
      return this.schedules.filter((s) => s.enabled).length;
    },

    get pendingCount() {
      return this.schedules.filter((s) => this.isRunPending(s)).length;
    },

    get displaySchedules() {
      return this.sortRows(this.schedules);
    },

    colCount() {
      return this.global ? 9 : 8;
    },

    get detailTitle() {
      return this.selectedSchedule ? this.selectedSchedule.name : 'Schedule';
    },

    get detailStatus() {
      if (!this.selectedSchedule) return '';
      return this.selectedSchedule.enabled ? 'enabled' : 'disabled';
    },

    detailStatusClass() {
      return this.selectedSchedule?.enabled
        ? 'bg-success-subtle text-success-emphasis'
        : 'bg-secondary-subtle text-secondary-emphasis';
    },

    get detailRows() {
      const cur = this.selectedSchedule;
      return cur ? [Object.assign({}, cur, { _id: cur.name, _inDrawer: true })] : [];
    },

    viewDetail(schedule) {
      this.selectedSchedule = schedule;
      this.$store.cronEdit.editing = false;
      showDrawer('cronDetailDrawer');
    },

    init() {
      // Global mode is a top-level view mounted by x-if. The per-queue tab is
      // a Bootstrap tab scoped to the selected queue.
      if (this.global) {
        this.initPollingMounted();
      } else {
        this.initPolling('#tab-cron', {
          onQueueChange: () => { this.disarm(); this.schedules = []; },
        });
      }
    },

    destroy() {
      this.teardownPolling();
      this.disarm();
      this.$store.cronEdit.releaseHost(this.hostId);
    },

    async loadSchedules() {
      const queue = this.global ? undefined : this.$store.app.selectedQueue;
      if (!this.global && !queue) {
        this.schedules = [];
        return;
      }
      await guardedLoad(this, 'Failed to load cron schedules', async (seq, isStale) => {
        const data = await ArbiterAPI.listCronSchedules({ queue });
        if (isStale()) return;
        this.schedules = data.cronSchedules || [];
        this.resyncDetailSelection();
      });
    },

    effectiveExpression(s) {
      return s.overrideExpression || s.defaultExpression;
    },

    effectiveOverlap(s) {
      return s.overrideOverlap || s.defaultOverlap;
    },

    effectiveTimezone(s) {
      return s.overrideTimezone || s.defaultTimezone || '';
    },

    describeExpression(s) {
      return safeCronDescribe(this.effectiveExpression(s));
    },

    isOverridden(s, field) {
      if (field === 'expression') return s.overrideExpression !== null;
      if (field === 'overlap') return s.overrideOverlap !== null;
      if (field === 'timezone') return s.overrideTimezone !== null;
      return false;
    },

    openEdit(s) {
      this.$store.cronEdit.openEdit(this, s);
    },

    onToggleEnabled(schedule, ev) {
      this.$store.cronEdit.onToggleEnabled(this, schedule, ev);
    },

    async applyEnabled(name, target) {
      await this.withBusyRow(name, async () => {
        try {
          await ArbiterAPI.updateCronSchedule(name, { enabled: target });
        } catch (e) {
          showToast('Failed to toggle: ' + e.message);
        } finally {
          await this.loadSchedules();
        }
      });
    },

    // An unclaimed request expires, so a run offered where no pool takes it
    // costs a wait rather than wedging the row.
    canRun(s) {
      return s.enabled && !this.isRunPending(s);
    },

    runTitle(s) {
      if (!s.enabled) return 'Schedule is disabled';
      if (this.isRunPending(s)) return 'A run is already pending';
      return 'Request a run now';
    },

    async runNow(schedule) {
      if (!this.canRun(schedule) || this.busyRows[schedule.name]) return;
      if (!this.confirmArmed('run:' + schedule.name)) return;
      await this.withBusyRow(schedule.name, async () => {
        try {
          await ArbiterAPI.runCronSchedule(schedule.name);
          // The request is claimed asynchronously by a serving pool, and a
          // SkipOverlap schedule drops it while one of its jobs is still active,
          // so this reports the request rather than a completed run.
          const note = this.effectiveOverlap(schedule) === 'SkipOverlap'
            ? ' (skipped if a job is already running)'
            : '';
          showToast('Run requested for ' + schedule.name + note, 'info');
        } catch (e) {
          showToast('Failed to run: ' + e.message);
        } finally {
          await this.loadSchedules();
        }
      });
    },

    isRunPending(s) {
      return s.runRequestedAt != null;
    },

    lastFired(s) {
      const manual = s.lastManualRunAt;
      // Both timestamps are minute floors server-side, so a same-minute manual
      // run ties the scheduled fire. Break the tie toward the manual run.
      if (manual && (!s.lastFiredAt || Date.parse(manual) >= Date.parse(s.lastFiredAt))) {
        return { at: manual, manual: true };
      }
      return { at: s.lastFiredAt, manual: false };
    },
  }));
});
