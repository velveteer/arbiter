// Cron schedules with an override editor in the drawer. A queue's Cron tab, or
// every queue's schedules on the global view when no queue is given.
import { shallowRef, reactive, computed, provide, inject } from '../../vendor/vue.esm-browser.prod.js';
import { api } from '../api.js';
import { CONFIG } from '../config.js';
import { EMPTY, changedOverrides, formatCountdown } from '../format.js';
import { toast } from '../store.js';
import { submitForm, useArm, useBusy, useDetail, useSort, useSummary, useView } from '../use.js';
/** @import { Schema } from '../../../types/client' */

// Display text for an overlap policy. The wire value stays as declared.
const overlapLabel = (v) => ({ SkipOverlap: 'Skip overlap', AllowOverlap: 'Allow overlap' })[v] ?? v ?? EMPTY;

// A human description of a cron expression, or '' when it does not parse.
function cronText(expr) {
  try {
    return cronstrue.toString(expr);
  } catch {
    // An expression that does not parse has no description.
    return '';
  }
}

// The description of a listed schedule's expression, cached.
const described = new Map();
function describe(expr) {
  if (!described.has(expr)) described.set(expr, cronText(expr));
  return described.get(expr);
}

const expression = (s) => s.overrideExpression || s.defaultExpression;
const overlap = (s) => s.overrideOverlap || s.defaultOverlap;
const timezone = (s) => s.overrideTimezone || s.defaultTimezone || 'UTC';
const pending = (s) => s.runRequestedAt != null;

// The newer of the last scheduled fire and the last manual run. Both are minute
// floors, so a same-minute tie goes to the manual run.
function lastFired(s) {
  const manual = s.lastManualRunAt;
  if (manual && (!s.lastFiredAt || Date.parse(manual) >= Date.parse(s.lastFiredAt))) return { at: manual, manual: true };
  return { at: s.lastFiredAt, manual: false };
}

const SORT_KEYS = {
  name: (s) => s.name,
  queue: (s) => s.queueName || '',
  overlap,
  timezone,
  enabled: (s) => (s.enabled ? 1 : 0),
  nextRun: (s) => Date.parse(s.nextRunAt || '') || Number.MAX_SAFE_INTEGER,
  lastFired: (s) => Date.parse(lastFired(s).at || '') || -1,
  lastChecked: (s) => Date.parse(s.lastCheckedAt || '') || -1,
};

const COLS = [
  { key: 'name', label: 'Name', weight: 11, sort: 'name' },
  { key: 'queue', label: 'Queue', weight: 10, sort: 'queue', global: true },
  { key: 'expression', label: 'Expression', weight: 17 },
  { key: 'overlap', label: 'Overlap policy', weight: 9, sort: 'overlap' },
  { key: 'timezone', label: 'Timezone', weight: 10, sort: 'timezone' },
  { key: 'enabled', label: 'Enabled', weight: 5, sort: 'enabled' },
  { key: 'nextRun', label: 'Next run', weight: 12, sort: 'nextRun' },
  { key: 'lastFired', label: 'Last fired', weight: 11, sort: 'lastFired' },
  { key: 'lastChecked', label: 'Last checked', weight: 10, sort: 'lastChecked' },
  { key: 'actions', label: 'Actions', weight: 5, cls: 'cell-actions' },
];

// Suggestions only. The server refuses a zone it cannot resolve.
const ZONES = typeof Intl.supportedValuesOf === 'function' ? [...new Set([...Intl.supportedValuesOf('timeZone'), 'UTC'])].sort() : ['UTC'];

const CronMenu = {
  props: { s: Object, close: Function },
  setup: () => inject('cron'),
  template: /* html */ `
    <button v-if="!edit.on" type="button" class="dropdown-item" :disabled="busy.has(s.name)" @click="openEdit(s); close()">Edit</button>
    <arm-button :arm="arm" :k="'run:' + s.name" cls="dropdown-item" on="fw-semibold" :disabled="busy.has(s.name) || !canRun(s)"
      :title="runTitle(s)" label="Run now" confirm="Confirm run" @fire="runNow(s); close()"/>`,
};

export const CronView = {
  components: { CronMenu },
  props: { queue: String },
  setup(props) {
    const global = !props.queue;
    const schedules = shallowRef(/** @type {Schema<'CronScheduleView'>[]} */ ([]));
    const arm = useArm();
    const busy = useBusy();
    const edit = reactive({
      on: false,
      name: '',
      expr: '',
      exprOn: false,
      overlap: 'SkipOverlap',
      overlapOn: false,
      tz: '',
      tzOn: false,
      row: {},
      def: {},
      saving: false,
      error: '',
    });
    const disabling = reactive({ open: false, name: '' });
    const view = useView({
      noun: 'cron schedules',
      key: 'arb.cronRefresh',
      mode: '1m',
      empty: () => schedules.value.length === 0,
      async load(stale) {
        const data = await api.cron(props.queue);
        if (stale()) return;
        schedules.value = data.cronSchedules;
        d.resync(d.close);
      },
    });
    const sort = useSort(() => schedules.value, SORT_KEYS, 'name', ['name'], ['name', 'queue', 'overlap', 'timezone']);
    const d = useDetail({
      rows: () => sort.rows,
      id: (s) => s.name,
      held: () => edit.on,
      reset: () => {
        edit.on = false;
      },
    });
    const summary = global ? useSummary('arb.summary.cron', view, () => schedules.value.length > 0) : '';
    const cols = computed(() => COLS.filter((c) => global || !c.global));

    // The drawer is the editor's home, so open it on this schedule first.
    function openEdit(s) {
      const values = {
        exprOn: s.overrideExpression != null,
        expr: s.overrideExpression ?? '',
        overlapOn: s.overrideOverlap != null,
        overlap: s.overrideOverlap ?? s.defaultOverlap,
        tzOn: s.overrideTimezone != null,
        tz: s.overrideTimezone ?? (s.defaultTimezone || 'UTC'),
      };
      if (!d.view(s)) return;
      Object.assign(edit, values, {
        on: true,
        name: s.name,
        row: s,
        saving: false,
        error: '',
        def: { expr: s.defaultExpression, overlap: s.defaultOverlap, tz: s.defaultTimezone || 'UTC' },
      });
    }

    // A field goes as a value (override on) or null (revert), and only when it changed.
    function saveEdit() {
      if (edit.exprOn && !edit.expr.trim()) return (edit.error = 'Expression cannot be empty');
      if (edit.tzOn && !edit.tz.trim()) return (edit.error = 'Timezone cannot be empty');
      const body = changedOverrides(
        {
          overrideExpression: edit.exprOn ? edit.expr.trim() : null,
          overrideOverlap: edit.overlapOn ? edit.overlap : null,
          overrideTimezone: edit.tzOn ? edit.tz.trim() : null,
        },
        edit.row,
      );
      return submitForm(edit, async () => {
        await api.updateCron(edit.name, body);
        edit.on = false;
        await view.reload();
      });
    }

    const setEnabled = (name, enabled) =>
      busy.run(
        name,
        async () => {
          try {
            await api.updateCron(name, { enabled });
          } finally {
            await view.reload();
          }
        },
        'Failed to toggle',
      );

    // The switch shows only the loaded state. A switch to on applies at once. A switch to off asks for confirmation, as a queue pause does.
    function onToggle(s) {
      if (!s.enabled || CONFIG.cronConfirm === 'off') return setEnabled(s.name, !s.enabled);
      Object.assign(disabling, { open: true, name: s.name });
    }

    // A pool that serves the queue claims the request later. SkipOverlap drops it while a job
    // is active. Thus this reports the request, not a finished run.
    const runNow = (s) =>
      busy.run(
        s.name,
        async () => {
          try {
            await api.runCron(s.name);
            toast(`Run requested for ${s.name}` + (overlap(s) === 'SkipOverlap' ? ' (skipped if a job is already running)' : ''), 'info');
          } finally {
            await view.reload();
          }
        },
        'Failed to run',
      );

    const ctx = {
      global,
      schedules,
      view,
      sort,
      d,
      arm,
      busy,
      edit,
      disabling,
      summary,
      cols,
      ZONES,
      expression,
      overlap,
      timezone,
      pending,
      lastFired,
      overlapLabel,
      describe,
      cronText,
      openEdit,
      saveEdit,
      onToggle,
      runNow,
      setEnabled,
      overridden: (s, field) => s[field] != null,
      canRun: (s) => s.enabled && !pending(s),
      runTitle: (s) => (!s.enabled ? 'Schedule is disabled' : pending(s) ? 'A run is already pending' : 'Request a run now'),
      nextRun: (s) => (s.nextRunAt ? formatCountdown(s.nextRunAt) : s.enabled ? EMPTY : 'Disabled'),
      enabledCount: computed(() => schedules.value.filter((s) => s.enabled).length),
      pendingCount: computed(() => schedules.value.filter(pending).length),
    };
    provide('cron', ctx);
    return ctx;
  },
  template: /* html */ `
    <div v-if="global" class="queue-summary" :class="summary">
      <qs :v="schedules.length" :l="pluralize(schedules.length, 'schedule')"/>
      <qs :v="enabledCount" l="enabled"/>
      <qs :v="pendingCount" l="run pending" :c="{ warn: pendingCount > 0 }"/>
    </div>
    <div class="toolbar" v-show="view.ready"><refresh-control :view="view"/></div>
    <load-error :view="view"/>
    <div v-scroll-edges class="table-responsive" :aria-busy="view.loading" v-show="view.ready">
      <table class="table table-hover table-sm sticky-head table-fixed">
        <t-head :cols="cols" :sort="sort"/>
        <tbody>
          <tr v-for="s in sort.rows" :key="s.name" class="detail-row" tabindex="0" @click="isRowClick($event) && d.view(s)"
            @keydown.enter.self.prevent="d.toggle(s)" @keydown.space.self.prevent="d.toggle(s)">
            <td class="text-truncate" :title="s.name">{{ s.name }}</td>
            <td v-if="global" class="text-truncate"><a v-bind="queueLink(s.queueName)" :title="'Open ' + s.queueName">{{ s.queueName }}</a></td>
            <td>
              <div class="d-flex flex-column">
                <div class="d-flex flex-wrap align-items-center gap-1">
                  <span class="font-monospace text-truncate cell-shrink" :title="expression(s)">{{ expression(s) }}</span>
                  <span v-if="overridden(s, 'overrideExpression')" class="badge bg-info-subtle text-info-emphasis">override</span>
                </div>
                <small class="text-muted">{{ describe(expression(s)) }}</small>
              </div>
            </td>
            <td>{{ overlapLabel(overlap(s)) }} <span v-if="overridden(s, 'overrideOverlap')" class="badge bg-info-subtle text-info-emphasis">override</span></td>
            <td>
              <div class="d-flex align-items-center gap-1">
                <span class="text-truncate cell-shrink" :title="timezone(s)">{{ timezone(s) }}</span>
                <span v-if="overridden(s, 'overrideTimezone')" class="badge bg-info-subtle text-info-emphasis">override</span>
              </div>
            </td>
            <td>
              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :checked="s.enabled" aria-label="Enabled" :disabled="busy.has(s.name)" @click.prevent="onToggle(s)">
              </div>
            </td>
            <td class="text-nowrap" :title="formatTime(s.nextRunAt)"><tick>{{ nextRun(s) }}</tick></td>
            <td>
              <span class="text-nowrap d-block" :title="formatTime(lastFired(s).at)"><tick>{{ formatAge(lastFired(s).at, 'Never') }}</tick></span>
              <span v-if="lastFired(s).manual" class="badge bg-secondary-subtle text-secondary-emphasis me-1" title="Last fired by a manual run, not the schedule">manual</span>
              <span v-if="pending(s)" class="badge bg-warning-subtle text-warning-emphasis" title="A manual run is waiting for a worker pool to claim it">run pending</span>
            </td>
            <td class="text-nowrap" :title="formatTime(s.lastCheckedAt)"><tick>{{ formatAge(s.lastCheckedAt, 'Never') }}</tick></td>
            <td class="cell-actions">
              <action-menu row :detail="() => d.view(s)" :disabled="busy.has(s.name)" v-slot="{ close }"><cron-menu :s="s" :close="close"/></action-menu>
            </td>
          </tr>
        </tbody>
        <skeleton-rows :view="view" :span="cols.length" :empty="!schedules.length">No cron schedules configured.</skeleton-rows>
      </table>
    </div>

    <drawer :d="d" :title="d.cur ? d.cur.name : 'Schedule'" :status="d.cur ? (d.cur.enabled ? 'enabled' : 'disabled') : ''"
      :status-class="d.cur?.enabled ? 'bg-success-subtle text-success-emphasis' : 'bg-secondary-subtle text-secondary-emphasis'" :sticky="edit.on">
      <template #actions>
        <action-menu v-if="d.cur" :disabled="busy.has(d.cur.name)" v-slot="{ close }"><cron-menu :s="d.cur" :close="close"/></action-menu>
      </template>
      <div v-if="edit.on" class="offcanvas-body drawer-edit arb-edit" tabindex="0">
        <p class="edit-note">Overrides take effect on the next tick. Uncheck a field to revert it to the declared default.</p>
        <div class="edit-field">
          <div class="form-check form-switch">
            <input class="form-check-input" type="checkbox" id="cronExprOn" v-model="edit.exprOn">
            <label class="form-check-label" for="cronExprOn">Expression</label>
          </div>
          <input type="text" class="form-control form-control-sm font-monospace" v-model="edit.expr" :disabled="!edit.exprOn"
            @keydown.enter.prevent="saveEdit" :placeholder="'default ' + edit.def.expr">
          <small class="edit-hint">{{ cronText(edit.exprOn ? edit.expr : edit.def.expr) }}</small>
        </div>
        <div class="edit-field">
          <div class="form-check form-switch">
            <input class="form-check-input" type="checkbox" id="cronOverlapOn" v-model="edit.overlapOn">
            <label class="form-check-label" for="cronOverlapOn">Overlap policy</label>
          </div>
          <select class="form-select form-select-sm" v-model="edit.overlap" :disabled="!edit.overlapOn">
            <option value="SkipOverlap">Skip overlap</option>
            <option value="AllowOverlap">Allow overlap</option>
          </select>
          <small class="edit-hint" v-if="!edit.overlapOn">default {{ overlapLabel(edit.def.overlap) }}</small>
        </div>
        <div class="edit-field">
          <div class="form-check form-switch">
            <input class="form-check-input" type="checkbox" id="cronTzOn" v-model="edit.tzOn">
            <label class="form-check-label" for="cronTzOn">Timezone</label>
          </div>
          <input type="text" class="form-control form-control-sm" list="cronTzOptions" v-model="edit.tz" :disabled="!edit.tzOn"
            autocomplete="off" spellcheck="false" @keydown.enter.prevent="saveEdit" placeholder="IANA zone, e.g. America/New_York">
          <datalist id="cronTzOptions"><option v-for="z in ZONES" :key="z" :value="z"></option></datalist>
          <small class="edit-hint" v-if="!edit.tzOn">default {{ edit.def.tz }}</small>
        </div>
        <edit-actions :form="edit" @cancel="edit.on = false; edit.error = ''" @save="saveEdit"/>
      </div>
      <div v-else-if="d.cur" class="offcanvas-body" tabindex="0">
        <dl class="row">
          <kv l="Queue">{{ d.cur.queueName ?? EMPTY }}</kv>
          <kv l="Expression"><span class="font-monospace small">{{ expression(d.cur) }}</span></kv>
          <kv l="Runs">{{ describe(expression(d.cur)) || EMPTY }}</kv>
          <kv l="Declared expression"><span class="font-monospace small">{{ d.cur.defaultExpression ?? EMPTY }}</span></kv>
          <kv l="Overlap policy">{{ overlapLabel(overlap(d.cur)) }}</kv>
          <kv l="Timezone">{{ timezone(d.cur) }}</kv>
          <kv l="Enabled">{{ d.cur.enabled ? 'Yes' : 'No' }}</kv>
          <kv l="Last fired">{{ formatTime(lastFired(d.cur).at, 'Never') }}</kv>
          <kv l="Next run"><span :title="formatTime(d.cur.nextRunAt)"><tick>{{ nextRun(d.cur) }}</tick></span></kv>
          <kv l="Last checked">{{ formatTime(d.cur.lastCheckedAt, 'Never') }}</kv>
        </dl>
      </div>
    </drawer>

    <confirm-modal v-model:open="disabling.open" title="Disable schedule" :subject="disabling.name" :target="disabling.name"
      action="Disable schedule" :busy="busy.has(disabling.name)" prompt="Type the schedule name to confirm:"
      note="Disabling stops scheduled ticks for this schedule. Manual runs are refused until it is re-enabled."
      @confirm="setEnabled(disabling.name, false)"/>`,
};
