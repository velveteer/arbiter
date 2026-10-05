// A queue's jobs: filters, tree of parents and children, actions, insert, detail.
import { ref, shallowRef, reactive, shallowReactive, computed, watch, provide, inject } from '../../vendor/vue.esm-browser.prod.js';
import { api } from '../api.js';
import { TIMING } from '../config.js';
import {
  EMPTY,
  JOB_STATUSES,
  SECONDS_PER_MINUTE,
  SECONDS_PER_HOUR,
  SECONDS_PER_DAY,
  MS_PER_SECOND,
  formatCountdown,
  formatDuration,
  formatTime,
  mapLimit,
  parseOptionalInt,
  parsePayload,
  toIsoInstant,
  toLocalInput,
} from '../format.js';
import { plainClick, queueUrl } from '../router.js';
import { toast } from '../store.js';
import { ancestors, fillAncestors, submitForm, useArm, useBusy, useColumns, useDetail, useStored, useTable, useView } from '../use.js';
/** @import { Res, Schema } from '../../../types/client' */
/** @typedef {Res<'/api/v1/queue/jobs'>['items'][number]} Job */

// Header and cell order. weight is a share of the width among the columns shown.
const COLS = [
  { key: 'select', label: '', weight: 3, required: true, narrow: false },
  { key: 'id', label: 'ID', weight: 4, required: true, sort: 'id' },
  { key: 'kind', label: 'Kind', weight: 8, autoHide: true, narrow: false },
  { key: 'payload', label: 'Payload', weight: 14 },
  { key: 'group', label: 'Group', weight: 7, autoHide: true, narrow: false, sort: 'group_key' },
  { key: 'parent', label: 'Parent', weight: 7, autoHide: true, narrow: false, sort: 'parent_id' },
  { key: 'children', label: 'Children', weight: 9, autoHide: true, narrow: false },
  { key: 'priority', label: 'Priority', weight: 8, autoHide: true, narrow: false, sort: 'priority' },
  { key: 'attempts', label: 'Attempts', weight: 8, autoHide: true, narrow: false, sort: 'attempts' },
  { key: 'status', label: 'Status', weight: 7 },
  { key: 'inserted', label: 'Inserted', weight: 8, sort: 'inserted_at' },
  { key: 'visible', label: 'Visible', weight: 10, autoHide: true, narrow: false, sort: 'not_visible_until' },
  { key: 'gates', label: 'Gates', weight: 10, autoHide: true, narrow: false },
  { key: 'actions', label: 'Actions', weight: 4, cls: 'cell-actions' },
];

const SORTS = ['id', 'priority', 'attempts', 'inserted_at', 'not_visible_until', 'group_key', 'parent_id', 'last_attempted_at'];

// The filters that keep the tree. Any other filter renders flat.
const TREE_FILTERS = ['group_key', 'parent_id', 'job_id', 'kind'];

// Statuses a reschedule is refused for.
/** @type {Schema<'JobStatus'>[]} */
const RESCHEDULE_REFUSED = ['in_flight', 'suspended', 'cancelled', 'exhausted'];

// A visibility time still in the future.
const notYetVisible = (job) => !!job.notVisibleUntil && new Date(job.notVisibleUntil) > new Date();

// Statuses only a claim reaches, so an insert never lands in them.
/** @type {Schema<'JobStatus'>[]} */
const CLAIMED_STATUSES = ['in_flight', 'backoff', 'throttled', 'cancelled', 'exhausted'];

const STATES = JOB_STATUSES.map((s) => [s, s === 'in_flight' ? 'In-flight' : s[0].toUpperCase() + s.slice(1)]);

// Quick reschedule choices, measured from now.
const DELAYS = [
  { label: '+5m', secs: 5 * SECONDS_PER_MINUTE },
  { label: '+15m', secs: 15 * SECONDS_PER_MINUTE },
  { label: '+1h', secs: SECONDS_PER_HOUR },
  { label: '+6h', secs: 6 * SECONDS_PER_HOUR },
  { label: '+1d', secs: SECONDS_PER_DAY },
];

// Bulk actions whose request can take a selected job's descendants with it.
const CASCADES = new Set(['cancel', 'move-to-dlq']);

const blankInsert = () => ({
  open: false,
  payload: '',
  groupKey: '',
  dedupKey: '',
  dedupStrategy: 'ignore',
  priority: 0,
  notVisibleUntil: '',
  maxAttempts: '',
  error: '',
  saving: false,
});

// Row actions, shared by the row menu and the drawer header.
const JobMenu = {
  props: { job: Object, close: Function },
  setup: (props) => ({ ...inject('jobs'), parent: computed(() => props.job._childCount > 0) }),
  template: /* html */ `
    <button v-if="job.status === 'scheduled' || job.status === 'backoff'" type="button" class="dropdown-item" @click="act(job, 'promote'); close()">Promote</button>
    <button v-if="canReschedule(job)" type="button" class="dropdown-item" @click="openReschedule(job); close()">Reschedule</button>
    <button v-if="parent" type="button" class="dropdown-item" @click="act(job, 'pause-children'); close()">Pause descendants</button>
    <button v-else-if="canSuspend(job)" type="button" class="dropdown-item" @click="act(job, 'suspend'); close()">Suspend</button>
    <button v-if="parent" type="button" class="dropdown-item" @click="act(job, 'resume-children'); close()">Resume descendants</button>
    <button v-else-if="job.status === 'suspended'" type="button" class="dropdown-item" @click="act(job, 'resume'); close()">Resume</button>
    <hr class="dropdown-divider">
    <arm-button :arm="arm" :k="'cancel:' + job.primaryKey" cls="dropdown-item" on="text-warning fw-semibold" label="Cancel"
      :confirm="'Confirm cancel' + (job._childCount ? ' (+' + job._childCount + ' ' + pluralize(job._childCount, 'child', 'children') + ')' : '')" @fire="remove(job, 'cancel'); close()"/>
    <arm-button v-if="job.status === 'in_flight'" :arm="arm" :k="'fcancel:' + job.primaryKey" cls="dropdown-item" on="text-warning fw-semibold" label="Force cancel"
      confirm="Confirm force cancel (interrupts handler)" @fire="remove(job, 'force-cancel'); close()"/>
    <arm-button :arm="arm" :k="'movedlq:' + job.primaryKey" cls="dropdown-item" on="text-warning fw-semibold" label="Move to DLQ"
      :confirm="'Confirm move to DLQ' + (job._childCount ? ' (+' + job._childCount + ' ' + pluralize(job._childCount, 'child', 'children') + ')' : '')" @fire="remove(job, 'move-to-dlq'); close()"/>`,
};

export const JobsTab = {
  components: { JobMenu },
  props: { queue: String },
  setup(props) {
    const queue = props.queue;
    /** @typedef {Pick<Res<'/api/v1/queue/jobs'>, 'items' | 'total' | 'childCounts' | 'dlqChildCounts'>} Level */
    const root = shallowRef(/** @type {Level} */ ({ items: [], total: 0, childCounts: {}, dlqChildCounts: {} }));
    // Open expansions by parent id.
    const expanded = shallowReactive(/** @type {Record<string, Level>} */ ({}));
    const expandSeq = {};
    // The expansion seq of each children fetch in flight.
    const opening = {};
    const viewMode = ref('tree');
    const notVisibleFormat = useStored('arb.notVisibleFormat', 'countdown');
    const detail = reactive({ error: '', note: '', id: null, gone: false });
    const reschedule = reactive({ open: false, job: /** @type {Job | null} */ (null), at: '', error: '', saving: false });
    const insert = reactive(blankInsert());
    const arm = useArm();
    const busy = useBusy();
    const cols = useColumns(COLS, 'arb.jobCols.v2');

    const collapse = () => {
      for (const k of Object.keys(expanded)) delete expanded[k];
      for (const k of Object.keys(expandSeq)) expandSeq[k]++;
    };

    // A filter outside TREE_FILTERS renders flat. A child it matches can have no visible parent to nest under.
    const flatOnly = computed(() => Object.entries(t.q).some(([k, v]) => v && !TREE_FILTERS.includes(k)));
    const mode = computed(() => (flatOnly.value ? 'flat' : viewMode.value));

    // Roots, each followed by its open expansion, and a "showing N of M" row where cut.
    const rows = computed(() => {
      const out = [];
      const roots = root.value.items;
      const rootKeys = new Set(roots.map((j) => String(j.primaryKey)));
      // A truncated expansion lists only what it shows, so a child past the cut keeps its own row.
      const nested = new Set(Object.values(expanded).flatMap((e) => e.items.map((j) => String(j.primaryKey))));
      // In tree view a child is reached by expanding its parent.
      const reached = (job) =>
        !!job.parentId && !t.q.parent_id && ((mode.value === 'tree' && rootKeys.has(String(job.parentId))) || nested.has(String(job.primaryKey)));
      const walk = (list, depth, level) => {
        for (const job of list) {
          if (depth === 0 && reached(job)) continue;
          const key = job.primaryKey;
          out.push({ ...job, _depth: depth, _childCount: level.childCounts[key] || 0, _dlqChildCount: level.dlqChildCounts[key] || 0 });
          const e = expanded[key];
          if (!e) continue;
          walk(e.items, depth + 1, e);
          if (e.items.length > 0 && e.total > e.items.length) {
            out.push({ _more: true, _depth: depth + 1, _parent: key, _shown: e.items.length, _total: e.total, primaryKey: 'more-' + key });
          }
        }
      };
      walk(roots, 0, root.value);
      return out;
    });
    const selectable = computed(() => rows.value.filter((j) => !j._more));

    // Open expansions count, so a column only children fill shows when they land.
    watch(selectable, (js) => {
      if (!js.length) return;
      cols.measure({
        kind: js.every((j) => !j.kind),
        group: js.every((j) => !j.groupKey),
        parent: js.every((j) => !j.parentId),
        children: js.every((j) => !j._childCount && !j._dlqChildCount),
        priority: js.every((j) => !j.priority),
        attempts: js.every((j) => !j.attempts),
        visible: js.every((j) => !j.notVisibleUntil),
        gates: js.every((j) => !j.rateLimit && !j.concurrency),
      });
    });

    const view = useView({
      noun: 'jobs',
      key: 'arb.jobsRefresh',
      load,
      empty: () => rows.value.length === 0,
      events: (batch) => {
        const types = CLAIMED_STATUSES.includes(t.q.status) ? ['job_updated', 'job_deleted'] : ['job_inserted', 'job_updated', 'job_deleted'];
        return batch.filter((e) => e.table === queue && !e.dlq && types.includes(e.event)).length;
      },
    });

    const t = useTable({
      view,
      tab: 'jobs',
      filters: ['group_key', 'parent_id', 'job_id', 'claimed_by', 'kind', 'payload', 'rate_limit_prefix', 'concurrency_prefix'],
      extra: { status: JOB_STATUSES },
      sorts: SORTS,
      pageKey: 'arb.jobsPageSize',
      ids: () => selectable.value.map((j) => j.primaryKey),
      onReset: collapse,
    });

    const d = useDetail({ rows: () => selectable.value, id: (j) => j.primaryKey, show: (row) => showJob(row.primaryKey) });

    const childPage = (id) => api.jobs(queue, { parent_id: id, limit: TIMING.childPageLimit, sort_by: t.sort.by, sort_dir: t.sort.dir });

    // Keep the expansions still reachable from the roots.
    function dropUnreachable() {
      const reachable = new Set();
      const collect = (list) =>
        list.forEach((j) => {
          reachable.add(String(j.primaryKey));
          if (expanded[j.primaryKey]) collect(expanded[j.primaryKey].items);
        });
      collect(root.value.items);
      for (const id of Object.keys(expanded)) if (!reachable.has(id)) delete expanded[id];
    }

    const fetchOpen = () => {
      const open = Object.keys(expanded);
      return { open, seqs: open.map((id) => expandSeq[id]), pages: mapLimit(open, TIMING.bulkConcurrency, childPage) };
    };

    // The params of the last page whose expansions were pruned.
    let prunedFor = '';

    // A new page or order can drop expansions, so their children wait for the roots.
    async function load(stale) {
      const params = { ...t.params(), roots_only: mode.value === 'tree' && !t.filtered };
      const key = JSON.stringify(params);
      const early = key === prunedFor ? fetchOpen() : null;
      const data = await api.jobs(queue, params);
      if (stale()) return;
      root.value = data;
      dropUnreachable();
      prunedFor = key;
      if (t.landed(data.total)) return;
      const { open, seqs, pages } = early ?? fetchOpen();
      const fresh = await pages;
      if (stale()) return;
      // A failed page keeps the last one until a refetch lands.
      open.forEach((id, i) => {
        if (fresh[i].status === 'fulfilled' && expanded[id] && expandSeq[id] === seqs[i]) expanded[id] = fresh[i].value;
      });
      t.prune();
      // Resync after the children land, so an open child job re-points at its fresh row.
      d.resync(refreshDetail);
      retryDetail();
    }

    // A click while the children load cancels the expansion.
    async function toggleChildren(id) {
      const loading = id in opening && opening[id] === expandSeq[id];
      const seq = (expandSeq[id] || 0) + 1;
      expandSeq[id] = seq;
      if (expanded[id] || loading) {
        delete expanded[id];
        dropUnreachable();
        t.prune();
        return;
      }
      opening[id] = seq;
      try {
        const page = await childPage(id);
        if (expandSeq[id] === seq) {
          expanded[id] = page;
          dropUnreachable();
        }
      } catch (e) {
        if (expandSeq[id] === seq) toast('Could not load children: ' + e.message);
      } finally {
        if (opening[id] === seq) delete opening[id];
      }
    }

    // ---- Detail ----

    async function showJob(id) {
      const live = d.ticket();
      try {
        const data = await api.job(queue, id);
        if (!live()) return;
        if (!data?.job) return failDetail(id, null);
        const kids = await offPageChildren(data.job);
        if (!live()) return;
        Object.assign(detail, { error: '', note: '', id: null, gone: false });
        d.land(kids == null ? data.job : { ...data.job, _childCount: kids });
      } catch (e) {
        if (live()) failDetail(id, e.status === 404 ? null : e);
      }
    }

    // The list's counts cover only its own rows, so a parent off the list asks for its own.
    const offPageChildren = (job) =>
      !job.isRollup || selectable.value.some((r) => String(r.primaryKey) === String(job.primaryKey))
        ? null
        : api.jobs(queue, { job_id: job.primaryKey, limit: 1 }).then((r) => r.childCounts[job.primaryKey] ?? 0);

    // In an open drawer, a failure shows a message in place of the job. Its
    // neighbours stay pinned, so a vanished job does not strand the reader.
    function failDetail(id, err) {
      const headline = err ? 'Could not load this job.' : 'This job is no longer in the queue.';
      const note = err ? err.message : 'It may have completed, been archived, or moved to the DLQ.';
      if (!d.open) return toast(headline + ' ' + note);
      d.pin(id);
      d.cur = null;
      Object.assign(detail, { error: headline, note, id, gone: !err });
    }

    const refreshDetail = () => (d.open && d.cur ? showJob(d.cur.primaryKey) : null);

    // A job that could not load is asked for again, unless another job is on its way.
    function retryDetail() {
      if (d.open && detail.error && !detail.gone && String(d.asked?.primaryKey) === String(detail.id)) showJob(detail.id);
    }

    // The open job shaped like a row, with the counts of whichever level it sits at.
    const drawerRow = computed(() => {
      if (!d.cur) return null;
      const k = String(d.cur.primaryKey);
      const row = selectable.value.find((r) => String(r.primaryKey) === k);
      return { ...d.cur, _childCount: row?._childCount ?? d.cur._childCount ?? 0 };
    });

    // ---- Actions ----

    async function act(job, action) {
      await busy.run(
        job.primaryKey,
        async () => {
          await api.jobAction(queue, job.primaryKey, action);
          await view.reload();
        },
        'Failed',
      );
    }

    const send = (id, action) => (action === 'cancel' ? api.cancelJob(queue, id) : api.jobAction(queue, id, action));

    // Cancel, force cancel and move to the DLQ take the job off the list.
    async function remove(job, action) {
      const id = job.primaryKey;
      await busy.run(
        id,
        async () => {
          await send(id, action);
          d.closeIf(id);
          if (String(id) === t.q.parent_id) t.set('parent_id', '');
          else await view.reload();
        },
        `Failed to ${action.replaceAll('-', ' ')}`,
      );
    }

    // Cancel takes a job's descendants with it, and a move to the DLQ takes a rollup's.
    // The topmost selected ancestor whose request takes this job with it.
    function cascadeRoot(byId, sel, id, action) {
      if (!CASCADES.has(action)) return null;
      let root = null;
      for (const up of ancestors(byId, byId.get(String(id)), (j) => j.parentId)) {
        if (sel.has(up.primaryKey) && (action === 'cancel' || up.isRollup)) root = up.primaryKey;
      }
      return root;
    }

    // A selected job under a selected ancestor's cascade shares that ancestor's request and its outcome.
    // Ancestors off the page are fetched only when a selected job can cascade.
    function bulk(action, done) {
      const byId = new Map(selectable.value.map((j) => [String(j.primaryKey), j]));
      const cascades = (j) => CASCADES.has(action) && j?._childCount > 0 && (action === 'cancel' || j.isRollup);
      const prepare = async (all) => {
        const sel = new Set(all);
        const picked = all.map((id) => byId.get(String(id)));
        if (!picked.some(cascades)) return null;
        await fillAncestors(
          byId,
          picked,
          (j) => j.parentId,
          (up) => api.job(queue, up).then((r) => r.job),
        );
        return (id) => cascadeRoot(byId, sel, id, action) ?? id;
      };
      return t.each((id) => send(id, action), { done, one: 'job', prepare });
    }

    function openReschedule(job) {
      const current = notYetVisible(job) ? job.notVisibleUntil : '';
      Object.assign(reschedule, { open: true, job, at: toLocalInput(current), error: '', saving: false });
    }

    function rescheduleIn(now) {
      const iso = toIsoInstant(reschedule.at);
      if (!iso) return '';
      const secs = (Date.parse(iso) - now) / MS_PER_SECOND;
      return secs <= 0 ? 'Visible now' : 'Visible in ' + formatDuration(secs);
    }

    // A 409 means the server refuses this job's state. The reader cannot fix that here.
    function submitReschedule() {
      const r = reschedule;
      const runAt = toIsoInstant(r.at);
      if (!r.job) return;
      if (!runAt) return (r.error = 'Set a date and time.');
      const id = r.job.primaryKey;
      return submitForm(r, async () => {
        try {
          await api.rescheduleJob(queue, id, runAt);
          toast(`Job ${id} rescheduled`, 'success');
        } catch (e) {
          if (e.status !== 409) throw e;
          toast(`Job ${id} not rescheduled: ${e.message}`, 'warning');
        }
        r.open = false;
        await view.reload();
      });
    }

    // Insert opens clean of the last error, but keeps what was typed.
    const openInsert = () => Object.assign(insert, { open: true, error: '' });
    const insertInvalid = computed(() => !!insert.payload.trim() && !!parsePayload(insert.payload.trim()).error);

    function submitInsert() {
      const f = insert;
      const raw = f.payload.trim();
      const parsed = raw ? parsePayload(raw) : { error: true };
      const priority = parseOptionalInt(f.priority);
      const maxAttempts = parseOptionalInt(f.maxAttempts, 1);
      const at = f.notVisibleUntil ? toIsoInstant(f.notVisibleUntil) : null;
      const problem = [
        [!raw, 'Payload is required.'],
        [parsed.error, 'Invalid JSON: ' + parsed.error],
        [at === undefined, 'Invalid "Scheduled for" time.'],
        [priority.error, 'Priority must be a whole number.'],
        [maxAttempts.error, 'Max attempts must be a whole number of at least 1.'],
      ].find(([failed]) => failed);
      f.error = problem ? problem[1] : '';
      if (f.error) return;
      const body = { payload: parsed.value };
      if (f.groupKey) body.groupKey = f.groupKey;
      if (f.dedupKey) body.dedupKey = { key: f.dedupKey, strategy: f.dedupStrategy };
      if (priority.value != null) body.priority = priority.value;
      if (at) body.notVisibleUntil = at;
      if (maxAttempts.value != null) body.maxAttempts = maxAttempts.value;
      return submitForm(f, async () => {
        await api.insertJob(queue, body);
        Object.assign(insert, blankInsert());
        view.refresh();
        toast('Job inserted', 'success');
      });
    }

    const toggleVisibleFormat = () => {
      notVisibleFormat.value = notVisibleFormat.value === 'countdown' ? 'absolute' : 'countdown';
    };
    const visibleText = (iso) => (!iso ? EMPTY : notVisibleFormat.value === 'countdown' ? formatCountdown(iso) : formatTime(iso));
    const visibleTitle = (iso) =>
      'Click to switch format' + (iso ? ' · ' + (notVisibleFormat.value === 'countdown' ? formatTime(iso) : formatCountdown(iso)) : '');

    const ctx = {
      queue,
      t,
      view,
      cols,
      d,
      arm,
      busy,
      rows,
      mode,
      flatOnly,
      expanded,
      detail,
      drawerRow,
      reschedule,
      insert,
      insertInvalid,
      DELAYS,
      STATES,
      timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      notVisibleFormat,
      act,
      remove,
      bulk,
      toggleChildren,
      openReschedule,
      rescheduleIn,
      submitReschedule,
      openInsert,
      submitInsert,
      toggleVisibleFormat,
      visibleText,
      visibleTitle,
      canReschedule: (job) => !RESCHEDULE_REFUSED.includes(job.status),
      // A suspend is refused for a retried job whose lease or wait is still live.
      canSuspend: (job) => job.status !== 'suspended' && !(job.attempts > 0 && notYetVisible(job)),
      toggleMode: () => {
        viewMode.value = viewMode.value === 'tree' ? 'flat' : 'tree';
        t.reset();
      },
      setDelay: (secs) => Object.assign(reschedule, { at: toLocalInput(new Date(Date.now() + secs * MS_PER_SECOND)), error: '' }),
      workerUrl: (worker) => queueUrl(queue, 'jobs', { claimed_by: worker }),
      workerJobs(e, worker) {
        if (!plainClick(e)) return;
        d.close();
        t.only('claimed_by', worker);
      },
    };
    provide('jobs', ctx);
    return ctx;
  },
  template: /* html */ `
    <load-error :view="view" :t="t"/>
    <div class="toolbar" v-show="view.ready">
      <refresh-control :view="view"/>
      <filter-builder :t="t"/>
      <select class="form-select form-select-sm" style="width: auto;" title="Filter by job state" aria-label="Filter by job state"
        :value="t.q.status" @change="t.set('status', $event.target.value)">
        <option value="">All states</option>
        <option v-for="[s, label] in STATES" :key="s" :value="s">{{ label }}</option>
      </select>
      <button class="btn btn-sm" :class="mode === 'tree' ? 'btn-primary' : 'btn-outline-primary'" @click="toggleMode" :aria-pressed="mode === 'tree'" :disabled="flatOnly"
        :title="flatOnly ? 'This filter always renders flat' : mode === 'tree' ? 'Tree view: children hidden until expanded' : 'Flat view: all jobs shown'">{{ mode === 'tree' ? 'Tree' : 'Flat' }}</button>
      <columns-menu :cols="cols"/>
      <button class="btn btn-outline-success btn-sm" @click="openInsert">+ Insert</button>
    </div>
    <pending-note :view="view"/>

    <bulk-bar :t="t">
      <arm-button :arm="arm" k="bulkPromote" on="btn-primary" off="btn-outline-primary" :disabled="t.busy" label="Promote selected"
        :confirm="'Confirm promote ' + t.sel.size" @fire="bulk('promote', 'Promoted')"/>
      <arm-button :arm="arm" k="bulkCancel" on="btn-warning" off="btn-outline-warning" :disabled="t.busy" label="Cancel selected"
        :confirm="'Confirm cancel ' + t.sel.size + ' (with descendants)'" @fire="bulk('cancel', 'Cancelled')"/>
      <arm-button :arm="arm" k="bulkDlq" on="btn-danger" off="btn-outline-danger" :disabled="t.busy" label="Move selected to DLQ"
        :confirm="'Confirm move ' + t.sel.size + ' to DLQ'" @fire="bulk('move-to-dlq', 'Moved')"/>
    </bulk-bar>

    <pager :t="t" :view="view" one="job"/>
    <div v-scroll-edges class="table-responsive" :aria-busy="view.loading" v-show="view.ready">
      <table class="table table-striped table-hover table-sm sticky-head table-fixed">
        <t-head :cols="cols.shown" :sort="t.sort" :table="t">
          <template #visible>{{ notVisibleFormat === 'countdown' ? 'Visible In' : 'Visible At' }}</template>
        </t-head>
        <tbody>
          <tr v-for="job in rows" :key="job.primaryKey + '-' + job._depth" :tabindex="job._more ? null : 0" @click="!job._more && isRowClick($event) && d.view(job)"
            @keydown.enter.self.prevent="!job._more && d.toggle(job)" @keydown.space.self.prevent="!job._more && d.toggle(job)"
            :class="{ 'child-row': job._depth > 0, 'detail-row': !job._more, 'table-active': t.sel.has(job.primaryKey) }">
            <td v-if="job._more" :colspan="cols.shown.length" class="text-center text-muted small fst-italic" :style="treeIndentStyle(job._depth)">
              Showing {{ job._shown }} of {{ job._total }} children of #{{ job._parent }} &mdash;
              <a href="#" @click.prevent="t.only('parent_id', job._parent)">view all</a>
            </td>
            <template v-else>
              <td v-if="cols.on.select" class="select-cell"><input class="form-check-input m-0" type="checkbox" aria-label="Select job"
                :checked="t.sel.has(job.primaryKey)" @change="t.toggle(job.primaryKey)"></td>
              <td class="text-truncate" :style="treeIndentStyle(job._depth)">{{ job.primaryKey }}</td>
              <td v-if="cols.on.kind" class="text-truncate">{{ job.kind || EMPTY }}</td>
              <td v-if="cols.on.payload" class="text-truncate" :title="jsonText(job.payload)">{{ truncatePayload(job.payload) }}</td>
              <td v-if="cols.on.group" class="text-truncate">{{ job.groupKey || EMPTY }}</td>
              <td v-if="cols.on.parent">
                <a v-if="job.parentId" href="#" class="text-decoration-none" @click.prevent="t.only('parent_id', job.parentId)">{{ job.parentId }}</a>
                <template v-else>{{ EMPTY }}</template>
              </td>
              <td v-if="cols.on.children">
                <span v-if="job._childCount || job._dlqChildCount" class="d-inline-flex gap-1 flex-wrap align-items-center">
                  <template v-if="job._childCount">
                    <a v-if="mode === 'tree'" href="#" class="text-decoration-none d-inline-flex align-items-center gap-1" @click.prevent="toggleChildren(job.primaryKey)">
                      <span class="expand-arrow">{{ expanded[job.primaryKey] ? '▼' : '▶' }}</span>
                      <span class="badge bg-info-subtle text-info-emphasis">{{ job._childCount }} {{ pluralize(job._childCount, 'child', 'children') }}</span>
                    </a>
                    <span v-else class="text-muted small">{{ job._childCount }} {{ pluralize(job._childCount, 'child', 'children') }}</span>
                  </template>
                  <span v-if="job._dlqChildCount" class="badge bg-danger-subtle text-danger-emphasis">{{ job._dlqChildCount }} in DLQ</span>
                </span>
                <template v-else>{{ EMPTY }}</template>
              </td>
              <td v-if="cols.on.priority">{{ job.priority }}</td>
              <td v-if="cols.on.attempts">{{ job.attempts }}</td>
              <td v-if="cols.on.status"><span class="badge" :class="statusBadgeClass(job.status)">{{ job.status }}</span></td>
              <td v-if="cols.on.inserted" class="text-truncate" :title="formatTime(job.insertedAt, EMPTY)">{{ formatAge(job.insertedAt) }}</td>
              <td v-if="cols.on.visible" class="text-truncate font-monospace">
                <span class="format-toggle-cell" :class="{ 'has-no-value': !job.notVisibleUntil }" tabindex="0" role="button" :title="visibleTitle(job.notVisibleUntil)"
                  @click="toggleVisibleFormat" @keydown.enter.prevent="toggleVisibleFormat" @keydown.space.prevent="toggleVisibleFormat"><tick v-if="job.notVisibleUntil && notVisibleFormat === 'countdown'">{{ visibleText(job.notVisibleUntil) }}</tick><template v-else>{{ visibleText(job.notVisibleUntil) }}</template></span>
              </td>
              <td v-if="cols.on.gates"><gate-badges :rate="job.rateLimit" :conc="job.concurrency"/></td>
              <td v-if="cols.on.actions" class="cell-actions">
                <action-menu row :detail="() => d.view(job)" :disabled="busy.has(job.primaryKey)" v-slot="{ close }"><job-menu :job="job" :close="close"/></action-menu>
              </td>
            </template>
          </tr>
        </tbody>
        <skeleton-rows :view="view" :span="cols.shown.length" :empty="!rows.length">No jobs found.</skeleton-rows>
      </table>
    </div>
    <pager :t="t" :view="view" bottom/>

    <drawer :d="d" :title="d.cur ? 'Job ' + d.cur.primaryKey : detail.id != null ? 'Job ' + detail.id : 'Job'" :status="d.cur?.status || (detail.gone ? 'gone' : '')"
      :status-class="statusBadgeClass(d.cur?.status)">
      <template #actions>
        <action-menu v-if="drawerRow" :disabled="busy.has(drawerRow.primaryKey)" v-slot="{ close }"><job-menu :job="drawerRow" :close="close"/></action-menu>
      </template>
      <div v-if="detail.error" class="offcanvas-body drawer-empty" tabindex="0">
        <svg class="drawer-empty-icon" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.4">
          <circle cx="12" cy="12" r="9"/><path d="M12 7.5v5.5" stroke-linecap="round"/><circle cx="12" cy="16.4" r="0.9" fill="currentColor" stroke="none"/>
        </svg>
        <p class="drawer-empty-title">{{ detail.error }}</p>
        <p class="drawer-empty-note" v-if="detail.note">{{ detail.note }}</p>
        <div class="drawer-empty-actions">
          <button class="btn btn-outline-secondary btn-sm" v-if="!detail.gone" @click="d.view({ primaryKey: detail.id })">Retry</button>
          <button class="btn btn-outline-secondary btn-sm" v-if="d.neighbour(1)" @click="d.step(1)">Next job</button>
        </div>
      </div>
      <div v-else-if="d.cur" class="offcanvas-body" tabindex="0">
        <dl class="row">
          <kv l="Queue">{{ d.cur.queueName }}</kv>
          <kv l="Status">{{ d.cur.status ?? EMPTY }}</kv>
          <kv l="Group Key">{{ d.cur.groupKey || EMPTY }}</kv>
          <kv l="Parent ID">{{ d.cur.parentId || EMPTY }}</kv>
          <kv l="Suspended">{{ d.cur.suspended ? 'Yes' : 'No' }}</kv>
          <kv l="Priority">{{ d.cur.priority }}</kv>
          <kv l="Attempts">{{ d.cur.attempts }}</kv>
          <kv l="Max Attempts">{{ d.cur.maxAttempts ?? EMPTY }}</kv>
          <kv l="Dedup Key">{{ d.cur.dedupKey ? d.cur.dedupKey.key + ' (' + d.cur.dedupKey.strategy + ')' : EMPTY }}</kv>
          <kv v-for="[g, label, v] in [[d.cur.rateLimit, 'Rate Limit', 'ratelimits'], [d.cur.concurrency, 'Concurrency', 'concurrency']]" :key="v" :l="label">
            <a v-if="g" v-bind="policyLink(v, g.prefix)" class="font-monospace small" title="Open policy">{{ gateLabel(g) }}</a>
            <template v-else>{{ EMPTY }}</template>
          </kv>
          <kv l="Inserted At">{{ formatTime(d.cur.insertedAt, EMPTY) }}</kv>
          <kv l="Updated At">{{ formatTime(d.cur.updatedAt, EMPTY) }}</kv>
          <kv l="Last Attempt">{{ formatTime(d.cur.lastAttemptedAt, EMPTY) }}</kv>
          <kv l="Visible At">{{ formatTime(d.cur.notVisibleUntil, EMPTY) }}</kv>
          <kv l="Claimed By">
            <span v-if="d.cur.claimedBy" class="d-flex flex-column align-items-start gap-1">
              <code class="small">{{ d.cur.claimedBy }}</code>
              <a class="drill-link" :href="workerUrl(d.cur.claimedBy)" @click="workerJobs($event, d.cur.claimedBy)" title="Every job this worker holds">siblings &rarr;</a>
            </span>
            <template v-else>{{ EMPTY }}</template>
          </kv>
          <kv l="Claim Seq">{{ d.cur.claimSeq ?? EMPTY }}</kv>
          <kv l="Archive For">{{ d.cur.archiveFor ? formatDuration(d.cur.archiveFor) : EMPTY }}</kv>
          <kv l="Trace">
            <copy-block v-if="d.cur.traceparent" class="font-monospace small" :text="d.cur.traceparent + (d.cur.tracestate ? '\\n' + d.cur.tracestate : '')"/>
            <template v-else>{{ EMPTY }}</template>
          </kv>
          <kv v-if="d.cur.parentState != null" l="Child Results" wide><copy-block :text="formatJson(d.cur.parentState)"/></kv>
          <kv l="Last Error" wide>
            <copy-block v-if="d.cur.lastError" :text="d.cur.lastError"/>
            <template v-else>{{ EMPTY }}</template>
          </kv>
          <kv l="Payload" wide><copy-block :text="formatJson(d.cur.payload)"/></kv>
        </dl>
      </div>
    </drawer>

    <modal v-model:open="insert.open" title="Insert job" :subject="queue" size="lg" :error="insert.error">
      <div class="row g-3">
        <div class="col-12">
          <label class="form-label">Payload (JSON)</label>
          <textarea class="form-control font-monospace" rows="4" v-model="insert.payload" :class="{ 'is-invalid': insertInvalid }"></textarea>
          <small class="text-danger" v-if="insertInvalid">Invalid JSON</small>
        </div>
        <div class="col-sm-6"><label class="form-label">Group Key</label><input type="text" class="form-control" v-model="insert.groupKey" placeholder="Optional"></div>
        <div class="col-sm-6"><label class="form-label">Dedup Key</label><input type="text" class="form-control" v-model="insert.dedupKey" placeholder="Optional"></div>
        <div class="col-sm-6" v-if="insert.dedupKey">
          <label class="form-label">Strategy</label>
          <select class="form-select" v-model="insert.dedupStrategy"><option value="ignore">Ignore</option><option value="replace">Replace</option></select>
        </div>
        <div class="col-sm-6"><label class="form-label">Priority</label><input type="number" class="form-control" v-model="insert.priority"></div>
        <div class="col-sm-6"><label class="form-label">Max Attempts</label><input type="number" min="1" class="form-control" v-model="insert.maxAttempts" placeholder="Default"></div>
        <div class="col-sm-6">
          <label class="form-label">Scheduled For</label>
          <input type="datetime-local" class="form-control" v-model="insert.notVisibleUntil" title="Optional: the job stays scheduled and is not claimable until this local time">
        </div>
      </div>
      <template #footer>
        <button type="button" class="btn btn-primary btn-sm" @click="submitInsert" :disabled="insert.saving">{{ insert.saving ? 'Inserting…' : 'Insert job' }}</button>
      </template>
    </modal>

    <modal v-model:open="reschedule.open" title="Reschedule job" :subject="reschedule.job ? 'Job ' + reschedule.job.primaryKey + ' · ' + reschedule.job.status : ''" :error="reschedule.error">
      <label class="form-label" for="rescheduleAt">Visible at</label>
      <input id="rescheduleAt" type="datetime-local" step="1" class="form-control" v-model="reschedule.at" @keydown.enter.prevent="submitReschedule">
      <div class="reschedule-meta">
        <span class="reschedule-zone">Time zone: {{ timeZone }}</span>
        <tick v-slot="{ now }"><span class="reschedule-in">{{ rescheduleIn(now) }}</span></tick>
      </div>
      <div class="delay-chips" role="group" aria-label="Quick delays from now">
        <button v-for="x in DELAYS" :key="x.label" type="button" class="delay-chip"
          @click="setDelay(x.secs)">{{ x.label }}</button>
      </div>
      <template #footer>
        <button type="button" class="btn btn-primary btn-sm" @click="submitReschedule" :disabled="reschedule.saving || !reschedule.at">{{ reschedule.saving ? 'Rescheduling…' : 'Reschedule' }}</button>
      </template>
    </modal>`,
};
