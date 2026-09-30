// A drilled-into queue: its header, pause toggle, sub-tabs, stats and groups.
import { ref, shallowRef, reactive, computed, nextTick, watch } from '../../vendor/vue.esm-browser.prod.js';
import { api } from '../api.js';
import { CONFIG } from '../config.js';
import { pct, statusCount } from '../format.js';
import { route, navigate, plainClick, queueUrl, QUEUE_TABS, TAB_KEYS } from '../router.js';
import { app } from '../store.js';
import { provideKinds, useArm, useBusy, useColumns, useKinds, useScrollActive, useTable, useView } from '../use.js';
/** @import { Schema } from '../../../types/client' */
import { JobsTab } from './jobs.js';
import { DlqTab, ArchiveTab } from './snapshots.js';
import { CronView } from './cron.js';
import { WorkersView } from './workers.js';

// ---- Stats ----

const STATE_CARDS = [
  { label: 'Total Jobs', status: '' },
  { label: 'Waiting', status: 'ready' },
  { label: 'In-Flight', status: 'in_flight', title: 'Jobs currently leased by a worker' },
  { label: 'Scheduled', status: 'scheduled', title: 'Jobs delayed until a future time, not yet attempted' },
  { label: 'Backoff', status: 'backoff', title: 'Failed jobs waiting out a retry backoff delay' },
  { label: 'Throttled', status: 'throttled', title: 'Jobs parked by a rate limit until tokens refill', warn: true },
  { label: 'Suspended', status: 'suspended', title: 'Suspended jobs (e.g. rollup finalizers awaiting children)' },
  { label: 'Cancelled', status: 'cancelled', title: 'Force-cancelled jobs flagged for teardown, not yet reaped' },
];

// The stats query reads all of the queue table, so stream events never start it.
// A busy queue sends events faster than any refresh interval.
const StatsTab = {
  props: { queue: String },
  setup(props) {
    const stats = ref(/** @type {Schema<'QueueStats'> | null} */ (null));
    const kinds = useKinds();
    const view = useView({
      noun: 'stats',
      key: 'arb.statsRefresh',
      mode: '30s',
      empty: () => !stats.value,
      async load(stale) {
        kinds.load();
        const data = await api.stats(props.queue);
        if (stale()) return;
        stats.value = data.stats;
      },
    });
    const value = (c) => statusCount(stats.value, c.status);
    // Declared kinds with live depth and DLQ count, deepest first.
    const kindRows = computed(() => {
      const live = stats.value?.kindCounts || {};
      const dead = stats.value?.dlqKindCounts || {};
      const rows = kinds.list.value
        .map((kind) => ({ kind, depth: live[kind] || 0, dlq: dead[kind] || 0 }))
        .sort((a, b) => b.depth - a.depth || b.dlq - a.dlq || a.kind.localeCompare(b.kind));
      const top = rows[0]?.depth;
      return rows.map((r) => ({ ...r, bar: top ? pct(r.depth / top) : 0 }));
    });
    const url = (tab, extra) => queueUrl(props.queue, tab, extra);
    // The bare DLQ link switches tabs, so the DLQ keeps its filters.
    const openDlq = (e) => {
      if (plainClick(e)) navigate(url('dlq'), { quiet: true });
    };
    return { stats, view, value, kindRows, url, openDlq, STATE_CARDS };
  },
  template: /* html */ `
    <load-error :view="view"/>
    <div v-show="view.ready">
      <div class="toolbar"><refresh-control :view="view"/></div>
      <h2 class="stat-section-label">Jobs by state</h2>
      <div class="row g-3">
        <div v-for="c in STATE_CARDS" :key="c.label" class="col-sm-6 col-lg-3">
          <a class="card text-center stat-card" :title="c.title" v-bind="queueLink(queue, 'jobs', { status: c.status })">
            <div class="card-body">
              <h3 class="card-title text-muted">{{ c.label }}</h3>
              <p class="display-6" :class="c.warn && value(c) > 0 ? 'warn' : zeroClass(value(c))">{{ value(c) ?? EMPTY }}</p>
              <p v-if="c.status === 'ready' && stats?.blockedJobs > 0" class="stat-card-sub" :title="BLOCKED_TITLE">{{ stats.readyJobs }} ready &middot; {{ stats.blockedJobs }} blocked</p>
            </div>
          </a>
        </div>
      </div>
      <h2 class="stat-section-label">Health</h2>
      <div class="row g-3">
        <div class="col-sm-6 col-lg-4">
          <a class="card text-center stat-card" :class="{ 'is-danger': stats?.dlqJobs > 0 }" title="Entries in this queue's dead-letter queue" :href="url('dlq')" @click="openDlq">
            <div class="card-body">
              <h3 class="card-title text-muted">DLQ</h3>
              <p class="display-6" :class="stats?.dlqJobs > 0 ? 'bad' : zeroClass(stats?.dlqJobs)">{{ stats?.dlqJobs ?? EMPTY }}</p>
              <p class="stat-card-sub" v-if="stats?.exhaustedJobs > 0" title="Visible jobs out of attempts, awaiting the reaper's sweep into the DLQ">{{ stats.exhaustedJobs }} exhausted</p>
            </div>
          </a>
        </div>
        <div class="col-sm-6 col-lg-4">
          <div class="card text-center" :title="OLDEST_READY_TITLE">
            <div class="card-body">
              <h3 class="card-title text-muted">Oldest Ready</h3>
              <p class="display-6" :class="zeroClass(stats?.oldestReadyAgeSeconds)">{{ formatDuration(stats?.oldestReadyAgeSeconds) }}</p>
            </div>
          </div>
        </div>
        <div class="col-sm-6 col-lg-4">
          <div class="card text-center" :title="OLDEST_RUNNING_TITLE">
            <div class="card-body">
              <h3 class="card-title text-muted">Oldest Running</h3>
              <p class="display-6" :class="zeroClass(stats?.oldestInFlightAgeSeconds)">{{ formatDuration(stats?.oldestInFlightAgeSeconds) }}</p>
            </div>
          </div>
        </div>
      </div>
      <section v-if="kindRows.length" class="kind-breakdown" aria-labelledby="kindBreakdownLabel">
        <h2 class="stat-section-label" id="kindBreakdownLabel">By kind</h2>
        <div class="kind-list">
          <div class="kind-list-head" aria-hidden="true"><span>Kind</span><span></span><span class="num">Depth</span><span class="num">DLQ</span></div>
          <div v-for="r in kindRows" :key="r.kind" class="kind-row">
            <span class="kind-name" :title="r.kind">{{ r.kind }}</span>
            <span class="kind-bar" aria-hidden="true"><span class="kind-bar-fill" :style="{ width: r.bar + '%' }"></span></span>
            <a class="kind-count num" :class="zeroClass(r.depth)" v-bind="queueLink(queue, 'jobs', { kind: r.kind })"
              :title="'Show the ' + r.kind + ' jobs'" :aria-label="r.depth + ' ' + r.kind + ' jobs'">{{ formatCompact(r.depth) }}</a>
            <a class="kind-count num" :class="r.dlq > 0 ? 'bad' : 'is-zero'" v-bind="queueLink(queue, 'dlq', { kind: r.kind })"
              :title="'Show the ' + r.kind + ' DLQ entries'" :aria-label="r.dlq + ' ' + r.kind + ' DLQ entries'">{{ formatCompact(r.dlq) }}</a>
          </div>
        </div>
      </section>
    </div>`,
};

// ---- Groups ----

const GROUP_COLS = [
  { key: 'group', label: 'Group', weight: 22, required: true },
  { key: 'jobs', label: 'Jobs', weight: 9, cls: 'num', title: 'Jobs in the group' },
  { key: 'ready', label: 'Ready', weight: 9, narrow: false, cls: 'num', title: 'Jobs a claim can take now, before the group lets one through' },
  { key: 'head', label: 'Head job', weight: 20, title: 'The job that holds the group, or the job its next claim takes' },
  { key: 'held', was: 'inflight', label: 'Held', weight: 20, title: 'Time until the head job lets the group go: its lease, backoff or throttle' },
  { key: 'due', label: 'Next due', weight: 20, narrow: false, title: 'When the earliest scheduled job becomes visible' },
];

const lapsed = (g, now) => !!g.inFlightUntil && Date.parse(g.inFlightUntil) <= now;

const GroupsTab = {
  props: { queue: String },
  setup(props) {
    const groups = shallowRef(/** @type {Schema<'GroupSummary'>[]} */ ([]));
    const cols = useColumns(GROUP_COLS, 'arb.groupCols');
    const view = useView({
      noun: 'groups',
      key: 'arb.groupsRefresh',
      empty: () => groups.value.length === 0,
      events: (batch) => batch.filter((e) => e.table === props.queue && !e.dlq).length,
      async load(stale) {
        const data = await api.groups(props.queue, t.params());
        if (stale()) return;
        groups.value = data.items;
        t.landed(data.total);
      },
    });
    const t = useTable({
      view,
      tab: 'groups',
      filters: ['group_key'],
      sorts: [],
      pageKey: 'arb.groupsPageSize',
      ids: () => [],
    });
    return { groups, cols, view, t, lapsed };
  },
  template: /* html */ `
    <load-error :view="view" :t="t"/>
    <div class="toolbar" v-show="view.ready">
      <refresh-control :view="view"/>
      <filter-builder :t="t"/>
      <columns-menu :cols="cols"/>
    </div>
    <pending-note :view="view"/>
    <pager :t="t" :view="view" one="group"/>
    <div v-scroll-edges class="table-responsive" :aria-busy="view.loading" v-show="view.ready">
      <table class="table table-striped table-hover table-sm sticky-head groups-table table-fixed">
        <t-head :cols="cols.shown"/>
        <tbody>
          <tr v-for="g in groups" :key="g.groupKey">
            <td class="text-truncate">
              <a class="group-key-link" v-bind="queueLink(queue, 'jobs', { group_key: g.groupKey })" :title="'Show the jobs in ' + g.groupKey">{{ g.groupKey }}</a>
            </td>
            <td v-if="cols.on.jobs" class="num">{{ formatCompact(g.jobCount) }}</td>
            <td v-if="cols.on.ready" class="num" :class="{ 'is-zero': !g.readyCount }">{{ formatCompact(g.readyCount) }}</td>
            <td v-if="cols.on.head">
              <span v-if="g.headJobId != null" class="group-head">
                <a class="text-decoration-none" v-bind="queueLink(queue, 'jobs', { job_id: g.headJobId })" :title="'Show job ' + g.headJobId">#{{ g.headJobId }}</a>
                <span v-if="g.headBlocked" class="badge" :class="statusBadgeClass('blocked')" title="Ready, but a full concurrency key or an empty rate-limit bucket holds it back">blocked</span>
                <span v-else-if="g.headStatus" class="badge" :class="statusBadgeClass(g.headStatus)">{{ g.headStatus }}</span>
              </span>
              <span v-else class="text-muted">{{ EMPTY }}</span>
            </td>
            <td v-if="cols.on.held" class="font-monospace">
              <tick v-if="g.inFlight" v-slot="{ now }">
                <span class="group-lease" :class="{ 'is-lapsed': lapsed(g, now) }" :title="'Held until ' + formatTime(g.inFlightUntil)">
                  <span class="group-lease-dot" aria-hidden="true"></span>
                  <span>{{ lapsed(g, now) ? 'hold ended' : formatCountdown(g.inFlightUntil, EMPTY) }}</span>
                </span>
              </tick>
              <span v-else class="text-muted">{{ EMPTY }}</span>
            </td>
            <td v-if="cols.on.due" class="font-monospace" :title="formatTime(g.nextDue)" :class="{ 'text-muted': !g.nextDue }"><tick v-if="g.nextDue">{{ formatCountdown(g.nextDue, EMPTY) }}</tick><template v-else>{{ EMPTY }}</template></td>
          </tr>
        </tbody>
        <skeleton-rows :view="view" :span="cols.shown.length" :empty="!groups.length">{{ t.q.group_key ? 'No open group matches this key.' : 'No open groups.' }}</skeleton-rows>
      </table>
    </div>
    <pager :t="t" :view="view" bottom/>`,
};

// ---- Pause toggle ----

// A pause is an operator action the stream never reports, so it polls on its own.
const PAUSE_POLL_MODE = '10s';

const PauseToggle = {
  props: { queue: String },
  setup(props) {
    const s = reactive({ paused: false, pausedAt: null, unknown: false, error: '', confirming: false });
    const arm = useArm();
    const busy = useBusy();
    const mode = CONFIG.pauseConfirm;
    // The header outlives its tabs, so a navigation closes the modal here.
    watch(
      () => route.nav,
      () => (s.confirming = false),
    );

    async function load(stale) {
      try {
        const details = await api.queueDetails(props.queue);
        if (!stale()) Object.assign(s, { paused: !!details?.paused, pausedAt: details?.pausedAt || null, unknown: false });
      } catch (e) {
        if (stale()) return;
        if (e.status === 404) Object.assign(s, { paused: false, pausedAt: null, unknown: false });
        else Object.assign(s, { unknown: true, error: e.message });
      }
    }
    const view = useView({ noun: 'pause state', load, mode: PAUSE_POLL_MODE });

    const apply = (pause) =>
      busy.run(
        'pause',
        async () => {
          await api.setQueuePaused(props.queue, pause);
          await view.reload();
        },
        `Failed to ${pause ? 'pause' : 'resume'} queue`,
      );

    // Resume and arm-mode pause take a second click. Type-mode pause opens the modal.
    function toggle() {
      if (busy.has('pause')) return;
      if (!s.paused && mode === 'type') s.confirming = true;
      else if (arm.fire('toggle')) apply(!s.paused);
    }

    return { s, arm, busy, mode, toggle, apply };
  },
  template: /* html */ `
    <div class="ms-auto d-flex align-items-center gap-3">
      <span v-if="s.unknown" class="badge pause-unknown-badge" :title="'Cannot read the pause state: ' + s.error">pause state unknown</span>
      <span v-else-if="s.paused" class="badge paused-badge" :title="s.pausedAt ? formatTime(s.pausedAt) : ''">
        queue paused<tick v-if="s.pausedAt"><span class="ms-1 fw-normal">({{ formatAge(s.pausedAt) }})</span></tick>
      </span>
      <button v-if="!s.unknown && (s.paused || mode !== 'off')" class="btn btn-sm text-nowrap" :disabled="busy.has('pause')" @click="toggle"
        :class="arm.is('toggle') ? 'btn-warning' : s.paused ? 'btn-success' : 'btn-outline-warning'">{{ arm.is('toggle') ? 'Click to confirm' : s.paused ? 'Resume queue' : 'Pause queue' }}</button>
      <confirm-modal v-model:open="s.confirming" title="Pause queue" :subject="queue" :target="queue" action="Pause queue" :busy="busy.has('pause')"
        note="Pausing stops all workers from claiming jobs in this queue until it is resumed. In-flight jobs finish, new ones wait."
        prompt="Type the queue name to confirm:" @confirm="apply(true)"/>
    </div>`,
};

// ---- Detail shell ----

const TABS = { stats: StatsTab, jobs: JobsTab, groups: GroupsTab, dlq: DlqTab, archive: ArchiveTab, cron: CronView, workers: WorkersView };

// Each tab stays alive while the queue does, so a return to it keeps its filters.
export const QueueDetail = {
  components: { PauseToggle },
  props: { queue: String },
  setup(props) {
    const tabs = ref(/** @type {HTMLElement | null} */ (null));
    // A tab switch keeps the tab's state, so it moves the route without a navigation.
    const open = (tab) => {
      if (tab !== route.tab) navigate(queueUrl(props.queue, tab), { quiet: true });
    };
    const go = (i) => {
      open(TAB_KEYS[(i + TAB_KEYS.length) % TAB_KEYS.length]);
      nextTick(() => /** @type {HTMLElement | null | undefined} */ (tabs.value?.querySelector('.active'))?.focus());
    };
    const step = (delta) => go(TAB_KEYS.indexOf(route.tab) + delta);
    useScrollActive(tabs, () => route.tab);
    provideKinds(props.queue);
    return { route, app, tabs, QUEUE_TABS, open, go, step, current: computed(() => TABS[route.tab]) };
  },
  template: /* html */ `
    <div class="d-flex flex-wrap align-items-center gap-2 mb-3">
      <span v-if="app.queues.length <= 1" class="queue-title">{{ queue }}</span>
      <div v-else class="dropdown">
        <drop-down :label="queue" toggle-class="queue-title queue-switch dropdown-toggle" menu-class="queue-switch-menu" title="Switch queue">
          <button v-for="q in app.queues" :key="q" type="button" class="dropdown-item" :class="{ active: q === queue }"
            @click="navigate(queueUrl(q, route.tab))">{{ q }}</button>
        </drop-down>
      </div>
      <pause-toggle :queue="queue"/>
    </div>
    <ul ref="tabs" v-scroll-edges class="nav nav-tabs" role="tablist" @keydown.right.prevent="step(1)" @keydown.down.prevent="step(1)"
      @keydown.left.prevent="step(-1)" @keydown.up.prevent="step(-1)" @keydown.home.prevent="go(0)" @keydown.end.prevent="go(-1)">
      <li v-for="(label, key) in QUEUE_TABS" :key="key" class="nav-item" role="presentation">
        <button :id="'tab-btn-' + key" class="nav-link" :class="{ active: route.tab === key }" type="button" role="tab" :aria-selected="route.tab === key"
          :aria-controls="route.tab === key ? 'tab-' + key : null"
          :tabindex="route.tab === key ? 0 : -1" @click="open(key)">{{ label }}</button>
      </li>
    </ul>
    <div :id="'tab-' + route.tab" class="tab-content mt-3" role="tabpanel" :aria-labelledby="'tab-btn-' + route.tab">
      <keep-alive><component :is="current" :key="route.tab" :queue="queue"/></keep-alive>
    </div>`,
};
