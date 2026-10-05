// The landing: every queue as a card or a row, from one bulk stats request.
import { ref, shallowRef, computed } from '../../vendor/vue.esm-browser.prod.js';
import { api } from '../api.js';
import { TIMING } from '../config.js';
import { BLOCKED_TITLE, READY_TITLE, WAITING_TITLE, pluralize, statusCount, waitingJobs, zeroClass } from '../format.js';
import { toast } from '../store.js';
import { useArm, useBusy, useSort, useStored, useSummary, useView } from '../use.js';
/** @import { Schema } from '../../../types/client' */

// An absent age sorts below any present one.
const SORT_KEYS = {
  queue: (r) => r.queue,
  waiting: (r) => waitingJobs(r.stats) ?? 0,
  ready: (r) => r.stats.readyJobs,
  blocked: (r) => r.stats.blockedJobs,
  inFlight: (r) => statusCount(r.stats, 'in_flight'),
  scheduled: (r) => statusCount(r.stats, 'scheduled'),
  backoff: (r) => statusCount(r.stats, 'backoff'),
  throttled: (r) => statusCount(r.stats, 'throttled'),
  dlq: (r) => r.stats.dlqJobs,
  oldest: (r) => r.stats.oldestReadyAgeSeconds ?? -1,
  oldestInFlight: (r) => r.stats.oldestInFlightAgeSeconds ?? -1,
  workers: (r) => r.workersLive,
};

const COLS = [
  { key: 'queue', label: 'Queue', sort: 'queue' },
  { key: 'waiting', label: 'Waiting', sort: 'waiting', cls: 'num', title: WAITING_TITLE },
  { key: 'ready', label: 'Ready', sort: 'ready', cls: 'num', title: READY_TITLE },
  { key: 'blocked', label: 'Blocked', sort: 'blocked', cls: 'num', title: BLOCKED_TITLE },
  { key: 'inFlight', label: 'In-flight', sort: 'inFlight', cls: 'num' },
  { key: 'scheduled', label: 'Scheduled', sort: 'scheduled', cls: 'num' },
  { key: 'backoff', label: 'Backoff', sort: 'backoff', cls: 'num' },
  { key: 'throttled', label: 'Throttled', sort: 'throttled', cls: 'num' },
  { key: 'dlq', label: 'DLQ', sort: 'dlq', cls: 'num' },
  { key: 'oldest', label: 'Oldest ready', sort: 'oldest', cls: 'num' },
  { key: 'oldestInFlight', label: 'Oldest running', sort: 'oldestInFlight', cls: 'num' },
  { key: 'workers', label: 'Workers', sort: 'workers', cls: 'num' },
  { key: 'state', label: 'State' },
];

// Card sort choices. The order that holds still while counts move comes first.
const CARD_SORTS = [
  ['queue', 'Name'],
  ['waiting', 'Waiting'],
  ['ready', 'Ready'],
  ['blocked', 'Blocked'],
  ['inFlight', 'In-flight'],
  ['backoff', 'Backoff'],
  ['scheduled', 'Scheduled'],
  ['throttled', 'Throttled'],
  ['dlq', 'DLQ'],
  ['oldest', 'Oldest ready'],
  ['oldestInFlight', 'Oldest running'],
  ['workers', 'Workers'],
];

// Card stats that link to the jobs in that state. A throttled job warns.
const CARD_STATS = [
  ['in_flight', 'in-flight'],
  ['scheduled', 'scheduled'],
  ['backoff', 'backoff'],
  ['throttled', 'throttled'],
];

const statusClass = (r, status) => {
  const n = statusCount(r.stats, status);
  return status === 'throttled' && n > 0 ? 'warn' : zeroClass(n);
};

// A paused queue holds all work, so it outranks paused workers. All paused reads stronger than some.
function pauseState(r) {
  const live = r.workersLive;
  const paused = r.workersPaused;
  if (r.paused) return { key: 'queue', label: 'paused' };
  if (paused > 0 && paused >= live) return { key: 'workers', label: 'workers paused' };
  if (paused > 0) return { key: 'some', label: `${paused}/${live} paused` };
  return null;
}

// A queue's pause state, or a placeholder where a list cell needs one.
const PauseBadge = {
  props: { row: { type: Object, required: true }, placeholder: Boolean },
  setup(props) {
    return { state: computed(() => pauseState(props.row)) };
  },
  template: /* html */ `
    <span v-if="state" class="queue-card-pause" :class="'is-' + state.key">{{ state.label }}</span>
    <span v-else-if="placeholder" class="text-muted">{{ EMPTY }}</span>`,
};

export const QueueList = {
  components: { PauseBadge },
  setup() {
    const rows = shallowRef(/** @type {Schema<'QueueOverview'>[]} */ ([]));
    const search = ref('');
    const chosen = useStored('arb.queueView', '');
    // Until the reader picks one, a long queue list opens as a list.
    const mode = computed({
      get: () => chosen.value || (rows.value.length > TIMING.queueListThreshold ? 'list' : 'cards'),
      set: (v) => {
        chosen.value = v;
      },
    });
    const busy = useBusy();
    const arm = useArm();
    const view = useView({
      noun: 'queues',
      key: 'arb.queuesRefresh',
      mode: '10s',
      empty: () => rows.value.length === 0,
      async load(stale) {
        const data = await api.allStats();
        if (stale()) return;
        rows.value = data.queues;
      },
    });
    const matching = () => {
      const needle = search.value.trim().toLowerCase();
      return needle ? rows.value.filter((r) => r.queue.toLowerCase().includes(needle)) : rows.value;
    };
    const sort = useSort(matching, SORT_KEYS, 'queue', ['queue'], ['queue']);
    const summaryClass = useSummary('arb.summary.queues', view, () => rows.value.length > 0);
    const summary = computed(() =>
      rows.value.reduce(
        (acc, r) => {
          const s = r.stats;
          acc.ready += s.readyJobs;
          acc.blocked += s.blockedJobs;
          acc.inFlight += statusCount(s, 'in_flight');
          acc.throttled += statusCount(s, 'throttled');
          acc.dlq += s.dlqJobs;
          acc.workersLive += r.workersLive;
          acc.workersPaused += r.workersPaused;
          acc.queuesPaused += r.paused ? 1 : 0;
          return acc;
        },
        { ready: 0, blocked: 0, inFlight: 0, throttled: 0, dlq: 0, workersLive: 0, workersPaused: 0, queuesPaused: 0 },
      ),
    );

    // One pass of the work a pool's reaper does, for when no pool runs. An
    // operation that another caller already runs is skipped and not reported.
    const maintain = () =>
      busy.run(
        'maintain',
        async () => {
          const res = await api.maintenance();
          const touched = Object.values(res.ops).reduce((a, b) => a + b, 0);
          const failed = res.failed.length;
          if (failed) toast(`Maintenance finished, ${failed} ${pluralize(failed, 'operation')} failed`, 'warning');
          else toast(touched > 0 ? `Maintenance touched ${touched} ${pluralize(touched, 'row')}` : 'Maintenance found nothing to do', 'success');
          view.refresh();
        },
        'Maintenance failed',
      );

    return {
      rows,
      search,
      mode,
      busy,
      arm,
      view,
      sort,
      summary,
      summaryClass,
      maintain,
      COLS,
      CARD_SORTS,
      CARD_STATS,
      statusClass,
    };
  },
  template: /* html */ `
    <load-error :view="view"/>
    <div class="queue-summary" :class="summaryClass">
      <qs :v="formatCompact(summary.ready + summary.blocked)" l="waiting" :title="WAITING_TITLE"/>
      <qs :v="formatCompact(summary.ready)" l="ready" :title="READY_TITLE"/>
      <qs :v="formatCompact(summary.blocked)" l="blocked" :title="BLOCKED_TITLE"/>
      <qs :v="formatCompact(summary.inFlight)" l="in-flight"/>
      <qs :v="formatCompact(summary.throttled)" l="throttled" :c="{ warn: summary.throttled > 0 }"/>
      <qs :v="formatCompact(summary.dlq)" l="in DLQ" :c="{ bad: summary.dlq > 0 }"/>
      <div class="qs-sep" aria-hidden="true"></div>
      <qs :v="summary.workersLive" :l="pluralize(summary.workersLive, 'worker')"/>
      <qs v-if="summary.workersPaused" :v="summary.workersPaused" l="paused" c="warn"/>
      <qs v-if="summary.queuesPaused" :v="summary.queuesPaused" :l="pluralize(summary.queuesPaused, 'queue paused', 'queues paused')" c="warn"/>
    </div>

    <div class="queue-toolbar" :class="summaryClass">
      <refresh-control :view="view"/>
      <div class="queue-search">
        <svg class="queue-search-icon" viewBox="0 0 16 16" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.6">
          <circle cx="6.8" cy="6.8" r="4.6"/><path d="M10.2 10.2 L14 14" stroke-linecap="round"/>
        </svg>
        <input type="search" class="queue-search-input" v-model="search" placeholder="Filter queues" aria-label="Filter queues by name" autocomplete="off" spellcheck="false">
        <button type="button" class="queue-search-clear" v-if="search" @click="search = ''" title="Clear filter" aria-label="Clear filter">&#x2715;</button>
      </div>
      <div class="queue-toolbar-end">
        <arm-button :arm="arm" k="maintenance" on="btn-warning" off="btn-outline-secondary" :disabled="busy.has('maintain')" @fire="maintain"
          title="Sweep exhausted jobs to the DLQ, clear cancelled ones, retire stale workers and refresh group counts, across every queue. A worker pool's reaper does this on its own."
          :label="busy.has('maintain') ? ' Running…' : 'Run maintenance'" confirm="Confirm run"><span v-if="busy.has('maintain')" class="spin" aria-hidden="true">&#x21bb;</span></arm-button>
        <div class="queue-sort" v-if="mode === 'cards'">
          <label class="queue-sort-label" for="queue-sort-select">Sort</label>
          <select id="queue-sort-select" class="queue-sort-select" :value="sort.by" @change="sort.set($event.target.value)">
            <option v-for="[k, label] in CARD_SORTS" :key="k" :value="k">{{ label }}</option>
          </select>
          <button type="button" class="queue-sort-dir" @click="sort.dir = sort.dir === 'asc' ? 'desc' : 'asc'"
            :title="sort.dir === 'asc' ? 'Ascending. Click for descending.' : 'Descending. Click for ascending.'"
            :aria-label="sort.dir === 'asc' ? 'Sorted ascending' : 'Sorted descending'">{{ sort.dir === 'asc' ? '\\u2191' : '\\u2193' }}</button>
        </div>
        <div class="view-toggle" role="group" aria-label="Queue layout">
          <button type="button" class="view-toggle-btn" :class="{ active: mode !== 'list' }" :aria-pressed="mode !== 'list'" @click="mode = 'cards'" title="Card view" aria-label="Card view">
            <svg viewBox="0 0 16 16" aria-hidden="true" fill="currentColor">
              <rect x="1" y="1" width="6" height="6" rx="1.2"/><rect x="9" y="1" width="6" height="6" rx="1.2"/>
              <rect x="1" y="9" width="6" height="6" rx="1.2"/><rect x="9" y="9" width="6" height="6" rx="1.2"/>
            </svg>
          </button>
          <button type="button" class="view-toggle-btn" :class="{ active: mode === 'list' }" :aria-pressed="mode === 'list'" @click="mode = 'list'" title="List view" aria-label="List view">
            <svg viewBox="0 0 16 16" aria-hidden="true" fill="currentColor">
              <rect x="1" y="2.2" width="14" height="2.4" rx="1.2"/><rect x="1" y="6.8" width="14" height="2.4" rx="1.2"/><rect x="1" y="11.4" width="14" height="2.4" rx="1.2"/>
            </svg>
          </button>
        </div>
      </div>
    </div>

    <div class="queue-grid" v-if="view.ready && mode !== 'list'">
      <div v-for="r in sort.rows" :key="r.queue" class="queue-card" :class="{ 'is-throttled': r.stats.throttledJobs > 0, 'is-paused': r.paused, 'is-failing': r.stats.dlqJobs > 0 }">
        <a class="queue-card-head" v-bind="queueLink(r.queue)">
          <span class="queue-card-name" :title="r.queue">{{ r.queue }}</span>
          <span class="queue-card-arrow" aria-hidden="true">&rarr;</span>
        </a>
        <div class="queue-card-flags">
          <pause-badge :row="r"/>
          <a v-if="r.stats.dlqJobs > 0" class="queue-card-dlq" v-bind="queueLink(r.queue, 'dlq')"
            :title="'Open the dead-letter queue for ' + r.queue">{{ formatCompact(r.stats.dlqJobs) }} in DLQ</a>
        </div>
        <div class="queue-card-body">
          <div class="queue-card-headline">
            <a class="queue-card-hero" v-bind="queueLink(r.queue, 'jobs', { status: 'ready' })">
              <span class="queue-card-ready" :class="zeroClass(waitingJobs(r.stats))">{{ formatCompact(waitingJobs(r.stats)) }}</span>
              <span class="queue-card-ready-label">waiting</span>
            </a>
            <span class="queue-card-split">
              <span class="queue-card-split-ready" :title="READY_TITLE">
                <span class="qc-val" :class="zeroClass(r.stats.readyJobs)">{{ formatCompact(r.stats.readyJobs) }}</span><span class="qc-lbl">ready</span>
              </span>
              <span class="queue-card-split-blocked" :class="zeroClass(r.stats.blockedJobs)" :title="BLOCKED_TITLE">
                <span class="qc-val">{{ formatCompact(r.stats.blockedJobs) }}</span><span class="qc-lbl">blocked</span>
              </span>
            </span>
          </div>
          <div class="queue-card-stats">
            <a v-for="[status, label] in CARD_STATS" :key="status" class="queue-card-stat" v-bind="queueLink(r.queue, 'jobs', { status })">
              <span class="qc-val" :class="statusClass(r, status)">{{ formatCompact(statusCount(r.stats, status)) }}</span><span class="qc-lbl">{{ label }}</span>
            </a>
          </div>
          <div class="queue-card-ages">
            <a class="queue-card-age" v-bind="queueLink(r.queue, 'jobs', { status: 'ready' })"
              :title="OLDEST_READY_TITLE">
              <span class="qa-lbl">oldest ready</span>
              <span class="qa-val" :class="zeroClass(r.stats.oldestReadyAgeSeconds)">{{ formatDuration(r.stats.oldestReadyAgeSeconds) }}</span>
            </a>
            <a class="queue-card-age" v-bind="queueLink(r.queue, 'jobs', { status: 'in_flight' })"
              :title="OLDEST_RUNNING_TITLE">
              <span class="qa-lbl">oldest running</span>
              <span class="qa-val" :class="zeroClass(r.stats.oldestInFlightAgeSeconds)">{{ formatDuration(r.stats.oldestInFlightAgeSeconds) }}</span>
            </a>
          </div>
        </div>
      </div>
    </div>

    <div v-scroll-edges class="table-responsive" v-else-if="view.ready">
      <table class="table table-striped table-hover table-sm sticky-head queue-table">
        <t-head :cols="COLS" :sort="sort"/>
        <tbody>
          <tr v-for="r in sort.rows" :key="r.queue" class="detail-row" @click="isRowClick($event) && navigate(queueUrl(r.queue, 'jobs'))">
            <td class="text-truncate"><a class="queue-row-name" v-bind="queueLink(r.queue, 'jobs')" :title="r.queue">{{ r.queue }}</a></td>
            <td class="num"><a class="queue-row-num" :class="zeroClass(waitingJobs(r.stats))" v-bind="queueLink(r.queue, 'jobs', { status: 'ready' })">{{ formatCompact(waitingJobs(r.stats)) }}</a></td>
            <td class="num" :class="zeroClass(r.stats.readyJobs)">{{ formatCompact(r.stats.readyJobs) }}</td>
            <td class="num" :class="zeroClass(r.stats.blockedJobs)">{{ formatCompact(r.stats.blockedJobs) }}</td>
            <td v-for="[status] in CARD_STATS" :key="status" class="num">
              <a class="queue-row-num" :class="statusClass(r, status)" v-bind="queueLink(r.queue, 'jobs', { status })">{{ formatCompact(statusCount(r.stats, status)) }}</a>
            </td>
            <td class="num"><a class="queue-row-num" :class="r.stats.dlqJobs > 0 ? 'bad' : 'is-zero'" v-bind="queueLink(r.queue, 'dlq')">{{ formatCompact(r.stats.dlqJobs) }}</a></td>
            <td class="num" :class="zeroClass(r.stats.oldestReadyAgeSeconds)">{{ formatDuration(r.stats.oldestReadyAgeSeconds) }}</td>
            <td class="num" :class="zeroClass(r.stats.oldestInFlightAgeSeconds)">{{ formatDuration(r.stats.oldestInFlightAgeSeconds) }}</td>
            <td class="num" :class="zeroClass(r.workersLive)">{{ r.workersLive ?? 0 }}</td>
            <td><pause-badge :row="r" placeholder/></td>
          </tr>
          <tr v-if="!sort.rows.length && rows.length"><td :colspan="COLS.length" class="text-muted text-center">No queue matches that filter.</td></tr>
        </tbody>
      </table>
    </div>

    <div class="queue-list-empty" v-if="mode !== 'list' && !sort.rows.length && rows.length">No queue matches that filter.</div>
    <div class="empty-state" v-if="view.loaded && !view.errored && !rows.length">
      <svg class="empty-state-icon" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.3">
        <rect x="3" y="4.5" width="18" height="5" rx="1.4"/><rect x="3" y="14.5" width="18" height="5" rx="1.4"/><path d="M7 7h.01M7 17h.01" stroke-linecap="round"/>
      </svg>
      <p class="empty-state-title">No queues yet</p>
      <p class="empty-state-note">Queues come from the registry this server was built with. Add a <code>Queue</code> to it, run the migrations,
        and each one appears here with its jobs, dead letters, archive and schedules.</p>
      <a class="empty-state-link" href="https://arbiterq.dev/docs" target="_blank" rel="noreferrer">Read the guide &rarr;</a>
    </div>`,
};
