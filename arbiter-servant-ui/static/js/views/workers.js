// Worker pools with pause and resume. A queue's Workers tab, or every pool on
// the global view when no queue is given.
import { shallowRef, reactive, computed, provide, inject } from '../../vendor/vue.esm-browser.prod.js';
import { api } from '../api.js';
import { shortId } from '../format.js';
import { useArm, useBusy, useDetail, useSort, useStored, useSummary, useView } from '../use.js';
/** @import { Schema } from '../../../types/client' */

// How many trailing worker-id characters the pause confirmation asks for.
const CONFIRM_CHARS = 6;

// The workers worth looking at come first.
function healthRank(w) {
  if (w.health === 'stale') return 0;
  if (w.paused) return 1;
  if (w.health === 'draining') return 2;
  return 3;
}

const SORT_KEYS = {
  workerId: (w) => String(w.workerId),
  queue: (w) => w.queueName || '',
  host: (w) => w.hostName || '',
  threads: (w) => w.workerCount ?? 0,
  heartbeat: (w) => Date.parse(w.lastHeartbeat || '') || -1,
  status: healthRank,
};

const COLS = [
  { key: 'workerId', label: 'Worker ID', weight: 10, sort: 'workerId' },
  { key: 'queue', label: 'Queue', weight: 13, sort: 'queue' },
  { key: 'host', label: 'Host', weight: 20, sort: 'host' },
  { key: 'threads', label: 'Threads', weight: 6, sort: 'threads' },
  { key: 'heartbeat', label: 'Last heartbeat', weight: 14, sort: 'heartbeat' },
  { key: 'metadata', label: 'Metadata', weight: 14 },
  { key: 'status', label: 'Status', weight: 10, sort: 'status' },
  { key: 'actions', label: 'Actions', weight: 13, cls: 'cell-actions' },
];

// Health is computed by the server on the database clock, apart from the paused flag.
/** @type {Record<Schema<'WorkerHealth'>, [string, string]>} */
const HEALTH = {
  live: ['badge bg-success-subtle text-success-emphasis', 'Heartbeat is current'],
  stale: ['badge bg-danger-subtle text-danger-emphasis', 'No heartbeat inside the stale threshold'],
  draining: ['badge draining-badge', 'Stops claiming, finishes its in-flight jobs, then stops'],
};
const healthClass = (w) => HEALTH[w.health]?.[0] || 'badge bg-secondary-subtle text-secondary-emphasis';

const WorkerMenu = {
  props: { w: Object, close: Function },
  setup: () => inject('workers'),
  template: /* html */ `
    <a class="dropdown-item" v-bind="queueLink(w.queueName, 'jobs', { claimed_by: w.workerId })" @click="close()">View held jobs</a>
    <arm-button v-if="w.paused" :arm="arm" :k="'toggle:' + w.workerId" cls="dropdown-item" on="text-warning fw-semibold"
      :disabled="w.shuttingDown" label="Resume" confirm="Click to confirm" @fire="setPaused(w, false); close()"/>
    <button v-else type="button" class="dropdown-item" :disabled="w.shuttingDown" @click="askPause(w); close()">Pause</button>`,
};

export const WorkersView = {
  components: { WorkerMenu },
  props: { queue: String },
  setup(props) {
    const global = !props.queue;
    const workers = shallowRef(/** @type {Schema<'WorkerRow'>[]} */ ([]));
    const liveOnly = useStored('arb.workersLiveOnly', false);
    const pausing = reactive({ open: false, worker: null });
    const arm = useArm();
    const busy = useBusy();
    const view = useView({
      noun: 'workers',
      key: 'arb.workersRefresh',
      mode: '30s',
      empty: () => workers.value.length === 0,
      async load(stale) {
        const data = await api.workers(props.queue);
        if (stale()) return;
        workers.value = data.workers;
        // Hide stale can drop the open worker from the rows. Only a worker gone from the server closes it.
        d.resync(() => {
          const fresh = workers.value.find((w) => w.workerId === d.cur.workerId);
          if (fresh) d.cur = fresh;
          else d.close();
        });
      },
    });
    const visible = () => (liveOnly.value ? workers.value.filter((w) => w.health !== 'stale') : workers.value);
    // The global view leads with the workers worth a look.
    const sort = useSort(visible, SORT_KEYS, global ? 'status' : '', ['queue', 'workerId'], ['workerId', 'queue', 'host']);
    const d = useDetail({ rows: () => sort.rows, id: (w) => w.workerId });
    const anyMetadata = computed(() => sort.rows.some((w) => w.metadata));
    const cols = computed(() => COLS.filter((c) => (c.key !== 'queue' || global) && (c.key !== 'metadata' || anyMetadata.value)));
    const summary = useSummary(global && 'arb.summary.workers', view, () => workers.value.length > 0);
    const counts = computed(() =>
      workers.value.reduce(
        (acc, w) => {
          acc[w.health] = (acc[w.health] || 0) + 1;
          acc.paused += w.paused ? 1 : 0;
          acc.threads += w.workerCount || 0;
          return acc;
        },
        { live: 0, stale: 0, draining: 0, paused: 0, threads: 0 },
      ),
    );

    const setPaused = (w, paused) =>
      busy.run(
        w.workerId,
        async () => {
          await api.setWorkerPaused(w.workerId, paused);
          await view.reload();
        },
        `Failed to toggle worker ${shortId(w.workerId)}`,
      );

    const ctx = {
      global,
      workers,
      anyMetadata,
      liveOnly,
      pausing,
      arm,
      busy,
      view,
      sort,
      d,
      cols,
      summary,
      counts,
      CONFIRM_CHARS,
      healthClass,
      setPaused,
      healthTitle: (w) => HEALTH[w.health]?.[1] || '',
      askPause: (w) => Object.assign(pausing, { open: true, worker: w }),
    };
    provide('workers', ctx);
    return ctx;
  },
  template: /* html */ `
    <div v-if="global" class="queue-summary" :class="summary">
      <qs :v="workers.length" :l="pluralize(workers.length, 'worker')"/>
      <qs :v="counts.threads" :l="pluralize(counts.threads, 'thread')"/>
      <qs :v="counts.live" l="live"/>
      <qs :v="counts.stale" l="stale" :c="{ bad: counts.stale > 0 }"/>
      <qs v-if="counts.draining" :v="counts.draining" l="draining"/>
      <qs :v="counts.paused" l="paused" :c="{ warn: counts.paused > 0 }"/>
    </div>
    <div class="toolbar" v-show="view.ready">
      <refresh-control :view="view"/>
      <div class="form-check form-switch mb-0">
        <input class="form-check-input" type="checkbox" :id="'workers-live-only-' + global" v-model="liveOnly">
        <label class="form-check-label small" :for="'workers-live-only-' + global">Hide stale</label>
      </div>
    </div>
    <load-error :view="view"/>
    <div v-scroll-edges class="table-responsive" :aria-busy="view.loading" v-show="view.ready">
      <table class="table table-striped table-hover table-sm sticky-head table-fixed">
        <t-head :cols="cols" :sort="sort"/>
        <tbody>
          <tr v-for="w in sort.rows" :key="w.workerId" class="detail-row" @click="isRowClick($event) && d.view(w)">
            <td class="text-truncate"><code class="small" :title="w.workerId">{{ shortId(w.workerId) }}</code></td>
            <td v-if="global" class="text-truncate"><a v-bind="queueLink(w.queueName, 'workers')" :title="'Open ' + w.queueName">{{ w.queueName }}</a></td>
            <td class="text-truncate" :title="w.hostName ?? ''">{{ w.hostName ?? EMPTY }}</td>
            <td>{{ w.workerCount ?? EMPTY }}</td>
            <td><span :title="formatTime(w.lastHeartbeat)">{{ formatAge(w.lastHeartbeat) }}</span></td>
            <td v-if="anyMetadata" class="text-truncate small font-monospace" :title="w.metadata ? jsonText(w.metadata) : ''">{{ w.metadata ? jsonText(w.metadata) : EMPTY }}</td>
            <td>
              <span :class="healthClass(w)" :title="healthTitle(w)">{{ w.health || 'live' }}</span>
              <span v-if="w.paused" class="badge paused-badge ms-1">paused</span>
            </td>
            <td class="cell-actions">
              <action-menu row :detail="() => d.view(w)" :disabled="busy.has(w.workerId)" v-slot="{ close }"><worker-menu :w="w" :close="close"/></action-menu>
            </td>
          </tr>
        </tbody>
        <skeleton-rows :view="view" :span="cols.length" :empty="!sort.rows.length">{{ liveOnly && workers.length ? 'No active workers.' : 'No workers registered.' }}</skeleton-rows>
      </table>
    </div>

    <drawer :d="d" :title="d.cur ? 'Worker ' + shortId(d.cur.workerId) : 'Worker'" :status="d.cur ? d.cur.health || 'live' : ''" :status-class="d.cur ? healthClass(d.cur) : ''">
      <template #actions>
        <action-menu v-if="d.cur" :disabled="busy.has(d.cur.workerId)" v-slot="{ close }"><worker-menu :w="d.cur" :close="close"/></action-menu>
      </template>
      <div v-if="d.cur" class="offcanvas-body" tabindex="0">
        <dl class="row">
          <kv l="Worker ID"><code class="small">{{ d.cur.workerId }}</code></kv>
          <kv l="Queue">{{ d.cur.queueName }}</kv>
          <kv l="Host">{{ d.cur.hostName ?? EMPTY }}</kv>
          <kv l="Threads">{{ d.cur.workerCount ?? EMPTY }}</kv>
          <kv l="Started At">{{ formatTime(d.cur.startedAt, EMPTY) }}</kv>
          <kv l="Last Heartbeat">{{ formatTime(d.cur.lastHeartbeat, EMPTY) }} <span class="text-muted ms-2">({{ formatAge(d.cur.lastHeartbeat) }})</span></kv>
          <kv l="Stale Threshold">{{ d.cur.staleThresholdSecs }}s</kv>
          <kv l="Health"><span :class="healthClass(d.cur)">{{ d.cur.health || 'live' }}</span></kv>
          <kv l="Paused">{{ d.cur.paused ? 'Yes' : 'No' }}</kv>
          <kv l="Metadata" wide><copy-block :text="formatJson(d.cur.metadata)"/></kv>
        </dl>
      </div>
    </drawer>

    <confirm-modal v-model:open="pausing.open" title="Pause worker" :subject="pausing.worker?.workerId"
      :target="String(pausing.worker?.workerId ?? '').slice(-CONFIRM_CHARS)" action="Pause worker" :busy="busy.has(pausing.worker?.workerId)"
      :prompt="'Type the last ' + CONFIRM_CHARS + ' characters to confirm:'"
      note="Pausing stops this worker pool from claiming new jobs until it is resumed. In-flight jobs finish, new ones wait."
      @confirm="setPaused(pausing.worker, true)"/>`,
};
