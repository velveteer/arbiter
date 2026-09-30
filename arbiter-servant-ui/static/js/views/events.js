// The live event log: a tail of the stream, newest first. Filters narrow it.
import { ref, reactive, computed } from '../../vendor/vue.esm-browser.prod.js';
import { queueLink } from '../router.js';
import { app, toggleSSE } from '../store.js';
import { TIMING } from '../config.js';
import { memoByObject } from '../format.js';

const TYPES = {
  job_inserted: ['Inserted', 'bg-primary-subtle text-primary-emphasis'],
  job_updated: ['Updated', 'bg-warning-subtle text-warning-emphasis'],
  job_deleted: ['Deleted', 'bg-danger-subtle text-danger-emphasis'],
  job_dlq: ['DLQ', 'bg-secondary-subtle text-secondary-emphasis'],
};

const STATE_TEXT = {
  off: ['Live updates off', 'Live updates are off. Switch them on to stream events.'],
  connected: ['Live updates connected', 'No events yet. They appear here as they happen.'],
  connecting: ['Connecting to live updates', 'Connecting to the event stream.'],
  disconnected: ['Reconnecting to live updates', 'Reconnecting. Events resume when the stream is back.'],
};

const COLS = [
  { key: 'time', label: 'Time', weight: 22 },
  { key: 'event', label: 'Event', weight: 18 },
  { key: 'queue', label: 'Queue', weight: 42 },
  { key: 'job', label: 'Job ID', weight: 18 },
];

// A link to the tab that lists an event's job. A deleted row is no longer where the event saw it.
const jobLink = memoByObject((e) =>
  !e.table || !e.job_id || e.event === 'job_deleted' ? null : queueLink(e.table, e.dlq ? 'dlq' : 'jobs', { job_id: e.job_id }),
);

const tableLink = memoByObject((e) => queueLink(e.table));

export const EventsView = {
  setup() {
    const queue = ref('');
    const types = reactive(Object.fromEntries(Object.keys(TYPES).map((k) => [k, true])));
    const matched = computed(() => app.events.filter((e) => (!queue.value || e.table === queue.value) && types[e.event] !== false));
    const shown = computed(() => matched.value.slice(0, TIMING.maxEventRows));
    return { app, queue, types, matched, shown, COLS, TYPES, STATE_TEXT, jobLink, tableLink, toggleSSE };
  },
  template: /* html */ `
    <div class="toolbar">
      <span class="status-pip" role="status" :class="app.sseState" :title="STATE_TEXT[app.sseState][0]" :aria-label="STATE_TEXT[app.sseState][0]">
        <span class="status-dot" aria-hidden="true"></span>
      </span>
      <div class="form-check form-switch mb-0">
        <input class="form-check-input" type="checkbox" role="switch" id="events-live" :checked="!app.sseOff"
          @change="toggleSSE" :title="STATE_TEXT[app.sseState][0]">
        <label class="form-check-label" for="events-live">Live updates</label>
      </div>
      <select class="form-select form-select-sm event-queue-select" v-model="queue" aria-label="Filter events by queue">
        <option value="">All Queues</option>
        <option v-for="q in app.queues" :key="q" :value="q">{{ q }}</option>
      </select>
      <div class="d-flex gap-2">
        <div v-for="([label, cls], k) in TYPES" :key="k" class="form-check form-check-inline">
          <input class="form-check-input" type="checkbox" :id="'evt-' + k" v-model="types[k]">
          <label class="form-check-label" :for="'evt-' + k"><span class="badge" :class="cls">{{ label }}</span></label>
        </div>
      </div>
      <button class="btn btn-outline-secondary btn-sm" @click="app.events = []">Clear</button>
    </div>
    <div class="event-log">
      <table class="table table-sm table-hover sticky-head table-fixed">
        <t-head :cols="COLS"/>
        <tbody>
          <tr v-for="e in shown" :key="e._seq">
            <td class="text-truncate" :title="formatTime(e.receivedAt)">{{ formatClock(e.receivedAt) }}</td>
            <td class="text-truncate"><span class="badge" :class="TYPES[e.event]?.[1] || 'bg-info-subtle text-info-emphasis'" :title="e.event">{{ TYPES[e.event]?.[0] || e.event }}</span></td>
            <td class="text-truncate">
              <a v-if="e.table" v-bind="tableLink(e)" :title="'Open ' + e.table">{{ e.table }}</a>
              <span v-else class="text-muted">{{ EMPTY }}</span>
            </td>
            <td class="text-truncate">
              <a v-if="jobLink(e)" v-bind="jobLink(e)" :title="e.dlq ? 'Show this job in the DLQ' : 'Show this job'">{{ e.job_id }}</a>
              <span v-else class="text-muted">{{ e.job_id || EMPTY }}</span>
            </td>
          </tr>
          <tr v-if="matched.length > shown.length"><td colspan="4" class="text-muted text-center">The newest {{ shown.length }} of {{ matched.length }}. Filter by queue to see more.</td></tr>
          <tr v-if="!shown.length"><td colspan="4" class="text-muted text-center">{{ STATE_TEXT[app.sseState][1] }}</td></tr>
        </tbody>
      </table>
    </div>`,
};
