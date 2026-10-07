// API client. The API lives under the page's own path: /foo/ serves /foo/api/.
// Types come from the OpenAPI document. Queue routes are typed as /api/queues/queue/...
/** @import { Res, Query, Body } from '../../types/client' */
import { TIMING } from './config.js';
import { parseJson } from './format.js';
import { listUrl, qs } from './router.js';

const BASE = listUrl().replace(/\/(index\.html)?$/, '') + '/api';
// Longest plain-text error body used as the message as it is.
const MAX_TEXT_ERROR = 200;
const enc = encodeURIComponent;

function fail(message, status, body) {
  return Object.assign(new Error(message), { status, body });
}

/**
 * @param {string} path
 * @param {{ method?: string, body?: unknown }} [opts]
 * @returns {Promise<any>}
 */
async function call(path, { method = 'GET', body } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMING.fetchTimeoutMs);
  try {
    const res = await fetch(BASE + path, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller.signal,
    });
    const text = await res.text();
    if (!res.ok) {
      let message = `${res.status} ${res.statusText}`.trim();
      try {
        const parsed = JSON.parse(text);
        message = parsed?.error || parsed?.message || message;
      } catch {
        if (text && text.length <= MAX_TEXT_ERROR) message = text;
      }
      throw fail(message, res.status, text);
    }
    if (!text) return null;
    try {
      return parseJson(text);
    } catch {
      throw fail('Invalid JSON response', res.status, text);
    }
  } catch (e) {
    throw e.name === 'AbortError' ? fail('Request timed out', 0) : e;
  } finally {
    clearTimeout(timer);
  }
}

/** @type {(path: string, params?: object) => Promise<any>} */
const get = (path, params) => call(path + qs(params));
const post = (path, body) => call(path, { method: 'POST', body });
const patch = (path, body) => call(path, { method: 'PATCH', body });
const del = (path) => call(path, { method: 'DELETE' });
// With a payload the job runs on it in place of the stored one.
const withPayload = (payload) => (payload === undefined ? undefined : { payload });

export const api = {
  // A 503 carries the same body as a 200, and an unreachable API is itself the answer.
  /** @returns {Promise<Res<'/api/health'> | { status: 'down', reachable: false }>} */
  async health() {
    try {
      return await call('/health');
    } catch (e) {
      try {
        const parsed = JSON.parse(e.body);
        if (parsed?.status === 'ok' || parsed?.status === 'down') return parsed;
      } catch {
        // Not the health body.
      }
      return { status: 'down', reachable: false };
    }
  },

  /** @returns {Promise<Res<'/api/queues'>>} */
  queues: () => get('/queues'),
  /** @returns {Promise<Res<'/api/queues/stats'>>} */
  allStats: () => get('/queues/stats'),
  /** @returns {Promise<Res<'/api/queues/queue/details'>>} */
  queueDetails: (q) => get(`/queues/${enc(q)}/details`),
  setQueuePaused: (q, paused) => post(`/queues/${enc(q)}/${paused ? 'pause' : 'resume'}`),
  /** @returns {Promise<Res<'/api/maintenance', 'post'>>} */
  maintenance: () => post('/maintenance'),

  /** @returns {Promise<Res<'/api/queues/queue/kinds'>>} */
  kinds: (q) => get(`/queues/${enc(q)}/kinds`),
  /** @returns {Promise<Res<'/api/queues/queue/stats'>>} */
  stats: (q) => get(`/queues/${enc(q)}/stats`),
  /** @type {(q: string, params: Query<'/api/queues/queue/groups'>) => Promise<Res<'/api/queues/queue/groups'>>} */
  groups: (q, params) => get(`/queues/${enc(q)}/groups`, params),

  /** @type {(q: string, params: Query<'/api/queues/queue/jobs'>) => Promise<Res<'/api/queues/queue/jobs'>>} */
  jobs: (q, params) => get(`/queues/${enc(q)}/jobs`, params),
  /** @returns {Promise<Res<'/api/queues/queue/jobs/{id}'>>} */
  job: (q, id) => get(`/queues/${enc(q)}/jobs/${id}`),
  /** @type {(q: string, body: Body<'/api/queues/queue/jobs', 'post'>) => Promise<Res<'/api/queues/queue/jobs', 'post'>>} */
  insertJob: (q, body) => post(`/queues/${enc(q)}/jobs`, body),
  /** @type {(q: string, id: number, action: 'promote' | 'force-cancel' | 'move-to-dlq' | 'suspend' | 'resume' | 'pause-children' | 'resume-children') => Promise<null>} */
  jobAction: (q, id, action) => post(`/queues/${enc(q)}/jobs/${id}/${action}`),
  cancelJob: (q, id) => del(`/queues/${enc(q)}/jobs/${id}`),
  /** @type {(q: string, id: number, runAt: string) => Promise<null>} */
  rescheduleJob: (q, id, runAt) => post(`/queues/${enc(q)}/jobs/${id}/reschedule`, /** @type {Body<'/api/queues/queue/jobs/{id}/reschedule', 'post'>} */ ({ runAt })),

  /** @type {(q: string, params: Query<'/api/queues/queue/dlq'>) => Promise<Res<'/api/queues/queue/dlq'>>} */
  dlq: (q, params) => get(`/queues/${enc(q)}/dlq`, params),
  retryDlq: (q, id, payload) => post(`/queues/${enc(q)}/dlq/${id}/retry`, withPayload(payload)),
  deleteDlq: (q, id) => del(`/queues/${enc(q)}/dlq/${id}`),
  /** @returns {Promise<Res<'/api/queues/queue/dlq/batch-delete', 'post'>>} */
  deleteDlqMany: (q, ids) => post(`/queues/${enc(q)}/dlq/batch-delete`, { ids }),

  /** @type {(q: string, params: Query<'/api/queues/queue/archive'>) => Promise<Res<'/api/queues/queue/archive'>>} */
  archive: (q, params) => get(`/queues/${enc(q)}/archive`, params),
  requeueArchive: (q, id, payload) => post(`/queues/${enc(q)}/archive/${id}/reenqueue`, withPayload(payload)),
  deleteArchive: (q, id) => del(`/queues/${enc(q)}/archive/${id}`),
  /** @returns {Promise<Res<'/api/queues/queue/archive/batch-delete', 'post'>>} */
  deleteArchiveMany: (q, ids) => post(`/queues/${enc(q)}/archive/batch-delete`, { ids }),

  /** @returns {Promise<Res<'/api/cron/schedules'>>} */
  cron: (queue) => get('/cron/schedules', { queue }),
  /** @type {(name: string, body: Body<'/api/cron/schedules/{name}', 'patch'>) => Promise<Res<'/api/cron/schedules/{name}', 'patch'>>} */
  updateCron: (name, body) => patch(`/cron/schedules/${enc(name)}`, body),
  runCron: (name) => post(`/cron/schedules/${enc(name)}/run`),

  /** @returns {Promise<Res<'/api/workers'>>} */
  workers: (queue) => get('/workers', { queue }),
  setWorkerPaused: (id, paused) => post(`/workers/${enc(id)}/${paused ? 'pause' : 'resume'}`),

  /** @returns {Promise<Res<'/api/rate-limits'>>} */
  rateLimits: () => get('/rate-limits'),
  /** @type {(prefix: string, params: Query<'/api/rate-limits/{prefix}/buckets'>) => Promise<Res<'/api/rate-limits/{prefix}/buckets'>>} */
  buckets: (prefix, params) => get(`/rate-limits/${enc(prefix)}/buckets`, params),
  /** @type {(prefix: string, body: Body<'/api/rate-limits/{prefix}', 'patch'>) => Promise<Res<'/api/rate-limits/{prefix}', 'patch'>>} */
  updateRateLimit: (prefix, body) => patch(`/rate-limits/${enc(prefix)}`, body),
  /** @returns {Promise<Res<'/api/rate-limits/{prefix}/reset', 'post'>>} */
  resetBuckets: (prefix) => post(`/rate-limits/${enc(prefix)}/reset`),
  /** @returns {Promise<Res<'/api/rate-limits/{prefix}/buckets/{key}/tokens', 'post'>>} */
  addTokens: (prefix, key, tokens) => post(`/rate-limits/${enc(prefix)}/buckets/${enc(key)}/tokens`, { tokens }),
  /** @returns {Promise<Res<'/api/rate-limits/prune', 'post'>>} */
  pruneBuckets: () => post('/rate-limits/prune'),

  /** @returns {Promise<Res<'/api/concurrency'>>} */
  concurrency: () => get('/concurrency'),
  /** @type {(prefix: string, params: Query<'/api/concurrency/{prefix}/keys'>) => Promise<Res<'/api/concurrency/{prefix}/keys'>>} */
  concurrencyKeys: (prefix, params) => get(`/concurrency/${enc(prefix)}/keys`, params),
  /** @type {(prefix: string, body: Body<'/api/concurrency/{prefix}', 'patch'>) => Promise<Res<'/api/concurrency/{prefix}', 'patch'>>} */
  updateConcurrency: (prefix, body) => patch(`/concurrency/${enc(prefix)}`, body),
  /** @returns {Promise<Res<'/api/concurrency/reconcile', 'post'>>} */
  reconcileConcurrency: () => post('/concurrency/reconcile'),
  /** @returns {Promise<Res<'/api/concurrency/prune', 'post'>>} */
  pruneConcurrencyKeys: () => post('/concurrency/prune'),

  events: () => new EventSource(BASE + '/events/stream'),
};
