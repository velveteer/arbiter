/**
 * Arbiter API client - centralized fetch wrappers.
 * Base URL auto-discovered from the admin UI path.
 * If the page loads at /foo/, the API is at /foo/api/v1/.
 */
const ArbiterAPI = {
  baseUrl() {
    const base = location.pathname.replace(/\/(index\.html)?$/, '');
    return `${location.protocol}//${location.host}${base}/api/v1`;
  },

  async _fetch(path, options = {}) {
    const url = `${this.baseUrl()}${path}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ARB_TIMING.fetchTimeoutMs);
    try {
      const res = await fetch(url, {
        headers: { 'Content-Type': 'application/json', ...options.headers },
        ...options,
        signal: controller.signal,
      });
      if (!res.ok) {
        const text = await res.text();
        let message = `${res.status} ${res.statusText}`.trim();
        try {
          const parsed = JSON.parse(text);
          if (parsed && (parsed.error || parsed.message)) message = parsed.error || parsed.message;
        } catch {
          if (text && text.length <= 200) message = text;
        }
        const err = new Error(message);
        err.status = res.status;
        err.body = text;
        throw err;
      }
      if (res.status === 204) return null;
      const text = await res.text();
      if (!text) return null;
      try {
        return JSON.parse(text);
      } catch {
        const err = new Error('Invalid JSON response');
        err.status = res.status;
        err.body = text;
        throw err;
      }
    } catch (e) {
      if (e.name === 'AbortError') {
        const err = new Error('Request timed out');
        err.status = 0;
        throw err;
      }
      throw e;
    } finally {
      clearTimeout(timer);
    }
  },

  _pageQuery({ limit, offset } = {}) {
    const qs = [];
    if (limit != null) qs.push(`limit=${encodeURIComponent(limit)}`);
    if (offset != null) qs.push(`offset=${encodeURIComponent(offset)}`);
    return qs.length ? `?${qs.join('&')}` : '';
  },

  // Queues
  listQueues() {
    return this._fetch('/queues');
  },

  // Jobs
  listJobs(table, { limit = 50, offset = 0, groupKey, parentId, status, rootsOnly, sortBy, sortDir } = {}) {
    let qs = `?limit=${limit}&offset=${offset}`;
    if (groupKey) qs += `&group_key=${encodeURIComponent(groupKey)}`;
    if (parentId) qs += `&parent_id=${parentId}`;
    if (status) qs += `&status=${encodeURIComponent(status)}`;
    if (rootsOnly) qs += `&roots_only=true`;
    if (sortBy) qs += `&sort_by=${encodeURIComponent(sortBy)}`;
    if (sortDir) qs += `&sort_dir=${encodeURIComponent(sortDir)}`;
    return this._fetch(`/${table}/jobs${qs}`);
  },

  getJob(table, id) {
    return this._fetch(`/${table}/jobs/${id}`);
  },

  insertJob(table, body) {
    return this._fetch(`/${table}/jobs`, {
      method: 'POST',
      body: JSON.stringify(body),
    });
  },

  cancelJob(table, id) {
    return this._fetch(`/${table}/jobs/${id}`, { method: 'DELETE' });
  },

  forceCancelJob(table, id) {
    return this._fetch(`/${table}/jobs/${id}/force-cancel`, { method: 'POST' });
  },

  promoteJob(table, id) {
    return this._fetch(`/${table}/jobs/${id}/promote`, { method: 'POST' });
  },

  moveToDLQ(table, id) {
    return this._fetch(`/${table}/jobs/${id}/move-to-dlq`, { method: 'POST' });
  },

  pauseChildren(table, id) {
    return this._fetch(`/${table}/jobs/${id}/pause-children`, { method: 'POST' });
  },

  resumeChildren(table, id) {
    return this._fetch(`/${table}/jobs/${id}/resume-children`, { method: 'POST' });
  },

  suspendJob(table, id) {
    return this._fetch(`/${table}/jobs/${id}/suspend`, { method: 'POST' });
  },

  resumeJob(table, id) {
    return this._fetch(`/${table}/jobs/${id}/resume`, { method: 'POST' });
  },

  // DLQ
  listDLQ(table, { limit = 50, offset = 0, parentId, groupKey, sortBy, sortDir } = {}) {
    let qs = `?limit=${limit}&offset=${offset}`;
    if (parentId) qs += `&parent_id=${parentId}`;
    if (groupKey) qs += `&group_key=${encodeURIComponent(groupKey)}`;
    if (sortBy) qs += `&sort_by=${encodeURIComponent(sortBy)}`;
    if (sortDir) qs += `&sort_dir=${encodeURIComponent(sortDir)}`;
    return this._fetch(`/${table}/dlq${qs}`);
  },

  retryFromDLQ(table, id) {
    return this._fetch(`/${table}/dlq/${id}/retry`, { method: 'POST' });
  },

  deleteDLQ(table, id) {
    return this._fetch(`/${table}/dlq/${id}`, { method: 'DELETE' });
  },

  deleteDLQBatch(table, ids) {
    return this._fetch(`/${table}/dlq/batch-delete`, {
      method: 'POST',
      body: JSON.stringify({ ids }),
    });
  },

  // Stats
  getStats(table) {
    return this._fetch(`/${table}/stats`);
  },

  // Per-queue stats for every queue in one request (landing overview).
  getAllStats() {
    return this._fetch('/queues/stats');
  },

  // Cron
  listCronSchedules({ queue } = {}) {
    const qs = queue ? `?queue=${encodeURIComponent(queue)}` : '';
    return this._fetch(`/cron/schedules${qs}`);
  },

  updateCronSchedule(name, body) {
    return this._fetch(`/cron/schedules/${encodeURIComponent(name)}`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    });
  },

  // Queue details (pause/resume)
  getQueueDetails(queue) {
    return this._fetch(`/queues/${encodeURIComponent(queue)}/details`);
  },

  pauseQueue(queue) {
    return this._fetch(`/queues/${encodeURIComponent(queue)}/pause`, { method: 'POST' });
  },

  resumeQueue(queue) {
    return this._fetch(`/queues/${encodeURIComponent(queue)}/resume`, { method: 'POST' });
  },

  // Workers
  listWorkers({ queue } = {}) {
    const qs = queue ? `?queue=${encodeURIComponent(queue)}` : '';
    return this._fetch(`/workers${qs}`);
  },

  pauseWorker(workerId) {
    return this._fetch(`/workers/${encodeURIComponent(workerId)}/pause`, { method: 'POST' });
  },

  resumeWorker(workerId) {
    return this._fetch(`/workers/${encodeURIComponent(workerId)}/resume`, { method: 'POST' });
  },

  // Rate limits
  listRateLimits() {
    return this._fetch('/rate-limits');
  },

  listRateLimitBuckets(prefix, page = {}) {
    return this._fetch(`/rate-limits/${encodeURIComponent(prefix)}/buckets${this._pageQuery(page)}`);
  },

  updateRateLimitPolicy(prefix, body) {
    return this._fetch(`/rate-limits/${encodeURIComponent(prefix)}`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    });
  },

  resetRateLimitBuckets(prefix) {
    return this._fetch(`/rate-limits/${encodeURIComponent(prefix)}/reset`, { method: 'POST' });
  },

  // Concurrency
  listConcurrency() {
    return this._fetch('/concurrency');
  },

  listConcurrencyKeys(prefix, page = {}) {
    return this._fetch(`/concurrency/${encodeURIComponent(prefix)}/keys${this._pageQuery(page)}`);
  },

  updateConcurrencyPolicy(prefix, body) {
    return this._fetch(`/concurrency/${encodeURIComponent(prefix)}`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    });
  },

  reconcileConcurrency() {
    return this._fetch('/concurrency/reconcile', { method: 'POST' });
  },

  // SSE
  connectSSE(onMessage, onError) {
    const es = new EventSource(`${this.baseUrl()}/events/stream`);
    es.onmessage = onMessage;
    es.onerror = onError;
    return es;
  },
};
