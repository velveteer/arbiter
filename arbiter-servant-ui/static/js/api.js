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
    const res = await fetch(url, {
      headers: { 'Content-Type': 'application/json', ...options.headers },
      ...options,
    });
    if (!res.ok) {
      const text = await res.text();
      const err = new Error(`${res.status}: ${text}`);
      err.status = res.status;
      err.body = text;
      throw err;
    }
    if (res.status === 204) return null;
    return res.json();
  },

  // Queues
  listQueues() {
    return this._fetch('/queues');
  },

  // Jobs
  listJobs(table, { limit = 50, offset = 0, groupKey, parentId, suspended, rootsOnly, inFlight, sortBy, sortDir } = {}) {
    let qs = `?limit=${limit}&offset=${offset}`;
    if (groupKey) qs += `&group_key=${encodeURIComponent(groupKey)}`;
    if (parentId) qs += `&parent_id=${parentId}`;
    if (suspended !== undefined && suspended !== '') qs += `&suspended=${suspended}`;
    if (rootsOnly) qs += `&roots_only=true`;
    if (inFlight) qs += `&in_flight=true`;
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
  listWorkers({ queue, liveSecs } = {}) {
    const params = new URLSearchParams();
    if (queue) params.set('queue', queue);
    if (liveSecs != null) params.set('live', String(liveSecs));
    const qs = params.toString() ? `?${params.toString()}` : '';
    return this._fetch(`/workers${qs}`);
  },

  pauseWorker(workerId) {
    return this._fetch(`/workers/${encodeURIComponent(workerId)}/pause`, { method: 'POST' });
  },

  resumeWorker(workerId) {
    return this._fetch(`/workers/${encodeURIComponent(workerId)}/resume`, { method: 'POST' });
  },

  // SSE
  connectSSE(onMessage, onError) {
    const es = new EventSource(`${this.baseUrl()}/events/stream`);
    es.onmessage = onMessage;
    es.onerror = onError;
    return es;
  },
};
