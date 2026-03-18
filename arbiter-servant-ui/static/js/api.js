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
      throw new Error(`${res.status}: ${text}`);
    }
    if (res.status === 204) return null;
    return res.json();
  },

  // Queues
  listQueues() {
    return this._fetch('/queues');
  },

  // Jobs
  listJobs(table, { limit = 50, offset = 0, groupKey, parentId, suspended } = {}) {
    let qs = `?limit=${limit}&offset=${offset}`;
    if (groupKey) qs += `&group_key=${encodeURIComponent(groupKey)}`;
    if (parentId) qs += `&parent_id=${parentId}`;
    if (suspended !== undefined && suspended !== '') qs += `&suspended=${suspended}`;
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

  getInFlightJobs(table, { limit = 50, offset = 0 } = {}) {
    return this._fetch(`/${table}/jobs/in-flight?limit=${limit}&offset=${offset}`);
  },

  cancelJob(table, id) {
    return this._fetch(`/${table}/jobs/${id}`, { method: 'DELETE' });
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
  listDLQ(table, { limit = 50, offset = 0, parentId, groupKey } = {}) {
    let qs = `?limit=${limit}&offset=${offset}`;
    if (parentId) qs += `&parent_id=${parentId}`;
    if (groupKey) qs += `&group_key=${encodeURIComponent(groupKey)}`;
    return this._fetch(`/${table}/dlq${qs}`);
  },

  retryFromDLQ(table, id) {
    return this._fetch(`/${table}/dlq/${id}/retry`, { method: 'POST' });
  },

  deleteDLQ(table, id) {
    return this._fetch(`/${table}/dlq/${id}`, { method: 'DELETE' });
  },

  // Stats
  getStats(table) {
    return this._fetch(`/${table}/stats`);
  },

  // Cron
  listCronSchedules() {
    return this._fetch('/cron/schedules');
  },

  updateCronSchedule(name, body) {
    return this._fetch(`/cron/schedules/${encodeURIComponent(name)}`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    });
  },

  // SSE
  connectSSE(onMessage, onError) {
    const es = new EventSource(`${this.baseUrl()}/events/stream`);
    es.onmessage = onMessage;
    es.onerror = onError;
    return es;
  },
};
