// App-wide state: the queue set, health, the event stream, the top loader and toasts.
import { markRaw, reactive, watch } from '../vendor/vue.esm-browser.prod.js';
import { api } from './api.js';
import { TIMING, NARROW_MQ } from './config.js';
import { route, navigate, listUrl, SYSTEM_VIEWS } from './router.js';

/** @typedef {{ id: number, message: string, type: string, count: number, timer: any, held: boolean }} Toast */

// Storage throws where site data is blocked. State then lasts for the page only.
function stored(use) {
  try {
    return use(localStorage);
  } catch {
    return null;
  }
}

// Reads JSON, or the raw string a value was saved as before.
export function load(key, fallback) {
  const raw = stored((s) => s.getItem(key));
  if (raw == null) return fallback;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

export function save(key, value) {
  stored((s) => s.setItem(key, JSON.stringify(value)));
}

export function forget(key) {
  stored((s) => s.removeItem(key));
}

const narrowQuery = matchMedia(NARROW_MQ);

export const app = reactive({
  queues: /** @type {string[]} */ ([]),
  queuesFailed: false,
  narrow: narrowQuery.matches,
  theme: document.documentElement.getAttribute('data-bs-theme') || 'dark',
  health: /** @type {Awaited<ReturnType<typeof api.health>> | null} */ (null),
  // Off unless switched on, so a reader opts into the stream.
  sseOff: load('arb.eventsOff', 1) !== 0,
  sseDisabled: false,
  connected: false,
  dropped: false,
  events: /** @type {any[]} */ ([]),
  loaders: 0,
  showLoader: false,
  toasts: /** @type {Toast[]} */ ([]),

  // The drilled-into queue, once the queue set confirms it exists.
  get queue() {
    return route.view === 'queues' && this.queues.includes(route.queue) ? route.queue : '';
  },

  get pageTitle() {
    if (this.queue) return this.queue;
    return SYSTEM_VIEWS[route.view] || 'Queues';
  },

  get healthState() {
    return this.health ? this.health.status : 'unknown';
  },

  get healthTitle() {
    if (this.health && 'reachable' in this.health) return 'Cannot reach the server';
    return { ok: 'Server healthy', down: 'Server cannot reach its database', unknown: 'Checking server health' }[this.healthState];
  },

  // A stream that has not answered yet connects. Only one that was live and went away is disconnected.
  get sseState() {
    if (this.sseOff) return 'off';
    if (this.connected) return 'connected';
    return this.dropped ? 'disconnected' : 'connecting';
  },
});

narrowQuery.addEventListener('change', (e) => {
  app.narrow = e.matches;
});

// ---- Bus ----

const handlers = {};

function emit(name, data) {
  handlers[name]?.forEach((fn) => fn(data));
}

export function on(name, fn) {
  (handlers[name] ??= new Set()).add(fn);
  return () => handlers[name].delete(fn);
}

// ---- Top loader ----

// Holds the top bar while a first load is out. The release spends once.
export function claimLoader() {
  let held = true;
  app.loaders++;
  return () => {
    if (held) app.loaders--;
    held = false;
  };
}

// The bar shows only once loads outlast a delay, so fast ones never flash it.
let loaderTimer = null;
watch(
  () => app.loaders > 0,
  (busy) => {
    clearTimeout(loaderTimer);
    if (busy)
      loaderTimer = setTimeout(() => {
        app.showLoader = app.loaders > 0;
      }, TIMING.loaderDelayMs);
    else app.showLoader = false;
  },
);

// ---- Toasts ----

let toastSeq = 0;

// A repeat of a toast on screen bumps its count and restarts its timer. It adds no second toast.
export function toast(message, type = 'danger') {
  const same = app.toasts.find((t) => t.type === type && t.message === message);
  if (same) {
    same.count++;
    if (!same.held) holdToast(same, false);
    return;
  }
  while (app.toasts.length >= TIMING.toastMaxVisible) clearTimeout(app.toasts.shift()?.timer);
  const t = { id: ++toastSeq, message, type, count: 1, timer: null, held: false };
  app.toasts.push(t);
  holdToast(t, false);
}

export function dismissToast(t) {
  clearTimeout(t.timer);
  const i = app.toasts.findIndex((x) => x.id === t.id);
  if (i >= 0) app.toasts.splice(i, 1);
}

// A hovered or focused toast stays until the pointer and the focus leave.
export function holdToast(t, held) {
  t.held = held;
  clearTimeout(t.timer);
  if (!held) t.timer = setTimeout(() => dismissToast(t), TIMING.toastDelays[t.type] ?? TIMING.toastDelays.danger);
}

// ---- Theme ----

export function toggleTheme() {
  app.theme = app.theme === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-bs-theme', app.theme);
  // Raw, since theme-boot.js reads it before any module runs.
  stored((s) => s.setItem('arbiter-theme', app.theme));
}

// ---- Event stream ----

let source = null;
let hasConnected = false;
let retryTimer = null;
let retryMs = 0;
let buffer = [];
let flushTimer = null;
let eventSeq = 0;

export function toggleSSE() {
  app.sseOff = !app.sseOff;
  save('arb.eventsOff', app.sseOff ? 1 : 0);
  if (app.sseOff) closeSSE();
  else {
    retryMs = 0;
    connectSSE();
  }
}

function closeSSE() {
  clearTimeout(retryTimer);
  retryTimer = null;
  source?.close();
  source = null;
  app.connected = false;
  app.dropped = false;
}

// Doubles the last delay from base, up to max.
const nextBackoff = (prev, base, max) => Math.min(prev ? prev * 2 : base, max);

// The browser retries a stream that drops. One the server refuses closes for
// good, so re-arm it here and back off to a slow re-probe.
function retrySSE() {
  if (app.sseOff || retryTimer) return;
  retryMs = nextBackoff(retryMs, TIMING.sseRetryMs, TIMING.sseRetryMaxMs);
  retryTimer = setTimeout(() => {
    retryTimer = null;
    connectSSE();
  }, retryMs);
}

// A message that is not JSON is skipped.
function parseEvent(event) {
  try {
    return JSON.parse(event.data);
  } catch {
    return null;
  }
}

function onMessage(event) {
  app.connected = true;
  const data = parseEvent(event);
  if (!data) return;
  if (data.event === 'disabled') {
    closeSSE();
    app.sseDisabled = true;
    retrySSE();
  } else if (data.event === 'connected') {
    // A reconnect missed events, so every view reloads.
    if (hasConnected) emit('reconnect');
    hasConnected = true;
    app.dropped = false;
    app.sseDisabled = false;
    retryMs = 0;
    clearTimeout(retryTimer);
    retryTimer = null;
  } else {
    buffer.push(markRaw({ ...data, receivedAt: new Date().toISOString(), _seq: ++eventSeq }));
    flushTimer ??= setTimeout(flush, TIMING.flushMs);
  }
}

function connectSSE() {
  if (app.sseOff) return;
  source?.close();
  source = api.events();
  source.onmessage = onMessage;
  source.onerror = () => {
    if (app.connected) app.dropped = true;
    app.connected = false;
    if (!source || source.readyState === EventSource.CLOSED) retrySSE();
  };
}

// The stream's first answer says whether the server streams events at all. A refusal asks again at the slow backoff.
function probeSSE() {
  if (!app.sseOff) return;
  const probe = api.events();
  probe.onmessage = (event) => {
    probe.close();
    app.sseDisabled = parseEvent(event)?.event === 'disabled';
    if (app.sseDisabled) setTimeout(probeSSE, TIMING.sseRetryMaxMs);
  };
  probe.onerror = () => probe.close();
}

// Retention is per queue, so a busy queue does not evict a quiet one's tail.
function flush() {
  flushTimer = null;
  const batch = buffer;
  buffer = [];
  const kept = new Map();
  app.events = [...batch]
    .reverse()
    .concat(app.events)
    .filter((e) => {
      const q = e.table || '';
      const n = (kept.get(q) ?? 0) + 1;
      kept.set(q, n);
      return n <= TIMING.maxEventsPerQueue;
    });
  emit('sse', batch);
}

// ---- Boot ----

// A tick is skipped while a probe is out, so a slow server does not collect them.
let healthReq = null;
function loadHealth() {
  healthReq ??= api
    .health()
    .then((h) => {
      app.health = h;
    })
    .finally(() => {
      healthReq = null;
    });
}

// The registry fixes the queue set, so one good fetch holds until a queue in the URL is missing from it. A failed fetch asks again after a delay.
let queuesReq = null;
let queuesTimer = null;
let queuesRetryMs = 0;
let queuesLanded = false;
function loadQueues() {
  clearTimeout(queuesTimer);
  queuesReq ??= api
    .queues()
    .then((r) => {
      // An unchanged set keeps its array, so the watchers on it stay quiet.
      if (!queuesLanded || r.queues.length !== app.queues.length || r.queues.some((q, i) => q !== app.queues[i])) app.queues = r.queues;
      queuesLanded = true;
      app.queuesFailed = false;
      queuesRetryMs = 0;
      return true;
    })
    .catch((e) => {
      queuesReq = null;
      app.queuesFailed = true;
      queuesRetryMs = nextBackoff(queuesRetryMs, TIMING.queuesRetryMs, TIMING.queuesRetryMaxMs);
      queuesTimer = setTimeout(loadQueues, queuesRetryMs);
      toast('Could not load queues: ' + e.message);
      return false;
    });
  return queuesReq;
}

// A redeploy can add a queue while the page stays open.
function reloadQueues() {
  queuesReq = null;
  return loadQueues();
}

// A queue in the URL mounts only once the queue set confirms it. Each navigation
// and each landed queue set asks again.
async function confirmQueue() {
  if (!route.queue || app.queuesFailed || app.queues.includes(route.queue)) return;
  if (!(await loadQueues())) return;
  if (!route.queue || app.queues.includes(route.queue)) return;
  if (!(await reloadQueues())) return;
  if (!route.queue || app.queues.includes(route.queue)) return;
  toast(`Queue "${route.queue}" not found`, 'warning');
  navigate(listUrl(), { replace: true });
}

export async function boot() {
  loadHealth();
  setInterval(() => {
    if (!document.hidden) loadHealth();
  }, TIMING.healthPollMs);
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) loadHealth();
  });
  watch([() => route.nav, () => app.queues, () => app.queuesFailed], confirmQueue);
  watch(
    () => app.sseDisabled && route.view === 'events',
    (gone) => {
      if (gone) navigate(listUrl(), { replace: true });
    },
  );
  if (app.sseOff) probeSSE();
  else connectSSE();
  // A page in the back/forward cache must not hold a connection from the per-host pool.
  addEventListener('pagehide', closeSSE);
  addEventListener('pageshow', (e) => {
    if (e.persisted) connectSSE();
  });
  const release = claimLoader();
  try {
    await loadQueues();
  } finally {
    release();
  }
}
