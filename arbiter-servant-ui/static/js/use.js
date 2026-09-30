// Composables: view lifecycle, tables, columns, sorting, detail stepping.
import { reactive, ref, computed, watch, provide, inject, onMounted, onBeforeUnmount, onActivated, onDeactivated } from '../vendor/vue.esm-browser.prod.js';
import { api } from './api.js';
import { TIMING } from './config.js';
import { formatTime, mapLimit, pluralize, shortId, toIsoInstant } from './format.js';
import { route, params, replaceParams } from './router.js';
import { app, on, claimLoader, toast, load, save, forget } from './store.js';
/** @import { Query } from '../../types/client' */

// Keep the active item of a scrolling strip in view as source changes.
export function useScrollActive(el, source) {
  const show = () => el.value?.querySelector('.active')?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
  onMounted(show);
  watch(source, show, { flush: 'post' });
}

function useBus(name, fn) {
  onBeforeUnmount(on(name, fn));
}

// A ref kept in localStorage.
export function useStored(key, fallback) {
  const r = ref(load(key, fallback));
  watch(r, (v) => save(key, v));
  return r;
}

// A roll-up strip's class. A view that had a roll-up last time holds its slot
// from the first frame. The numbers then land in place and do not push rows down.
// With no key it neither reads nor records a strip.
export function useSummary(key, view, has) {
  const expected = key && load(key, 0);
  watch(
    () => (view.loaded && !view.errored ? Number(!!has()) : null),
    (seen) => {
      if (seen !== null && key) save(key, seen);
    },
    { immediate: true },
  );
  return computed(() => {
    if (!view.loaded) return expected ? 'is-pending' : 'd-none';
    return has() ? '' : 'd-none';
  });
}

// One moving clock for every countdown on screen. It ticks while any of them shows.
const clock = ref(Date.now());
let clockTimer = null;
let clockUsers = 0;

export function useTick() {
  let ticking = false;
  const start = () => {
    if (ticking) return;
    ticking = true;
    if (clockUsers++) return;
    clock.value = Date.now();
    clockTimer = setInterval(() => {
      if (!document.hidden) clock.value = Date.now();
    }, TIMING.countdownTickMs);
  };
  const stop = () => {
    if (!ticking) return;
    ticking = false;
    if (--clockUsers) return;
    clearInterval(clockTimer);
    clockTimer = null;
  };
  onMounted(start);
  onActivated(start);
  onDeactivated(stop);
  onBeforeUnmount(stop);
  return clock;
}

// Click-to-arm confirmation. fire(k) arms k and answers false. A second fire on
// k inside the window answers true. A fire on another key disarms the first.
export function useArm() {
  let timer = null;
  const a = reactive({
    key: null,
    is: (k) => a.key === k,
    fire(k) {
      clearTimeout(timer);
      if (a.key === k) {
        a.key = null;
        return true;
      }
      a.key = k;
      timer = setTimeout(() => {
        a.key = null;
      }, TIMING.armWindowMs);
      return false;
    },
    clear() {
      clearTimeout(timer);
      a.key = null;
    },
  });
  onBeforeUnmount(a.clear);
  return a;
}

const KINDS = Symbol('kinds');
const BULK = 'bulk';

// The registry fixes a queue's kinds, so its tabs share one fetch. The next caller retries a failure.
export function provideKinds(queue) {
  const list = ref(/** @type {string[]} */ ([]));
  let req = null;
  const load = () =>
    (req ??= api.kinds(queue).then(
      (k) => {
        list.value = k;
      },
      () => {
        req = null;
      },
    ));
  provide(KINDS, { list, load });
}

export function useKinds() {
  const kinds = inject(KINDS);
  kinds.load();
  return kinds;
}

// Per-key single flight. With failText a failure becomes a toast.
export function useBusy() {
  const b = reactive({
    keys: new Set(),
    has: (k) => b.keys.has(k),
    async run(k, fn, failText) {
      if (b.keys.has(k)) return;
      b.keys.add(k);
      try {
        await fn();
      } catch (e) {
        if (!failText) throw e;
        toast(`${failText}: ${e.message}`);
      } finally {
        b.keys.delete(k);
      }
    },
  });
  return b;
}

// Fetch the rows' ancestors missing from byId, one level at a time. byId is keyed by job id.
// A failed fetch throws, since a partial chain would split a tree across requests. A gone ancestor ends its chain.
export async function fillAncestors(byId, rows, parentOf, fetch) {
  const missing = (list) => [
    ...new Set(
      list
        .map((r) => r && parentOf(r))
        .filter((up) => up != null && !byId.has(String(up)))
        .map(String),
    ),
  ];
  for (let ups = missing(rows); ups.length;) {
    const results = await mapLimit(ups, TIMING.bulkConcurrency, fetch);
    const lost = results.find((r) => r.status === 'rejected' && r.reason?.status !== 404);
    if (lost) throw lost.reason;
    const found = [];
    results.forEach((r, i) => {
      if (r.status === 'rejected' || !r.value) return;
      byId.set(ups[i], r.value);
      found.push(r.value);
    });
    ups = missing(found);
  }
}

// The row's ancestors in byId, nearest first. byId is keyed by job id.
export function* ancestors(byId, row, parentOf) {
  for (let up = row && byId.get(String(parentOf(row))); up; up = byId.get(String(parentOf(up)))) yield up;
}

// A form's submit, one at a time. A failure's message stays on the form.
/** @param {{ saving: boolean, error: string }} form @param {() => Promise<void>} fn */
export async function submitForm(form, fn, message = (e) => e.message) {
  if (form.saving) return;
  form.error = '';
  form.saving = true;
  try {
    await fn();
  } catch (e) {
    form.error = message(e);
  } finally {
    form.saving = false;
  }
}

// A view's load lifecycle. See the opts type for each callback.
/**
 * @param {{ noun: string, load: (stale: () => boolean) => Promise<void>, empty?: () => boolean,
 *   key?: string, mode?: string, events?: (batch: any[]) => number }} opts
 * empty() says if a failure takes the view or stays a toast. events(batch) counts unreloaded stream events.
 */
export function useView({ noun, load: body, empty = () => true, key, mode = '5s', events }) {
  let seq = 0;
  let inFlight = 0;
  let release = null;
  let slowTimer = null;
  let spinStart = 0;
  let spinTimer = null;
  let pollTimer = null;
  let queued = false;
  let dead = false;
  let revisit = false;

  const v = reactive({
    noun,
    loading: false,
    loaded: false,
    slow: false,
    errored: false,
    error: '',
    spinning: false,
    active: false,
    pending: 0,
    mode: storedMode(key, mode),
    // A revisited view's rows predate the hide, so its failed reload takes the view.
    fresh: false,
    get failed() {
      return v.errored && (empty() || !v.fresh);
    },
    get ready() {
      return !v.failed && (v.loaded || v.slow);
    },
    reload,
    refresh,
    setMode(m) {
      v.mode = m;
      if (key) save(key, m);
      schedule();
    },
  });

  // Placeholders would sit beside a revisited view's old rows, so a revisit shows only the loader.
  function startFirstLoad() {
    release ??= claimLoader();
    if (!revisit)
      slowTimer ??= setTimeout(() => {
        v.slow = !v.loaded;
      }, TIMING.loaderDelayMs);
  }

  function endFirstLoad() {
    clearTimeout(slowTimer);
    slowTimer = null;
    release?.();
    release = null;
  }

  // The Refresh spinner covers polls only, held to whole turns so it never snaps back.
  function spin(on) {
    clearTimeout(spinTimer);
    if (on) {
      spinStart = Date.now();
      v.spinning = true;
    } else if (v.spinning) {
      const elapsed = Date.now() - spinStart;
      const turn = TIMING.spinPeriodMs;
      spinTimer = setTimeout(
        () => {
          v.spinning = false;
        },
        Math.max(1, Math.ceil(elapsed / turn)) * turn - elapsed,
      );
    }
  }

  async function reload() {
    if (dead || !v.active) return;
    if (v.loaded) spin(true);
    else startFirstLoad();
    const mine = ++seq;
    const pendingAtStart = v.pending;
    const stale = () => mine !== seq;
    inFlight++;
    v.loading = true;
    try {
      await body(stale);
      if (stale()) return;
      v.errored = false;
      v.fresh = true;
      v.pending = Math.max(0, v.pending - pendingAtStart);
    } catch (e) {
      if (stale()) return;
      console.error(`Could not load ${noun}:`, e);
      const first = !v.errored;
      v.errored = true;
      v.error = e.message;
      if (first && !v.failed) toast(`Could not load ${noun}: ${e.message}`);
    } finally {
      inFlight--;
      if (!stale() || !inFlight) {
        v.loading = false;
        spin(false);
      }
      if (!stale()) {
        v.slow = false;
        v.loaded = true;
        endFirstLoad();
      }
    }
  }

  // Coalesces the reloads one tick asks for.
  function refresh() {
    if (queued) return;
    queued = true;
    queueMicrotask(() => {
      queued = false;
      reload();
    });
  }

  function schedule() {
    clearInterval(pollTimer);
    pollTimer = null;
    const ms = TIMING.refreshModes[v.mode];
    if (v.active && ms)
      pollTimer = setInterval(() => {
        if (!document.hidden && !v.loading) reload();
      }, ms);
  }

  function show() {
    if (v.active) return;
    v.active = true;
    refresh();
    schedule();
  }

  // An outstanding load no longer lands, so its loader claim goes too. A
  // kept-alive view comes back as a first load, so it never shows old rows.
  function hide() {
    if (!v.active) return;
    v.active = false;
    seq++;
    endFirstLoad();
    revisit = true;
    Object.assign(v, { loaded: false, slow: false, errored: false, fresh: false });
    schedule();
  }

  const onVisible = () => {
    if (!document.hidden && v.active && !v.loading && TIMING.refreshModes[v.mode]) refresh();
  };
  useBus('reconnect', () => {
    if (v.active) refresh();
  });
  if (events)
    useBus('sse', (batch) => {
      v.pending += events(batch);
    });
  document.addEventListener('visibilitychange', onVisible);
  onMounted(show);
  onActivated(show);
  onDeactivated(hide);
  onBeforeUnmount(() => {
    hide();
    dead = true;
    clearTimeout(spinTimer);
    document.removeEventListener('visibilitychange', onVisible);
  });
  return v;
}

function storedMode(key, fallback) {
  const saved = key && load(key, null);
  return saved === 'paused' || TIMING.refreshModes[saved] ? saved : fallback;
}

// Every filter a table can offer, keyed by its URL and API param.
const FILTERS = {
  group_key: { label: 'Group' },
  parent_id: { label: 'Parent ID', int: true },
  // Job ID locates one row, so it does not combine with the others.
  job_id: { label: 'Job ID', int: true, exclusive: true },
  claimed_by: { label: 'Worker', uuid: true, format: shortId },
  kind: { label: 'Kind', options: true },
  payload: { label: 'Payload' },
  error: { label: 'Error' },
  rate_limit_prefix: { label: 'Rate limit' },
  concurrency_prefix: { label: 'Concurrency' },
  completed_after: { label: 'Completed after', time: true, format: formatTime },
  completed_before: { label: 'Completed before', time: true, format: formatTime },
};

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const POSITIVE_INT_PATTERN = /^[1-9]\d*$/;
const MAX_INT64 = 2n ** 63n - 1n;
/** @typedef {Query<'/api/v1/queue/jobs'>['sort_dir']} SortDir */
/** @type {SortDir[]} */
const SORT_DIRS = ['asc', 'desc'];

// A typed or linked time becomes the instant the server reads.
const filterValue = (f, v) => (f.time ? toIsoInstant(v) || v : v);

// Why the server would refuse a filter value, or '' when it takes it.
function filterError(f, v) {
  if (f.int && !(POSITIVE_INT_PATTERN.test(v) && BigInt(v) <= MAX_INT64)) return f.label + ' must be a positive integer';
  if (f.uuid && !UUID_PATTERN.test(v)) return f.label + ' must be a UUID';
  if (f.time && Number.isNaN(Date.parse(v))) return f.label + ' must be a date and time';
  return '';
}

// A server-paged table, mirrored into the URL while its tab is the route's.
/**
 * @param {{ view: ReturnType<typeof useView>, tab: string, filters: string[], sorts: string[],
 *   extra?: Record<string, string[]>, pageKey: string, ids: () => any[], onReset?: () => void }} opts
 * extra maps a non-chip param to its allowed values. ids() lists the row ids on screen.
 */
export function useTable({ view, tab, filters, sorts, extra = {}, pageKey, ids, onReset }) {
  const owned = [...filters, ...Object.keys(extra), 'sort_by', 'sort_dir'];
  const savedLimit = load(pageKey, 0);
  const bulkBusy = useBusy();
  const t = reactive({
    fields: filters.map((param) => ({ param, ...FILTERS[param] })),
    q: Object.fromEntries([...filters, ...Object.keys(extra)].map((k) => [k, ''])),
    sort: { by: '', dir: /** @type {SortDir | ''} */ (''), toggle: toggleSort },
    draft: { param: filters[0], value: '' },
    kinds: filters.includes('kind') ? useKinds().list : [],
    limit: TIMING.pageSizes.includes(savedLimit) ? savedLimit : TIMING.pageLimit,
    offset: 0,
    total: 0,
    sel: new Set(),
    get busy() {
      return bulkBusy.has(BULK);
    },

    get page() {
      return Math.floor(t.offset / t.limit) + 1;
    },
    get pages() {
      return Math.max(1, Math.ceil(t.total / t.limit));
    },
    get field() {
      return t.fields.find((f) => f.param === t.draft.param) || t.fields[0];
    },
    get chips() {
      return t.fields
        .filter((f) => t.q[f.param])
        .map((f) => ({
          param: f.param,
          label: f.label,
          value: f.format ? f.format(t.q[f.param]) : t.q[f.param],
        }));
    },
    get filtered() {
      return Object.values(t.q).some(Boolean);
    },
    get allSelected() {
      const all = ids();
      return all.length > 0 && all.every((id) => t.sel.has(id));
    },

    // The API params for a load.
    params: () => ({ ...urlParams(), limit: t.limit, offset: t.offset }),

    add() {
      const f = t.field;
      const raw = t.draft.value.trim();
      if (!raw) return;
      const v = filterValue(f, raw);
      const err = filterError(f, v);
      if (err) return toast(err, 'warning');
      t.draft.value = '';
      t.set(f.param, v);
    },
    // Apply one filter and clear the rest, for links that jump to a narrowed list.
    only(param, value) {
      blank();
      t.q[param] = String(value);
      reset();
    },
    // An exclusive param clears every other. Any other clears the exclusive ones.
    set(param, value) {
      if (value) clearOthers(param);
      t.q[param] = value;
      reset();
    },
    clear() {
      blank();
      reset();
    },
    reset,

    setLimit(n) {
      if (!TIMING.pageSizes.includes(n) || n === t.limit) return;
      t.offset = Math.floor(t.offset / n) * n;
      t.limit = n;
      save(pageKey, n);
      view.refresh();
    },
    goTo(page) {
      const n = parseInt(page, 10);
      if (!Number.isFinite(n)) return;
      const offset = (Math.max(1, Math.min(t.pages, n)) - 1) * t.limit;
      if (offset === t.offset) return;
      t.offset = offset;
      view.refresh();
    },

    toggle(id) {
      if (t.sel.has(id)) t.sel.delete(id);
      else t.sel.add(id);
    },
    toggleAll() {
      t.sel = new Set(t.allSelected ? [] : ids());
    },

    // Drop selected ids no longer on screen.
    prune() {
      const present = new Set(ids().map(String));
      for (const id of t.sel) if (!present.has(String(id))) t.sel.delete(id);
    },

    // Record a landed page: prune the selection, mirror the URL, and step back
    // from a page a delete emptied.
    landed(total) {
      t.total = total;
      t.prune();
      syncUrl();
      if (t.offset > 0 && t.offset >= total) {
        t.offset = (t.pages - 1) * t.limit;
        view.refresh();
      }
    },

    // One bulk action at a time over the table's selection. fn(moved) tells if the reader left the queue.
    bulk: (fn, failText) => {
      const queue = route.queue;
      return bulkBusy.run(BULK, () => fn(() => route.queue !== queue), failText);
    },

    // Run fn over the selection. Failures stay selected. prepare(all) runs first, under the same lock.
    // It can give via(id), the selected id whose request also covers id. Each request goes once, and the ids it covers share its outcome.
    async each(fn, { done, one, many, prepare }) {
      if (!t.sel.size) return;
      await t.bulk(async (moved) => {
        const all = [...t.sel];
        const via = (await prepare?.(all)) ?? ((id) => id);
        const sent = new Map();
        const once = (id) => {
          const key = via(id);
          if (!sent.has(key)) sent.set(key, fn(key));
          return sent.get(key);
        };
        const results = await mapLimit(all, TIMING.bulkConcurrency, once);
        if (moved()) return;
        const failed = all.filter((_, i) => results[i].status === 'rejected');
        t.sel = new Set(failed);
        await view.reload();
        if (failed.length) toast(`${failed.length} of ${all.length} ${pluralize(all.length, one, many)} failed`);
        else toast(`${done} ${all.length} ${pluralize(all.length, one, many)}`, 'success');
      }, 'Failed');
    },
  });

  function toggleSort(col) {
    if (t.sort.by !== col) Object.assign(t.sort, { by: col, dir: 'desc' });
    else if (t.sort.dir === 'desc') t.sort.dir = 'asc';
    else Object.assign(t.sort, { by: '', dir: '' });
    t.offset = 0;
    view.refresh();
  }

  function reset() {
    t.offset = 0;
    onReset?.();
    view.refresh();
  }

  // A value the server would refuse is dropped, so a bad link still opens the list.
  function readUrl() {
    const p = params();
    for (const f of t.fields) {
      const v = filterValue(f, p.get(f.param) ?? '');
      t.q[f.param] = v && !filterError(f, v) ? v : '';
    }
    for (const [k, allowed] of Object.entries(extra)) {
      const v = p.get(k) ?? '';
      t.q[k] = allowed.includes(v) ? v : '';
    }
    const only = t.fields.find((f) => FILTERS[f.param]?.exclusive && t.q[f.param]);
    if (only) clearOthers(only.param);
    const by = p.get('sort_by') ?? '';
    t.sort.by = sorts.includes(by) ? by : '';
    const dir = /** @type {SortDir} */ (p.get('sort_dir')?.toLowerCase());
    // The server sorts descending when a column comes without a direction.
    t.sort.dir = !t.sort.by ? '' : SORT_DIRS.includes(dir) ? dir : 'desc';
  }

  function blank() {
    for (const k of Object.keys(t.q)) t.q[k] = '';
  }

  function clearOthers(param) {
    const exclusive = FILTERS[param]?.exclusive;
    for (const k of Object.keys(t.q)) if (k !== param && (exclusive || FILTERS[k]?.exclusive)) t.q[k] = '';
  }

  function urlParams() {
    return { ...t.q, sort_by: t.sort.by, sort_dir: t.sort.dir };
  }

  // A load that lands after the reader moved on must not rewrite their URL.
  function syncUrl() {
    if (route.tab === tab) replaceParams(owned, urlParams());
  }

  if (route.tab === tab) readUrl();
  onActivated(syncUrl);
  watch(
    () => route.nav,
    () => {
      if (route.tab !== tab) return;
      const before = JSON.stringify(urlParams());
      readUrl();
      // A history step back to this tab's own filters keeps its page and selection.
      if (route.popped && JSON.stringify(urlParams()) === before) return;
      reset();
    },
  );
  return t;
}

// Persisted column visibility. autoHide hides an unfilled column. narrow: false hides it on a phone.
const autoMemo = {};

export function useColumns(defs, key) {
  const saved = load(key, {}) || {};
  const vis = reactive(Object.fromEntries(defs.filter((d) => d.key in saved).map((d) => [d.key, saved[d.key] !== false])));
  // The last queue's measure stands until this one's first, so columns hold still.
  const auto = reactive({ ...autoMemo[key] });
  const seen = {};
  const isOn = (d) => {
    if (d.narrow === false && app.narrow) return false;
    if (d.required) return true;
    return d.key in vis ? vis[d.key] : !auto[d.key];
  };
  return reactive({
    shown: computed(() => defs.filter(isOn)),
    on: computed(() => Object.fromEntries(defs.map((d) => [d.key, isOn(d)]))),
    menu: computed(() => defs.filter((d) => !d.required && !(d.narrow === false && app.narrow))),
    toggle(k) {
      vis[k] = !isOn(defs.find((d) => d.key === k));
      save(key, vis);
    },
    reset() {
      for (const k of Object.keys(vis)) delete vis[k];
      forget(key);
    },
    // empty maps an autoHide key to whether no row fills it. A column that has
    // carried data stays for the queue, so polls do not resize the table.
    measure(empty) {
      for (const d of defs) {
        if (!d.autoHide || empty[d.key] === undefined) continue;
        if (!empty[d.key]) seen[d.key] = true;
        auto[d.key] = empty[d.key] && !seen[d.key];
      }
      autoMemo[key] = { ...auto };
    },
  });
}

// Client-side sort. text lists the columns that open ascending. ties breaks equal rows.
export function useSort(rows, keys, by = '', ties = [], text = []) {
  const cmp = (x, y) => {
    if (x == null && y == null) return 0;
    if (x == null) return 1;
    if (y == null) return -1;
    return typeof x === 'string' ? x.localeCompare(y) : x - y;
  };
  const s = reactive({
    by,
    dir: 'asc',
    set(k) {
      s.by = k;
      s.dir = text.includes(k) ? 'asc' : 'desc';
    },
    toggle(k) {
      if (!keys[k]) return;
      if (s.by === k) s.dir = s.dir === 'asc' ? 'desc' : 'asc';
      else s.set(k);
    },
    // A sorted copy, so the server's order is still underneath.
    rows: computed(() => {
      const read = keys[s.by];
      if (!read) return rows();
      const dir = s.dir === 'asc' ? 1 : -1;
      const reads = ties.map((k) => keys[k]);
      return rows()
        .slice()
        .sort((a, b) => cmp(read(a), read(b)) * dir || reads.reduce((c, t) => c || cmp(t(a), t(b)), 0));
    }),
  });
  return s;
}

// The drawer's selection and prev/next stepping over the rows on screen.
/**
 * @param {{ rows: () => any[], id: (row: any) => any, show?: (row: any) => void, held?: () => boolean, reset?: () => void }} opts
 * show(row) fetches a row's detail. held() keeps the open row during an unsaved edit. reset() clears per-row state.
 */
export function useDetail({ rows, id, show, held, reset }) {
  let epoch = 0;
  const hasId = (row, rid) => String(id(row)) === String(rid);
  const same = (a, b) => a != null && b != null && hasId(a, id(b));
  const index = computed(() => {
    const at = d.asked ?? d.cur;
    return at ? rows().findIndex((r) => same(r, at)) : -1;
  });
  const d = reactive({
    cur: null,
    // The row that a fetch loads. Steps count from it, not from the shown row.
    asked: null,
    open: false,
    pinned: null,
    view(row) {
      if (d.held()) {
        if (!same(row, d.cur)) toast('Save or cancel the edit first', 'warning');
        return false;
      }
      reset?.();
      if (show) {
        d.asked = row;
        d.pinned = null;
        show(row);
      } else {
        d.cur = row;
        d.open = true;
      }
      return true;
    },
    held: () => d.open && !!held?.(),
    close() {
      epoch++;
      d.asked = null;
      d.open = false;
    },
    // A fetch keeps its result only while no later fetch or close has happened.
    ticket() {
      const mine = ++epoch;
      return () => mine === epoch;
    },
    is: (row) => d.open && same(row, d.cur),
    neighbour(delta) {
      if (d.held()) return null;
      if (d.pinned) return (delta < 0 ? d.pinned.prev : d.pinned.next) || null;
      const i = index.value;
      return i < 0 ? null : rows()[i + delta] || null;
    },
    step(delta) {
      const n = d.neighbour(delta);
      if (n) d.view(n);
    },
    // Pinned neighbours mean the row is gone, so there is no position to report.
    get position() {
      const i = d.pinned ? -1 : index.value;
      return i < 0 ? '' : `${i + 1} of ${rows().length}`;
    },
    pin(aroundId) {
      const all = rows();
      const i = all.findIndex((r) => hasId(r, aroundId));
      if (i >= 0) d.pinned = { prev: all[i - 1] || null, next: all[i + 1] || null };
      else if (seen && String(seen.id) === String(aroundId)) d.pinned = { prev: seen.prev, next: seen.next };
    },
    // Re-point the selection at the fresh row, so the drawer and list agree.
    resync(onMissing) {
      // A fetch for another row is on its way, and its result replaces this one.
      if (!d.cur || (d.asked && !same(d.asked, d.cur))) return;
      const fresh = rows().find((r) => same(r, d.cur));
      if (fresh) d.cur = fresh;
      else if (d.open) onMissing?.();
    },
    closeIf(rowId) {
      if (d.cur && hasId(d.cur, rowId)) d.close();
    },
  });
  // The neighbours last seen around the open row, for a reload that drops it.
  let seen = null;
  watch(
    () => {
      const i = index.value;
      const all = rows();
      return i < 0 ? null : { id: id(all[i]), prev: all[i - 1] || null, next: all[i + 1] || null };
    },
    (around) => {
      if (around) seen = around;
    },
  );
  if (reset)
    watch(
      () => d.open,
      (open) => {
        if (!open) reset();
      },
    );
  onDeactivated(d.close);
  return d;
}
