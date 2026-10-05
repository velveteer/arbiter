// The address bar is the route. ?view= names a system view, ?queue= a queue,
// the hash its sub-tab, and the other params belong to the view on screen.
import { reactive } from '../vendor/vue.esm-browser.prod.js';

// Each system view and queue tab by its route key, with its label.
export const SYSTEM_VIEWS = { ratelimits: 'Rate Limits', concurrency: 'Concurrency', cron: 'Cron', workers: 'Workers', events: 'Events' };
export const QUEUE_TABS = { stats: 'Stats', jobs: 'Jobs', groups: 'Groups', dlq: 'DLQ', archive: 'Archive', cron: 'Cron', workers: 'Workers' };
export const TAB_KEYS = Object.keys(QUEUE_TABS);

// nav counts navigations: a view adopts the URL's params when it moves.
// popped marks a navigation made by a history step.
export const route = reactive({ view: 'queues', queue: '', tab: TAB_KEYS[0], nav: 0, popped: false });

// A guard answers true to keep the reader on the page, for an unsaved edit.
const guards = new Set();
const held = () => [...guards].some((g) => g());
// The position of the entry on screen. A refused history step goes back to it.
let at = history.state?.at ?? 0;

/** @param {() => boolean} guard @returns {() => void} the release */
export function guardLeave(guard) {
  guards.add(guard);
  return () => guards.delete(guard);
}

function read() {
  const p = new URLSearchParams(location.search);
  const view = p.get('view');
  const tab = location.hash.slice(1);
  route.view = view && Object.hasOwn(SYSTEM_VIEWS, view) ? view : 'queues';
  route.queue = route.view === 'queues' ? p.get('queue') || '' : '';
  route.tab = Object.hasOwn(QUEUE_TABS, tab) ? tab : TAB_KEYS[0];
  // An unknown tab falls back to the first, and the address bar says so.
  if (route.queue && tab && tab !== route.tab) history.replaceState(history.state, '', new URL('#' + route.tab, location.href));
}

// A navigation pushes, so Back walks the views visited. quiet moves the route
// and does not ask views to adopt the URL, for a tab switch that keeps their state.
export function navigate(url, { replace = false, quiet = false } = {}) {
  if (held()) return false;
  const next = new URL(url, location.href);
  if (next.href !== location.href) {
    if (replace) history.replaceState(history.state, '', next);
    else history.pushState({ at: ++at }, '', next);
  }
  read();
  if (quiet) return true;
  route.popped = false;
  route.nav++;
  return true;
}

// Rewrite the current entry's params. A filter narrows where the reader is, so it takes no history step.
export function replaceParams(owned, values) {
  const url = new URL(location.href);
  for (const k of owned) url.searchParams.delete(k);
  for (const [k, v] of Object.entries(values)) if (v) url.searchParams.set(k, String(v));
  if (url.href !== location.href) history.replaceState(history.state, '', url);
}

export const params = () => new URLSearchParams(location.search);

// A query string from the params that carry a value.
export function qs(values = {}) {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(values)) {
    if (v != null && v !== '' && v !== false) p.set(k, String(v));
  }
  const s = p.toString();
  return s ? '?' + s : '';
}

export const listUrl = () => location.pathname.replace(/\/{2,}/g, '/');
export const viewUrl = (view, extra) => qs({ view, ...extra });
export const queueUrl = (queue, tab, extra) => qs({ queue, ...extra }) + (tab ? '#' + tab : '');

// A plain left click is handled in-app. Modified and middle clicks fall through to the href.
export function plainClick(e) {
  if (e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return false;
  e.preventDefault();
  return true;
}

// Link click handler: navigate in-app on a plain click.
export function go(e, url) {
  if (plainClick(e)) navigate(url);
}

// An in-app link to a queue view, bound with v-bind.
export function queueLink(queue, tab, extra) {
  const href = queueUrl(queue, tab, extra);
  return { href, onClick: (e) => go(e, href) };
}

// An in-app link to a policy page, bound with v-bind.
export function policyLink(view, prefix) {
  const href = viewUrl(view, { prefix });
  return { href, onClick: (e) => go(e, href) };
}

window.addEventListener('popstate', (e) => {
  // An entry with no position is new, from an edited address or a plain hash link.
  const to = e.state?.at ?? at + 1;
  if (to === at) return;
  if (held()) return history.go(at - to);
  at = to;
  history.replaceState({ at }, '');
  read();
  route.popped = true;
  route.nav++;
});
history.replaceState({ at }, '');
read();
