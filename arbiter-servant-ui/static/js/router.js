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

function read() {
  const p = new URLSearchParams(location.search);
  const view = p.get('view');
  const tab = location.hash.slice(1);
  route.view = view && Object.hasOwn(SYSTEM_VIEWS, view) ? view : 'queues';
  route.queue = route.view === 'queues' ? p.get('queue') || '' : '';
  route.tab = Object.hasOwn(QUEUE_TABS, tab) ? tab : TAB_KEYS[0];
  // An unknown tab falls back to the first, and the address bar says so.
  if (route.queue && tab && tab !== route.tab) history.replaceState(null, '', new URL('#' + route.tab, location.href));
}

// A navigation pushes, so Back walks the views visited. quiet moves the route
// without asking views to adopt the URL, for a tab switch that keeps their state.
export function navigate(url, { replace = false, quiet = false } = {}) {
  const next = new URL(url, location.href);
  if (next.href !== location.href) history[replace ? 'replaceState' : 'pushState'](null, '', next);
  read();
  if (quiet) return;
  route.popped = false;
  route.nav++;
}

// Rewrite the current entry's params. A filter narrows where the reader is, so it takes no history step.
export function replaceParams(owned, values) {
  const url = new URL(location.href);
  for (const k of owned) url.searchParams.delete(k);
  for (const [k, v] of Object.entries(values)) if (v) url.searchParams.set(k, String(v));
  if (url.href !== location.href) history.replaceState(null, '', url);
}

export const params = () => new URLSearchParams(location.search);

// A query string from the params that carry a value.
export function qs(params = {}) {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== '' && v !== false) p.set(k, String(v));
  }
  const s = p.toString();
  return s ? '?' + s : '';
}

export const listUrl = () => location.pathname;
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

window.addEventListener('popstate', () => {
  read();
  route.popped = true;
  route.nav++;
});
read();
