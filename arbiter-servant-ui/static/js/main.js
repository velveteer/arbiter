// The app shell: navbar, the view the route names, and global registrations.
import { createApp, computed, watchEffect, ref } from '../vendor/vue.esm-browser.prod.js';
import * as format from './format.js';
import { route, navigate, go, listUrl, viewUrl, queueLink, policyLink, queueUrl, SYSTEM_VIEWS } from './router.js';
import { app, boot, toggleTheme } from './store.js';
import * as ui from './ui.js';
import { useScrollActive } from './use.js';
import { QueueList } from './views/queues.js';
import { QueueDetail } from './views/queue.js';
import { RateLimitsView, ConcurrencyView } from './views/policies.js';
import { CronView } from './views/cron.js';
import { WorkersView } from './views/workers.js';
import { EventsView } from './views/events.js';

const NAV = [['queues', 'Queues'], ...Object.entries(SYSTEM_VIEWS)];
// A server that streams no events has no events view.
const navItems = computed(() => (app.sseDisabled ? NAV.filter(([v]) => v !== 'events') : NAV));
// Resolved against this module, so it shares the module's versioned path.
const MARK = new URL('../arbiter-mark.svg', import.meta.url).href;
const SYSTEM = { ratelimits: RateLimitsView, concurrency: ConcurrencyView, cron: CronView, workers: WorkersView, events: EventsView };

const App = {
  setup() {
    const nav = ref(/** @type {HTMLElement | null} */ (null));
    watchEffect(() => {
      document.title = app.pageTitle + ' · Arbiter';
    });
    useScrollActive(nav, () => route.view);
    const open = (view) => navigate(view === 'queues' ? listUrl() : viewUrl(view));
    return { app, route, nav, navItems, MARK, SYSTEM, open, toggleTheme };
  },
  template: /* html */ `
    <div class="top-loader" v-if="app.showLoader" role="status" aria-label="Loading"></div>
    <nav class="navbar navbar-expand-lg">
      <div class="container-fluid">
        <a class="navbar-brand" :href="listUrl()" aria-label="Arbiter" title="Arbiter" @click="go($event, listUrl())">
          <img class="brand-mark" :src="MARK" alt="">
        </a>
        <nav ref="nav" v-scroll-edges class="nav-primary" aria-label="Primary">
          <button v-for="[v, label] in navItems" :key="v" type="button" class="nav-primary-link" :class="{ active: route.view === v }" @click="open(v)">{{ label }}</button>
        </nav>
        <div class="nav-controls d-flex align-items-center gap-3 ms-auto">
          <span class="status-pip" role="status" :class="app.healthState" :title="app.healthTitle" :aria-label="app.healthTitle">
            <span class="status-dot" aria-hidden="true"></span>
          </span>
          <button class="btn btn-sm nav-theme-toggle" @click="toggleTheme"
            :title="app.theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'" :aria-label="app.theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'"
            >{{ app.theme === 'dark' ? '\\u263c' : '\\u263d' }}</button>
        </div>
      </div>
    </nav>
    <main class="container-fluid mt-3">
      <h1 class="visually-hidden">{{ app.pageTitle }}</h1>
      <template v-if="route.view === 'queues'">
        <queue-detail v-if="app.queue" :key="app.queue" :queue="app.queue"/>
        <queue-list v-else-if="!route.queue || app.queuesFailed"/>
      </template>
      <div v-else :id="'view-' + route.view" class="mt-3"><component :is="SYSTEM[route.view]"/></div>
    </main>
    <toasts/>`,
};

const vue = createApp(App);
Object.assign(vue.config.globalProperties, format, {
  go,
  navigate,
  listUrl,
  queueLink,
  policyLink,
  queueUrl,
  isRowClick: ui.isRowClick,
});
for (const [name, c] of Object.entries(ui)) {
  if (typeof c === 'object' && ('template' in c || 'setup' in c)) vue.component(name, c);
}
vue.component('queue-list', QueueList);
vue.component('queue-detail', QueueDetail);
vue.directive('scroll-edges', ui.scrollEdges);
vue.directive('select-on-mount', ui.selectOnMount);
vue.mount('#app');
boot();
