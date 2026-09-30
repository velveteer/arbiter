// Rate-limit and concurrency policies. Both list policies and drill into one
// prefix's keys in a drawer that also holds the override editor.
import { ref, shallowRef, reactive, computed, watch, provide, inject } from '../../vendor/vue.esm-browser.prod.js';
import { api } from '../api.js';
import { TIMING } from '../config.js';
import { EMPTY, PERCENT, formatCompact, mapLimit, parseOverride, pct, pluralize } from '../format.js';
import { route, navigate, params, queueUrl, replaceParams } from '../router.js';
import { toast } from '../store.js';
import { submitForm, useArm, useBusy, useDetail, useSort, useSummary, useView } from '../use.js';

// Keys a drawer lists. The policy row carries the full count.
const ITEM_LIMIT = 100;

// The shared lifecycle. It polls the policies and keeps the key list of an open
// drawer fresh without a spinner. It closes a drawer whose policy vanished. It
// copies the open prefix into ?prefix=, so the drawer is a link.
function usePolicies({ noun, key, summaryKey, fetch, fetchItems, itemsField, countField, sortKeys }) {
  const policies = shallowRef(/** @type {any[]} */ ([]));
  const items = shallowRef(/** @type {any[]} */ ([]));
  const itemsLoading = ref(false);
  const editing = ref(false);
  let want = params().get('prefix');

  const view = useView({
    noun,
    key,
    mode: '30s',
    empty: () => policies.value.length === 0,
    async load(stale) {
      if (d.open) loadItems(d.cur.prefix, true);
      const data = await fetch();
      if (stale()) return;
      policies.value = data.policies;
      if (d.cur && !policies.value.some((p) => p.prefix === d.cur.prefix)) d.close();
      else d.resync();
      openWanted();
    },
  });
  const sort = useSort(() => policies.value, sortKeys, 'prefix', ['prefix'], ['prefix']);
  const d = useDetail({
    rows: () => sort.rows,
    id: (p) => p.prefix,
    held: () => editing.value,
    reset: () => {
      editing.value = false;
    },
    show(p) {
      const fresh = !(d.open && d.cur?.prefix === p.prefix);
      d.cur = p;
      d.open = true;
      // Drop the last prefix's rows so they never render under the new heading.
      if (fresh) {
        items.value = [];
        loadItems(p.prefix);
      }
    },
  });

  // A poll leaves a visible load to finish.
  async function loadItems(prefix, silent = false) {
    if (silent && itemsLoading.value) return;
    const live = d.ticket();
    if (!silent) itemsLoading.value = true;
    let data;
    let err;
    try {
      data = await fetchItems(prefix, { limit: ITEM_LIMIT });
    } catch (e) {
      err = e;
    }
    // A superseded fetch, or one for a closed drawer, owns nothing.
    if (!live()) return;
    itemsLoading.value = false;
    // A failed poll keeps the list it has.
    if (err && silent) return;
    if (err) toast(`Could not load keys: ${err.message}`);
    items.value = err ? [] : data[itemsField];
  }

  // A wanted prefix that names no policy leaves the URL.
  function openWanted() {
    if (!want) return;
    const p = policies.value.find((x) => x.prefix === want);
    want = null;
    if (!p) {
      d.close();
      return replaceParams(['prefix'], { prefix: '' });
    }
    if (!(d.open && d.cur?.prefix === p.prefix) && !d.view(p)) replaceParams(['prefix'], { prefix: d.cur.prefix });
  }

  watch(
    () => route.nav,
    () => {
      want = params().get('prefix');
      if (!want && d.held()) {
        toast('Save or cancel the edit first', 'warning');
        replaceParams(['prefix'], { prefix: d.cur.prefix });
      } else if (!want) d.close();
      if (view.loaded) openWanted();
    },
  );
  watch(
    () => (d.open ? d.cur?.prefix : ''),
    (prefix) => replaceParams(['prefix'], { prefix }),
  );

  const total = computed(() => (d.cur ? d.cur[countField] : items.value.length));
  const ctx = reactive({
    policies,
    items,
    itemsLoading,
    editing,
    view,
    sort,
    d,
    arm: useArm(),
    busy: useBusy(),
    summary: useSummary(summaryKey, view, () => policies.value.length > 0),
    // Placeholders for the rows on the way, so the list opens at the height it keeps.
    expected: computed(() => Math.max(1, Math.min(total.value, ITEM_LIMIT))),
    itemsLabel: computed(() => `Keys (${total.value > ITEM_LIMIT ? ITEM_LIMIT + ' of ' : ''}${formatCompact(total.value)})`),
    toggle: (p) => (d.is(p) && !editing.value ? d.close() : d.view(p)),
    // Run a maintenance call for its count, then reload.
    maintain: (k, call, report, what = k) =>
      ctx.busy.run(
        k,
        async () => {
          const res = await call();
          toast(report(res), 'success');
          await view.reload();
        },
        `Failed to ${what}`,
      ),
    save: (apiFn, body) =>
      submitForm(ctx.edit, async () => {
        await apiFn(ctx.edit.prefix, body);
        editing.value = false;
        await view.reload();
      }),
    edit: /** @type {{ saving: boolean, error: string, [k: string]: any }} */ ({ saving: false, error: '' }),
    // The drawer is the editor's home, so open it on this policy first.
    openEdit(p, values) {
      if (!d.view(p)) return;
      ctx.edit = { prefix: p.prefix, saving: false, error: '', ...values };
      editing.value = true;
    },
  });
  return ctx;
}

// ---- Rate limits ----

// A policy's settings in force: the override, else the default.
const effMax = (p) => p.overrideMaxTokens ?? p.defaultMaxTokens;
const effRefill = (p) => p.overrideRefillAmount ?? p.defaultRefillAmount;
const effInterval = (p) => p.overrideInterval ?? p.defaultInterval;
// Average remaining tokens as a fraction of max. Unknown without a reading or a max.
const fillRatio = (p) => (p.avgTokens == null || !effMax(p) ? null : p.avgTokens / effMax(p));

const RL_SORT_KEYS = {
  prefix: (p) => p.prefix,
  rate: (p) => effRefill(p) / effInterval(p),
  burst: effMax,
  keys: (p) => p.bucketCount,
  throttled: (p) => p.throttledCount,
  fill: (p) => fillRatio(p) ?? -1,
};

const RL_COLS = [
  { key: 'prefix', label: 'Prefix', weight: 22, sort: 'prefix' },
  { key: 'rate', label: 'Rate', weight: 12, sort: 'rate' },
  { key: 'burst', label: 'Burst', weight: 8, sort: 'burst' },
  { key: 'keys', label: 'Keys', weight: 8, sort: 'keys' },
  { key: 'throttled', label: 'Throttled', weight: 10, sort: 'throttled' },
  { key: 'fill', label: 'Avg fill', weight: 16, sort: 'fill' },
  { key: 'actions', label: 'Actions', weight: 24, cls: 'cell-actions' },
];

const RL_KEY_COLS = [
  { key: 'key', label: 'Key', weight: 40 },
  { key: 'tokens', label: 'Tokens', weight: 22 },
  { key: 'fill', label: 'Fill', weight: 28 },
  { key: 'actions', label: 'Actions', weight: 10 },
];

/** @type {{ key: string, label: string, override: string, def: string, ok: (n: number) => boolean, error: string }[]} */
const RL_FIELDS = [
  {
    key: 'max',
    label: 'Max tokens (burst)',
    override: 'overrideMaxTokens',
    def: 'defaultMaxTokens',
    ok: (n) => n >= 0,
    error: 'Max tokens must be a number >= 0',
  },
  {
    key: 'refill',
    label: 'Refill amount',
    override: 'overrideRefillAmount',
    def: 'defaultRefillAmount',
    ok: (n) => n >= 0,
    error: 'Refill amount must be a number >= 0',
  },
  {
    key: 'interval',
    label: 'Interval (seconds)',
    override: 'overrideInterval',
    def: 'defaultInterval',
    ok: (n) => n > 0,
    error: 'Interval must be a number > 0',
  },
];

// A fractional reading keeps two decimal places.
const DECIMAL_SCALE = 100;

const fmtNum = (n) => (n == null ? EMPTY : Number.isInteger(n) ? String(n) : String(Math.round(n * DECIMAL_SCALE) / DECIMAL_SCALE));
// Average remaining tokens across the prefix's buckets, as a percent of max.
const avgFill = (p) => pct(fillRatio(p));

const RlMenu = {
  props: { p: Object, close: Function },
  setup: () => inject('policies'),
  template: /* html */ `
    <button v-if="!P.editing" type="button" class="dropdown-item" @click="openRlEdit(p); close()">Edit</button>
    <arm-button :arm="P.arm" :k="'reset:' + p.prefix" cls="dropdown-item" on="fw-semibold" label="Reset" confirm="Confirm reset" @fire="reset(p); close()"/>`,
};

// A policy's throttled count, linked to the jobs it throttles.
const RlThrottled = {
  props: { p: Object },
  setup: () => inject('policies'),
  template: /* html */ `
    <a v-if="p.throttledCount > 0" href="#" class="badge bg-warning-subtle text-warning-emphasis text-decoration-none" @click.prevent="openThrottled(p)"
      title="Show the jobs this policy is throttling">{{ p.throttledCount }}</a>
    <span v-else class="text-muted">0</span>`,
};

// A policy's average bucket fill, or none without buckets.
const RlFill = {
  props: { p: Object },
  setup: () => inject('policies'),
  template: /* html */ `
    <fill-bar v-if="p.bucketCount > 0" :pct="avgFill(p)" :cls="lowFillClass(avgFill(p))" :title="fillTitle(p)"/>
    <span v-else class="text-muted">{{ EMPTY }}</span>`,
};

export const RateLimitsView = {
  components: { RlMenu, RlThrottled, RlFill },
  setup() {
    const P = usePolicies({
      noun: 'rate limits',
      key: 'arb.rateLimitRefresh',
      summaryKey: 'arb.summary.ratelimits',
      fetch: api.rateLimits,
      fetchItems: api.buckets,
      itemsField: 'buckets',
      countField: 'bucketCount',
      sortKeys: RL_SORT_KEYS,
    });
    const grant = reactive({ key: null, tokens: '' });
    const cancelGrant = () => Object.assign(grant, { key: null, tokens: '' });
    watch(() => (P.d.open ? P.d.cur?.prefix : ''), cancelGrant);

    const summary = computed(() =>
      P.policies.reduce(
        (acc, p) => {
          acc.keys += p.bucketCount;
          acc.throttled += p.throttledCount;
          if (p.bucketCount > 0) acc.lowest = Math.min(acc.lowest, avgFill(p));
          return acc;
        },
        { keys: 0, throttled: 0, lowest: Infinity },
      ),
    );

    // A policy carries no queue. The overview names the queues that hold throttled work.
    // A queue's throttled count covers every policy, so each of two or more is asked.
    const openThrottled = (p) =>
      P.busy.run(
        'throttled:' + p.prefix,
        async () => {
          const nav = route.nav;
          const holding = (await api.allStats()).queues.filter((q) => q.stats.throttledJobs > 0).map((q) => q.queue);
          const counts =
            holding.length > 1
              ? await mapLimit(holding, TIMING.bulkConcurrency, async (q) => [
                  q,
                  (await api.jobs(q, { limit: 1, status: 'throttled', rate_limit_prefix: p.prefix })).jobsTotal,
                ])
              : [];
          if (route.nav !== nav) return;
          const hot = holding.length > 1 ? counts.filter((c) => c.status === 'fulfilled' && c.value[1] > 0).map((c) => c.value[0]) : holding;
          const failed = counts.find((c) => c.status === 'rejected');
          if (!hot.length && failed) throw failed.reason;
          if (!hot.length) return toast('No queue is holding jobs this policy throttled', 'info');
          navigate(queueUrl(hot[0], 'jobs', { status: 'throttled', rate_limit_prefix: p.prefix }));
          if (hot.length > 1) toast(`${hot.length} queues hold jobs this policy throttled. Showing ${hot[0]}.`, 'info');
        },
        'Failed to find throttled jobs',
      );

    function submitGrant() {
      const tokens = Number(grant.tokens);
      if (!grant.key || !P.d.cur) return;
      if (!(tokens > 0)) return toast('Tokens must be a number more than 0', 'warning');
      const { key } = grant;
      return P.busy.run(
        'grant:' + key,
        async () => {
          const { woken } = await api.addTokens(P.d.cur.prefix, key, tokens);
          toast(`Added ${fmtNum(tokens)} tokens to ${key}. ${woken} ${pluralize(woken, 'job')} woken.`, 'success');
          if (grant.key === key) cancelGrant();
          await P.view.reload();
        },
        `Failed to add tokens to ${key}`,
      );
    }

    const ctx = {
      P,
      grant,
      summary,
      RL_COLS,
      RL_KEY_COLS,
      RL_FIELDS,
      effMax,
      fmtNum,
      avgFill,
      openThrottled,
      cancelGrant,
      submitGrant,
      rate: (p) => (effRefill(p) ? `${fmtNum(effRefill(p))} / ${fmtNum(effInterval(p))}s` : 'manual'),
      overridden: (p) => RL_FIELDS.some((f) => p[f.override] != null),
      fillTitle: (p) => `min ${fmtNum(p.minTokens)}, avg ${fmtNum(p.avgTokens)} of ${fmtNum(effMax(p))} tokens`,
      // The grant form opens with what refills the bucket to its burst.
      openGrant: (b) => Object.assign(grant, { key: b.key, tokens: String(Math.ceil(Math.max(0, (b.maxTokens ?? 0) - (b.tokens ?? 0))) || 1) }),
      openRlEdit: (p) =>
        P.openEdit(
          p,
          Object.fromEntries(
            RL_FIELDS.flatMap(({ key: k, override: o, def }) => [
              [k + 'On', p[o] != null],
              [k, p[o] ?? ''],
              [k + 'Default', p[def]],
              [k + 'Was', p[o] ?? null],
            ]),
          ),
        ),
      saveRl() {
        const e = P.edit;
        const body = {};
        for (const { key: k, override, ok, error } of RL_FIELDS) {
          const r = parseOverride(e[k + 'On'], e[k], (n) => Number.isFinite(n) && ok(n));
          if (r.error) return (e.error = error);
          if (r.value !== e[k + 'Was']) body[override] = r.value;
        }
        P.save(api.updateRateLimit, body);
      },
      prune: () => P.maintain('prune', api.pruneBuckets, (r) => `Pruned ${r.pruned} idle ${pluralize(r.pruned, 'bucket')}`),
      reset: (p) =>
        P.maintain(
          'reset:' + p.prefix,
          () => api.resetBuckets(p.prefix),
          (r) => `Reset ${r.reset} ${pluralize(r.reset, 'bucket')} for ${p.prefix}`,
          'reset ' + p.prefix,
        ),
    };
    provide('policies', ctx);
    return ctx;
  },
  template: /* html */ `
    <load-error :view="P.view"/>
    <div class="queue-summary" :class="P.summary">
      <qs :v="P.policies.length" :l="pluralize(P.policies.length, 'policy', 'policies')"/>
      <qs :v="formatCompact(summary.keys)" :l="pluralize(summary.keys, 'key tracked', 'keys tracked')"/>
      <qs :v="formatCompact(summary.throttled)" l="throttled" :c="{ warn: summary.throttled > 0 }"/>
      <qs :v="Number.isFinite(summary.lowest) ? summary.lowest + '%' : EMPTY" l="lowest fill"/>
    </div>
    <div class="toolbar" v-show="P.view.ready">
      <refresh-control :view="P.view"/>
      <arm-button :arm="P.arm" k="prune" on="btn-warning" off="btn-outline-secondary" :disabled="P.busy.has('prune')"
        title="Delete full buckets that had no use for the server's idle time" label="Prune idle buckets" confirm="Confirm prune" @fire="prune"/>
    </div>
    <div v-scroll-edges class="table-responsive" :aria-busy="P.view.loading" v-show="P.view.ready">
      <table class="table table-hover table-sm sticky-head table-fixed">
        <t-head :cols="RL_COLS" :sort="P.sort"/>
        <tbody>
          <tr v-for="p in P.sort.rows" :key="p.prefix" class="detail-row" tabindex="0" :class="{ 'policy-warn': p.throttledCount > 0, 'table-active': P.d.is(p) }"
            @click="isRowClick($event) && P.d.view(p)" @keydown.enter.self.prevent="P.toggle(p)" @keydown.space.self.prevent="P.toggle(p)">
            <td class="text-break">{{ p.prefix }} <span v-if="overridden(p)" class="badge bg-info-subtle text-info-emphasis ms-1">override</span></td>
            <td>{{ rate(p) }}</td>
            <td>{{ fmtNum(effMax(p)) }}</td>
            <td>{{ p.bucketCount }}</td>
            <td><rl-throttled :p="p"/></td>
            <td><rl-fill :p="p"/></td>
            <td class="cell-actions"><action-menu row v-slot="{ close }"><rl-menu :p="p" :close="close"/></action-menu></td>
          </tr>
        </tbody>
        <skeleton-rows :view="P.view" :span="RL_COLS.length" :empty="!P.policies.length">No rate-limit policies declared.</skeleton-rows>
      </table>
    </div>

    <drawer :d="P.d" :title="P.d.cur?.prefix || 'Policy'" :status="P.d.cur?.throttledCount > 0 ? 'throttling' : ''"
      status-class="bg-warning-subtle text-warning-emphasis" :sticky="P.editing">
      <template #actions>
        <action-menu v-if="P.d.cur" v-slot="{ close }"><rl-menu :p="P.d.cur" :close="close"/></action-menu>
      </template>
      <div v-if="P.editing" class="offcanvas-body drawer-edit arb-edit" tabindex="0">
        <div v-for="{ key: k, label } in RL_FIELDS" :key="k" class="edit-field">
          <div class="form-check form-switch">
            <input class="form-check-input" type="checkbox" :id="'rl-' + k" v-model="P.edit[k + 'On']">
            <label class="form-check-label" :for="'rl-' + k">{{ label }}</label>
          </div>
          <input type="number" min="0" step="any" class="form-control form-control-sm" v-model="P.edit[k]" :disabled="!P.edit[k + 'On']"
            @keydown.enter.prevent="saveRl" :placeholder="'default ' + P.edit[k + 'Default']">
        </div>
        <p class="edit-note">Overrides take effect on the next consume. Uncheck a field to revert it to the declared default.</p>
        <edit-actions :form="P.edit" @cancel="P.editing = false" @save="saveRl"/>
      </div>
      <div v-else-if="P.d.cur" class="offcanvas-body" tabindex="0">
        <dl class="row">
          <kv l="Rate">{{ rate(P.d.cur) }}</kv>
          <kv l="Burst">{{ fmtNum(effMax(P.d.cur)) }}</kv>
          <kv l="Keys tracked">{{ formatCompact(P.d.cur.bucketCount) }}</kv>
          <kv l="Throttled"><rl-throttled :p="P.d.cur"/></kv>
          <kv l="Avg fill"><rl-fill :p="P.d.cur"/></kv>
          <kv :l="P.itemsLabel" wide>
            <div v-scroll-edges class="table-responsive" :aria-busy="P.itemsLoading">
              <table class="table table-sm drill-table table-fixed">
                <t-head :cols="RL_KEY_COLS"><template #actions><span class="visually-hidden">Actions</span></template></t-head>
                <tbody>
                  <tr v-for="b in P.items" :key="b.key" :class="{ 'is-granting': grant.key === b.key }">
                    <td class="text-break"><code>{{ b.key }}</code></td>
                    <td v-if="grant.key === b.key" colspan="3">
                      <form class="grant-form" @submit.prevent="submitGrant">
                        <input type="number" min="0" step="any" class="form-control form-control-sm" v-model="grant.tokens" aria-label="Tokens to add"
                          v-select-on-mount @keydown.escape.prevent="cancelGrant">
                        <button type="submit" class="btn btn-primary btn-sm" :disabled="P.busy.has('grant:' + grant.key)">Add tokens</button>
                        <button type="button" class="grant-cancel" @click="cancelGrant" title="Cancel" aria-label="Cancel">&#x2715;</button>
                      </form>
                    </td>
                    <template v-else>
                      <td>{{ fmtNum(b.tokens) }} / {{ fmtNum(b.maxTokens) }}</td>
                      <td><fill-bar thin :pct="pct(b.fillFraction)" :cls="lowFillClass(pct(b.fillFraction))"/></td>
                      <td class="text-end"><button type="button" class="grant-btn" @click="openGrant(b)" title="Add tokens" aria-label="Add tokens">+</button></td>
                    </template>
                  </tr>
                  <tr v-for="i in (P.itemsLoading ? P.expected : 0)" :key="'sk-' + i"><td colspan="4" class="drill-loading text-muted text-center"><span class="skeleton-bar"></span></td></tr>
                  <tr v-if="!P.itemsLoading && !P.items.length"><td colspan="4" class="text-muted text-center">No active buckets for this prefix.</td></tr>
                </tbody>
              </table>
            </div>
          </kv>
        </dl>
      </div>
    </drawer>`,
};

// ---- Concurrency ----

const effLimit = (p) => p.overrideLimit ?? p.defaultLimit;
const isIdle = (p) => !p.maxInFlight;
// The busiest key's slots in use as a fraction of the effective limit. Unknown without both.
const busiestRatio = (p) => {
  const lim = effLimit(p);
  if (lim === 0) return p.maxInFlight > 0 ? 1 : 0;
  return lim == null || p.maxInFlight == null ? null : p.maxInFlight / lim;
};
const busiest = (p) => pct(busiestRatio(p));
const saturated = (p) => p.keyCount > 0 && busiest(p) >= PERCENT;

const CC_SORT_KEYS = {
  prefix: (p) => p.prefix,
  limit: effLimit,
  keys: (p) => p.keyCount,
  inFlight: (p) => p.totalInFlight,
  busiest: (p) => busiestRatio(p) ?? -1,
};

const CC_COLS = [
  { key: 'prefix', label: 'Prefix', weight: 40, sort: 'prefix' },
  { key: 'limit', label: 'Limit', weight: 12, sort: 'limit' },
  { key: 'keys', label: 'Keys', weight: 10, sort: 'keys' },
  { key: 'inFlight', label: 'In flight', weight: 12, sort: 'inFlight' },
  { key: 'busiest', label: 'Busiest key', weight: 26, sort: 'busiest' },
];

const CC_KEY_COLS = [
  { key: 'key', label: 'Key', weight: 46 },
  { key: 'inFlight', label: 'In flight', weight: 22 },
  { key: 'fill', label: 'Fill', weight: 32 },
];

// A pool's busiest key: a bar, or why there is none.
const Busiest = {
  props: { p: Object },
  setup: () => ({ busiest, effLimit, isIdle }),
  template: /* html */ `
    <fill-bar v-if="p.keyCount > 0 && !isIdle(p)" :pct="busiest(p)" :cls="highFillClass(busiest(p))" wide
      :label="(p.maxInFlight ?? 0) + '/' + effLimit(p)" :title="'busiest key: ' + (p.maxInFlight ?? EMPTY) + ' of ' + effLimit(p) + ' in flight'"/>
    <span v-else-if="p.keyCount > 0" class="text-muted small" :title="'No key is holding a slot of ' + effLimit(p)">idle</span>
    <span v-else class="text-muted">no keys</span>`,
};

export const ConcurrencyView = {
  components: { Busiest },
  setup() {
    const P = usePolicies({
      noun: 'concurrency pools',
      key: 'arb.concurrencyRefresh',
      summaryKey: 'arb.summary.concurrency',
      fetch: api.concurrency,
      fetchItems: api.concurrencyKeys,
      itemsField: 'keys',
      countField: 'keyCount',
      sortKeys: CC_SORT_KEYS,
    });
    const summary = computed(() =>
      P.policies.reduce(
        (acc, p) => {
          acc.keys += p.keyCount;
          acc.inFlight += p.totalInFlight;
          acc.saturated += saturated(p) ? 1 : 0;
          return acc;
        },
        { keys: 0, inFlight: 0, saturated: 0 },
      ),
    );
    return {
      P,
      summary,
      CC_COLS,
      CC_KEY_COLS,
      effLimit,
      saturated,
      openEdit: (p) => P.openEdit(p, { limitOn: p.overrideLimit != null, limit: p.overrideLimit ?? '', defaultLimit: p.defaultLimit }),
      saveEdit() {
        const limit = parseOverride(P.edit.limitOn, P.edit.limit, (n) => Number.isInteger(n) && n >= 0);
        if (limit.error) return (P.edit.error = 'Limit must be a whole number >= 0');
        P.save(api.updateConcurrency, { overrideLimit: limit.value });
      },
      reconcile: () => P.maintain('reconcile', api.reconcileConcurrency, (r) => `Reconciled ${r.reconciled} key ${pluralize(r.reconciled, 'count')}`),
      prune: () => P.maintain('prune', api.pruneConcurrencyKeys, (r) => `Pruned ${r.pruned} ${pluralize(r.pruned, 'key')}`),
    };
  },
  template: /* html */ `
    <load-error :view="P.view"/>
    <div class="queue-summary" :class="P.summary">
      <qs :v="P.policies.length" :l="pluralize(P.policies.length, 'pool')"/>
      <qs :v="formatCompact(summary.keys)" :l="pluralize(summary.keys, 'key tracked', 'keys tracked')"/>
      <qs :v="formatCompact(summary.inFlight)" l="in flight"/>
      <qs :v="summary.saturated" :l="pluralize(summary.saturated, 'pool at limit', 'pools at limit')" :c="{ warn: summary.saturated > 0 }"/>
    </div>
    <div class="toolbar" v-show="P.view.ready">
      <refresh-control :view="P.view"/>
      <arm-button :arm="P.arm" k="reconcile" on="btn-warning" off="btn-outline-secondary" :disabled="P.busy.has('reconcile')"
        label="Reconcile counts" confirm="Confirm reconcile" @fire="reconcile"/>
      <arm-button :arm="P.arm" k="prune" on="btn-warning" off="btn-outline-secondary" :disabled="P.busy.has('prune')"
        title="Delete keys that hold no slot and have no live job" label="Prune keys" confirm="Confirm prune" @fire="prune"/>
    </div>
    <div v-scroll-edges class="table-responsive" :aria-busy="P.view.loading" v-show="P.view.ready">
      <table class="table table-hover table-sm sticky-head table-fixed">
        <t-head :cols="CC_COLS" :sort="P.sort"/>
        <tbody>
          <tr v-for="p in P.sort.rows" :key="p.prefix" class="detail-row" tabindex="0" :class="{ 'policy-bad': saturated(p), 'table-active': P.d.is(p) }"
            @click="isRowClick($event) && P.d.view(p)" @keydown.enter.self.prevent="P.toggle(p)" @keydown.space.self.prevent="P.toggle(p)">
            <td class="text-break">{{ p.prefix }} <span v-if="p.overrideLimit != null" class="badge bg-info-subtle text-info-emphasis ms-1">override</span></td>
            <td>{{ effLimit(p) }}</td>
            <td>{{ p.keyCount }}</td>
            <td>{{ p.totalInFlight }}</td>
            <td><busiest :p="p"/></td>
          </tr>
        </tbody>
        <skeleton-rows :view="P.view" :span="CC_COLS.length" :empty="!P.policies.length">No concurrency pools declared.</skeleton-rows>
      </table>
    </div>

    <drawer :d="P.d" :title="P.d.cur?.prefix || 'Pool'" :status="P.d.cur && saturated(P.d.cur) ? 'at limit' : ''"
      status-class="bg-danger-subtle text-danger-emphasis" :sticky="P.editing">
      <template #actions>
        <action-menu v-if="P.d.cur && !P.editing" v-slot="{ close }">
          <button type="button" class="dropdown-item" @click="openEdit(P.d.cur); close()">Edit</button>
        </action-menu>
      </template>
      <div v-if="P.editing" class="offcanvas-body drawer-edit arb-edit" tabindex="0">
        <div class="edit-field">
          <div class="form-check form-switch">
            <input class="form-check-input" type="checkbox" id="ccLimitOn" v-model="P.edit.limitOn">
            <label class="form-check-label" for="ccLimitOn">Concurrent limit per key</label>
          </div>
          <input type="number" min="0" step="1" class="form-control form-control-sm" v-model="P.edit.limit" :disabled="!P.edit.limitOn"
            @keydown.enter.prevent="saveEdit" :placeholder="'default ' + P.edit.defaultLimit">
        </div>
        <p class="edit-note">A lower limit does not preempt in-flight jobs. It applies as they drain. Uncheck to revert to the declared default.</p>
        <edit-actions :form="P.edit" @cancel="P.editing = false" @save="saveEdit"/>
      </div>
      <div v-else-if="P.d.cur" class="offcanvas-body" tabindex="0">
        <dl class="row">
          <kv l="Limit per key">{{ effLimit(P.d.cur) }}</kv>
          <kv l="Keys tracked">{{ formatCompact(P.d.cur.keyCount) }}</kv>
          <kv l="In flight">{{ formatCompact(P.d.cur.totalInFlight) }}</kv>
          <kv l="Busiest key"><busiest :p="P.d.cur"/></kv>
          <kv :l="P.itemsLabel" wide>
            <div v-scroll-edges class="table-responsive" :aria-busy="P.itemsLoading">
              <table class="table table-sm drill-table table-fixed">
                <t-head :cols="CC_KEY_COLS"/>
                <tbody>
                  <tr v-for="k in P.items" :key="k.key">
                    <td class="text-break"><code>{{ k.key }}</code></td>
                    <td>{{ k.inFlight }} / {{ k.effectiveLimit }}</td>
                    <td><fill-bar thin :pct="pct(k.fillFraction)" :cls="highFillClass(pct(k.fillFraction))"/></td>
                  </tr>
                  <tr v-for="i in (P.itemsLoading ? P.expected : 0)" :key="'sk-' + i"><td colspan="3" class="drill-loading text-muted text-center"><span class="skeleton-bar"></span></td></tr>
                  <tr v-if="!P.itemsLoading && !P.items.length"><td colspan="3" class="text-muted text-center">No active keys for this prefix.</td></tr>
                </tbody>
              </table>
            </div>
          </kv>
        </dl>
      </div>
    </drawer>`,
};
