/**
 * Alpine component: concurrency pools view.
 *
 * Global (not queue-scoped). Lists pools with live key and in-flight stats,
 * drills into a prefix's keys, edits the override limit, and reconciles counts
 * from live jobs. Polls while the Concurrency tab is visible.
 */
const CC_KEY_LIMIT = 100;

// Sort readers for the pool table.
const CC_SORT_KEYS = {
  prefix: (p) => p.prefix,
  limit: (p) => p.overrideLimit ?? p.defaultLimit ?? 0,
  keys: (p) => p.keyCount ?? 0,
  inFlight: (p) => p.totalInFlight ?? 0,
  busiest: (p) => {
    const lim = p.overrideLimit ?? p.defaultLimit;
    if (p.maxInFlight == null) return -1;
    return lim ? p.maxInFlight / lim : (p.maxInFlight > 0 ? 1 : 0);
  },
};

// Row actions offered by the drawer header, which binds each row as `job`.
const CC_ACTIONS_HTML = `
<li x-show="!editing"><a class="dropdown-item" href="#" @click.prevent="openEdit(job); closeDropdown($el)">Edit</a></li>`;

document.addEventListener('alpine:init', () => {
  Alpine.data('concurrencyTab', () => ({
    ...pollingTab('loadPolicies', ARB_TIMING.concurrencyPollMs, 'arb.concurrencyRefresh'),
    loadNoun: 'concurrency pools',
    ...summaryMemory('arb.summary.concurrency'),
    ...clientSort('policies', CC_SORT_KEYS, 'prefix', 'prefix'),
    ...confirmArm(),
    ...drillDownTab({
      listField: 'keys',
      loadingField: 'keysLoading',
      toggleName: 'toggleKeys',
      loadName: 'loadKeys',
      countField: 'keyCount',
      itemLimit: CC_KEY_LIMIT,
      itemLabel: 'keys',
      drawerId: 'concurrencyDrawer',
      fetchPolicies: () => ArbiterAPI.listConcurrency(),
      fetchItems: (prefix, opts) => ArbiterAPI.listConcurrencyKeys(prefix, opts),
    }),
    ...rowDetail('displayPolicies', 'prefix', 'selectedPolicy', { drawer: 'concurrencyDrawer' }),
    detailActionsHtml: CC_ACTIONS_HTML,
    edit: {
      prefix: '',
      limitOn: false, limit: '',
      defaultLimit: 0,
      saving: false, error: '',
    },

    fmtCount: formatCompact,

    // Static header, so the count is fixed.
    colCount() {
      return 5;
    },

    get detailTitle() {
      return this.selectedPolicy ? this.selectedPolicy.prefix : 'Pool';
    },

    get detailStatus() {
      return this.selectedPolicy && this.isSaturated(this.selectedPolicy) ? 'at limit' : '';
    },

    detailStatusClass() {
      return 'bg-danger-subtle text-danger-emphasis';
    },

    // The header's actions menu walks this, so the open pool is its one row.
    get detailRows() {
      const cur = this.selectedPolicy;
      return cur ? [Object.assign({}, cur, { _id: cur.prefix })] : [];
    },

    // Heading for the drawer's key list, naming what the cap is hiding.
    keysLabel() {
      const total = this.itemTotal();
      return this.hasMoreItems() ? `Keys (${this.itemCap} of ${this.fmtCount(total)})` : `Keys (${this.fmtCount(total)})`;
    },

    get displayPolicies() {
      return this.sortRows(this.policies);
    },

    // Instance-wide roll-up for the header strip.
    get summary() {
      return this.policies.reduce((acc, p) => {
        acc.keys += p.keyCount || 0;
        acc.inFlight += p.totalInFlight || 0;
        if (p.keyCount > 0 && this.busiestPct(p) >= 100) acc.saturated += 1;
        return acc;
      }, { keys: 0, inFlight: 0, saturated: 0 });
    },

    // With nothing in flight there is no busiest key to draw.
    isIdle(p) {
      return !p.maxInFlight;
    },

    isSaturated(p) {
      return p.keyCount > 0 && this.busiestPct(p) >= 100;
    },

    init() {
      this.initPollingMounted();
      this.bindDrillDrawer();
    },

    destroy() {
      this.teardownPolling();
      this.unbindDrillDrawer();
    },

    // Effective cap (override falls back to default).
    effLimit(p) { return p.overrideLimit ?? p.defaultLimit; },
    hasOverride(p) { return p.overrideLimit != null; },

    // Utilization of the busiest key, as a percent of the effective limit.
    busiestPct(p) {
      const lim = this.effLimit(p);
      if (lim === 0) return p.maxInFlight > 0 ? 100 : 0;
      if (lim == null || p.maxInFlight == null) return 0;
      return clampPct(p.maxInFlight / lim);
    },
    busiestClass(p) { return highFillClass(this.busiestPct(p)); },
    busiestLabel(p) {
      return `${p.maxInFlight ?? 0}/${this.effLimit(p)}`;
    },
    busiestTitle(p) {
      const mx = p.maxInFlight ?? EMPTY;
      return `busiest key: ${mx} of ${this.effLimit(p)} in flight`;
    },

    keyFillPct(k) { return fillPct(k.fillFraction); },
    keyFillClass(k) { return highFillClass(this.keyFillPct(k)); },

    buildEdit(p) {
      this.edit = {
        prefix: p.prefix,
        limitOn: p.overrideLimit != null,
        limit: p.overrideLimit ?? '',
        defaultLimit: p.defaultLimit,
        saving: false,
        error: '',
      };
    },

    async saveEdit() {
      await saveOverrides(this.edit, {
        apiFn: (prefix, body) => ArbiterAPI.updateConcurrencyPolicy(prefix, body),
        close: () => { this.editing = false; },
        reload: () => this.loadPolicies(),
        buildBody: (e) => {
          // The override is sent as a value (override on) or null (revert to default).
          const body = { overrideLimit: e.limitOn ? parseOverride(e.limit, Number.isInteger) : null };
          if (e.limitOn && (body.overrideLimit == null || body.overrideLimit < 0)) return { error: 'Limit must be a whole number >= 0' };
          return { body };
        },
      });
    },

    async reconcile() {
      if (this.busyRows['reconcile']) return;
      if (!this.confirmArmed('reconcile')) return;
      await this.withBusyRow('reconcile', async () => {
        try {
          const res = await ArbiterAPI.reconcileConcurrency();
          const n = res?.reconciled ?? 0;
          showToast(`Reconciled ${n} key count(s)`, 'success');
          // loadPolicies silently reloads the open prefix's keys, so no separate fetch.
          await this.loadPolicies();
        } catch (e) {
          showToast(`Failed to reconcile: ${e.message}`);
        }
      });
    },
  }));
});
