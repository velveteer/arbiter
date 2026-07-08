/**
 * Alpine component: concurrency pools view.
 *
 * Global (not queue-scoped). Lists pools with live key and in-flight stats,
 * drills into a prefix's keys, edits the override limit, and reconciles counts
 * from live jobs. Polls while the Concurrency tab is visible.
 */
const CC_KEY_LIMIT = 100;

document.addEventListener('alpine:init', () => {
  Alpine.data('concurrencyTab', () => ({
    ...pollingTab('loadPolicies', ARB_TIMING.concurrencyPollMs),
    ...confirmArm(),
    ...drillDownTab({
      listField: 'keys',
      loadingField: 'keysLoading',
      toggleName: 'toggleKeys',
      loadName: 'loadKeys',
      countField: 'keyCount',
      itemLimit: CC_KEY_LIMIT,
      itemLabel: 'keys',
      policyError: 'Failed to load concurrency pools',
      fetchPolicies: () => ArbiterAPI.listConcurrency(),
      fetchItems: (prefix, opts) => ArbiterAPI.listConcurrencyKeys(prefix, opts),
    }),
    edit: {
      prefix: '',
      limitOn: false, limit: '',
      defaultLimit: 0,
      saving: false, error: '',
    },

    init() {
      this.initPollingMounted();
    },

    destroy() {
      this.teardownPolling();
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
      const mx = p.maxInFlight ?? '-';
      return `busiest key: ${mx} of ${this.effLimit(p)} in flight`;
    },

    keyFillPct(k) { return fillPct(k.fillFraction); },
    keyFillClass(k) { return highFillClass(this.keyFillPct(k)); },

    openEdit(p) {
      this.edit = {
        prefix: p.prefix,
        limitOn: p.overrideLimit != null,
        limit: p.overrideLimit ?? '',
        defaultLimit: p.defaultLimit,
        saving: false,
        error: '',
      };
      showModal('concurrencyEditModal');
    },

    async saveEdit() {
      await saveOverrides(this.edit, {
        apiFn: (prefix, body) => ArbiterAPI.updateConcurrencyPolicy(prefix, body),
        modalId: 'concurrencyEditModal',
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
