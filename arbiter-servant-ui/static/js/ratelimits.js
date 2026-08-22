/**
 * Alpine component: rate-limit policies view.
 *
 * Global (not queue-scoped). Lists policies with bucket and throttle stats,
 * drills into a prefix's buckets, edits override params, and resets buckets.
 * Polls while the Rate Limits tab is visible.
 */
const RL_BUCKET_LIMIT = 100;

// Sort readers for the policy table.
const RL_SORT_KEYS = {
  prefix: (p) => p.prefix,
  rate: (p) => (p.overrideRefillAmount ?? p.defaultRefillAmount ?? 0) / (p.overrideInterval ?? p.defaultInterval ?? 1),
  burst: (p) => p.overrideMaxTokens ?? p.defaultMaxTokens ?? 0,
  keys: (p) => p.bucketCount ?? 0,
  throttled: (p) => p.throttledCount ?? 0,
  fill: (p) => {
    const max = p.overrideMaxTokens ?? p.defaultMaxTokens;
    return p.avgTokens == null || !max ? -1 : p.avgTokens / max;
  },
};

document.addEventListener('alpine:init', () => {
  Alpine.data('rateLimitsTab', () => ({
    ...pollingTab('loadPolicies', ARB_TIMING.rateLimitPollMs, 'arb.rateLimitRefresh'),
    ...clientSort('policies', RL_SORT_KEYS, 'prefix', 'prefix'),
    ...confirmArm(),
    ...drillDownTab({
      listField: 'buckets',
      loadingField: 'bucketsLoading',
      toggleName: 'toggleBuckets',
      loadName: 'loadBuckets',
      countField: 'bucketCount',
      itemLimit: RL_BUCKET_LIMIT,
      itemLabel: 'buckets',
      policyError: 'Failed to load rate limits',
      fetchPolicies: () => ArbiterAPI.listRateLimits(),
      fetchItems: (prefix, opts) => ArbiterAPI.listRateLimitBuckets(prefix, opts),
    }),
    edit: {
      prefix: '',
      maxOn: false, max: '',
      refillOn: false, refill: '',
      intervalOn: false, interval: '',
      defaultMax: 0, defaultRefill: 0, defaultInterval: 0,
      saving: false, error: '',
    },

    fmtCount: formatCompact,

    // Static header, so the count is fixed.
    colCount() {
      return 7;
    },

    get displayPolicies() {
      return this.sortRows(this.policies);
    },

    // Instance-wide roll-up for the header strip.
    get summary() {
      return this.policies.reduce((acc, p) => {
        acc.keys += p.bucketCount || 0;
        acc.throttled += p.throttledCount || 0;
        if (p.bucketCount > 0) acc.lowestFill = Math.min(acc.lowestFill, this.avgFillPct(p));
        return acc;
      }, { keys: 0, throttled: 0, lowestFill: Infinity });
    },

    lowestFillText() {
      const f = this.summary.lowestFill;
      return Number.isFinite(f) ? f + '%' : '\u2014';
    },

    // A policy row carries no queue, so the overview names the queues holding
    // throttled work. One goes straight to its jobs, otherwise the queue list
    // shows the throttled counts side by side.
    async openThrottled(p) {
      if (!p.throttledCount) return;
      try {
        const data = await ArbiterAPI.getAllStats();
        const hot = (data.queues || []).filter((q) => (q.stats?.throttledJobs || 0) > 0);
        if (hot.length === 1) Alpine.store('app').openQueueJobs(hot[0].queue, 'throttled');
        else Alpine.store('app').setView('queues');
      } catch (e) {
        showToast('Failed to find throttled jobs: ' + e.message);
      }
    },

    init() {
      this.initPollingMounted();
    },

    destroy() {
      this.teardownPolling();
    },

    // Effective params (override falls back to default).
    effMax(p) { return p.overrideMaxTokens ?? p.defaultMaxTokens; },
    effRefill(p) { return p.overrideRefillAmount ?? p.defaultRefillAmount; },
    effInterval(p) { return p.overrideInterval ?? p.defaultInterval; },
    hasOverride(p) {
      return p.overrideMaxTokens != null || p.overrideRefillAmount != null || p.overrideInterval != null;
    },

    rateText(p) {
      const r = this.effRefill(p);
      if (!r) return 'manual';
      return `${this.fmtNum(r)} / ${this.fmtNum(this.effInterval(p))}s`;
    },

    fmtNum(n) {
      if (n == null) return EMPTY;
      return Number.isInteger(n) ? String(n) : String(Math.round(n * 100) / 100);
    },

    // Average remaining tokens across the prefix's buckets, as a percent of max.
    avgFillPct(p) {
      const max = this.effMax(p);
      if (!max || p.avgTokens == null) return 0;
      return clampPct(p.avgTokens / max);
    },
    fillClass(p) { return lowFillClass(this.avgFillPct(p)); },
    fillTitle(p) {
      const mn = p.minTokens == null ? EMPTY : this.fmtNum(p.minTokens);
      const av = p.avgTokens == null ? EMPTY : this.fmtNum(p.avgTokens);
      return `min ${mn}, avg ${av} of ${this.fmtNum(this.effMax(p))} tokens`;
    },

    bucketFillPct(b) { return fillPct(b.fillFraction); },
    bucketFillClass(b) { return lowFillClass(this.bucketFillPct(b)); },

    buildEdit(p) {
      this.edit = {
        prefix: p.prefix,
        maxOn: p.overrideMaxTokens != null,
        max: p.overrideMaxTokens ?? '',
        refillOn: p.overrideRefillAmount != null,
        refill: p.overrideRefillAmount ?? '',
        intervalOn: p.overrideInterval != null,
        interval: p.overrideInterval ?? '',
        defaultMax: p.defaultMaxTokens,
        defaultRefill: p.defaultRefillAmount,
        defaultInterval: p.defaultInterval,
        saving: false,
        error: '',
      };
    },

    async saveEdit() {
      await saveOverrides(this.edit, {
        apiFn: (prefix, body) => ArbiterAPI.updateRateLimitPolicy(prefix, body),
        reload: () => this.loadPolicies(),
        buildBody: (e) => {
          // Each field is sent as a value (override on) or null (revert to default).
          const body = {
            overrideMaxTokens: e.maxOn ? parseOverride(e.max, Number.isFinite) : null,
            overrideRefillAmount: e.refillOn ? parseOverride(e.refill, Number.isFinite) : null,
            overrideInterval: e.intervalOn ? parseOverride(e.interval, Number.isFinite) : null,
          };
          if (e.maxOn && (body.overrideMaxTokens == null || body.overrideMaxTokens < 0)) return { error: 'Max tokens must be a number >= 0' };
          if (e.refillOn && (body.overrideRefillAmount == null || body.overrideRefillAmount < 0)) return { error: 'Refill amount must be a number >= 0' };
          if (e.intervalOn && (body.overrideInterval == null || body.overrideInterval <= 0)) return { error: 'Interval must be a number > 0' };
          return { body };
        },
      });
    },

    async resetPrefix(p) {
      if (this.busyRows[p.prefix]) return;
      if (!this.confirmArmed('reset:' + p.prefix)) return;
      await this.withBusyRow(p.prefix, async () => {
        try {
          const res = await ArbiterAPI.resetRateLimitBuckets(p.prefix);
          const n = res?.reset ?? 0;
          showToast(`Reset ${n} bucket(s) for ${p.prefix}`, 'success');
          // loadPolicies silently reloads the open prefix's buckets, so no separate fetch.
          await this.loadPolicies();
        } catch (e) {
          showToast(`Failed to reset ${p.prefix}: ${e.message}`);
        }
      });
    },
  }));
});
