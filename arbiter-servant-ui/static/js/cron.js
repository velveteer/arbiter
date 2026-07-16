/**
 * Alpine component: cron schedule table with a modal override editor.
 *
 * Used in two places: the per-queue Cron tab (scoped to the selected queue)
 * and the global Cron view (all queues, mounted via cronTab({ global: true })).
 * Only polls while its view is active and the browser tab is visible.
 * The edit/confirm modals render once at page level, driven by the cronEdit store.
 */
// Human-readable description of a cron expression, or '' if it can't be parsed.
function safeCronDescribe(expr) {
  try {
    return cronstrue.toString(expr);
  } catch {
    return '';
  }
}

document.addEventListener('alpine:init', () => {
  Alpine.store('cronEdit', {
    host: null,
    tzList: [],
    toggleConfirm: { name: '', text: '' },
    edit: {
      prefix: '', name: '', queueName: '',
      exprOn: false, expr: '',
      overlapOn: false, overlap: 'SkipOverlap',
      tzOn: false, tz: '',
      orig: {},
      defaultExpression: '', defaultOverlap: '', defaultTimezone: '',
      saving: false, error: '',
    },

    populateTimezones() {
      if (this.tzList.length > 0) return;
      if (typeof Intl.supportedValuesOf !== 'function') {
        this.tzList = ['UTC'];
        return;
      }
      const zones = Intl.supportedValuesOf('timeZone').slice();
      if (!zones.includes('UTC')) zones.push('UTC');
      this.tzList = zones.sort();
    },

    // IANA zones grouped by region for the timezone <select> optgroups.
    tzGroups() {
      const groups = new Map();
      for (const z of this.tzList) {
        const slash = z.indexOf('/');
        const region = slash === -1 ? 'Other' : z.slice(0, slash);
        if (!groups.has(region)) groups.set(region, []);
        groups.get(region).push(z);
      }
      return Array.from(groups, ([name, zones]) => ({ name, zones }));
    },

    // 'type' | 'off' — how disabling a schedule is confirmed.
    get cronConfirmMode() {
      return (typeof ARB_CONFIG !== 'undefined' && ARB_CONFIG.cronConfirm) || 'type';
    },
    // The confirm button unlocks only on an exact match of the schedule name.
    get toggleConfirmValid() {
      return this.toggleConfirm.text === this.toggleConfirm.name;
    },

    isHostBusy(name) {
      return !!(this.host && this.host.isBusy(name));
    },

    // Open the override editor for a schedule, hosted by the active table.
    async openEdit(host, s) {
      this.host = host;
      const tz = s.overrideTimezone ?? (s.defaultTimezone || 'UTC');
      // x-model resolves against rendered options, so the zone needs one first.
      if (tz && !this.tzList.includes(tz)) {
        this.tzList = [...this.tzList, tz].sort();
        await Alpine.nextTick();
      }
      const values = {
        exprOn: s.overrideExpression !== null,
        expr: s.overrideExpression ?? '',
        overlapOn: s.overrideOverlap !== null,
        overlap: s.overrideOverlap ?? s.defaultOverlap,
        tzOn: s.overrideTimezone !== null,
        tz,
      };
      this.edit = {
        prefix: s.name,
        name: s.name,
        queueName: s.queueName,
        ...values,
        orig: values,
        defaultExpression: s.defaultExpression,
        defaultOverlap: s.defaultOverlap,
        defaultTimezone: s.defaultTimezone || 'UTC',
        saving: false,
        error: '',
      };
      showModal('cronEditModal');
    },

    // Live human-readable description of the expression being edited.
    editDescribe() {
      const expr = this.edit.exprOn ? this.edit.expr : this.edit.defaultExpression;
      return safeCronDescribe(expr);
    },

    async saveEdit() {
      const host = this.host;
      await saveOverrides(this.edit, {
        apiFn: (name, body) => ArbiterAPI.updateCronSchedule(name, body),
        modalId: 'cronEditModal',
        reload: () => host.loadSchedules(),
        buildBody: (e) => {
          if (e.exprOn && !e.expr.trim()) return { error: 'Expression cannot be empty' };
          if (e.tzOn && !e.tz.trim()) return { error: 'Timezone cannot be empty' };
          // Sent as a value (override on) or null (revert), only when changed.
          const body = {};
          const put = (key, on, value, origOn, origValue) => {
            const next = on ? value : null;
            const prev = origOn ? origValue : null;
            if (next !== prev) body[key] = next;
          };
          put('overrideExpression', e.exprOn, e.expr.trim(), e.orig.exprOn, e.orig.expr);
          put('overrideOverlap', e.overlapOn, e.overlap, e.orig.overlapOn, e.orig.overlap);
          put('overrideTimezone', e.tzOn, e.tz.trim(), e.orig.tzOn, e.orig.tz);
          return { body };
        },
      });
    },

    // Checkbox change handler. Enabling applies immediately. Disabling is guarded
    // like pausing a queue: revert the switch and open the confirm modal.
    onToggleEnabled(host, schedule, ev) {
      this.host = host;
      if (host.isBusy(schedule.name)) {
        if (ev) ev.target.checked = schedule.enabled;
        return;
      }
      const target = !schedule.enabled;
      if (target || this.cronConfirmMode === 'off') {
        host.applyEnabled(schedule, target);
        return;
      }
      if (ev) ev.target.checked = schedule.enabled;
      this.toggleConfirm = { name: schedule.name, text: '' };
      showModal('cronToggleModal');
    },

    confirmToggleEnabled() {
      const host = this.host;
      if (!this.toggleConfirmValid || host.isBusy(this.toggleConfirm.name)) return;
      hideModal('cronToggleModal');
      const schedule = host.schedules.find((s) => s.name === this.toggleConfirm.name);
      if (schedule) host.applyEnabled(schedule, false);
    },
  });

  Alpine.data('cronTab', (opts = {}) => ({
    ...pollingTab('loadSchedules', ARB_TIMING.cronPollMs),
    ...confirmArm(),
    global: !!opts.global,
    schedules: [],
    loading: false,
    loaded: false,
    active: false,
    _loadErrored: false,

    init() {
      // Global mode is a top-level view mounted by x-if. The per-queue tab is
      // a Bootstrap tab scoped to the selected queue.
      if (this.global) {
        this.initPollingMounted();
      } else {
        this.initPolling('#tab-cron', {
          onQueueChange: () => { this.disarm(); this.schedules = []; },
        });
      }
      this.$store.cronEdit.populateTimezones();
    },

    destroy() {
      this.teardownPolling();
      this.disarm();
      if (this.$store.cronEdit.host === this) this.$store.cronEdit.host = null;
    },

    async loadSchedules() {
      const queue = this.global ? undefined : this.$store.app.selectedQueue;
      if (!this.global && !queue) {
        this.schedules = [];
        return;
      }
      await guardedLoad(this, 'Failed to load cron schedules', async (seq, isStale) => {
        const data = await ArbiterAPI.listCronSchedules(this.global ? {} : { queue });
        if (isStale()) return;
        this.schedules = data.cronSchedules || [];
      });
    },

    effectiveExpression(s) {
      return s.overrideExpression || s.defaultExpression;
    },

    effectiveOverlap(s) {
      return s.overrideOverlap || s.defaultOverlap;
    },

    effectiveTimezone(s) {
      return s.overrideTimezone || s.defaultTimezone || '';
    },

    describeExpression(s) {
      return safeCronDescribe(this.effectiveExpression(s));
    },

    isOverridden(s, field) {
      if (field === 'expression') return s.overrideExpression !== null;
      if (field === 'overlap') return s.overrideOverlap !== null;
      if (field === 'timezone') return s.overrideTimezone !== null;
      return false;
    },

    openEdit(s) {
      this.$store.cronEdit.openEdit(this, s);
    },

    onToggleEnabled(schedule, ev) {
      this.$store.cronEdit.onToggleEnabled(this, schedule, ev);
    },

    async applyEnabled(schedule, target) {
      await this.withBusyRow(schedule.name, async () => {
        try {
          await ArbiterAPI.updateCronSchedule(schedule.name, { enabled: target });
        } catch (e) {
          showToast('Failed to toggle: ' + e.message);
        } finally {
          await this.loadSchedules();
        }
      });
    },

    async runNow(schedule) {
      if (!schedule.enabled || this.busyRows[schedule.name]) return;
      if (!this.confirmArmed('run:' + schedule.name)) return;
      await this.withBusyRow(schedule.name, async () => {
        try {
          await ArbiterAPI.runCronSchedule(schedule.name);
          showToast('Run requested for ' + schedule.name, 'success');
        } catch (e) {
          showToast('Failed to run: ' + e.message);
        }
      });
    },

    isRunPending(s) {
      return s.runRequestedAt != null;
    },

    // Scheduled fires stamp the minute floor, manual runs the wall clock.
    lastFired(s) {
      const manual = s.lastManualRunAt;
      if (manual && (!s.lastFiredAt || manual > s.lastFiredAt)) return { at: manual, manual: true };
      return { at: s.lastFiredAt, manual: false };
    },

    // The schedule list spans the schema, so it names queues this server lacks.
    queueServed(s) {
      return this.$store.app.queues.includes(s.queueName);
    },

    openQueue(queue) {
      if (!this.$store.app.queues.includes(queue)) {
        showToast(`Queue "${queue}" is not served by this server`, 'warning');
        return;
      }
      this.$store.app.openQueue(queue);
    },
  }));
});
