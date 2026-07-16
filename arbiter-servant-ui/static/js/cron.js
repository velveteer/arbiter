/**
 * Alpine component: cron schedule table with a modal override editor.
 *
 * Used in two places: the per-queue Cron tab (scoped to the selected queue)
 * and the global Cron view (all queues, mounted via cronTab({ global: true })).
 * Only polls while its view is active and the browser tab is visible.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('cronTab', (opts = {}) => ({
    ...pollingTab('loadSchedules', ARB_TIMING.cronPollMs),
    ...confirmArm(),
    global: !!opts.global,
    schedules: [],
    loading: false,
    loaded: false,
    active: false,
    _loadErrored: false,
    tzList: [],
    toggleConfirm: { name: '', text: '' },
    edit: {
      prefix: '', name: '', queueName: '',
      exprOn: false, expr: '',
      overlapOn: false, overlap: 'SkipOverlap',
      tzOn: false, tz: '',
      defaultExpression: '', defaultOverlap: '', defaultTimezone: '',
      saving: false, error: '',
    },

    init() {
      // Global mode is a top-level view mounted by x-if. The per-queue tab is
      // a Bootstrap tab scoped to the selected queue.
      if (this.global) {
        this.initPollingMounted();
      } else {
        this.initPolling('#tab-cron', {
          onQueueChange: () => { this.schedules = []; },
        });
      }
      this.populateTimezones();
    },

    destroy() {
      this.teardownPolling();
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
      try {
        return cronstrue.toString(this.effectiveExpression(s));
      } catch {
        return '';
      }
    },

    isOverridden(s, field) {
      if (field === 'expression') return s.overrideExpression !== null;
      if (field === 'overlap') return s.overrideOverlap !== null;
      if (field === 'timezone') return s.overrideTimezone !== null;
      return false;
    },

    // Open the override editor for a schedule.
    openEdit(s) {
      this.edit = {
        prefix: s.name,
        name: s.name,
        queueName: s.queueName,
        exprOn: s.overrideExpression !== null,
        expr: s.overrideExpression ?? '',
        overlapOn: s.overrideOverlap !== null,
        overlap: s.overrideOverlap ?? s.defaultOverlap,
        tzOn: s.overrideTimezone !== null,
        tz: s.overrideTimezone ?? (s.defaultTimezone || 'UTC'),
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
      try {
        return cronstrue.toString(expr);
      } catch {
        return '';
      }
    },

    async saveEdit() {
      await saveOverrides(this.edit, {
        apiFn: (name, body) => ArbiterAPI.updateCronSchedule(name, body),
        modalId: 'cronEditModal',
        reload: () => this.loadSchedules(),
        buildBody: (e) => {
          // Each field is sent as a value (override on) or null (revert to default).
          if (e.exprOn && !e.expr.trim()) return { error: 'Expression cannot be empty' };
          if (e.tzOn && !e.tz.trim()) return { error: 'Timezone cannot be empty' };
          return {
            body: {
              overrideExpression: e.exprOn ? e.expr.trim() : null,
              overrideOverlap: e.overlapOn ? e.overlap : null,
              overrideTimezone: e.tzOn ? e.tz.trim() : null,
            },
          };
        },
      });
    },

    // 'type' | 'off' — how disabling a schedule is confirmed.
    get cronConfirmMode() {
      return (typeof ARB_CONFIG !== 'undefined' && ARB_CONFIG.cronConfirm) || 'type';
    },
    // The modal's confirm button unlocks only on an exact match of the schedule name.
    get toggleConfirmValid() {
      return this.toggleConfirm.text === this.toggleConfirm.name;
    },

    // Checkbox change handler. Enabling applies immediately. Disabling is guarded
    // like pausing a queue: revert the switch and open the confirm modal.
    onToggleEnabled(schedule, ev) {
      if (this.isBusy(schedule.name)) {
        if (ev) ev.target.checked = schedule.enabled;
        return;
      }
      const target = !schedule.enabled;
      if (target || this.cronConfirmMode === 'off') {
        this._applyEnabled(schedule, target);
        return;
      }
      if (ev) ev.target.checked = schedule.enabled;
      this.toggleConfirm = { name: schedule.name, text: '' };
      showModal('cronToggleModal');
    },

    // The modal's confirm button.
    confirmToggleEnabled() {
      if (!this.toggleConfirmValid || this.isBusy(this.toggleConfirm.name)) return;
      hideModal('cronToggleModal');
      const schedule = this.schedules.find((s) => s.name === this.toggleConfirm.name);
      if (schedule) this._applyEnabled(schedule, false);
    },

    async _applyEnabled(schedule, target) {
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

    // Drill from the global overview's Queue column into that queue.
    openQueue(queue) {
      this.$store.app.openQueue(queue);
    },
  }));
});
