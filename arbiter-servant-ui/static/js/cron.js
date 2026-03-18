/**
 * Alpine component: cron schedule table + inline edit + toggle
 *
 * Only polls while the Cron tab is active and the browser tab is visible.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('cronTab', () => ({
    schedules: [],
    loading: false,
    editingName: null,
    editingField: null,
    editValue: '',
    saveError: '',
    actionError: '',
    refreshInterval: null,
    active: false,

    init() {
      trackTabActive(this, '#tab-cron', {
        onShow: () => {
          this.loadSchedules();
          this.startPolling();
        },
        onHide: () => {
          this.stopPolling();
        },
      });

      document.addEventListener('visibilitychange', () => {
        if (document.hidden) {
          this.stopPolling();
        } else if (this.active) {
          this.loadSchedules();
          this.startPolling();
        }
      });
    },

    destroy() {
      this.stopPolling();
    },

    startPolling() {
      this.stopPolling();
      this.refreshInterval = setInterval(() => this.loadSchedules(), 60000);
    },

    stopPolling() {
      if (this.refreshInterval) {
        clearInterval(this.refreshInterval);
        this.refreshInterval = null;
      }
    },

    async loadSchedules() {
      this.loading = true;
      try {
        const data = await ArbiterAPI.listCronSchedules();
        this.schedules = data.cronSchedules || [];
      } catch (e) {
        console.error('Failed to load cron schedules:', e);
      } finally {
        this.loading = false;
      }
    },

    effectiveExpression(s) {
      return s.overrideExpression || s.defaultExpression;
    },

    describeExpression(s) {
      try {
        return cronstrue.toString(this.effectiveExpression(s));
      } catch {
        return '';
      }
    },

    effectiveOverlap(s) {
      return s.overrideOverlap || s.defaultOverlap;
    },

    isOverridden(s, field) {
      if (field === 'expression') return s.overrideExpression !== null;
      if (field === 'overlap') return s.overrideOverlap !== null;
      return false;
    },

    startEdit(name, field, currentValue) {
      this.editingName = name;
      this.editingField = field;
      this.editValue = currentValue;
      this.saveError = '';
      this.$nextTick(() => {
        const input = document.getElementById('inline-edit-input');
        if (input) input.focus();
      });
    },

    cancelEdit() {
      this.editingName = null;
      this.editingField = null;
      this.editValue = '';
      this.saveError = '';
    },

    async saveEdit() {
      const body = {};
      if (this.editingField === 'expression') {
        body.overrideExpression = this.editValue || null;
      } else if (this.editingField === 'overlap') {
        body.overrideOverlap = this.editValue || null;
      }

      try {
        await ArbiterAPI.updateCronSchedule(this.editingName, body);
        this.cancelEdit();
        this.loadSchedules();
      } catch (e) {
        this.saveError = e.message;
      }
    },

    async resetToDefault(name, field) {
      this.actionError = '';
      const body = {};
      if (field === 'expression') {
        body.overrideExpression = null;
      } else if (field === 'overlap') {
        body.overrideOverlap = null;
      }

      try {
        await ArbiterAPI.updateCronSchedule(name, body);
        this.loadSchedules();
      } catch (e) {
        this.actionError = 'Failed to reset: ' + e.message;
      }
    },

    async toggleEnabled(schedule) {
      this.actionError = '';
      try {
        await ArbiterAPI.updateCronSchedule(schedule.name, {
          enabled: !schedule.enabled,
        });
        this.loadSchedules();
      } catch (e) {
        this.actionError = 'Failed to toggle: ' + e.message;
      }
    },
  }));
});
