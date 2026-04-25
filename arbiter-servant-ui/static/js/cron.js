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
    busyRows: {},

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

      this._visibilityHandler = () => {
        if (document.hidden) {
          this.stopPolling();
        } else if (this.active) {
          this.loadSchedules();
          this.startPolling();
        }
      };
      document.addEventListener('visibilitychange', this._visibilityHandler);
    },

    destroy() {
      this.stopPolling();
      if (this._visibilityHandler) {
        document.removeEventListener('visibilitychange', this._visibilityHandler);
        this._visibilityHandler = null;
      }
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
      this._loadSeq = (this._loadSeq || 0) + 1;
      const seq = this._loadSeq;
      try {
        const data = await ArbiterAPI.listCronSchedules();
        if (seq !== this._loadSeq) return;
        this.schedules = data.cronSchedules || [];
      } catch (e) {
        if (seq !== this._loadSeq) return;
        console.error('Failed to load cron schedules:', e);
      } finally {
        if (seq === this._loadSeq) this.loading = false;
      }
    },

    isBusy(name) {
      return !!this.busyRows[name];
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
      const name = this.editingName;
      if (this.busyRows[name]) return;
      const body = {};
      if (this.editingField === 'expression') {
        body.overrideExpression = this.editValue || null;
      } else if (this.editingField === 'overlap') {
        body.overrideOverlap = this.editValue || null;
      }

      this.busyRows = { ...this.busyRows, [name]: true };
      try {
        await ArbiterAPI.updateCronSchedule(name, body);
        this.cancelEdit();
        await this.loadSchedules();
      } catch (e) {
        this.saveError = e.message;
      } finally {
        const next = { ...this.busyRows };
        delete next[name];
        this.busyRows = next;
      }
    },

    async resetToDefault(name, field) {
      if (this.busyRows[name]) return;
      this.actionError = '';
      const body = {};
      if (field === 'expression') {
        body.overrideExpression = null;
      } else if (field === 'overlap') {
        body.overrideOverlap = null;
      }

      this.busyRows = { ...this.busyRows, [name]: true };
      try {
        await ArbiterAPI.updateCronSchedule(name, body);
        await this.loadSchedules();
      } catch (e) {
        this.actionError = 'Failed to reset: ' + e.message;
      } finally {
        const next = { ...this.busyRows };
        delete next[name];
        this.busyRows = next;
      }
    },

    async toggleEnabled(schedule) {
      const name = schedule.name;
      if (this.busyRows[name]) return;
      this.actionError = '';
      this.busyRows = { ...this.busyRows, [name]: true };
      try {
        await ArbiterAPI.updateCronSchedule(name, {
          enabled: !schedule.enabled,
        });
        await this.loadSchedules();
      } catch (e) {
        this.actionError = 'Failed to toggle: ' + e.message;
      } finally {
        const next = { ...this.busyRows };
        delete next[name];
        this.busyRows = next;
      }
    },
  }));
});
