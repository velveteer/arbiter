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
    tzList: [],
    tzHighlight: null,
    tzPos: { top: 0, left: 0 },

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

      this.$watch('$store.app.selectedQueue', () => {
        this.schedules = [];
        if (this.active) this.loadSchedules();
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

      this.populateTimezones();
    },

    populateTimezones() {
      if (this.tzList.length > 0) return;
      if (typeof Intl.supportedValuesOf !== 'function') {
        this.tzList = ['UTC'];
        return;
      }
      this.tzList = Intl.supportedValuesOf('timeZone').slice().sort();
    },

    tzFiltered() {
      const q = (this.editValue || '').toLowerCase().trim();
      if (!q) return this.tzList;
      return this.tzList.filter((z) => z.toLowerCase().includes(q));
    },

    tzGrouped() {
      const groups = new Map();
      for (const z of this.tzFiltered()) {
        const slash = z.indexOf('/');
        const region = slash === -1 ? 'Other' : z.slice(0, slash);
        if (!groups.has(region)) groups.set(region, []);
        groups.get(region).push(z);
      }
      return Array.from(groups, ([name, zones]) => ({ name, zones }));
    },

    tzMove(delta) {
      const list = this.tzFiltered();
      if (list.length === 0) {
        this.tzHighlight = null;
        return;
      }
      const idx = list.indexOf(this.tzHighlight);
      let next = idx + delta;
      if (next < 0) next = list.length - 1;
      if (next >= list.length) next = 0;
      this.tzHighlight = list[next];
      this.$nextTick(() => {
        const el = document.querySelector('.tz-option.tz-highlighted');
        if (el) el.scrollIntoView({ block: 'nearest' });
      });
    },

    tzCommit() {
      if (this.tzHighlight) {
        this.editValue = this.tzHighlight;
      }
      this.saveEdit();
    },

    updateTzPos() {
      const input = document.getElementById('inline-edit-input');
      if (!input) return;
      const r = input.getBoundingClientRect();
      this.tzPos = { top: r.bottom + 4, left: r.left };
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
      const queue = this.$store.app.selectedQueue;
      if (!queue) {
        this.schedules = [];
        return;
      }
      this.loading = true;
      this._loadSeq = (this._loadSeq || 0) + 1;
      const seq = this._loadSeq;
      try {
        const data = await ArbiterAPI.listCronSchedules({ queue });
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

    effectiveTimezone(s) {
      return s.overrideTimezone || s.defaultTimezone || '';
    },

    isOverridden(s, field) {
      if (field === 'expression') return s.overrideExpression !== null;
      if (field === 'overlap') return s.overrideOverlap !== null;
      if (field === 'timezone') return s.overrideTimezone !== null;
      return false;
    },

    startEdit(name, field, currentValue) {
      this.editingName = name;
      this.editingField = field;
      this.editValue = currentValue;
      this.saveError = '';
      this.tzHighlight = null;
      this.$nextTick(() => {
        const input = document.getElementById('inline-edit-input');
        if (input) input.focus();
        if (field === 'timezone') {
          this.updateTzPos();
          this._tzReposition = () => this.updateTzPos();
          window.addEventListener('scroll', this._tzReposition, true);
          window.addEventListener('resize', this._tzReposition);
        }
      });
    },

    cancelEdit() {
      this.editingName = null;
      this.editingField = null;
      this.editValue = '';
      this.saveError = '';
      this.tzHighlight = null;
      if (this._tzReposition) {
        window.removeEventListener('scroll', this._tzReposition, true);
        window.removeEventListener('resize', this._tzReposition);
        this._tzReposition = null;
      }
    },

    async saveEdit() {
      const name = this.editingName;
      if (this.busyRows[name]) return;
      const body = {};
      if (this.editingField === 'expression') {
        body.overrideExpression = this.editValue || null;
      } else if (this.editingField === 'overlap') {
        body.overrideOverlap = this.editValue || null;
      } else if (this.editingField === 'timezone') {
        body.overrideTimezone = this.editValue ? this.editValue.trim() : null;
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
      } else if (field === 'timezone') {
        body.overrideTimezone = null;
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
