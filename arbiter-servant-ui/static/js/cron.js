/**
 * Alpine component: cron schedule table + inline edit + toggle
 *
 * Only polls while the Cron tab is active and the browser tab is visible.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('cronTab', () => ({
    ...pollingTab('loadSchedules', ARB_TIMING.cronPollMs),
    schedules: [],
    loading: false,
    loaded: false,
    editingName: null,
    editingField: null,
    editValue: '',
    saveError: '',
    active: false,
    _loadErrored: false,
    tzList: [],
    tzHighlight: null,
    tzPos: { top: 0, left: 0 },

    init() {
      this.initPolling('#tab-cron', {
        onHide: () => this.cancelEdit(),
        onQueueChange: () => { this.cancelEdit(); this.schedules = []; },
      });
      this.populateTimezones();
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
      let match = this.tzHighlight;
      if (!match && this.editValue.trim()) match = this.tzFiltered()[0];
      if (match) this.editValue = match;
      this.saveEdit();
    },

    updateTzPos() {
      const input = document.getElementById('inline-edit-input');
      if (!input) return;
      const r = input.getBoundingClientRect();
      this.tzPos = { top: r.bottom + 4, left: r.left };
    },

    destroy() {
      // Tears down the timezone picker's window scroll/resize listeners, which only
      // cancelEdit removes; a view/queue change skips onHide, so destroy must do it.
      this.cancelEdit();
      this.teardownPolling();
    },

    async loadSchedules() {
      if (this.editingName) return;
      const queue = this.$store.app.selectedQueue;
      if (!queue) {
        this.schedules = [];
        return;
      }
      await guardedLoad(this, 'Failed to load cron schedules', async (seq, isStale) => {
        const data = await ArbiterAPI.listCronSchedules({ queue });
        if (isStale()) return;
        this.schedules = data.cronSchedules || [];
      });
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
      if (this._tzReposition) {
        window.removeEventListener('scroll', this._tzReposition, true);
        window.removeEventListener('resize', this._tzReposition);
        this._tzReposition = null;
      }
      this.editingName = name;
      this.editingField = field;
      this.editValue = currentValue;
      this.saveError = '';
      this.tzHighlight = null;
      if (field === 'timezone') {
        this._tzReposition = () => this.updateTzPos();
        window.addEventListener('scroll', this._tzReposition, true);
        window.addEventListener('resize', this._tzReposition);
      }
      this.$nextTick(() => {
        const input = document.getElementById('inline-edit-input');
        if (input) input.focus();
        if (field === 'timezone') this.updateTzPos();
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
      const field = this.editingField;
      if (!name || this.busyRows[name]) return;
      const body = {};
      if (field === 'expression') {
        body.overrideExpression = this.editValue || null;
      } else if (field === 'overlap') {
        body.overrideOverlap = this.editValue || null;
      } else if (field === 'timezone') {
        body.overrideTimezone = this.editValue ? this.editValue.trim() : null;
      }

      await this.withBusyRow(name, async () => {
        try {
          await ArbiterAPI.updateCronSchedule(name, body);
          this.cancelEdit();
          await this.loadSchedules();
        } catch (e) {
          this.saveError = e.message;
        }
      });
    },

    async resetToDefault(name, field) {
      const body = {};
      if (field === 'expression') {
        body.overrideExpression = null;
      } else if (field === 'overlap') {
        body.overrideOverlap = null;
      } else if (field === 'timezone') {
        body.overrideTimezone = null;
      }

      await this.withBusyRow(name, async () => {
        try {
          await ArbiterAPI.updateCronSchedule(name, body);
          await this.loadSchedules();
        } catch (e) {
          showToast('Failed to reset: ' + e.message);
        }
      });
    },

    async toggleEnabled(schedule) {
      await this.withBusyRow(schedule.name, async () => {
        try {
          await ArbiterAPI.updateCronSchedule(schedule.name, {
            enabled: !schedule.enabled,
          });
          await this.loadSchedules();
        } catch (e) {
          showToast('Failed to toggle: ' + e.message);
        }
      });
    },
  }));
});
