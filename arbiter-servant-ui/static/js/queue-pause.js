/**
 * Alpine component: shows the selected queue's pause state and a toggle button.
 * Refreshes off the global event bus (poll-tick / sse-refresh / sse-reconnect),
 * so its cadence tracks whatever the rest of the UI is doing.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('queuePauseToggle', () => ({
    paused: false,
    pausedAt: null,
    pausedAgeStr: '',
    busy: false,
    ...confirmArm(),
    ...eventBusTab(),
    // 'type' | 'arm' | 'off' — how a pause must be confirmed (resume always uses the arm).
    ...typeToConfirm('pauseConfirm'),

    // In 'off' mode the pause affordance is hidden, but resume stays available.
    get showPauseToggle() {
      return this.paused || this.confirmMode() !== 'off';
    },

    init() {
      const refresh = () => this.refresh();
      this._bindBus({
        queueChanged: () => {
          this.disarm();
          this.resetConfirm();
          this.paused = false;
          this.pausedAt = null;
          if (Alpine.store('app').selectedQueue) this.refresh();
        },
        pollTick: refresh,
        sseRefresh: refresh,
        sseReconnect: refresh,
      });
      if (Alpine.store('app').selectedQueue) this.refresh();
    },

    destroy() {
      this._unbindBus();
    },

    async refresh() {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue) return;
      // Sequence guard: drop a stale response if a newer refresh (e.g. after a
      // queue change) started while this one was in flight.
      this._refreshSeq = (this._refreshSeq || 0) + 1;
      const seq = this._refreshSeq;
      try {
        const details = await ArbiterAPI.getQueueDetails(queue);
        if (seq !== this._refreshSeq) return;
        this.paused = details && !!details.paused;
        this.pausedAt = details && details.pausedAt || null;
      } catch (e) {
        if (seq !== this._refreshSeq) return;
        if (e.status === 404) {
          this.paused = false;
          this.pausedAt = null;
        } else {
          console.error('Failed to load queue details:', e);
        }
      }
      // Recompute every refresh so the relative age string ticks (formatAge reads
      // Date.now(), which Alpine cannot track on its own).
      this.pausedAgeStr = this.pausedAt ? formatAge(this.pausedAt) : '';
    },

    // Button click. Resume and 'arm'-mode pause use the two-click arm; 'type'-mode
    // pause opens the confirmation modal instead.
    onToggle() {
      if (this.busy) return;
      if (this.paused) {
        if (this.confirmArmed('toggle')) this._apply(false);
        return;
      }
      if (this.confirmMode() === 'type') {
        this.openConfirm(Alpine.store('app').selectedQueue);
        showModal('pauseConfirmModal');
        return;
      }
      if (this.confirmArmed('toggle')) this._apply(true);
    },

    // The modal's Pause button.
    confirmPause() {
      if (!this.confirmValid() || this.busy) return;
      hideModal('pauseConfirmModal');
      this._apply(true);
    },

    async _apply(pause) {
      const queue = Alpine.store('app').selectedQueue;
      if (!queue || this.busy) return;
      const verb = pause ? 'pause' : 'resume';
      this.busy = true;
      try {
        if (pause) {
          await ArbiterAPI.pauseQueue(queue);
        } else {
          await ArbiterAPI.resumeQueue(queue);
        }
        await this.refresh();
      } catch (e) {
        console.error(`Failed to ${verb} queue:`, e);
        showToast(`Failed to ${verb} queue: ` + e.message);
      } finally {
        this.busy = false;
      }
    },
  }));
});
