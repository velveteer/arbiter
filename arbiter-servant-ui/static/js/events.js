/**
 * Alpine component: SSE event log + filtering
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('eventsTab', () => ({
    active: false,
    filterQueue: '',
    filterTypes: {
      job_inserted: true,
      job_updated: true,
      job_deleted: true,
      job_dlq: true,
    },

    init() {
      trackTabActive(this, '#tab-events');
    },

    get events() {
      return Alpine.store('app').events;
    },

    get filteredEvents() {
      return this.events.filter((e) => {
        if (this.filterQueue && e.table !== this.filterQueue) return false;
        if (!this.filterTypes[e.event]) return false;
        return true;
      });
    },

    badgeClass(eventType) {
      switch (eventType) {
        case 'job_inserted':
          return 'bg-primary-subtle text-primary-emphasis';
        case 'job_updated':
          return 'bg-warning-subtle text-warning-emphasis';
        case 'job_deleted':
          return 'bg-danger-subtle text-danger-emphasis';
        case 'job_dlq':
          return 'bg-secondary-subtle text-secondary-emphasis';
        default:
          return 'bg-info-subtle text-info-emphasis';
      }
    },

    clearEvents() {
      Alpine.store('app').events = [];
    },
  }));
});
