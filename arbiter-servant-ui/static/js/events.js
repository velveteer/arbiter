/**
 * Alpine component: SSE event log + filtering
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('eventsTab', () => ({
    filterQueue: '',
    filterTypes: {
      job_inserted: true,
      job_updated: true,
      job_deleted: true,
      job_dlq: true,
    },

    // The tab that lists an event's job: the DLQ for a DLQ-table event, else Jobs.
    // A deleted row is no longer where the event saw it, so it gets none.
    eventTab(evt) {
      if (!evt.table || !evt.job_id || evt.event === 'job_deleted') return '';
      return evt.dlq ? 'dlq' : 'jobs';
    },

    eventUrl(evt) {
      return queueJobUrl(evt.table, evt.job_id, this.eventTab(evt));
    },

    navToJob(ev, evt) {
      if (!plainNavClick(ev)) return;
      Alpine.store('app').openQueueJob(evt.table, evt.job_id, this.eventTab(evt));
    },

    get events() {
      return Alpine.store('app').events;
    },

    // A tail of the stream, newest first. Filters narrow it, nothing reorders it.
    get filteredEvents() {
      return this.events.filter((e) => {
        if (this.filterQueue && e.table !== this.filterQueue) return false;
        if (!this.filterTypes[e.event]) return false;
        return true;
      });
    },

    // The same short wording the filter chips use, which also fits the column.
    badgeLabel(eventType) {
      switch (eventType) {
        case 'job_inserted': return 'Inserted';
        case 'job_updated': return 'Updated';
        case 'job_deleted': return 'Deleted';
        case 'job_dlq': return 'DLQ';
        default: return eventType;
      }
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
