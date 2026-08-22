// Timing/config constants, gathered so cadence is tunable in one place.
const ARB_TIMING = {
  sseHandshakeMs: 3000,
  sseRetryMs: 3000,
  sseRetryMaxMs: 60000,
  pollMs: 5000,
  refreshMs: 30000,
  healthPollMs: 10000,
  fetchTimeoutMs: 30000,
  flushMs: 250,
  loaderDelayMs: 180,
  // One turn of the .spin animation in dashboard.css. Keep the two in step.
  spinPeriodMs: 800,
  armWindowMs: 5000,
  cronPollMs: 60000,
  workerPollMs: 30000,
  queueListPollMs: 10000,
  rateLimitPollMs: 30000,
  concurrencyPollMs: 30000,
  bulkConcurrency: 5,
  childPageLimit: 50,
  pageLimit: 50,
  pageSizes: [25, 50, 100, 200],
  // Above this many queues the landing page opens as a list rather than cards.
  queueListThreshold: 12,
  toastMaxVisible: 5,
  toastDelays: { danger: 8000, warning: 6000, success: 4000, info: 4000 },
  refreshModes: { '1s': 1000, '5s': 5000, '10s': 10000, '30s': 30000, '1m': 60000 },
};

// Behavioral config. pauseConfirm gates how a queue is paused (resume always uses
// the light two-click arm): 'type' — modal, must type the queue name; 'arm' — inline
// two-click; 'off' — hide the pause button entirely.
// cronConfirm gates disabling a cron schedule (enabling always applies
// immediately): 'type' — modal, must type the schedule name; 'off' — immediate.
const ARB_CONFIG = {
  pauseConfirm: 'type',
  cronConfirm: 'type',
};

// Registry of window-dispatched event-bus names.
const ARB_EVENTS = {
  queueChanged: 'queue-changed',
  sseEvent: 'sse-event',
  sseReconnect: 'sse-reconnect',
  sseRefresh: 'sse-refresh',
  pollTick: 'poll-tick',
  filterJobs: 'filter-jobs',
};
