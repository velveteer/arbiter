// Timing constants, gathered so cadence is tunable in one place.
export const TIMING = {
  sseRetryMs: 3000,
  sseRetryMaxMs: 60000,
  queuesRetryMs: 3000,
  queuesRetryMaxMs: 60000,
  healthPollMs: 10000,
  fetchTimeoutMs: 30000,
  flushMs: 250,
  loaderDelayMs: 180,
  // One turn of the .spin animation in dashboard.css. Keep the two in step.
  spinPeriodMs: 800,
  armWindowMs: 5000,
  countdownTickMs: 1000,
  copiedFlashMs: 1200,
  drawerSlideMs: 300,
  bulkConcurrency: 5,
  childPageLimit: 50,
  pageLimit: 50,
  pageSizes: [25, 50, 100, 200],
  // Above this many queues the landing page opens as a list rather than cards.
  queueListThreshold: 12,
  maxEventsPerQueue: 200,
  toastMaxVisible: 5,
  toastDelays: { danger: 8000, warning: 6000, success: 4000, info: 4000 },
  refreshModes: { '1s': 1000, '5s': 5000, '10s': 10000, '30s': 30000, '1m': 60000 },
};

// How a destructive toggle is confirmed. pauseConfirm: 'type' opens a modal that
// asks for the queue name, 'arm' takes a second click, 'off' hides pause. Resume
// always takes the second click. cronConfirm: 'type' or 'off', for disabling only.
export const CONFIG = {
  pauseConfirm: 'type',
  cronConfirm: 'type',
};

// Below this width a table shows only its identifying columns. Matches dashboard.css.
export const NARROW_MQ = '(max-width: 640px)';

// Below this width a drawer covers the list, so it goes full width with a backdrop.
export const DRAWER_MODAL_MQ = '(max-width: 1200px)';
