// Process-wide runtime constants, extracted verbatim from the original
// Express monolith (lib/server/app.js lines 217-229).

const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMIT_MAX_REQUESTS = 30;
// Pinned to globalThis for the same reason as the session store: Next gives
// each server bundle its own module instance, and a counter split across
// bundles would let a caller exceed the limit once per bundle.
const RATE_LIMIT_KEY = Symbol.for("wesolvehr.rateLimit.store");
const rateLimitStore = (globalThis[RATE_LIMIT_KEY] ??= new Map());
const APP_TIMEZONE = "Asia/Kolkata";
const APP_TIMEZONE_OFFSET = "+05:30";
const DEFAULT_SHIFT_START_TEXT = "10:30 AM";
const LATE_APPROVAL_NOTICE_HOURS = 3;

// Attendance day settings
const ATTENDANCE_DAY_START_HOUR = 6; // 6:00 AM IST
const LONG_SHIFT_THRESHOLD_MIN = 10 * 60; // 10 hours
const LONG_BREAK_THRESHOLD_MIN = 2 * 60; // 2 hours
const HALF_DAY_THRESHOLD_MIN = 4 * 60; // optional future use

export {
  RATE_LIMIT_WINDOW_MS,
  RATE_LIMIT_MAX_REQUESTS,
  rateLimitStore,
  APP_TIMEZONE,
  APP_TIMEZONE_OFFSET,
  DEFAULT_SHIFT_START_TEXT,
  LATE_APPROVAL_NOTICE_HOURS,
  ATTENDANCE_DAY_START_HOUR,
  LONG_SHIFT_THRESHOLD_MIN,
  LONG_BREAK_THRESHOLD_MIN,
  HALF_DAY_THRESHOLD_MIN,
};
