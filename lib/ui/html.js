// HTML escaping, text normalisation and the date/time formatters shared by
// every page. Extracted verbatim from the original monolith:
//   escapeHtml               lines 2640-2646
//   normalizeText            lines 231-235
//   formatDateListForHumans  lines 14355-14368
//   formatDateTime           lines 14370-14386
//   formatDateTimeNoTz       lines 14389-14403
//   formatDateOnly           lines 14446-14458
//   formatTimeOnly           lines 14460-14473
//   badgeClass               lines 14475-14489

import { APP_TIMEZONE, APP_TIMEZONE_OFFSET } from "../server/runtime.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function normalizeText(text) {
  return String(text || "")
    .trim()
    .toLowerCase();
}

function formatDateListForHumans(dateList) {
  if (!dateList || !dateList.length) return "None";

  return dateList
    .map((dateStr) => {
      const date = new Date(`${dateStr}T00:00:00${APP_TIMEZONE_OFFSET}`);
      return date.toLocaleDateString("en-IN", {
        timeZone: APP_TIMEZONE,
        day: "numeric",
        month: "short",
      });
    })
    .join(", ");
}

function formatDateTime(isoString) {
  if (!isoString) return "-";
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return String(isoString);

  return (
    d.toLocaleString("en-IN", {
      timeZone: APP_TIMEZONE,
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }) + " IST"
  );
}

function formatDateTimeNoTz(isoString) {
  if (!isoString) return "-";
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return String(isoString);

  return d.toLocaleString("en-IN", {
    timeZone: APP_TIMEZONE,
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  });
}

function formatDateOnly(dateString) {
  if (!dateString) return "-";

  const d = new Date(`${dateString}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return String(dateString);

  return d.toLocaleDateString("en-IN", {
    timeZone: APP_TIMEZONE,
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatTimeOnly(isoString) {
  if (!isoString) return "-";
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return String(isoString);

  return (
    d.toLocaleString("en-IN", {
      timeZone: APP_TIMEZONE,
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }) + " IST"
  );
}

function badgeClass(value) {
  const v = normalizeText(value);

  if (["high", "urgent"].includes(v)) return "badge badge-danger";
  if (["medium"].includes(v)) return "badge badge-warn";
  if (["low"].includes(v)) return "badge badge-ok";

  if (["done", "logout"].includes(v)) return "badge badge-muted";
  if (["blocked", "break"].includes(v)) return "badge badge-danger";
  if (["in_progress", "back", "login"].includes(v)) return "badge badge-info";
  if (["open", "pending"].includes(v)) return "badge badge-warn";
  if (["cancelled"].includes(v)) return "badge badge-muted";

  return "badge badge-muted";
}

export {
  escapeHtml,
  normalizeText,
  formatDateListForHumans,
  formatDateTime,
  formatDateTimeNoTz,
  formatDateOnly,
  formatTimeOnly,
  badgeClass,
};
