// Status/priority → semantic badge kind. Ported from badgeClass() in
// lib/server/app.js, but returning a semantic key ("danger" | "warn" | "ok" |
// "info" | "muted") instead of global class strings, so each page maps the kind
// to its own CSS-module badge classes (styles.badgeDanger, etc.).

export function normalizeText(text) {
  return String(text || "")
    .trim()
    .toLowerCase();
}

export function badgeKind(value) {
  const v = normalizeText(value);

  if (["high", "urgent"].includes(v)) return "danger";
  if (["medium"].includes(v)) return "warn";
  if (["low"].includes(v)) return "ok";

  if (["done", "logout"].includes(v)) return "muted";
  if (["blocked", "break"].includes(v)) return "danger";
  if (["in_progress", "back", "login"].includes(v)) return "info";
  if (["open", "pending"].includes(v)) return "warn";
  if (["cancelled"].includes(v)) return "muted";

  return "muted";
}
