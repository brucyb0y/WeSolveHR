// SVG path data for the auto-report KPI icons.
//
// Extracted verbatim from CLIENT_REPORT_ICONS in lib/server/app.js so the
// shared chart kit can render them without importing the server module into a
// component. Each value is the inner markup of an 18x18 stroked <svg>.
//
// These are static path strings authored in this repo — never user input — which
// is why ArIcon can set them as innerHTML.

export const ICONS = {
  megaphone:
    '<path d="M3 8.5 13 5v9L3 11.5z"/><path d="M13 6.5 16 6v6l-3-.5z"/><path d="M5.5 11.5V14a2 2 0 0 0 3.6 1.2"/>',
  check: '<circle cx="9" cy="9" r="6.5"/><path d="M6 9.2 8.2 11.4 12 7"/>',
  calendar:
    '<rect x="3" y="4" width="12" height="11" rx="2"/><path d="M3 7.5h12M6.5 2.5V5M11.5 2.5V5"/>',
  doc: '<path d="M5 2.5h5l4 4V15a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V3.5a1 1 0 0 1 1-1z"/><path d="M10 2.5V6h4"/>',
  alert: '<path d="M9 3 16 15H2z"/><path d="M9 7.5v3.5M9 13h.01"/>',
  rupee: '<path d="M6 4h6M6 7h6M6 4.4c4 0 4.4 5.6.4 5.6H7l4.6 5.6"/>',
  userplus:
    '<circle cx="7.5" cy="6.5" r="2.8"/><path d="M3 15.5a4.5 4.5 0 0 1 9 0"/><path d="M14 6v4M12 8h4"/>',
  moves:
    '<path d="M3 5.5h3.3L13 13h2.7"/><path d="M13.2 11.3 15.7 13l-2.5 1.7"/><path d="M3 14.5h3.3L9 11.2"/>',
  layers: '<path d="M9 2.5 16 6l-7 3.5L2 6z"/><path d="M2 10.5 9 14l7-3.5"/>',
  percent:
    '<path d="M5 14 14 5"/><circle cx="6.2" cy="6.2" r="1.8"/><circle cx="12.8" cy="12.8" r="1.8"/>',
  flag: '<path d="M5 16V3"/><path d="M5 3.5h8l-2 3 2 3H5"/>',
};
