// Phone normalization helpers shared across the app. Ported verbatim from
// lib/server/app.js so behavior is identical between converted pages and the
// routes still served through the Express dispatch shim.

export function normalizePhoneForLogin(input) {
  if (!input) return "";

  let value = String(input).trim();

  // Remove whatsapp: if someone pastes it
  value = value.replace(/^whatsapp:/i, "");

  // Remove spaces, dashes, brackets, dots etc, but keep digits and +
  value = value.replace(/[^\d+]/g, "");

  // Convert 00... to +...
  if (value.startsWith("00")) {
    value = `+${value.slice(2)}`;
  }

  // If user entered full country code but no plus, add it
  if (value && !value.startsWith("+")) {
    value = `+${value}`;
  }

  return value;
}
