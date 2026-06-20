// Option lists for the business-leads page. Ported verbatim from
// RASSET_INDUSTRY_OPTIONS / RASSET_CAPABILITY_OPTIONS in lib/server/app.js plus
// the inline <option> lists in renderBusinessLeadsPage() (filters + lead form +
// L2 modal). Client-safe constants.

export const RASSET_INDUSTRY_OPTIONS = [
  "Automotive",
  "Industrial Equipment",
  "Construction / Concrete",
  "Electronics / Electrical",
  "Textile / Garments",
  "Food & Beverage",
  "Pharma / Chemicals",
  "Packaging",
  "Furniture / Wood",
  "Rubber / Plastics",
  "Paper / Printing",
  "Consumer Goods",
  "Metal Products",
  "Energy / Infrastructure",
  "General Manufacturing",
  "Other",
];

export const RASSET_CAPABILITY_OPTIONS = [
  "CNC Machining",
  "CNC Turning",
  "CNC Milling",
  "Laser Cutting",
  "Sheet Metal Fabrication",
  "Welding",
  "Casting",
  "Forging",
  "Injection Molding",
  "Plastic Processing",
  "Tool & Die Making",
  "Assembly",
  "Packaging",
  "Concrete Mixing",
  "Ready Mix Supply",
  "Textile Stitching",
  "Textile Printing",
  "Dyeing",
  "General Fabrication",
  "Other",
];

export const ENTITY_TYPES = [
  "Factory",
  "Service Provider",
  "Trading Company",
  "Supplier",
  "Training Institute",
];

export const RASSET_FILTER_STATUSES = [
  "new",
  "working",
  "busy",
  "unreachable",
  "invalid",
  "unsure",
  "in_progress",
  "completed",
];

export const LEAD_CATEGORY_OPTIONS = [
  { value: "b2b", label: "B2B" },
  { value: "b2c", label: "B2C" },
];

export const LEAD_STATUS_OPTIONS = [
  { value: "new", label: "New" },
  { value: "in_progress", label: "In Progress" },
  { value: "completed", label: "Completed" },
];

export const LEAD_SOURCE_OPTIONS = [
  { value: "manual", label: "Manual" },
  { value: "voice", label: "Voice" },
  { value: "website", label: "Website" },
  { value: "google_map", label: "Google Map" },
  { value: "yelp", label: "Yelp" },
];

export const LEAD_STAGE_OPTIONS = [
  { value: "", label: "Select stage" },
  { value: "new", label: "New" },
  { value: "prospect", label: "Prospect" },
  { value: "qualified", label: "Qualified" },
  { value: "not_fit", label: "Not Fit" },
  { value: "customer", label: "Customer" },
];

export const L2_BEHAVIOR_OPTIONS = [
  { value: "", label: "Behavior" },
  { value: "helpful", label: "Helpful" },
  { value: "busy", label: "Busy" },
  { value: "not_helpful", label: "Not helpful" },
  { value: "rude", label: "Rude" },
  { value: "interested", label: "Interested" },
  { value: "not_interested", label: "Not interested" },
];

export const L2_CALL_OUTCOME_OPTIONS = [
  { value: "", label: "Call Outcome" },
  { value: "connected", label: "Connected" },
  { value: "busy", label: "Busy" },
  { value: "wrong_number", label: "Wrong number" },
  { value: "owner_not_available", label: "Owner not available" },
  { value: "callback_requested", label: "Callback requested" },
  { value: "not_relevant", label: "Not relevant" },
];

// Parse a stored comma/semicolon/newline-separated value into the array a
// multi-select needs (ported from setMultiSelectValues).
export function parseMultiValue(value) {
  return String(value || "")
    .split(/[,;\n]/)
    .map((x) => x.trim())
    .filter(Boolean);
}
