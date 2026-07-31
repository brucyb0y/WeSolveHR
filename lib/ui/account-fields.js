// The editable department/designation selects on /account, extracted
// verbatim from the original monolith.

import { escapeHtml } from "./html.js";

const ACCOUNT_FIELD_OPTIONS = {
  department: ["GTM", "Leads", "Others"],
  designation: [
    "CEO",
    "Program Head",
    "Project Manager",
    "Sr. Manager",
    "Associate",
  ],
};

function renderAccountFieldSelect(field, currentValue) {
  const options = ACCOUNT_FIELD_OPTIONS[field] || [];
  const known = options.includes(currentValue);
  return `
    <select class="meta-select" data-field="${field}">
      ${options
        .map(
          (opt) =>
            `<option value="${escapeHtml(opt)}"${currentValue === opt ? " selected" : ""}>${escapeHtml(opt)}</option>`,
        )
        .join("")}
      ${known ? "" : `<option value="" selected>-</option>`}
    </select>
    <span id="${field}-status" class="save-status"></span>
  `;
}

export {
  ACCOUNT_FIELD_OPTIONS,
  renderAccountFieldSelect,
};
