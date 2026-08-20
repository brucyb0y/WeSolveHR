"use client";

// Add / Edit Lead — replaces openClientLeadModal / openClientLeadDetail /
// closeClientLeadModal / saveClientLead.
//
// NOTES ARE NEVER SENT IN THE PAYLOAD. The lead's note history is append-only,
// and writing a `notes` field would replace the whole trail with whatever the
// form happened to hold. A new note is instead appended afterwards through
// `add_note`, which records author and timestamp server-side — for creates as
// well as edits. That second request is why saving is two calls, not one.
//
// OPTIONAL SHEET FIELDS: some clients (currently Revivflow) have extra columns
// — persona, company size, last LinkedIn activity, monthly chargebacks, mode of
// payment, ICP category. They are only rendered when the workspace enables
// them, and only the rendered ones go into the payload. A column whose field is
// absent is left untouched by the server rather than nulled, so an unrelated
// client's import data is never wiped by an edit here.
//
// Editing FETCHES the lead rather than reading the row already on screen: the
// table carries a display subset, and saving from that would blank every column
// it does not show.

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Swal from "sweetalert2";
import { WorkModal, TextField, SelectField, TextAreaField } from "./WorkModal";

const CATEGORIES = [
  { value: "b2b", label: "B2B" },
  { value: "b2c", label: "B2C" },
];

const SOURCES = [
  { value: "manual", label: "Manual" },
  { value: "import", label: "Import" },
  { value: "referral", label: "Referral" },
  { value: "inbound", label: "Inbound" },
];

const OUTREACH_STATUSES = [
  { value: "not_started", label: "Not started" },
  { value: "in_progress", label: "In progress" },
  { value: "responded", label: "Responded" },
  { value: "no_response", label: "No response" },
];

const STATUSES = [
  { value: "new", label: "New" },
  { value: "active", label: "Active" },
  { value: "inactive", label: "Inactive" },
];

// The Revivflow sheet's own columns, shown only for the Revivflow workspace.
// Ordered to mirror the import CSV so an editor recognises them. Every column
// the "Final Format" sheet carries that isn't already one of the standard
// fields above is here, so nothing imported is uneditable.
// [state key, label, lead column]
const OPTIONAL_SHEET_FIELDS = [
  ["persona", "Persona", "persona"],
  ["company_email", "Company Email", "company_email"],
  ["company_hq_phone", "Company Number", "company_hq_phone"],
  ["company_size", "Company Size", "company_size"],
  ["company_instagram_url", "Instagram URL", "company_instagram_url"],
  [
    "last_linkedin_activity",
    "Last LinkedIn Activity",
    "last_linkedin_activity",
  ],
  [
    "last_instagram_activity",
    "Last Instagram Activity",
    "last_instagram_activity",
  ],
  ["mode_of_payment", "Mode of Payment", "mode_of_payment"],
  ["monthly_chargebacks", "Monthly Chargebacks", "monthly_chargebacks"],
  ["company_subtype", "Sub Category", "company_subtype"],
  ["icp_category", "ICP Category", "icp_category"],
];

// Revivflow's edit form mirrors its import sheet exactly: only the "Final
// Format" CSV's own columns, in the CSV's order, labelled with the CSV heading.
// Everything the generic lead form shows that the sheet does NOT carry (funding
// trio, Lead Category, Category Type, Source, Pipeline/Outreach/Demo/Status) is
// hidden — those stay editable from the table row and the quick-update popup.
// The lead keeps whatever it already had for the hidden fields; the form loads
// and re-saves them untouched, so nothing is wiped. [form key, CSV heading].
const REVIVFLOW_EDIT_FIELDS = [
  ["company", "Company name"],
  ["website", "Company website"],
  ["contact_name", "Full name"],
  ["persona", "Persona"],
  ["email", "Persona Email"],
  ["company_email", "Company Email"],
  ["phone", "Persona Number"],
  ["company_hq_phone", "Company Number"],
  ["person_linkedin_url", "Person LinkedIn URL"],
  ["company_linkedin_url", "Company LinkedIn"],
  ["last_linkedin_activity", "Last Linkedin Activity"],
  ["country", "Country"],
  ["state", "State"],
  ["city", "City"],
  ["company_instagram_url", "Instagram URL"],
  ["last_instagram_activity", "Last Instagram Activity"],
  ["mode_of_payment", "Mode of Payment"],
  ["company_subtype", "Sub Category"],
  ["phone_assigned_to", "Assign for Phone"],
  ["email_assigned_to", "Assign for email"],
  ["verified_by", "Verified By"],
];

const EMPTY = {
  company: "",
  contact_name: "",
  phone: "",
  email: "",
  city: "",
  state: "",
  country: "",
  website: "",
  person_linkedin_url: "",
  company_linkedin_url: "",
  company_last_round_amount: "",
  company_last_funding_date: "",
  company_funding_round: "",
  lead_category: "b2b",
  category_type: "",
  lead_source: "manual",
  pipeline_stage: "prospect_identified",
  outreach_status: "not_started",
  demo_status: "not_scheduled",
  status: "new",
  phone_assigned_to: "",
  email_assigned_to: "",
  verified_by: "",
  persona: "",
  company_size: "",
  last_linkedin_activity: "",
  monthly_chargebacks: "",
  mode_of_payment: "",
  icp_category: "",
  company_email: "",
  last_instagram_activity: "",
  company_hq_phone: "",
  company_instagram_url: "",
  company_subtype: "",
};

export default function ClientLeadModal({
  clientId,
  leadId,
  stages,
  demoStatuses,
  categoryTypes,
  showOptionalSheetFields,
  onClose,
}) {
  const router = useRouter();
  const isEdit = !!leadId;

  const [form, setForm] = useState(EMPTY);
  const [newNote, setNewNote] = useState("");
  const [loading, setLoading] = useState(isEdit);
  const [saving, setSaving] = useState(false);

  const set = (key) => (value) => setForm((f) => ({ ...f, [key]: value }));

  useEffect(() => {
    if (!isEdit) return;
    let cancelled = false;

    (async () => {
      try {
        const res = await fetch(`/api/clients/${clientId}/leads/${leadId}`);
        const json = await res.json();
        if (cancelled) return;

        if (!json.ok) {
          alert(json.error || "Failed to load lead");
          onClose();
          return;
        }

        const l = json.data || {};
        setForm({
          ...EMPTY,
          // The column is `company` on newer rows and `business_name` on
          // imported ones; both are written on save.
          company: l.company || l.business_name || "",
          ...Object.fromEntries(
            Object.keys(EMPTY)
              .filter((k) => k !== "company")
              .map((k) => [k, l[k] ?? EMPTY[k]]),
          ),
        });
      } catch {
        if (!cancelled) {
          alert("Failed to load lead");
          onClose();
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [clientId, leadId, isEdit, onClose]);

  async function save() {
    const company = form.company.trim();
    const phone = form.phone.trim();

    if (!company && !phone) {
      alert("Enter at least a company name or phone number.");
      return;
    }

    const trim = (k) => String(form[k] ?? "").trim();

    const payload = {
      // Both names are written so either read path finds the value.
      business_name: company,
      company,
      contact_name: trim("contact_name"),
      phone,
      email: trim("email"),
      city: trim("city"),
      state: trim("state"),
      country: trim("country"),
      website: trim("website"),
      person_linkedin_url: trim("person_linkedin_url"),
      company_linkedin_url: trim("company_linkedin_url"),
      company_last_round_amount: trim("company_last_round_amount"),
      company_last_funding_date: trim("company_last_funding_date"),
      company_funding_round: trim("company_funding_round"),
      lead_category: form.lead_category,
      category_type: trim("category_type"),
      lead_source: form.lead_source,
      pipeline_stage: form.pipeline_stage,
      outreach_status: form.outreach_status,
      demo_status: form.demo_status,
      status: form.status,
      phone_assigned_to: trim("phone_assigned_to"),
      email_assigned_to: trim("email_assigned_to"),
      verified_by: trim("verified_by"),
    };

    // Only the fields this workspace actually renders.
    if (showOptionalSheetFields) {
      for (const [key, , column] of OPTIONAL_SHEET_FIELDS) {
        payload[column] = trim(key);
      }
    }

    const note = newNote.trim();

    setSaving(true);
    Swal.fire({
      title: isEdit ? "Updating lead..." : "Creating lead...",
      allowOutsideClick: false,
      didOpen: () => Swal.showLoading(),
    });

    try {
      const res = await fetch(
        isEdit
          ? `/api/clients/${clientId}/leads/${leadId}`
          : `/api/clients/${clientId}/leads`,
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        Swal.close();
        alert(json.error || `Failed to ${isEdit ? "update" : "create"} lead`);
        return;
      }

      // Append the note separately so the existing history survives.
      const savedId = leadId || json.data?.id;
      if (savedId && note) {
        await fetch(`/api/clients/${clientId}/leads/${savedId}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ add_note: note }),
        });
      }

      Swal.close();
      onClose();
      router.refresh();
    } catch {
      Swal.close();
      alert(`Failed to ${isEdit ? "update" : "create"} lead`);
    } finally {
      setSaving(false);
    }
  }

  const asOptions = (list) =>
    list.map((s) => ({ value: s.key, label: s.label }));

  return (
    <WorkModal
      title={isEdit ? "Edit Lead" : "Add Lead"}
      saveLabel="Save Lead"
      saving={saving || loading}
      onSave={save}
      onClose={onClose}
    >
      {showOptionalSheetFields ? (
        // Revivflow: only the import sheet's own columns.
        REVIVFLOW_EDIT_FIELDS.map(([key, label]) => (
          <TextField
            key={key}
            label={label}
            value={form[key]}
            onChange={set(key)}
          />
        ))
      ) : (
        <>
          <TextField
            label="Company"
            value={form.company}
            onChange={set("company")}
          />
          <TextField
            label="Contact Name"
            value={form.contact_name}
            onChange={set("contact_name")}
          />
          <TextField label="Phone" value={form.phone} onChange={set("phone")} />
          <TextField label="Email" value={form.email} onChange={set("email")} />
          <TextField label="City" value={form.city} onChange={set("city")} />
          <TextField label="State" value={form.state} onChange={set("state")} />
          <TextField
            label="Country"
            value={form.country}
            onChange={set("country")}
          />
          <TextField
            label="Website"
            value={form.website}
            onChange={set("website")}
          />
          <TextField
            label="Person LinkedIn"
            value={form.person_linkedin_url}
            onChange={set("person_linkedin_url")}
          />
          <TextField
            label="Company LinkedIn"
            value={form.company_linkedin_url}
            onChange={set("company_linkedin_url")}
          />
          <TextField
            label="Last Round Amount"
            value={form.company_last_round_amount}
            onChange={set("company_last_round_amount")}
          />
          <TextField
            label="Last Funding Date"
            value={form.company_last_funding_date}
            onChange={set("company_last_funding_date")}
          />
          <TextField
            label="Funding Round"
            value={form.company_funding_round}
            onChange={set("company_funding_round")}
          />

          <SelectField
            label="Lead Category"
            options={CATEGORIES}
            value={form.lead_category}
            onChange={set("lead_category")}
          />
          <SelectField
            label="Category Type"
            options={[
              { value: "", label: "None" },
              ...categoryTypes.map((c) => ({ value: c.key, label: c.label })),
            ]}
            value={form.category_type}
            onChange={set("category_type")}
          />
          <SelectField
            label="Source"
            options={SOURCES}
            value={form.lead_source}
            onChange={set("lead_source")}
          />
          <SelectField
            label="Pipeline Stage"
            options={asOptions(stages)}
            value={form.pipeline_stage}
            onChange={set("pipeline_stage")}
          />
          <SelectField
            label="Outreach Status"
            options={OUTREACH_STATUSES}
            value={form.outreach_status}
            onChange={set("outreach_status")}
          />
          <SelectField
            label="Demo Status"
            options={asOptions(demoStatuses)}
            value={form.demo_status}
            onChange={set("demo_status")}
          />
          <SelectField
            label="Status"
            options={STATUSES}
            value={form.status}
            onChange={set("status")}
          />

          <TextField
            label="Assigned for Phone"
            value={form.phone_assigned_to}
            onChange={set("phone_assigned_to")}
          />
          <TextField
            label="Assigned for Email"
            value={form.email_assigned_to}
            onChange={set("email_assigned_to")}
          />
          <TextField
            label="Verified By"
            value={form.verified_by}
            onChange={set("verified_by")}
          />
        </>
      )}

      <TextAreaField
        label={showOptionalSheetFields ? "Notes" : "Add Note"}
        placeholder="Appended to the lead's note history — existing notes are kept."
        value={newNote}
        onChange={setNewNote}
      />
    </WorkModal>
  );
}
