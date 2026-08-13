"use client";

// Add / Edit Lead modal.
//
// Behaviour carried over: smart-paste parsing, debounced duplicate-phone check
// (350ms) that blocks saving a NEW lead, URL enrichment, the advanced-fields
// toggle, and the two multiselects that round-trip as comma-joined strings.
//
// THREE BUGS IN THE ORIGINAL THAT THIS FIXES BY CONSTRUCTION — each was a
// getElementById() against an id the form never had:
//
//   1. enrichLeadUrl() assigned to #leadCompany, which does not exist. That
//      threw a TypeError *before* the 17 assignments after it, so whenever the
//      enrichment API returned a company name the website / maps / email /
//      city / phone / owner fields were silently left unfilled. Here every
//      returned field is applied.
//   2. smartParseLeadInput() wrote detected capabilities into
//      #leadManufacturingCapabilities; the field is #leadCapabilities. Guarded
//      by a truthiness check, so it failed silently and never selected
//      anything. Here the detected capabilities are selected for real.
//   3. getLeadPayloadFromForm() read #leadQualificationDone and
//      #leadWorthTalking with ?.checked || false. Neither exists, so both
//      always submitted false. PRESERVED as false — whether those checkboxes
//      were meant to exist is a product question, not a migration one.
//
// Note on (2): the original's keyword list maps "fabrication" to the literal
// "Fabrication", which is not one of the capability options (they are "Sheet
// Metal Fabrication" / "General Fabrication"). Detected values are therefore
// filtered to real options, so that one keyword still selects nothing.

import { useCallback, useEffect, useRef, useState } from "react";
import styles from "./leads.module.css";

const DUPLICATE_DEBOUNCE_MS = 350;

const CATEGORIES = [
  ["b2b", "B2B"],
  ["b2c", "B2C"],
];
const STATUSES = [
  ["new", "New"],
  ["in_progress", "In Progress"],
  ["completed", "Completed"],
];
const SOURCES = [
  ["manual", "Manual"],
  ["voice", "Voice"],
  ["website", "Website"],
  ["google_map", "Google Map"],
  ["yelp", "Yelp"],
];
const STAGES = [
  ["", "Select stage"],
  ["new", "New"],
  ["prospect", "Prospect"],
  ["qualified", "Qualified"],
  ["not_fit", "Not Fit"],
  ["customer", "Customer"],
];

const CAPABILITY_KEYWORDS = [
  ["cnc", "CNC Machining"],
  ["laser", "Laser Cutting"],
  ["injection", "Injection Molding"],
  ["fabrication", "Fabrication"],
  ["casting", "Casting"],
];

const ADVANCED_TEXT_FIELDS = [
  ["pin_code", "Pin Code"],
  ["location", "Location"],
  ["country", "Country"],
  ["year_of_establishment", "Year of Establishment"],
  ["owner_name", "Owner"],
  ["number_of_employees", "No. of Employees"],
  ["company_size", "Company Size"],
];

const ADVANCED_URL_FIELDS = [
  ["email", "Email"],
  ["google_maps_url", "Google Maps URL"],
  ["yelp_url", "Yelp URL"],
  ["address", "Address"],
];

const emptyForm = (business) => ({
  id: "",
  lead_category: business === "rasset" ? "b2b" : "b2c",
  status: "new",
  phone: "",
  lead_source: "manual",
  l2_done: false,
  lead_stage: "",
  business_name: "",
  contact_name: "",
  website: "",
  city: "",
  state: "",
  industry: [],
  capability: [],
  notes: "",
  pin_code: "",
  location: "",
  country: "",
  year_of_establishment: "",
  owner_name: "",
  number_of_employees: "",
  company_size: "",
  enrichment_notes: "",
  email: "",
  google_maps_url: "",
  yelp_url: "",
  address: "",
  latest_transcript: "",
});

const toSelection = (value) =>
  String(value || "")
    .split(/[,;\n]/)
    .map((x) => x.trim())
    .filter(Boolean);

const normalizePhone = (v) => String(v || "").replace(/\D/g, "");
const extractPhone = (t) =>
  (String(t || "").match(/(?:\+?\d[\d\s().-]{8,}\d)/) || [""])[0].trim();
const extractUrl = (t) =>
  (String(t || "").match(/https?:\/\/[^\s]+/i) || [""])[0].trim();

// Defined at module scope on purpose: a component declared inside the render
// body is a new type on every render, so React unmounts and remounts it and the
// input loses focus after each keystroke.
function Field({ label, value, onChange, wide }) {
  return (
    <div className={`${styles.formField} ${wide ? styles.fieldWide : ""}`}>
      <label>{label}</label>
      <input value={value} onChange={onChange} />
    </div>
  );
}

export default function LeadFormModal({
  business,
  leadId,
  industryOptions,
  capabilityOptions,
  onClose,
}) {
  const [form, setForm] = useState(() => emptyForm(business));
  const [smartPaste, setSmartPaste] = useState("");
  const [enrichUrl, setEnrichUrl] = useState("");
  const [enrichMessage, setEnrichMessage] = useState("");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [duplicate, setDuplicate] = useState({ state: "idle", lead: null });
  const [saving, setSaving] = useState(false);

  const isEdit = leadId != null;
  const set = (key) => (e) => setForm((f) => ({ ...f, [key]: e.target.value }));
  const setMulti = (key) => (e) =>
    setForm((f) => ({
      ...f,
      [key]: Array.from(e.target.selectedOptions).map((o) => o.value),
    }));

  // ---- load on edit -----------------------------------------------------
  useEffect(() => {
    if (!isEdit) return undefined;
    let cancelled = false;

    (async () => {
      try {
        const res = await fetch(`/api/business-leads/${business}/${leadId}`);
        const json = await res.json();
        if (cancelled) return;

        if (!json.ok) {
          alert(json.error || "Failed to load lead");
          return;
        }

        const lead = json.data || {};
        setForm({
          id: lead.id || "",
          lead_category: lead.lead_category || "b2b",
          status: lead.status || "new",
          phone: lead.phone || "",
          lead_source: lead.lead_source || "manual",
          l2_done: !!lead.l2_done,
          lead_stage: lead.lead_stage || "",
          business_name: lead.business_name || "",
          contact_name: lead.contact_name || "",
          website: lead.website || "",
          city: lead.city || "",
          state: lead.state || "",
          industry: toSelection(lead.industry || lead.industry_primary || ""),
          capability: toSelection(lead.manufacturing_capabilities || ""),
          notes: lead.notes || "",
          pin_code: lead.pin_code || "",
          location: lead.location || "",
          country: lead.country || "",
          year_of_establishment: lead.year_of_establishment || "",
          owner_name: lead.owner_name || "",
          number_of_employees: lead.number_of_employees || "",
          company_size: lead.company_size || "",
          enrichment_notes: lead.enrichment_notes || "",
          email: lead.email || "",
          google_maps_url: lead.google_maps_url || "",
          yelp_url: lead.yelp_url || "",
          address: lead.address || "",
          latest_transcript: lead.latest_transcript || "",
        });
      } catch {
        alert("Failed to load lead");
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [business, leadId, isEdit]);

  // ---- duplicate phone check (debounced) --------------------------------
  const timer = useRef(null);

  const checkDuplicate = useCallback(
    (phone) => {
      clearTimeout(timer.current);
      timer.current = setTimeout(async () => {
        if (!phone || normalizePhone(phone).length < 8) {
          setDuplicate({ state: "idle", lead: null });
          return;
        }

        setDuplicate({ state: "checking", lead: null });

        try {
          const res = await fetch(
            `/api/business-leads/${business}/check-phone?phone=${encodeURIComponent(phone)}`,
          );
          const json = await res.json();

          if (!json.ok) {
            setDuplicate({ state: "error", lead: null });
            return;
          }

          if (json.data?.duplicate) {
            setDuplicate({ state: "duplicate", lead: json.data.lead || {} });
          } else {
            setDuplicate({ state: "clear", lead: null });
          }
        } catch {
          setDuplicate({ state: "error", lead: null });
        }
      }, DUPLICATE_DEBOUNCE_MS);
    },
    [business],
  );

  useEffect(() => () => clearTimeout(timer.current), []);

  function onPhoneChange(e) {
    const phone = e.target.value;
    setForm((f) => ({ ...f, phone }));
    checkDuplicate(phone.trim());
  }

  // ---- smart paste ------------------------------------------------------
  function onSmartPaste(e) {
    const text = e.target.value;
    setSmartPaste(text);

    const phone = extractPhone(text);
    const url = extractUrl(text);
    const lower = text.toLowerCase();

    const detected = CAPABILITY_KEYWORDS.filter(([kw]) => lower.includes(kw)).map(
      ([, label]) => label,
    );
    if (lower.includes("mould") || lower.includes("mold")) {
      detected.push("Tool & Die Making");
    }
    // Only values that are real options can be selected.
    const validDetected = detected.filter((x) => capabilityOptions.includes(x));

    if (url) setEnrichUrl(url);

    setForm((f) => {
      const next = { ...f };

      if (phone && !f.phone.trim()) {
        next.phone = phone;
        checkDuplicate(phone);
      }

      if (url) {
        if (url.includes("google.") || url.includes("maps")) {
          next.google_maps_url = url;
        } else {
          next.website = url;
        }
      }

      if (validDetected.length && !f.capability.length) {
        next.capability = validDetected;
      }

      if (!f.notes.trim()) next.notes = text;

      return next;
    });
  }

  // ---- enrichment -------------------------------------------------------
  async function enrich() {
    const website = form.website.trim() || enrichUrl.trim();
    const googleMapsUrl = form.google_maps_url.trim();

    if (!website && !googleMapsUrl) {
      alert("Add website or Google Map link first.");
      return;
    }

    setEnrichMessage("Trying to fetch company info...");

    try {
      const res = await fetch("/api/rasset-leads/enrich", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ website, google_maps_url: googleMapsUrl }),
      });
      const json = await res.json();

      if (!json.ok) {
        setEnrichMessage(json.error || "Could not fetch.");
        return;
      }

      const result = json.data || {};
      const d = result.data || {};
      setEnrichMessage(result.message || "Done.");

      setForm((f) => {
        const next = { ...f };
        if (d.company) next.business_name = d.company;
        if (d.website) next.website = d.website;
        if (d.google_maps_url) next.google_maps_url = d.google_maps_url;
        if (d.lead_source) next.lead_source = d.lead_source;
        if (d.email) next.email = d.email;
        if (d.industry) {
          const picked = toSelection(d.industry).filter((x) =>
            industryOptions.includes(x),
          );
          if (picked.length) next.industry = picked;
        }
        if (d.pin_code) next.pin_code = d.pin_code;
        if (d.city) next.city = d.city;
        if (d.location) next.location = d.location;
        if (d.phone) next.phone = d.phone;
        if (d.year_of_establishment)
          next.year_of_establishment = d.year_of_establishment;
        if (d.owner_name) next.owner_name = d.owner_name;
        if (d.number_of_employees)
          next.number_of_employees = d.number_of_employees;
        if (d.company_size) next.company_size = d.company_size;
        if (d.country) next.country = d.country;
        if (d.notes) next.notes = d.notes;
        if (d.enrichment_notes) next.enrichment_notes = d.enrichment_notes;
        return next;
      });
    } catch {
      setEnrichMessage("Could not fetch.");
    }
  }

  // ---- save -------------------------------------------------------------
  async function save() {
    if (!isEdit && duplicate.state === "duplicate") {
      alert(
        "Duplicate lead exists with this phone number. Please use the existing lead instead.",
      );
      return;
    }

    const payload = {
      phone: form.phone.trim(),
      lead_category: form.lead_category,
      status: form.status,
      lead_source: form.lead_source,
      business_name: form.business_name.trim(),
      contact_name: form.contact_name.trim(),
      email: form.email.trim(),
      website: form.website.trim(),
      google_maps_url: form.google_maps_url.trim(),
      yelp_url: form.yelp_url.trim(),
      city: form.city.trim(),
      state: form.state.trim(),
      industry: form.industry.join(", "),
      industry_primary: form.industry[0] || "",
      raw_industry: form.industry.join(", "),
      manufacturing_capabilities: form.capability.join(", "),
      address: form.address.trim(),
      notes: form.notes.trim(),
      company: form.business_name.trim(),
      lead_stage: form.lead_stage,
      pin_code: form.pin_code.trim(),
      location: form.location.trim(),
      country: form.country.trim(),
      year_of_establishment: form.year_of_establishment.trim(),
      owner_name: form.owner_name.trim(),
      number_of_employees: form.number_of_employees.trim(),
      company_size: form.company_size.trim(),
      enrichment_notes: form.enrichment_notes.trim(),
      // Neither checkbox exists in this form; both have always submitted false.
      qualification_done: false,
      worth_talking: false,
      l2_done: !!form.l2_done,
      latest_transcript: form.latest_transcript.trim(),
    };

    setSaving(true);
    try {
      const res = await fetch(
        isEdit
          ? `/api/business-leads/${business}/${leadId}`
          : `/api/business-leads/${business}`,
        {
          method: isEdit ? "PUT" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to save lead");
        return;
      }
      window.location.reload();
    } catch {
      alert("Failed to save lead");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div
      className={`${styles.modal} ${styles.open}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className={styles.modalCard}>
        <div className={styles.modalHead}>
          <div className={styles.modalTitle}>
            {isEdit ? `Edit Lead #${leadId}` : "Add Lead"}
          </div>
          <button className={styles.btn} type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className={styles.panel}>
          <h2 className={styles.panelHeadFlush}>Quick Enrichment</h2>
          <div className={styles.searchRow}>
            <input
              placeholder="Paste website, Google Maps link, or Yelp link"
              value={enrichUrl}
              onChange={(e) => setEnrichUrl(e.target.value)}
            />
            <button
              className={`${styles.btn} ${styles.btnPrimary}`}
              type="button"
              onClick={enrich}
            >
              Fetch Info
            </button>
            <button
              className={styles.btn}
              type="button"
              onClick={() => {
                setForm(emptyForm(business));
                setSmartPaste("");
                setEnrichUrl("");
                setEnrichMessage("");
                setDuplicate({ state: "idle", lead: null });
              }}
            >
              Clear
            </button>
          </div>
          <div className={`muted ${styles.enrichMessage}`}>{enrichMessage}</div>
        </div>

        <div className={styles.formGrid}>
          <div className={`${styles.formField} ${styles.fieldWide}`}>
            <label>Smart Add</label>
            <textarea
              className={styles.smartPaste}
              placeholder="Paste anything: phone, company, city, website, Google Maps link, WhatsApp text, CNC/laser/capability notes..."
              value={smartPaste}
              onChange={onSmartPaste}
            />
            <div className="hint">
              Example: Sharma CNC Rajkot +919876543210 does CNC turning and laser
              cutting
            </div>
          </div>

          {duplicate.state !== "idle" ? (
            <div className={styles.fieldWide}>
              {duplicate.state === "checking" ? (
                <div className={styles.dupChecking}>Checking duplicate...</div>
              ) : duplicate.state === "error" ? (
                <div className={styles.dupError}>Could not check duplicate.</div>
              ) : duplicate.state === "duplicate" ? (
                <div className={styles.dupFound}>
                  <strong>Duplicate found by phone number.</strong>
                  <br />
                  Lead #{duplicate.lead.id} —{" "}
                  {duplicate.lead.company ||
                    duplicate.lead.business_name ||
                    duplicate.lead.contact_name ||
                    "Existing lead"}
                  {duplicate.lead.city ? ` · ${duplicate.lead.city}` : ""}
                  {duplicate.lead.status ? ` · ${duplicate.lead.status}` : ""}
                </div>
              ) : (
                <div className={styles.dupClear}>
                  No duplicate found. Safe to add.
                </div>
              )}
            </div>
          ) : null}

          <div className={styles.formField}>
            <label>Lead Type</label>
            <select value={form.lead_category} onChange={set("lead_category")}>
              {CATEGORIES.map(([v, l]) => (
                <option value={v} key={v}>
                  {l}
                </option>
              ))}
            </select>
          </div>

          <div className={styles.formField}>
            <label>Status</label>
            <select value={form.status} onChange={set("status")}>
              {STATUSES.map(([v, l]) => (
                <option value={v} key={v}>
                  {l}
                </option>
              ))}
            </select>
          </div>

          <div className={styles.formField}>
            <label>Phone</label>
            <input
              placeholder="+91..."
              value={form.phone}
              onChange={onPhoneChange}
            />
          </div>

          <div className={styles.formField}>
            <label>Lead Source</label>
            <select value={form.lead_source} onChange={set("lead_source")}>
              {SOURCES.map(([v, l]) => (
                <option value={v} key={v}>
                  {l}
                </option>
              ))}
            </select>
          </div>

          <div className={`${styles.formField} ${styles.fieldWide}`}>
            <div className={styles.l2Box}>
              <label className={styles.l2Label}>
                <input
                  type="checkbox"
                  checked={form.l2_done}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, l2_done: e.target.checked }))
                  }
                />
                <span>L2 Done</span>
              </label>
            </div>
          </div>

          <div className={styles.formField}>
            <label>Lead Stage</label>
            <select value={form.lead_stage} onChange={set("lead_stage")}>
              {STAGES.map(([v, l]) => (
                <option value={v} key={v}>
                  {l}
                </option>
              ))}
            </select>
          </div>

          <Field label="Business / Organization Name" value={form.business_name} onChange={set("business_name")} />
          <Field label="Contact Name" value={form.contact_name} onChange={set("contact_name")} />
          <Field label="Website" value={form.website} onChange={set("website")} />
          <Field label="City" value={form.city} onChange={set("city")} />
          <Field label="State" value={form.state} onChange={set("state")} />

          <div className={styles.formField}>
            <label>Industry</label>
            <select
              multiple
              size={6}
              value={form.industry}
              onChange={setMulti("industry")}
            >
              {industryOptions.map((x) => (
                <option value={x} key={x}>
                  {x}
                </option>
              ))}
            </select>
            <div className="hint">Hold Cmd/Ctrl to select multiple.</div>
          </div>

          <div className={styles.formField}>
            <label>Capabilities</label>
            <select
              multiple
              size={6}
              value={form.capability}
              onChange={setMulti("capability")}
            >
              {capabilityOptions.map((x) => (
                <option value={x} key={x}>
                  {x}
                </option>
              ))}
            </select>
            <div className="hint">Select all matching capabilities.</div>
          </div>

          <div className={`${styles.formField} ${styles.fieldWide}`}>
            <label>Notes</label>
            <textarea value={form.notes} onChange={set("notes")} />
          </div>

          <div className={`${styles.formField} ${styles.fieldWide}`}>
            <button
              className={`${styles.btn} ${styles.advancedToggle}`}
              type="button"
              onClick={() => setShowAdvanced((v) => !v)}
            >
              Show / Hide Advanced Fields
            </button>
          </div>

          {showAdvanced ? (
            <div className={styles.fieldWide}>
              <div className={styles.formGrid}>
                {ADVANCED_TEXT_FIELDS.map(([name, label]) => (
                  <Field label={label} value={form[name]} onChange={set(name)} key={name} />
                ))}

                <div className={`${styles.formField} ${styles.fieldWide}`}>
                  <label>Enrichment Notes</label>
                  <textarea
                    value={form.enrichment_notes}
                    onChange={set("enrichment_notes")}
                  />
                </div>

                {ADVANCED_URL_FIELDS.map(([name, label]) => (
                  <Field label={label} value={form[name]} onChange={set(name)} key={name} />
                ))}

                <div className={`${styles.formField} ${styles.fieldWide}`}>
                  <label>Latest Transcript / Summary</label>
                  <textarea
                    value={form.latest_transcript}
                    onChange={set("latest_transcript")}
                  />
                </div>
              </div>
            </div>
          ) : null}
        </div>

        <div className={styles.modalActions}>
          <button className={styles.btn} type="button" onClick={onClose}>
            Cancel
          </button>
          <button
            className={`${styles.btn} ${styles.btnPrimary}`}
            type="button"
            onClick={save}
            disabled={saving}
          >
            {saving ? "Saving..." : "Save Lead"}
          </button>
        </div>
      </div>
    </div>
  );
}
