"use client";

// Add/Edit lead modal. Ported from the lead modal markup + its scripts
// (openLeadCreateModal/openLeadEditModal/clearLeadForm/getLeadPayloadFromForm/
// saveBusinessLead/enrichLeadUrl/smartParseLeadInput/checkLeadPhoneDuplicate) in
// renderBusinessLeadsPage(). All form fields are React state; save reloads the
// page (as the original did) so the server-rendered table reflects the change.

import { useEffect, useRef, useState } from "react";
import {
  RASSET_INDUSTRY_OPTIONS,
  RASSET_CAPABILITY_OPTIONS,
  LEAD_CATEGORY_OPTIONS,
  LEAD_STATUS_OPTIONS,
  LEAD_SOURCE_OPTIONS,
  LEAD_STAGE_OPTIONS,
  parseMultiValue,
} from "./leadOptions.js";

function emptyForm(business) {
  return {
    phone: "",
    lead_category: business === "rasset" ? "b2b" : "b2c",
    status: "new",
    lead_source: "manual",
    lead_stage: "",
    business_name: "",
    contact_name: "",
    email: "",
    website: "",
    google_maps_url: "",
    yelp_url: "",
    city: "",
    state: "",
    address: "",
    notes: "",
    latest_transcript: "",
    pin_code: "",
    location: "",
    country: "",
    year_of_establishment: "",
    owner_name: "",
    number_of_employees: "",
    company_size: "",
    enrichment_notes: "",
    l2_done: false,
  };
}

function normalizePhone(value) {
  return String(value || "").replace(/\D/g, "");
}
function extractPhoneFromText(text) {
  const match = String(text || "").match(/(?:\+?\d[\d\s().-]{8,}\d)/);
  return match ? match[0].trim() : "";
}
function extractUrlFromText(text) {
  const match = String(text || "").match(/https?:\/\/[^\s]+/i);
  return match ? match[0].trim() : "";
}

export default function LeadFormModal({ business, open, editId, onClose }) {
  const [form, setForm] = useState(() => emptyForm(business));
  const [industry, setIndustry] = useState([]);
  const [capabilities, setCapabilities] = useState([]);
  const [smartPaste, setSmartPaste] = useState("");
  const [enrichUrl, setEnrichUrl] = useState("");
  const [enrichMessage, setEnrichMessage] = useState("");
  const [showAdvanced, setShowAdvanced] = useState(false);
  // duplicate: null | { state: "checking" | "duplicate" | "clear", lead }
  const [duplicate, setDuplicate] = useState(null);
  const dupTimer = useRef(null);
  const duplicateFound = duplicate?.state === "duplicate";

  const setField = (name) => (e) =>
    setForm((f) => ({ ...f, [name]: e.target.value }));

  function resetForm() {
    setForm(emptyForm(business));
    setIndustry([]);
    setCapabilities([]);
    setSmartPaste("");
    setEnrichUrl("");
    setEnrichMessage("");
    setShowAdvanced(false);
    setDuplicate(null);
  }

  // Populate (edit) or clear (create) each time the modal opens.
  useEffect(() => {
    if (!open) return;
    if (!editId) {
      resetForm();
      return;
    }
    resetForm();
    let alive = true;
    fetch(`/api/business-leads/${business}/${editId}`)
      .then((r) => r.json())
      .then((json) => {
        if (!alive) return;
        if (!json.ok) {
          alert(json.error || "Failed to load lead");
          return;
        }
        const lead = json.data || {};
        setForm({
          phone: lead.phone || "",
          lead_category: lead.lead_category || "b2b",
          status: lead.status || "new",
          lead_source: lead.lead_source || "manual",
          lead_stage: lead.lead_stage || "",
          business_name: lead.business_name || "",
          contact_name: lead.contact_name || "",
          email: lead.email || "",
          website: lead.website || "",
          google_maps_url: lead.google_maps_url || "",
          yelp_url: lead.yelp_url || "",
          city: lead.city || "",
          state: lead.state || "",
          address: lead.address || "",
          notes: lead.notes || "",
          latest_transcript: lead.latest_transcript || "",
          pin_code: lead.pin_code || "",
          location: lead.location || "",
          country: lead.country || "",
          year_of_establishment: lead.year_of_establishment || "",
          owner_name: lead.owner_name || "",
          number_of_employees: lead.number_of_employees || "",
          company_size: lead.company_size || "",
          enrichment_notes: lead.enrichment_notes || "",
          l2_done: !!lead.l2_done,
        });
        setIndustry(parseMultiValue(lead.industry || lead.industry_primary || ""));
        setCapabilities(parseMultiValue(lead.manufacturing_capabilities || ""));
      })
      .catch(() => {
        if (alive) alert("Failed to load lead");
      });
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, editId, business]);

  function checkDuplicate(phoneValue) {
    clearTimeout(dupTimer.current);
    dupTimer.current = setTimeout(async () => {
      const phone = String(phoneValue || "").trim();
      if (!phone || normalizePhone(phone).length < 8) {
        setDuplicate(null);
        return;
      }
      setDuplicate({ state: "checking" });
      try {
        const res = await fetch(
          `/api/business-leads/${business}/check-phone?phone=${encodeURIComponent(phone)}`,
        );
        const json = await res.json();
        if (!json.ok) {
          setDuplicate({ state: "error" });
          return;
        }
        if (json.data && json.data.duplicate) {
          setDuplicate({ state: "duplicate", lead: json.data.lead || {} });
        } else {
          setDuplicate({ state: "clear" });
        }
      } catch {
        setDuplicate({ state: "error" });
      }
    }, 350);
  }

  function onPhoneChange(e) {
    const value = e.target.value;
    setForm((f) => ({ ...f, phone: value }));
    checkDuplicate(value);
  }

  function onSmartPaste(e) {
    const text = e.target.value;
    setSmartPaste(text);

    const phone = extractPhoneFromText(text);
    setForm((f) => {
      const next = { ...f };
      if (phone && !f.phone.trim()) next.phone = phone;
      if (!f.notes.trim()) next.notes = text;
      return next;
    });
    if (phone) checkDuplicate(phone);

    const url = extractUrlFromText(text);
    if (url) {
      setEnrichUrl(url);
      setForm((f) => {
        const next = { ...f };
        if (url.includes("google.") || url.includes("maps")) {
          next.google_maps_url = url;
        } else {
          next.website = url;
        }
        return next;
      });
    }
  }

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
      setForm((f) => ({
        ...f,
        business_name: d.company || f.business_name,
        website: d.website || f.website,
        google_maps_url: d.google_maps_url || f.google_maps_url,
        lead_source: d.lead_source || f.lead_source,
        email: d.email || f.email,
        pin_code: d.pin_code || f.pin_code,
        city: d.city || f.city,
        location: d.location || f.location,
        phone: d.phone || f.phone,
        year_of_establishment: d.year_of_establishment || f.year_of_establishment,
        owner_name: d.owner_name || f.owner_name,
        number_of_employees: d.number_of_employees || f.number_of_employees,
        company_size: d.company_size || f.company_size,
        country: d.country || f.country,
        notes: d.notes || f.notes,
        enrichment_notes: d.enrichment_notes || f.enrichment_notes,
      }));
      if (d.industry) setIndustry(parseMultiValue(d.industry));
    } catch {
      setEnrichMessage("Could not fetch.");
    }
  }

  async function save() {
    if (!editId && duplicateFound) {
      alert("Duplicate lead exists with this phone number. Please use the existing lead instead.");
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
      industry: industry.join(", "),
      industry_primary: industry[0] || "",
      raw_industry: industry.join(", "),
      manufacturing_capabilities: capabilities.join(", "),
      address: form.address.trim(),
      notes: form.notes.trim(),
      company: form.business_name.trim(),
      lead_stage: form.lead_stage || "",
      pin_code: form.pin_code.trim(),
      location: form.location.trim(),
      country: form.country.trim(),
      year_of_establishment: form.year_of_establishment.trim(),
      owner_name: form.owner_name.trim(),
      number_of_employees: form.number_of_employees.trim(),
      company_size: form.company_size.trim(),
      enrichment_notes: form.enrichment_notes.trim(),
      qualification_done: false,
      worth_talking: false,
      l2_done: form.l2_done,
      latest_transcript: form.latest_transcript.trim(),
    };

    const url = editId
      ? `/api/business-leads/${business}/${editId}`
      : `/api/business-leads/${business}`;
    try {
      const res = await fetch(url, {
        method: editId ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const json = await res.json();
      if (!json.ok) {
        alert(json.error || "Failed to save lead");
        return;
      }
      window.location.reload();
    } catch {
      alert("Failed to save lead");
    }
  }

  function onMultiChange(setter) {
    return (e) =>
      setter(Array.from(e.target.selectedOptions).map((o) => o.value));
  }

  return (
    <div
      className={`modal ${open ? "open" : ""}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="modal-card">
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: 12,
            alignItems: "center",
            marginBottom: 14,
          }}
        >
          <div style={{ fontSize: 22, fontWeight: 900 }}>
            {editId ? `Edit Lead #${editId}` : "Add Lead"}
          </div>
          <button className="btn" type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="panel">
          <h2 style={{ marginTop: 0 }}>Quick Enrichment</h2>
          <div className="search-row">
            <input
              placeholder="Paste website, Google Maps link, or Yelp link"
              value={enrichUrl}
              onChange={(e) => setEnrichUrl(e.target.value)}
            />
            <button className="btn btn-primary" type="button" onClick={enrich}>
              Fetch Info
            </button>
            <button className="btn" type="button" onClick={resetForm}>
              Clear
            </button>
          </div>
          <div className="muted" style={{ marginTop: 10 }}>
            {enrichMessage}
          </div>
        </div>

        <div className="form-grid">
          <div className="form-field" style={{ gridColumn: "1 / -1" }}>
            <label>Smart Add</label>
            <textarea
              placeholder="Paste anything: phone, company, city, website, Google Maps link, WhatsApp text, CNC/laser/capability notes..."
              value={smartPaste}
              onChange={onSmartPaste}
              style={{ minHeight: 90 }}
            />
            <div className="hint">
              Example: Sharma CNC Rajkot +919876543210 does CNC turning and laser
              cutting
            </div>
          </div>

          {duplicate ? (
            <div style={{ gridColumn: "1 / -1" }}>
              {duplicate.state === "checking" ? (
                <div style={{ padding: 10, borderRadius: 12, background: "rgba(255,255,255,0.06)" }}>
                  Checking duplicate...
                </div>
              ) : duplicate.state === "error" ? (
                <div style={{ padding: 10, borderRadius: 12, background: "rgba(239,107,115,0.14)" }}>
                  Could not check duplicate.
                </div>
              ) : duplicate.state === "duplicate" ? (
                <div
                  style={{
                    padding: 12,
                    borderRadius: 12,
                    background: "rgba(239,107,115,0.16)",
                    border: "1px solid rgba(239,107,115,0.35)",
                  }}
                >
                  <strong>Duplicate found by phone number.</strong>
                  <br />
                  {`Lead #${duplicate.lead?.id} — ${
                    duplicate.lead?.company ||
                    duplicate.lead?.business_name ||
                    duplicate.lead?.contact_name ||
                    "Existing lead"
                  }`}
                  {duplicate.lead?.city ? ` · ${duplicate.lead.city}` : ""}
                  {duplicate.lead?.status ? ` · ${duplicate.lead.status}` : ""}
                </div>
              ) : (
                <div
                  style={{
                    padding: 10,
                    borderRadius: 12,
                    background: "rgba(88,201,138,0.14)",
                    border: "1px solid rgba(88,201,138,0.28)",
                  }}
                >
                  No duplicate found. Safe to add.
                </div>
              )}
            </div>
          ) : null}

          <div className="form-field">
            <label>Lead Type</label>
            <select value={form.lead_category} onChange={setField("lead_category")}>
              {LEAD_CATEGORY_OPTIONS.map((o) => (
                <option value={o.value} key={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>

          <div className="form-field">
            <label>Status</label>
            <select value={form.status} onChange={setField("status")}>
              {LEAD_STATUS_OPTIONS.map((o) => (
                <option value={o.value} key={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>

          <div className="form-field">
            <label>Phone</label>
            <input value={form.phone} onChange={onPhoneChange} placeholder="+91..." />
          </div>

          <div className="form-field">
            <label>Lead Source</label>
            <select value={form.lead_source} onChange={setField("lead_source")}>
              {LEAD_SOURCE_OPTIONS.map((o) => (
                <option value={o.value} key={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>

          <div className="form-field" style={{ gridColumn: "1 / -1" }}>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr",
                gap: 12,
                padding: 12,
                border: "1px solid rgba(255,255,255,0.10)",
                borderRadius: 12,
                background: "rgba(255,255,255,0.03)",
              }}
            >
              <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", fontWeight: 800 }}>
                <input
                  type="checkbox"
                  style={{ margin: 0, width: "auto" }}
                  checked={form.l2_done}
                  onChange={(e) => setForm((f) => ({ ...f, l2_done: e.target.checked }))}
                />
                <span>L2 Done</span>
              </label>
            </div>
          </div>

          <div className="form-field">
            <label>Lead Stage</label>
            <select value={form.lead_stage} onChange={setField("lead_stage")}>
              {LEAD_STAGE_OPTIONS.map((o) => (
                <option value={o.value} key={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>

          <div className="form-field">
            <label>Business / Organization Name</label>
            <input value={form.business_name} onChange={setField("business_name")} />
          </div>

          <div className="form-field">
            <label>Contact Name</label>
            <input value={form.contact_name} onChange={setField("contact_name")} />
          </div>

          <div className="form-field">
            <label>Website</label>
            <input value={form.website} onChange={setField("website")} />
          </div>

          <div className="form-field">
            <label>City</label>
            <input value={form.city} onChange={setField("city")} />
          </div>

          <div className="form-field">
            <label>State</label>
            <input value={form.state} onChange={setField("state")} />
          </div>

          <div className="form-field">
            <label>Industry</label>
            <select multiple size={6} value={industry} onChange={onMultiChange(setIndustry)}>
              {RASSET_INDUSTRY_OPTIONS.map((x) => (
                <option value={x} key={x}>
                  {x}
                </option>
              ))}
            </select>
            <div className="hint">Hold Cmd/Ctrl to select multiple.</div>
          </div>

          <div className="form-field">
            <label>Capabilities</label>
            <select
              multiple
              size={6}
              value={capabilities}
              onChange={onMultiChange(setCapabilities)}
            >
              {RASSET_CAPABILITY_OPTIONS.map((x) => (
                <option value={x} key={x}>
                  {x}
                </option>
              ))}
            </select>
            <div className="hint">Select all matching capabilities.</div>
          </div>

          <div className="form-field" style={{ gridColumn: "1 / -1" }}>
            <label>Notes</label>
            <textarea value={form.notes} onChange={setField("notes")} />
          </div>

          <div className="form-field" style={{ gridColumn: "1 / -1" }}>
            <button
              className="btn"
              type="button"
              onClick={() => setShowAdvanced((s) => !s)}
              style={{ width: "100%", justifyContent: "center" }}
            >
              Show / Hide Advanced Fields
            </button>
          </div>

          <div
            style={{ gridColumn: "1 / -1", display: showAdvanced ? "block" : "none" }}
          >
            <div className="form-grid">
              <div className="form-field">
                <label>Pin Code</label>
                <input value={form.pin_code} onChange={setField("pin_code")} />
              </div>
              <div className="form-field">
                <label>Location</label>
                <input value={form.location} onChange={setField("location")} />
              </div>
              <div className="form-field">
                <label>Country</label>
                <input value={form.country} onChange={setField("country")} />
              </div>
              <div className="form-field">
                <label>Year of Establishment</label>
                <input
                  value={form.year_of_establishment}
                  onChange={setField("year_of_establishment")}
                />
              </div>
              <div className="form-field">
                <label>Owner</label>
                <input value={form.owner_name} onChange={setField("owner_name")} />
              </div>
              <div className="form-field">
                <label>No. of Employees</label>
                <input
                  value={form.number_of_employees}
                  onChange={setField("number_of_employees")}
                />
              </div>
              <div className="form-field">
                <label>Company Size</label>
                <input value={form.company_size} onChange={setField("company_size")} />
              </div>
              <div className="form-field" style={{ gridColumn: "1 / -1" }}>
                <label>Enrichment Notes</label>
                <textarea
                  value={form.enrichment_notes}
                  onChange={setField("enrichment_notes")}
                />
              </div>
              <div className="form-field">
                <label>Email</label>
                <input value={form.email} onChange={setField("email")} />
              </div>
              <div className="form-field">
                <label>Google Maps URL</label>
                <input
                  value={form.google_maps_url}
                  onChange={setField("google_maps_url")}
                />
              </div>
              <div className="form-field">
                <label>Yelp URL</label>
                <input value={form.yelp_url} onChange={setField("yelp_url")} />
              </div>
              <div className="form-field">
                <label>Address</label>
                <input value={form.address} onChange={setField("address")} />
              </div>
              <div className="form-field" style={{ gridColumn: "1 / -1" }}>
                <label>Latest Transcript / Summary</label>
                <textarea
                  value={form.latest_transcript}
                  onChange={setField("latest_transcript")}
                />
              </div>
            </div>
          </div>
        </div>

        <div style={{ display: "flex", justifyContent: "flex-end", gap: 10, marginTop: 16 }}>
          <button className="btn" type="button" onClick={onClose}>
            Cancel
          </button>
          <button className="btn btn-primary" type="button" onClick={save}>
            Save Lead
          </button>
        </div>
      </div>
    </div>
  );
}
