// Markup for GET /leads/:business.
//
// Body markup extracted verbatim from renderBusinessLeadsPage() (lib/server/app.js
// lines 25135-27698). The document shell now comes from
// app/layout.jsx, the <style> block from ./business-leads.css, and the inline
// <script> from public/js/.

import { RASSET_CAPABILITY_OPTIONS, RASSET_INDUSTRY_OPTIONS, renderMultiSelectOptions } from "@/lib/server/constants.js";
import { badgeClass, escapeHtml, formatDateTime } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderBusinessLeadsPage(data) {
  const business = data.business;
  const rows = data.rows || [];
  const selectedTab = data.selectedTab || "all";
  const counts = data.counts || {};
  const search = data.search || "";
  const pagination = data.pagination || {};
  const filters = data.filters || {};
  // Embedded mode: rendered inside the client workspace Leads tab via an
  // iframe. Hides the global top nav and the standalone page header so it reads
  // as a panel rather than a full page.
  const embed = !!data.embed;
  const filterQuery = new URLSearchParams({
    tab: selectedTab,
    search: search || "",
    industry: filters.industry || "",
    capability: filters.capability || "",
    entity_type: filters.entity_type || "",
    status: filters.status || "",
    city: filters.city || "",
    state: filters.state || "",
    assigned_to: filters.assigned_to || "",
    qualified: filters.qualified || "",
    worth_talking: filters.worth_talking || "",
    has_call_transcription: filters.has_call_transcription || "",
  }).toString();
  const tabLink = (key, label, count) => `
    <a class="tab ${selectedTab === key ? "active" : ""}"
       href="/leads/${encodeURIComponent(business)}?tab=${key}&search=${encodeURIComponent(search)}">
      ${label} (${count || 0})
    </a>
  `;

  const leadRowsHtml =
    selectedTab !== "voice_inbox"
      ? rows.length
        ? rows
            .map(
              (lead) => `
              <tr>
                <td class="lead-name-cell">
                  <div class="lead-company-name">
                    ${escapeHtml(lead.company || lead.business_name || lead.company_name || "Lead #" + lead.id)}
                    ${lead.factory_setup === "multiple_sites" ? `<span class="mini-chip">Multi-site</span>` : ""}
                  </div>

                  <div class="muted lead-contact-line">
                    ${escapeHtml([lead.contact_name || lead.owner_name, lead.phone].filter(Boolean).join(" · ") || "-")}
                  </div>

                  ${
                    lead.last_spoke_to_name
                      ? `
                        <div style="font-size:12px; margin-top:4px;">
                          <strong>Spoke to:</strong> ${escapeHtml(lead.last_spoke_to_name)}
                        </div>
                      `
                      : ""
                  }
                </td>

<td>
  <div class="lead-chip-row">
    ${
      lead.manufacturing_capabilities
        ? String(lead.manufacturing_capabilities)
            .split(",")
            .filter(Boolean)
            .slice(0, 4)
            .map(
              (x) => `<span class="lead-chip">${escapeHtml(x.trim())}</span>`,
            )
            .join("")
        : `<span class="muted">No capabilities</span>`
    }
  </div>
</td>

                <td>
                  <div><strong>${escapeHtml(lead.industry_primary || lead.industry || "-")}</strong></div>
                  <div class="muted">${escapeHtml(lead.raw_industry || "")}</div>

                  <div class="lead-chip-row">
                    ${lead.entity_type ? `<span class="lead-chip">${escapeHtml(lead.entity_type)}</span>` : ""}
                    ${lead.company_size ? `<span class="lead-chip">${escapeHtml(lead.company_size)}</span>` : ""}
                    ${lead.assigned_to ? `<span class="lead-chip">${escapeHtml(lead.assigned_to)}</span>` : ""}
                  </div>
                </td>

                <td>
                  <div>${escapeHtml([lead.city, lead.state, lead.country].filter(Boolean).join(", ") || "-")}</div>
                  <div class="muted">${escapeHtml(lead.pin_code || lead.location || "")}</div>
                </td>

                <td style="text-align:left; padding:10px 12px; min-width:135px;">
                  <div style="display:flex; flex-direction:column; gap:7px; align-items:flex-start;">
                    <label style="display:flex; align-items:center; gap:8px; cursor:pointer; white-space:nowrap;">
                      <input
                        type="checkbox"
                        style="margin:0; width:auto;"
                        ${lead.l2_done ? "checked" : ""}
                        onclick="toggleLeadCheckbox(event, '${escapeHtml(business)}', ${Number(lead.id)}, 'l2_done', this.checked)"
                      />
                      <span>L2 Done</span>
                    </label>

                  </div>
                </td>

                <td>
                  ${
                    business === "joolian"
                      ? `
      <div>${escapeHtml(lead.activity_category || lead.industry || "-")}</div>
      <div class="muted">${escapeHtml(lead.sub_activity_category || "")}</div>
      <div class="muted">Ages: ${escapeHtml(lead.age_group || "-")}</div>
      <div class="muted">Type: ${escapeHtml(lead.type_of_business || lead.company_size || "-")}</div>
      <div class="muted">Price: ${escapeHtml(lead.pricing_approx || "-")}</div>
    `
                      : `
      <div>${escapeHtml(lead.industry || "-")}</div>
      <div class="muted">Emp: ${escapeHtml(lead.number_of_employees || "-")}</div>
      <div class="muted">Machines: ${escapeHtml(lead.machine_count || "-")}</div>
    `
                  }                </td>

                <td>
                  <button class="btn" type="button" onclick="openCallSummaryModal('${escapeHtml(business)}', '${escapeHtml(lead.phone || "")}')">
                    Calls
                  </button>
                </td>

                <td class="actions-cell">
<button class="kebab-btn" type="button" onclick="toggleLeadActions(event, ${Number(lead.id)})">...</button>
                  <div id="leadActions-${Number(lead.id)}" class="lead-actions-menu">
                  <button type="button" onclick="openLeadEditModal(${Number(lead.id)})">Edit</button>
                  <button type="button" onclick="openLeadCallsModal('${escapeHtml(business)}', ${Number(lead.id)})">
  Save L2 Data / Calls
</button>

<button type="button" onclick="updateBusinessLeadStatus('${escapeHtml(business)}', ${Number(lead.id)}, 'new')">Mark New</button>
<button type="button" onclick="updateBusinessLeadStatus('${escapeHtml(business)}', ${Number(lead.id)}, 'in_progress')">Mark In Progress</button>
<button type="button" onclick="updateBusinessLeadStatus('${escapeHtml(business)}', ${Number(lead.id)}, 'completed')">Mark Completed</button>
<button type="button" class="danger-menu-item" onclick="deleteBusinessLead('${escapeHtml(business)}', ${Number(lead.id)})">Delete</button>  
                    
                  </div>
                </td>
              </tr>
            `,
            )
            .join("")
        : `<tr><td colspan="7" class="empty-cell">No leads found.</td></tr>`
      : "";

  const renderConversationRows = (lead) => {
    const rows = Array.isArray(lead.conversation_rows)
      ? lead.conversation_rows
      : [];

    if (!rows.length) {
      return `
      <textarea id="translated-${Number(lead.id)}" class="compact-transcript-textarea">${escapeHtml(lead.translated_text || "")}</textarea>
    `;
    }

    return `
    <div class="conversation-thread">
      ${rows
        .map((row, index) => {
          const speaker =
            row.speaker || row.person || row.role || `Person ${index + 1}`;
          const text = row.text || row.message || row.content || "";

          return `
            <div class="conversation-row">
              <div class="speaker-pill">${escapeHtml(speaker)}</div>
              <div class="conversation-text">${escapeHtml(text)}</div>
            </div>
          `;
        })
        .join("")}
    </div>

    <textarea id="translated-${Number(lead.id)}" class="compact-transcript-textarea hidden-transcript">${escapeHtml(lead.translated_text || "")}</textarea>
  `;
  };

  const voiceInboxHtml =
    selectedTab === "voice_inbox"
      ? rows.length
        ? `
        <div style="display:flex; justify-content:flex-end; margin-bottom:12px;">
          <button class="btn btn-danger" type="button" onclick="deleteSelectedVoiceUploads()">
            Delete Selected Voice Messages
          </button>
        </div>

${rows
  .map(
    (lead) => `
    <div class="lead-card compact-voice-card" id="lead-card-${Number(lead.id)}">

<!-- HEADER -->
<div class="voice-header">
  <div class="voice-main">
    <div class="voice-title-row">
      <label class="voice-check" title="Select for bulk delete">
        <input type="checkbox" class="voice-delete-checkbox" value="${Number(lead.id)}">
      </label>

      <div>
        <div class="voice-title">Voice Lead #${escapeHtml(lead.id)}</div>
        <div class="voice-meta">
          ${escapeHtml(formatDateTime(lead.created_at))}
          · ${escapeHtml(lead.lead_phone)}
          · ${escapeHtml(lead.sender_phone)}
        </div>
      </div>
    </div>
  </div>

  <div class="voice-side">
    <span class="${badgeClass(lead.status)}">${escapeHtml(lead.status)}</span>

    <button class="kebab-btn" type="button" onclick="toggleVoiceActions(event, ${Number(lead.id)})">⋯</button>

    <div id="voiceActions-${Number(lead.id)}" class="lead-actions-menu">
      <button type="button" onclick="deleteVoiceUpload(${Number(lead.id)})">Delete Voice</button>

      ${
        lead.status === "pending_review"
          ? `
            <button type="button" onclick="saveTranscript(${Number(lead.id)})">Save</button>
            <button type="button" onclick="approveLead(${Number(lead.id)})">Approve</button>
            <button type="button" onclick="rejectLead(${Number(lead.id)})">Reject</button>
            <button type="button" onclick="deleteVoiceTranscript(${Number(lead.id)})">Delete Transcript</button>
          `
          : ""
      }

      ${
        lead.status === "rejected"
          ? `<button type="button" onclick="saveTranscript(${Number(lead.id)})">Edit & Reopen</button>`
          : ""
      }
    </div>
  </div>
</div>
      <!-- AUDIO -->
      <div class="voice-audio">
        <audio controls preload="none">
          <source src="/api/lead-voice-uploads/${Number(lead.id)}/audio" type="${escapeHtml(lead.media_content_type || "audio/mpeg")}">
        </audio>
      </div>
      
<details class="voice-transcript">
  <summary>
    <span>🗣 Transcript</span>
    <span class="transcript-preview">
      ${escapeHtml((lead.translated_text || "").slice(0, 160))}
      ${(lead.translated_text || "").length > 160 ? "..." : ""}
    </span>
  </summary>

  <textarea id="translated-${Number(lead.id)}" class="transcript-textarea">${escapeHtml(lead.translated_text || "")}</textarea>
</details>

    </div>
  `,
  )
  .join("")}

      `
        : `<div class="panel">No voice leads need review.</div>`
      : "";

  return `
            ${
              // The original set <body class="embed"> so `body.embed .wrap`
              // applies. The document shell is now app/layout.jsx, which a page
              // cannot add attributes to, so the class is set during parsing —
              // before .wrap is parsed, so it still styles the first paint.
              embed
                ? '<script>document.body.classList.add("embed");</script>'
                : ""
            }
            ${embed ? "" : renderTopNav("leads")}

        <div class="wrap">
          ${
            embed
              ? ""
              : `<div class="topbar">
            <div>
              <div class="eyebrow">Business Lead CRM</div>
              <h1>${escapeHtml(business)} Leads</h1>
              <div class="subtitle">All leads, B2B/B2C split, manual onboarding, search, and voice inbox.</div>
            </div>
            <div style="display:flex; gap:10px; flex-wrap:wrap;">
              <a class="btn" href="/leads">← Leads Overview</a>
<a class="btn" href="/leads/${encodeURIComponent(business)}/intelligence">Intelligence</a>
<button class="btn btn-primary" type="button" onclick="openLeadCreateModal()">+ Add Lead</button>
            </div>
          </div>`
          }

          ${
            embed
              ? ""
              : `<div class="stats">
            <div class="stat-card"><div class="stat-label">All Leads</div><div class="stat-value">${counts.all || 0}</div></div>
            <div class="stat-card"><div class="stat-label">B2B</div><div class="stat-value">${counts.b2b || 0}</div></div>
            <div class="stat-card"><div class="stat-label">B2C</div><div class="stat-value">${counts.b2c || 0}</div></div>
            <div class="stat-card"><div class="stat-label">Voice Inbox</div><div class="stat-value">${counts.voice_inbox || 0}</div></div>
          </div>`
          }

          <div class="tabs">
            ${tabLink("all", "All Leads", counts.all)}
            ${tabLink("b2b", "B2B", counts.b2b)}
            ${tabLink("b2c", "B2C", counts.b2c)}
            ${tabLink("in_progress", "In Progress", counts.in_progress)}
            ${tabLink("completed", "Completed", counts.completed)}
            ${tabLink("voice_inbox", "Voice Inbox", counts.voice_inbox)}
          </div>
        
          ${
            selectedTab !== "voice_inbox"
              ? `
                <div class="panel">
                  <form method="GET" action="/leads/${encodeURIComponent(business)}">
                    <input type="hidden" name="tab" value="${escapeHtml(selectedTab)}" />

                    ${
                      business === "rasset"
                        ? `
                          <div class="advanced-filter-grid">
                            <input
                              name="search"
                              value="${escapeHtml(search)}"
                              placeholder="Search company, phone, city, CNC, laser, owner, notes..."
                            />

                            <select name="industry">
  <option value="">All Industries</option>
  ${RASSET_INDUSTRY_OPTIONS.map(
    (x) =>
      `<option value="${escapeHtml(x)}" ${
        filters.industry === x ? "selected" : ""
      }>${escapeHtml(x)}</option>`,
  ).join("")}
</select>

<select name="capability">
  <option value="">All Capabilities</option>
  ${RASSET_CAPABILITY_OPTIONS.map(
    (x) =>
      `<option value="${escapeHtml(x)}" ${
        filters.capability === x ? "selected" : ""
      }>${escapeHtml(x)}</option>`,
  ).join("")}
</select>

                            <select name="entity_type">
                              <option value="">All Entity Types</option>
                              ${[
                                "Factory",
                                "Service Provider",
                                "Trading Company",
                                "Supplier",
                                "Training Institute",
                              ]
                                .map(
                                  (x) =>
                                    `<option value="${escapeHtml(x)}" ${filters.entity_type === x ? "selected" : ""}>${escapeHtml(x)}</option>`,
                                )
                                .join("")}
                            </select>

                            <select name="status">
                              <option value="">All Status</option>
                              ${[
                                "new",
                                "working",
                                "busy",
                                "unreachable",
                                "invalid",
                                "unsure",
                                "in_progress",
                                "completed",
                              ]
                                .map(
                                  (x) =>
                                    `<option value="${escapeHtml(x)}" ${filters.status === x ? "selected" : ""}>${escapeHtml(x)}</option>`,
                                )
                                .join("")}
                            </select>

                            <input
                              name="city"
                              value="${escapeHtml(filters.city || "")}"
                              placeholder="City"
                            />

                            <input
                              name="state"
                              value="${escapeHtml(filters.state || "")}"
                              placeholder="State"
                            />

                            <input
                              name="assigned_to"
                              value="${escapeHtml(filters.assigned_to || "")}"
                              placeholder="Assigned to"
                            />

                            <select name="qualified">
                              <option value="">Qualified?</option>
                              <option value="yes" ${filters.qualified === "yes" ? "selected" : ""}>Qualified</option>
                              <option value="no" ${filters.qualified === "no" ? "selected" : ""}>Not Qualified</option>
                            </select>

                            <select name="worth_talking">
                              <option value="">Worth Talking?</option>
                              <option value="yes" ${filters.worth_talking === "yes" ? "selected" : ""}>Worth Talking</option>
                              <option value="no" ${filters.worth_talking === "no" ? "selected" : ""}>Not Worth Talking</option>
                            </select>
                            
                            
<select name="has_call_transcription">
  <option value="">Call Transcription?</option>
  <option value="yes" ${filters.has_call_transcription === "yes" ? "selected" : ""}>Has transcription</option>
  <option value="no" ${filters.has_call_transcription === "no" ? "selected" : ""}>No transcription</option>
</select>
                            
                          </div>
                        `
                        : `
                          <div class="search-row">
                            <input
                              name="search"
                              value="${escapeHtml(search)}"
                              placeholder="Search phone, business, contact, city, notes..."
                            />
                            <button class="btn btn-primary" type="submit">Search</button>
                            <a class="btn" href="/leads/${encodeURIComponent(business)}?tab=${escapeHtml(selectedTab)}">Clear</a>
                          </div>
                        `
                    }

                    ${
                      business === "rasset"
                        ? `
                          <div style="display:flex; gap:10px; margin-top:12px; flex-wrap:wrap;">
                            <button class="btn btn-primary" type="submit">Search / Filter</button>
                            <a class="btn" href="/leads/${encodeURIComponent(business)}?tab=${escapeHtml(selectedTab)}">Clear</a>
                          </div>
                        `
                        : ""
                    }
                  </form>
                </div>

${
  business === "rasset"
    ? `
  <div style="margin-bottom:12px;">
    
<button class="btn btn-primary" onclick="toggleUploadBox('rassetUploadBox')" 
  title="Upload Excel with Company, Email, Phone, Industry, Location, etc.">
  ＋ Import Rasset Excel
</button>

<a class="btn" href="/leads/rasset/imports">Import Logs</a>

    <div id="rassetUploadBox" style="display:none; margin-top:10px;">
<div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
  <input id="rassetExcelFile" type="file" accept=".xlsx,.xls,.csv" />
  <button class="btn btn-primary" type="button" onclick="uploadRassetExcel()">Upload</button>
</div>
      <div class="muted" style="margin-top:6px; font-size:12px;">
        Supports: Company, Website, Email, Industry, City, Phone, Owner, Employees, Size, Country
      </div>
    </div>

  </div>
`
    : ""
}



${
  business === "joolian"
    ? `
  <div style="margin-bottom:12px;">
    
    <button class="btn btn-primary" onclick="toggleUploadBox('joolianUploadBox')" 
      title="Upload Excel with AP details, category, pricing, etc.">
      ＋ Import Joolian Excel
    </button>

    <div id="joolianUploadBox" style="display:none; margin-top:10px;">
<div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
  <input id="joolianB2BExcelFile" type="file" accept=".xlsx,.xls,.csv" />
  <button class="btn btn-primary" type="button" onclick="uploadJoolianB2BExcel()">Upload</button>
</div>

      <div class="muted" style="margin-top:6px; font-size:12px;">
        Supports: AP Name, Phone, Email, City, Category, Pricing, Owner, etc.
      </div>
    </div>

  </div>
`
    : ""
}

                <div class="panel">
                  <table>
                    <thead>
                      <tr>
<th>Company / Contact</th>
<th>Category / Capability</th>
<th>Industry / Entity</th>
<th>Location</th>
<th>Lead Quality</th>
<th>Call Summary</th>
<th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>${leadRowsHtml}</tbody>
                  </table>

                  <div class="pagination">
${
  pagination.hasPrev
    ? `<a class="btn" href="/leads/${encodeURIComponent(business)}?${filterQuery}&page=${Number(pagination.page) - 1}">← Previous</a>`
    : ""
}
<span class="btn">Page ${escapeHtml(pagination.page || 1)}</span>
${
  pagination.hasNext
    ? `<a class="btn" href="/leads/${encodeURIComponent(business)}?${filterQuery}&page=${Number(pagination.page) + 1}">Next →</a>`
    : ""
}
                  </div>
                </div>
              `
              : `<div class="lead-list">${voiceInboxHtml}</div>`
          }
        </div>
        
        <div id="callSummaryModal" class="modal" onclick="closeCallSummaryModal(event)">
  <div class="modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="callSummaryTitle" style="font-size:22px; font-weight:900;">Call Summaries</div>
      <button class="btn" type="button" onclick="closeCallSummaryModal()">Close</button>
    </div>

    <div id="callSummaryBody" class="muted">Loading...</div>
  </div>
</div>

<div id="leadCallsModal" class="modal" onclick="closeLeadCallsModal(event)">
  <div class="modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:900;">Save L2 Data</div>
      <button class="btn" type="button" onclick="closeLeadCallsModal()">Close</button>
    </div>

    <div class="panel">
      <h2 style="margin-top:0;">Quick L2 Update</h2>

      <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
        <input id="l2SpokeToName" placeholder="Person spoken to" />
        <input id="l2Designation" placeholder="Designation" />
<select id="l2Industry" multiple size="6">
  ${renderMultiSelectOptions(RASSET_INDUSTRY_OPTIONS)}
</select>

<select id="l2Capability" multiple size="6">
  ${renderMultiSelectOptions(RASSET_CAPABILITY_OPTIONS)}
</select>

        <select id="l2Behavior">
          <option value="">Behavior</option>
          <option value="helpful">Helpful</option>
          <option value="busy">Busy</option>
          <option value="not_helpful">Not helpful</option>
          <option value="rude">Rude</option>
          <option value="interested">Interested</option>
          <option value="not_interested">Not interested</option>
        </select>

        <select id="l2CallOutcome">
          <option value="">Call Outcome</option>
          <option value="connected">Connected</option>
          <option value="busy">Busy</option>
          <option value="wrong_number">Wrong number</option>
          <option value="owner_not_available">Owner not available</option>
          <option value="callback_requested">Callback requested</option>
          <option value="not_relevant">Not relevant</option>
        </select>
      </div>

      <textarea id="l2Notes" placeholder="Short notes" style="margin-top:10px; width:100%; min-height:80px;"></textarea>

      <button
  class="btn btn-primary"
  type="button"
  onclick="saveLeadL2Data()"
  style="margin-top:12px;"
>
  Save L2 Data
</button>
    </div>

    <div class="panel">
      <h2 style="margin-top:0;">Call Audio / Transcript / Translation</h2>
      <div id="leadCallsList" class="muted">Loading...</div>
    </div>
  </div>
</div>


        <div id="leadModal" class="modal" onclick="closeLeadModal(event)">
          <div class="modal-card" onclick="event.stopPropagation()">
            <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
              <div id="leadModalTitle" style="font-size:22px; font-weight:900;">Add Lead</div>
              <button class="btn" type="button" onclick="closeLeadModal()">Close</button>
            </div>

            <input id="leadId" type="hidden" />

            <div class="panel">
              <h2 style="margin-top:0;">Quick Enrichment</h2>
              <div class="search-row">
                <input id="enrichUrl" placeholder="Paste website, Google Maps link, or Yelp link" />
                <button class="btn btn-primary" type="button" onclick="enrichLeadUrl()">Fetch Info</button>
                <button class="btn" type="button" onclick="clearLeadForm()">Clear</button>
              </div>
              <div id="enrichMessage" class="muted" style="margin-top:10px;"></div>
            </div>

            <div class="form-grid">
            <div class="form-field" style="grid-column:1 / -1;">
  <label>Smart Add</label>
  <textarea
    id="leadSmartPaste"
    placeholder="Paste anything: phone, company, city, website, Google Maps link, WhatsApp text, CNC/laser/capability notes..."
    oninput="smartParseLeadInput()"
    style="min-height:90px;"
  ></textarea>
  <div class="hint">
    Example: Sharma CNC Rajkot +919876543210 does CNC turning and laser cutting
  </div>
</div>

<div id="leadDuplicateMessage" style="grid-column:1 / -1; display:none;"></div>
              <div class="form-field">
                <label>Lead Type</label>
                <select id="leadCategory">
                  <option value="b2b">B2B</option>
                  <option value="b2c">B2C</option>
                </select>
              </div>

              <div class="form-field">
                <label>Status</label>
                <select id="leadStatus">
                  <option value="new">New</option>
                  <option value="in_progress">In Progress</option>
                  <option value="completed">Completed</option>
                </select>
              </div>

              <div class="form-field">
                <label>Phone</label>
                <input id="leadPhone" oninput="checkLeadPhoneDuplicate()" placeholder="+91..." />
              </div>

              <div class="form-field">
                <label>Lead Source</label>
                <select id="leadSource">
                  <option value="manual">Manual</option>
                  <option value="voice">Voice</option>
                  <option value="website">Website</option>
                  <option value="google_map">Google Map</option>
                  <option value="yelp">Yelp</option>
                </select>
              </div>
              
<div class="form-field" style="grid-column:1 / -1;">
  <div style="
    display:grid;
    grid-template-columns: 1fr;
    gap:12px;
    padding:12px;
    border:1px solid rgba(255,255,255,0.10);
    border-radius:12px;
    background:rgba(255,255,255,0.03);
  ">
    <label style="display:flex; align-items:center; gap:8px; cursor:pointer; font-weight:800;">
      <input id="leadL2Done" type="checkbox" style="margin:0; width:auto;" />
      <span>L2 Done</span>
    </label>
  </div>
</div>
              <div class="form-field">

<label>Lead Stage</label>
<select id="leadStage">
  <option value="">Select stage</option>
  <option value="new">New</option>
  <option value="prospect">Prospect</option>
  <option value="qualified">Qualified</option>
  <option value="not_fit">Not Fit</option>
  <option value="customer">Customer</option>

</select>
</div>

<div class="form-field">
  <label>Business / Organization Name</label>
  <input id="leadBusinessName" />
</div>

<div class="form-field">
  <label>Contact Name</label>
  <input id="leadContactName" />
</div>

<div class="form-field">
  <label>Website</label>
  <input id="leadWebsite" />
</div>

<div class="form-field">
  <label>City</label>
  <input id="leadCity" />
</div>

<div class="form-field">
  <label>State</label>
  <input id="leadState" />
</div>

<div class="form-field">
  <label>Industry</label>
  <select id="leadIndustry" multiple size="6">
    ${renderMultiSelectOptions(RASSET_INDUSTRY_OPTIONS)}
  </select>
  <div class="hint">Hold Cmd/Ctrl to select multiple.</div>
</div>

<div class="form-field">
  <label>Capabilities</label>
  <select id="leadCapabilities" multiple size="6">
    ${renderMultiSelectOptions(RASSET_CAPABILITY_OPTIONS)}
  </select>
  <div class="hint">Select all matching capabilities.</div>
</div>

<div class="form-field" style="grid-column:1 / -1;">
  <label>Notes</label>
  <textarea id="leadNotes"></textarea>
</div>

<div class="form-field" style="grid-column:1 / -1;">
  <button

    class="btn"
    type="button"
    onclick="toggleLeadAdvancedFields()"
    style="width:100%; justify-content:center;"
  >
    Show / Hide Advanced Fields
  </button>
</div>

<div id="leadAdvancedFields" style="grid-column:1 / -1; display:none;">
  <div class="form-grid">

<div class="form-field">
  <label>Pin Code</label>
  <input id="leadPinCode" />
</div>

<div class="form-field">
  <label>Location</label>
  <input id="leadLocation" />
</div>

<div class="form-field">
  <label>Country</label>
  <input id="leadCountry" />
</div>

<div class="form-field">
  <label>Year of Establishment</label>
  <input id="leadYearOfEstablishment" />
</div>

<div class="form-field">
  <label>Owner</label>
  <input id="leadOwnerName" />
</div>

<div class="form-field">
  <label>No. of Employees</label>
  <input id="leadNumberOfEmployees" />
</div>

<div class="form-field">
  <label>Company Size</label>
  <input id="leadCompanySize" />
</div>

<div class="form-field" style="grid-column:1 / -1;">
  <label>Enrichment Notes</label>
  <textarea id="leadEnrichmentNotes"></textarea>
</div>

              <div class="form-field">
                <label>Email</label>
                <input id="leadEmail" />
              </div>

              <div class="form-field">
                <label>Google Maps URL</label>
                <input id="leadGoogleMapsUrl" />
              </div>

              <div class="form-field">
                <label>Yelp URL</label>
                <input id="leadYelpUrl" />
              </div>

              <div class="form-field">
                <label>Address</label>
                <input id="leadAddress" />
              </div>

              <div class="form-field" style="grid-column:1 / -1;">
                <label>Latest Transcript / Summary</label>
                <textarea id="leadLatestTranscript"></textarea>
              </div>
            </div>
              </div>
</div>

            <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
              <button class="btn" type="button" onclick="closeLeadModal()">Cancel</button>
              <button class="btn btn-primary" type="button" onclick="saveBusinessLead()">Save Lead</button>
            </div>
          </div>
        </div>

        <script>

function toggleUploadBox(id) {
  const el = document.getElementById(id);
  if (!el) {
    console.error("Upload box not found:", id);
    return;
  }

  if (el.style.display === "none" || el.style.display === "") {
    el.style.display = "block";
  } else {
    el.style.display = "none";
  }
}

          const BUSINESS = ${JSON.stringify(business)};
          function getMultiSelectValues(id) {
  const el = document.getElementById(id);
  if (!el) return [];
  return Array.from(el.selectedOptions).map(function(option) {
    return option.value;
  });
}

function setMultiSelectValues(id, value) {
  const el = document.getElementById(id);
  if (!el) return;

  const values = String(value || "")
    .split(/[,;\\n]/)
    .map(function(x) { return x.trim(); })
    .filter(Boolean);

  Array.from(el.options).forEach(function(option) {
    option.selected = values.includes(option.value);
  });
}

function clearMultiSelect(id) {
  const el = document.getElementById(id);
  if (!el) return;

  Array.from(el.options).forEach(function(option) {
    option.selected = false;
  });
}
let leadDuplicateFound = false;
let leadDuplicateTimer = null;

function normalizePhoneClient(value) {
  return String(value || "").replace(/\D/g, "");
}

function extractPhoneFromText(text) {
  const match = String(text || "").match(/(?:\\+?\\d[\\d\\s().-]{8,}\\d)/);
  return match ? match[0].trim() : "";
}

function extractUrlFromText(text) {
  const match = String(text || "").match(/https?:\\/\\/[^\\s]+/i);
  return match ? match[0].trim() : "";
}

function smartParseLeadInput() {
  const text = document.getElementById("leadSmartPaste")?.value || "";

  const phone = extractPhoneFromText(text);
  const phoneInput = document.getElementById("leadPhone");

  if (phone && phoneInput && !phoneInput.value.trim()) {
    phoneInput.value = phone;
    checkLeadPhoneDuplicate();
  }

  const url = extractUrlFromText(text);
  if (url) {
    const enrichInput = document.getElementById("enrichUrl");
    if (enrichInput) enrichInput.value = url;

    const mapsInput = document.getElementById("leadGoogleMapsUrl");
    const websiteInput = document.getElementById("leadWebsite");

    if ((url.includes("google.") || url.includes("maps")) && mapsInput) {
      mapsInput.value = url;
    } else if (websiteInput) {
      websiteInput.value = url;
    }
  }

  const lower = text.toLowerCase();
  const capabilities = [];

  if (lower.includes("cnc")) capabilities.push("CNC Machining");
  if (lower.includes("laser")) capabilities.push("Laser Cutting");
  if (lower.includes("injection")) capabilities.push("Injection Molding");
  if (lower.includes("fabrication")) capabilities.push("Fabrication");
  if (lower.includes("casting")) capabilities.push("Casting");
  if (lower.includes("mould") || lower.includes("mold")) capabilities.push("Tool & Die Making");

  const capBox = document.getElementById("leadManufacturingCapabilities");
  if (capabilities.length && capBox && !capBox.value.trim()) {
    capBox.value = capabilities.join(", ");
  }

  const notesBox = document.getElementById("leadNotes");
  if (notesBox && !notesBox.value.trim()) {
    notesBox.value = text;
  }
}

async function checkLeadPhoneDuplicate() {
  clearTimeout(leadDuplicateTimer);

  leadDuplicateTimer = setTimeout(async function () {
    const phoneInput = document.getElementById("leadPhone");
    const phone = phoneInput ? phoneInput.value.trim() : "";
    const box = document.getElementById("leadDuplicateMessage");

    leadDuplicateFound = false;

    if (!box) return;

    if (!phone || normalizePhoneClient(phone).length < 8) {
      box.style.display = "none";
      box.innerHTML = "";
      return;
    }

    box.style.display = "block";
    box.innerHTML =
      '<div style="padding:10px;border-radius:12px;background:rgba(255,255,255,0.06);">Checking duplicate...</div>';

    const res = await fetch(
      "/api/business-leads/" + BUSINESS + "/check-phone?phone=" + encodeURIComponent(phone)
    );

    const json = await res.json();

    if (!json.ok) {
      box.innerHTML =
        '<div style="padding:10px;border-radius:12px;background:rgba(239,107,115,0.14);">Could not check duplicate.</div>';
      return;
    }

    if (json.data && json.data.duplicate) {
      leadDuplicateFound = true;
      const lead = json.data.lead || {};

      box.innerHTML =
        '<div style="padding:12px;border-radius:12px;background:rgba(239,107,115,0.16);border:1px solid rgba(239,107,115,0.35);">' +
          '<strong>Duplicate found by phone number.</strong><br/>' +
          'Lead #' + escapeHtmlClient(lead.id) + ' — ' +
          escapeHtmlClient(lead.company || lead.business_name || lead.contact_name || "Existing lead") +
          (lead.city ? " · " + escapeHtmlClient(lead.city) : "") +
          (lead.status ? " · " + escapeHtmlClient(lead.status) : "") +
        '</div>';
    } else {
      box.innerHTML =
        '<div style="padding:10px;border-radius:12px;background:rgba(88,201,138,0.14);border:1px solid rgba(88,201,138,0.28);">' +
          'No duplicate found. Safe to add.' +
        '</div>';
    }
  }, 350);
}
          function escapeHtmlClient(value) {
            return String(value ?? "")
              .replace(/&/g, "&amp;")
              .replace(/</g, "&lt;")
              .replace(/>/g, "&gt;")
              .replace(/"/g, "&quot;");
          }

function toggleLeadAdvancedFields() {
  const box = document.getElementById("leadAdvancedFields");
  if (!box) return;

  box.style.display = box.style.display === "none" ? "block" : "none";
}

          function openLeadCreateModal() {
            clearLeadForm();
            document.getElementById("leadModalTitle").textContent = "Add Lead";
            document.getElementById("leadModal").classList.add("open");
          }

          async function openLeadEditModal(id) {
            clearLeadForm();
            document.getElementById("leadModalTitle").textContent = "Edit Lead #" + id;
            document.getElementById("leadModal").classList.add("open");

            const res = await fetch("/api/business-leads/" + BUSINESS + "/" + id);
            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to load lead");
              return;
            }

            const lead = json.data || {};

            document.getElementById("leadId").value = lead.id || "";
            document.getElementById("leadCategory").value = lead.lead_category || "b2b";
            document.getElementById("leadStatus").value = lead.status || "new";
            document.getElementById("leadPhone").value = lead.phone || "";
            document.getElementById("leadSource").value = lead.lead_source || "manual";
            document.getElementById("leadBusinessName").value = lead.business_name || "";
            document.getElementById("leadContactName").value = lead.contact_name || "";
            document.getElementById("leadEmail").value = lead.email || "";
            document.getElementById("leadWebsite").value = lead.website || "";
            document.getElementById("leadGoogleMapsUrl").value = lead.google_maps_url || "";
            document.getElementById("leadYelpUrl").value = lead.yelp_url || "";
            document.getElementById("leadCity").value = lead.city || "";
            document.getElementById("leadState").value = lead.state || "";
            setMultiSelectValues("leadIndustry", lead.industry || lead.industry_primary || "");
setMultiSelectValues("leadCapabilities", lead.manufacturing_capabilities || "");
            document.getElementById("leadAddress").value = lead.address || "";
            document.getElementById("leadNotes").value = lead.notes || "";
            document.getElementById("leadLatestTranscript").value = lead.latest_transcript || "";
const leadStageEl = document.getElementById("leadStage");
if (leadStageEl) {
  leadStageEl.value = lead.lead_stage || "";
}
document.getElementById("leadPinCode").value = lead.pin_code || "";
document.getElementById("leadLocation").value = lead.location || "";
document.getElementById("leadCountry").value = lead.country || "";
document.getElementById("leadYearOfEstablishment").value = lead.year_of_establishment || "";
document.getElementById("leadOwnerName").value = lead.owner_name || "";
document.getElementById("leadNumberOfEmployees").value = lead.number_of_employees || "";
document.getElementById("leadCompanySize").value = lead.company_size || "";
document.getElementById("leadEnrichmentNotes").value = lead.enrichment_notes || "";

document.getElementById("leadL2Done").checked = !!lead.l2_done;
}

          function closeLeadModal(event) {
            if (event && event.target && event.target.id !== "leadModal") return;
            document.getElementById("leadModal").classList.remove("open");
          }

 function clearLeadForm() {
  [
    "leadId",
    "leadPhone",
    "leadBusinessName",
    "leadContactName",
    "leadEmail",
    "leadWebsite",
    "leadGoogleMapsUrl",
    "leadYelpUrl",
    "leadCity",
    "leadState",
    "leadIndustry",
    "leadAddress",
    "leadNotes",
    "leadLatestTranscript",
    "enrichUrl",
    "enrichMessage",
    "leadSmartPaste",
    "leadStage",
    "leadPinCode",
    "leadLocation",
    "leadCountry",
    "leadYearOfEstablishment",
    "leadOwnerName",
    "leadNumberOfEmployees",
    "leadCompanySize",
    "leadEnrichmentNotes"
  ].forEach(function(id) {
    const el = document.getElementById(id);
    if (!el) return;

    if (id === "enrichMessage") {
      el.textContent = "";
    } else {
      el.value = "";
    }
  });
  clearMultiSelect("leadIndustry");
clearMultiSelect("leadCapabilities");
clearMultiSelect("l2Industry");
clearMultiSelect("l2Capability");

  leadDuplicateFound = false;

  const duplicateBox = document.getElementById("leadDuplicateMessage");
  if (duplicateBox) {
    duplicateBox.style.display = "none";
    duplicateBox.innerHTML = "";
  }

  const leadCategory = document.getElementById("leadCategory");
  if (leadCategory) leadCategory.value = BUSINESS === "rasset" ? "b2b" : "b2c";

  const leadStatus = document.getElementById("leadStatus");
  if (leadStatus) leadStatus.value = "new";

  const leadSource = document.getElementById("leadSource");
  if (leadSource) leadSource.value = "manual";

const leadStage = document.getElementById("leadStage");
if (leadStage) leadStage.value = "";

  const l2Done = document.getElementById("leadL2Done");
  if (l2Done) l2Done.checked = false;

}
          
          async function toggleLeadCheckbox(event, business, id, field, value) {
  event.stopPropagation();

  const res = await fetch("/api/business-leads/" + business + "/" + id + "/quick-toggle", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      field,
      value
    })
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to update checkbox");
    event.target.checked = !value;
    return;
  }
}
          

async function deleteBusinessLead(business, id) {
  if (!confirm("Delete this lead and all related voice/call data? This cannot be undone.")) return;

  const res = await fetch("/api/business-leads/" + business + "/" + id, {
    method: "DELETE",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete lead");
    return;
  }

  window.location.reload();
}


async function uploadRassetExcel() {
  const input = document.getElementById("rassetExcelFile");

  if (!input || !input.files || !input.files[0]) {
    alert("Choose an Excel file first.");
    return;
  }

  const formData = new FormData();
  formData.append("file", input.files[0]);

  const res = await fetch("/api/rasset-leads/import-excel", {
    method: "POST",
    body: formData
  });

  const json = await res.json();

  if (!json.ok) {
    alert(["Excel import failed:", json.error || JSON.stringify(json)].join(String.fromCharCode(10)));
    console.error("Excel import failed:", json);
    return;
  }

  const d = json.data || {};

  alert([
    "Import complete",
    "Import ID: " + d.import_id,
    "Total: " + d.total,
    "Inserted: " + d.inserted,
    "Duplicates skipped: " + d.duplicates,
    "Skipped: " + d.skipped,
    "Errors: " + ((d.errors || []).length)
  ].join(String.fromCharCode(10)));

  window.location.href = "/leads/rasset/imports?import_id=" + d.import_id;
}


async function uploadJoolianB2BExcel() {
  const input = document.getElementById("joolianB2BExcelFile");

  if (!input || !input.files || !input.files[0]) {
    alert("Choose an Excel file first.");
    return;
  }

  const formData = new FormData();
  formData.append("file", input.files[0]);

  const res = await fetch("/api/joolian-leads/import-b2b-excel", {
    method: "POST",
    body: formData
  });

  const json = await res.json();

  if (!json.ok) {
    alert("Joolian B2B Excel import failed: " + (json.error || JSON.stringify(json)));
    console.error("Joolian import failed:", json);
    return;
  }

  const d = json.data || {};
  alert(
    "Joolian B2B import complete. Total: " + d.total +
    ", Inserted: " + d.inserted +
    ", Updated: " + d.updated +
    ", Skipped: " + d.skipped +
    ", Errors: " + (d.errors || []).length
  );

  window.location.reload();
}


          function getLeadPayloadFromForm() {
            return {
              phone: document.getElementById("leadPhone").value.trim(),
              lead_category: document.getElementById("leadCategory").value,
              status: document.getElementById("leadStatus").value,
              lead_source: document.getElementById("leadSource").value,
              business_name: document.getElementById("leadBusinessName").value.trim(),
              contact_name: document.getElementById("leadContactName").value.trim(),
              email: document.getElementById("leadEmail").value.trim(),
              website: document.getElementById("leadWebsite").value.trim(),
              google_maps_url: document.getElementById("leadGoogleMapsUrl").value.trim(),
              yelp_url: document.getElementById("leadYelpUrl").value.trim(),
              city: document.getElementById("leadCity").value.trim(),
              state: document.getElementById("leadState").value.trim(),
              industry: getMultiSelectValues("leadIndustry").join(", "),
industry_primary: getMultiSelectValues("leadIndustry")[0] || "",
raw_industry: getMultiSelectValues("leadIndustry").join(", "),
manufacturing_capabilities: getMultiSelectValues("leadCapabilities").join(", "),
              address: document.getElementById("leadAddress").value.trim(),
              notes: document.getElementById("leadNotes").value.trim(),
company: document.getElementById("leadBusinessName")?.value.trim() || "",
lead_stage: document.getElementById("leadStage")?.value || "",
pin_code: document.getElementById("leadPinCode")?.value.trim() || "",
location: document.getElementById("leadLocation")?.value.trim() || "",
country: document.getElementById("leadCountry")?.value.trim() || "",
year_of_establishment: document.getElementById("leadYearOfEstablishment")?.value.trim() || "",
owner_name: document.getElementById("leadOwnerName")?.value.trim() || "",
number_of_employees: document.getElementById("leadNumberOfEmployees")?.value.trim() || "",
company_size: document.getElementById("leadCompanySize")?.value.trim() || "",
enrichment_notes: document.getElementById("leadEnrichmentNotes")?.value.trim() || "",
qualification_done: document.getElementById("leadQualificationDone")?.checked || false,
worth_talking: document.getElementById("leadWorthTalking")?.checked || false,
l2_done: document.getElementById("leadL2Done")?.checked || false,
              latest_transcript: document.getElementById("leadLatestTranscript").value.trim()
            };
          }

async function saveBusinessLead() {
  const id = document.getElementById("leadId").value;
  const payload = getLeadPayloadFromForm();

  if (!id && leadDuplicateFound) {
    alert("Duplicate lead exists with this phone number. Please use the existing lead instead.");
    return;
  }

            const url = id
              ? "/api/business-leads/" + BUSINESS + "/" + id
              : "/api/business-leads/" + BUSINESS;

            const method = id ? "PUT" : "POST";

            const res = await fetch(url, {
              method,
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload)
            });

            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to save lead");
              return;
            }

            window.location.reload();
          }


async function enrichLeadUrl() {
  const website = document.getElementById("leadWebsite").value.trim() || document.getElementById("enrichUrl").value.trim();
  const googleMapsUrl = document.getElementById("leadGoogleMapsUrl").value.trim();

  if (!website && !googleMapsUrl) {
    alert("Add website or Google Map link first.");
    return;
  }

  document.getElementById("enrichMessage").textContent = "Trying to fetch company info...";

  const res = await fetch("/api/rasset-leads/enrich", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      website,
      google_maps_url: googleMapsUrl
    })
  });

  const json = await res.json();

  if (!json.ok) {
    document.getElementById("enrichMessage").textContent = json.error || "Could not fetch.";
    return;
  }

  const result = json.data || {};
  const d = result.data || {};

  document.getElementById("enrichMessage").textContent = result.message || "Done.";

  if (d.company) document.getElementById("leadCompany").value = d.company;
  if (d.company) document.getElementById("leadBusinessName").value = d.company;
  if (d.website) document.getElementById("leadWebsite").value = d.website;
  if (d.google_maps_url) document.getElementById("leadGoogleMapsUrl").value = d.google_maps_url;
  if (d.lead_source) document.getElementById("leadSource").value = d.lead_source;
  if (d.email) document.getElementById("leadEmail").value = d.email;
  if (d.industry) document.getElementById("leadIndustry").value = d.industry;
  if (d.pin_code) document.getElementById("leadPinCode").value = d.pin_code;
  if (d.city) document.getElementById("leadCity").value = d.city;
  if (d.location) document.getElementById("leadLocation").value = d.location;
  if (d.phone) document.getElementById("leadPhone").value = d.phone;
  if (d.year_of_establishment) document.getElementById("leadYearOfEstablishment").value = d.year_of_establishment;
  if (d.owner_name) document.getElementById("leadOwnerName").value = d.owner_name;
  if (d.number_of_employees) document.getElementById("leadNumberOfEmployees").value = d.number_of_employees;
  if (d.company_size) document.getElementById("leadCompanySize").value = d.company_size;
  if (d.country) document.getElementById("leadCountry").value = d.country;
  if (d.notes) document.getElementById("leadNotes").value = d.notes;
  if (d.enrichment_notes) document.getElementById("leadEnrichmentNotes").value = d.enrichment_notes;
}

function formatHumanDateTime(value) {
  if (!value) return "-";

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";

  return date.toLocaleString("en-US", {
    timeZone: "America/Los_Angeles",
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  });
}

async function openCallSummaryModal(business, phone) {
  const modal = document.getElementById("callSummaryModal");
  const title = document.getElementById("callSummaryTitle");
  const body = document.getElementById("callSummaryBody");

  if (!modal || !title || !body) {
    alert("Call summary modal is missing");
    return;
  }

  title.textContent = "Call Summaries · " + phone;
  body.innerHTML = "Loading call summaries...";
  modal.classList.add("open");

  const res = await fetch(
    "/api/business-leads/" +
      encodeURIComponent(business) +
      "/call-summaries?phone=" +
      encodeURIComponent(phone)
  );

  const json = await res.json();

  if (!json.ok) {
    body.innerHTML = escapeHtmlClient(json.error || "Failed to load call summaries");
    return;
  }

  const rows = json.data || [];

  if (!rows.length) {
    body.innerHTML = "No call summaries found for this phone number yet.";
    return;
  }

  body.innerHTML = rows
    .map(function(item) {
      const summary =
        item.translated_text ||
        item.cleaned_transcript ||
        item.raw_transcript ||
        "No transcript available yet.";

      const conversationRows = Array.isArray(item.conversation_rows)
        ? item.conversation_rows
        : [];

      const conversationHtml = conversationRows.length
        ? (
            '<table style="width:100%; border-collapse:collapse; margin-top:12px;">' +
              '<thead>' +
                '<tr>' +
                  '<th style="text-align:left; padding:8px;">Speaker</th>' +
                  '<th style="text-align:left; padding:8px;">What was said</th>' +
                '</tr>' +
              '</thead>' +
              '<tbody>' +
                conversationRows.map(function(row) {
                  return (
                    '<tr>' +
                      '<td style="width:150px; font-weight:900; padding:8px; vertical-align:top;">' +
                        escapeHtmlClient(row.speaker || "Unknown") +
                      '</td>' +
                      '<td style="white-space:pre-wrap; padding:8px; vertical-align:top; line-height:1.55;">' +
                        escapeHtmlClient(row.text || "") +
                      '</td>' +
                    '</tr>'
                  );
                }).join("") +
              '</tbody>' +
            '</table>'
          )
        : (
            '<div style="white-space:pre-wrap; margin-top:12px; line-height:1.55;">' +
              escapeHtmlClient(summary) +
            '</div>'
          );

      return (
        '<div class="call-summary-card">' +
        
        (item.spoke_to_name
  ? '<div style="margin-bottom:8px;"><strong>Spoke to:</strong> ' +
      escapeHtmlClient(item.spoke_to_name) +
    '</div>'
  : ''
) +

          '<div style="display:flex; justify-content:space-between; gap:12px; align-items:flex-start;">' +

            '<div>' +
              '<div style="font-weight:900;">Call #' + escapeHtmlClient(item.id) + '</div>' +
              '<div class="muted" style="line-height:1.6; margin-top:4px;">' +
                'Created: ' + escapeHtmlClient(formatHumanDateTime(item.created_at)) + '<br>' +
                'Uploaded by: ' + escapeHtmlClient(item.sender_phone || "-") + '<br>' +
                'Verified by: ' + escapeHtmlClient(item.verified_by || "Not verified") + '<br>' +
                'Verified at: ' + escapeHtmlClient(formatHumanDateTime(item.verified_at)) + '<br>' +
                'Status: ' + escapeHtmlClient(item.status || "-") +
              '</div>' +
            '</div>' +

            '<div style="display:flex; gap:8px; flex-wrap:wrap;">' +

              '<audio controls preload="none" style="max-width:260px; height:36px;">' +
  '<source src="/api/lead-voice-uploads/' + Number(item.id) + '/audio">' +
  'Your browser does not support audio playback.' +
'</audio>' +

              '<button class="btn btn-danger" type="button" data-call-id="' +
                Number(item.id) +
                '" data-business="' +
                escapeHtmlClient(business) +
                '" data-phone="' +
                escapeHtmlClient(phone) +
                '" onclick="handleDeleteCallSummaryClick(this)">Delete</button>' +

            '</div>' +

          '</div>' +

          conversationHtml +

        '</div>'
      );
    })
    .join("");
}

function closeCallSummaryModal(event) {
  if (event && event.target && event.target.id !== "callSummaryModal") return;
  document.getElementById("callSummaryModal").classList.remove("open");
}

async function deleteCallSummary(id, business, phone) {
  if (!confirm("Delete this call summary?")) return;

  const res = await fetch("/api/lead-voice-uploads/" + id, {
    method: "DELETE"
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete call summary");
    return;
  }

  await openCallSummaryModal(business, phone);
}

async function handleDeleteCallSummaryClick(button) {
  const id = Number(button.getAttribute("data-call-id"));
  const business = button.getAttribute("data-business") || "";
  const phone = button.getAttribute("data-phone") || "";

  await deleteCallSummary(id, business, phone);
}

async function deleteVoiceTranscript(id) {
  if (!confirm("Delete this transcription only? The audio will remain and you can transcribe again.")) {
    return;
  }

  const res = await fetch("/api/lead-voice-uploads/" + id + "/transcription", {
    method: "DELETE"
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete transcription");
    return;
  }

  alert("Transcription deleted.");
  window.location.reload();
}

async function deleteSelectedVoiceUploads() {
  const ids = Array.from(document.querySelectorAll(".voice-delete-checkbox:checked"))
    .map(function(el) {
      return Number(el.value);
    })
    .filter(Boolean);

  if (!ids.length) {
    alert("Select at least one voice message.");
    return;
  }

  if (!confirm("Delete " + ids.length + " selected voice message(s)? This cannot be undone.")) {
    return;
  }

  const res = await fetch("/api/lead-voice-uploads/bulk-delete", {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ids: ids })
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete selected voice messages");
    return;
  }

  window.location.reload();
}

async function deleteVoiceUpload(id) {
  if (!confirm("Delete this voice lead completely? This removes audio link, transcript, notes, and review data.")) {
    return;
  }

  const res = await fetch("/api/lead-voice-uploads/" + id, {
    method: "DELETE"
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to delete voice lead");
    return;
  }

  alert("Voice lead deleted.");
  window.location.reload();
}

window.transcribeLead = async function transcribeLead(id) {
  const btn = document.querySelector('[data-transcribe-id="' + id + '"]');

  if (btn) {
    btn.disabled = true;
    btn.textContent = "Transcribing...";
  }

  try {
    const res = await fetch("/api/leads/" + id + "/transcribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });

    const text = await res.text();
    let json;

    try {
      json = JSON.parse(text);
    } catch {
      alert("Server returned non-JSON response: " + text.slice(0, 800));
      return;
    }

    if (!json.ok) {
      alert("Transcription failed: " + (json.error || JSON.stringify(json)));
      return;
    }

    alert("Transcription completed.");
    window.location.reload();
  } catch (error) {
    alert("Transcription request failed: " + (error?.message || error));
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = "Transcribe";
    }
  }
};

async function saveTranscript(id) {
  const translated = document.getElementById("translated-" + id)?.value || "";
  const notes = document.getElementById("notes-" + id)?.value || "";

  if (!translated.trim()) {
    alert("English translation is required.");
    return;
  }

  const res = await fetch("/api/leads/" + id + "/transcript", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      cleaned_transcript: translated,
      translated_text: translated,
      review_notes: notes
    })
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to save transcript");
    return;
  }

  alert("Transcript saved.");
  window.location.reload();
}

          async function approveLead(id) {
            if (!confirm("Approve this transcript and create/update business lead?")) return;

            const res = await fetch("/api/leads/" + id + "/approve", {
              method: "POST",
              headers: { "Content-Type": "application/json" }
            });

            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to approve lead");
              return;
            }

            alert("Lead approved. It will now show under All Leads.");
            window.location.href = "/leads/" + BUSINESS + "?tab=all";
          }

          async function rejectLead(id) {
            const reason = prompt("Reason for rejection?", "");

            const res = await fetch("/api/leads/" + id + "/reject", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ reason: reason || "" })
            });

            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to reject lead");
              return;
            }

            alert("Lead rejected.");
            window.location.reload();
          }

          async function updateBusinessLeadStatus(business, id, status) {
            const res = await fetch("/api/business-leads/" + business + "/" + id + "/status", {
              method: "PATCH",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ status })
            });

            const json = await res.json();

            if (!json.ok) {
              alert(json.error || "Failed to update status");
              return;
            }

            window.location.reload();
          }

          document.addEventListener("keydown", function(event) {
            if (event.key === "Escape") {
              closeLeadModal();
            }
          });
          
          
          function toggleVoiceActions(event, leadId) {
  event.preventDefault();
  event.stopPropagation();

  document.querySelectorAll(".lead-actions-menu.open").forEach(function(menu) {
    if (menu.id !== "voiceActions-" + leadId) {
      menu.classList.remove("open");
    }
  });

  const menu = document.getElementById("voiceActions-" + leadId);
  if (!menu) return;

  const rect = event.currentTarget.getBoundingClientRect();

  menu.style.top = rect.bottom + 6 + "px";
  menu.style.left = Math.max(12, rect.right - 180) + "px";
  menu.classList.toggle("open");
}

function toggleLeadActions(event, leadId) {
  event.preventDefault();
  event.stopPropagation();

  document.querySelectorAll(".lead-actions-menu.open").forEach(function(menu) {
    if (menu.id !== "leadActions-" + leadId) {
      menu.classList.remove("open");
    }
  });

  const menu = document.getElementById("leadActions-" + leadId);
  if (!menu) {
    console.error("Menu not found for lead:", leadId);
    return;
  }

  const rect = event.currentTarget.getBoundingClientRect();

  menu.style.top = rect.bottom + 6 + "px";
  menu.style.left = Math.max(12, rect.right - 180) + "px";
  menu.classList.toggle("open");
}

let currentLeadCallsBusiness = null;
let currentLeadCallsLeadId = null;


async function openLeadCallsModal(business, leadId) {
  currentLeadCallsBusiness = business;
  currentLeadCallsLeadId = leadId;

  const modal = document.getElementById("leadCallsModal");
  if (modal) modal.classList.add("open");

  // LOAD LEAD DETAILS
  const res = await fetch("/api/business-leads/" + BUSINESS + "/" + leadId);
  const json = await res.json();

  if (json.ok && json.data) {
    const lead = json.data;

    document.getElementById("l2SpokeToName").value =
      lead.contact_name || lead.spoke_to_name || "";

    document.getElementById("l2Designation").value =
      lead.contact_designation || lead.designation || "";

    document.getElementById("l2Notes").value =
      lead.notes || "";

    document.getElementById("l2Behavior").value =
      lead.behavior || "";

    document.getElementById("l2CallOutcome").value =
      lead.last_call_outcome || lead.call_outcome || "";

    // IMPORTANT
    setMultiSelectValues(
      "l2Industry",
      lead.industry || lead.industry_primary || ""
    );

    setMultiSelectValues(
      "l2Capability",
      lead.manufacturing_capabilities || lead.capability || ""
    );
  }

  await loadLeadCalls();
}


async function saveLeadL2Data() {
  const payload = {
    spoke_to_name: document.getElementById("l2SpokeToName").value.trim(),
    designation: document.getElementById("l2Designation").value.trim(),
industry: getMultiSelectValues("l2Industry").join(", "),
capability: getMultiSelectValues("l2Capability").join(", "),
    behavior: document.getElementById("l2Behavior").value,
    call_outcome: document.getElementById("l2CallOutcome").value,
    notes: document.getElementById("l2Notes").value.trim(),
  };

  const res = await fetch(
    "/api/leads/" +
      encodeURIComponent(currentLeadCallsBusiness) +
      "/" +
      encodeURIComponent(currentLeadCallsLeadId) +
      "/l2",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }
  );

  const json = await res.json();

  if (!json.success) {
    alert(json.error || "Failed to save L2 data");
    return;
  }

  alert("L2 data saved");
  window.location.reload();
}


function closeLeadCallsModal(event) {
  if (event && event.target && event.target.id !== "leadCallsModal") return;

  const modal = document.getElementById("leadCallsModal");
  if (modal) modal.classList.remove("open");
}

async function loadLeadCalls() {
  const container = document.getElementById("leadCallsList");
  if (!container) return;

  container.innerHTML = "Loading calls...";

  const res = await fetch(
    "/api/leads/" +
      encodeURIComponent(currentLeadCallsBusiness) +
      "/" +
      encodeURIComponent(currentLeadCallsLeadId) +
      "/calls"
  );

  const json = await res.json();

  if (!json.success) {
    container.innerHTML = json.error || "Failed to load calls";
    return;
  }

  if (!json.calls || !json.calls.length) {
    container.innerHTML = "No calls uploaded yet.";
    return;
  }

  container.innerHTML = json.calls
    .map(function (call) {
      const audioHtml = call.audio_url
        ? '<audio controls src="' +
          escapeHtmlClient(call.audio_url) +
          '" style="width:100%; margin-bottom:10px;"></audio>'
        : '<div class="muted">No audio</div>';

      return (
        '<div style="border:1px solid rgba(255,255,255,0.10); border-radius:12px; padding:12px; margin-bottom:12px;">' +
          '<div style="font-weight:800; margin-bottom:8px;">' +
            escapeHtmlClient(call.created_at || "") +
          '</div>' +
          audioHtml +
          '<div style="margin-top:10px;">' +
            '<strong>Transcript</strong>' +
            '<div class="muted" style="white-space:pre-wrap; margin-top:6px;">' +
              escapeHtmlClient(call.transcript || "No transcript yet") +
            '</div>' +
          '</div>' +
        '</div>'
      );
    })
    .join("");
}



document.addEventListener("click", function () {
  document.querySelectorAll(".lead-actions-menu.open").forEach(function(menu) {
    menu.classList.remove("open");
  });
});

        </script>
      
  `;
}

export {
  renderBusinessLeadsPage,
};
