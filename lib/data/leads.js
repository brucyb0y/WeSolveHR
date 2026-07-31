// Lead engine reads: the cross-business overview, the per-business list
// (filters, tabs, paging) and the shared business-name helpers. Extracted
// verbatim from the original monolith.

import { CLIENT_LEADS_TABLE, CLIENT_LEAD_DEMO_STATUSES, CLIENT_LEAD_PIPELINE_STAGES, DEFAULT_CLIENT_LEAD_STAGE, INLINE_CLIENT_LEADS_BUSINESSES, REACH_VIA_CHANNELS, getActiveLeadBusinesses, getBusinessConfig } from "../server/constants.js";
import { APP_TIMEZONE } from "../server/runtime.js";
import { supabase } from "../server/supabase.js";
import { getDateStringInTimeZone, getTodayDateStringInTimeZone } from "../server/time.js";

async function getBusinessLeadsData(
  orgId,
  business,
  selectedTab = "all",
  search = "",
  page = 1,
  filters = {},
) {
  const normalizedBusiness = getBusinessCanonicalName(business);
  const { tableName, clientId } = resolveLeadSource(normalizedBusiness);

  const safePage = Math.max(1, Number(page) || 1);
  const pageSize = 25;
  const from = (safePage - 1) * pageSize;
  const to = from + pageSize - 1;
  const q = String(search || "").trim();
  const industryFilter = String(filters.industry || "").trim();
  const capabilityFilter = String(filters.capability || "").trim();
  const entityTypeFilter = String(filters.entity_type || "").trim();
  const statusFilter = String(filters.status || "").trim();
  const cityFilter = String(filters.city || "").trim();
  const stateFilter = String(filters.state || "").trim();
  const assignedToFilter = String(filters.assigned_to || "").trim();
  // "Assign for Phone" / "Assign for Email" (client_leads only). Named after the
  // filter-popup controls that set them; both take the same values as the
  // assigned_to filter, including the "__unassigned__" sentinel.
  const phoneAssignedToFilter = String(filters.phone_assignee || "").trim();
  const emailAssignedToFilter = String(filters.email_assignee || "").trim();
  // "My leads only" — the logged-in user's name, matched across every assignee
  // role rather than a single column (see below).
  const mineNameFilter = String(filters.mine_name || "").trim();
  const qualifiedFilter = String(filters.qualified || "").trim();
  const worthTalkingFilter = String(filters.worth_talking || "").trim();
  const hasCallTranscriptionFilter = String(
    filters.has_call_transcription || "",
  ).trim();
  // Client-lead-only filters (Status / Demo / Reached-via columns + Notes).
  const pipelineStageFilter = String(filters.pipeline_stage || "").trim();
  const demoStatusFilter = String(filters.demo_status || "").trim();
  const categoryTypeFilter = String(filters.category_type || "").trim();
  const locationFilter = String(filters.location || "").trim();
  const callbackDateFromFilter = String(
    filters.callback_date_from || "",
  ).trim();
  const callbackDateToFilter = String(filters.callback_date_to || "").trim();
  const missedCallbackFilter = String(filters.missed_callback || "").trim();
  const reachedViaFilter = String(filters.reached_via || "").trim();
  const notesFilter = String(filters.notes || "").trim();
  const notesByFilter = String(filters.notes_by || "").trim();
  const noteAudioFilter = String(filters.has_note_audio || "").trim();
  const hasPhoneFilter = String(filters.has_phone || "").trim();
  // Updated-at date-range filter (IST calendar dates, YYYY-MM-DD) and column
  // sorting — applied in JS after the rows are fetched so they can reuse the
  // parsed notes history and the pipeline/demo ordering.
  const updatedFromFilter = String(filters.updated_from || "").trim();
  const updatedToFilter = String(filters.updated_to || "").trim();
  const sortField = String(filters.sort || "").trim();
  const sortDir =
    String(filters.sort_dir || "").trim().toLowerCase() === "asc"
      ? "asc"
      : "desc";
  const { data: voiceRows, error: voiceError } = await supabase
    .from("lead_voice_uploads")
    .select("*")
    .eq("org_id", orgId)
    .eq("business", normalizedBusiness)
    .order("created_at", { ascending: false });

  if (voiceError) throw voiceError;

  let businessRows = [];
  let totalBusinessCount = 0;
  let b2bBusinessCount = 0;
  let b2cBusinessCount = 0;
  if (tableName) {
    let query = supabase
      .from(tableName)
      .select("*", { count: "exact" })
      .eq("org_id", orgId)
      .order("updated_at", { ascending: false });

    if (clientId) {
      query = query.eq("client_id", clientId);
    }

    if (tableName === "rasset_leads" || tableName === CLIENT_LEADS_TABLE) {
      query = query.or("is_deleted.is.null,is_deleted.eq.false");
    }

    // Bulk email search: pasting a list of emails (newline / comma / space /
    // semicolon separated) matches leads whose email equals ANY of them
    // (case-insensitive exact match, not substring). Only kicks in when every
    // token looks like an email, so normal free-text search is untouched.
    const bulkEmailTokens = q
      .split(/[\s,;]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    // Strict charset (no parens/commas/wildcards) so the tokens are safe to
    // embed in the PostgREST or() expression below.
    const isBulkEmailSearch =
      bulkEmailTokens.length > 1 &&
      bulkEmailTokens.every((t) =>
        /^[A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$/.test(t),
      );
    if (isBulkEmailSearch) {
      query = query.or(
        bulkEmailTokens.map((t) => `email.ilike.${t}`).join(","),
      );
    } else if (q) {
      const commonSearchFields = [
        `industry_primary.ilike.%${q}%`,
        `manufacturing_capabilities.ilike.%${q}%`,
        `entity_type.ilike.%${q}%`,
        `raw_industry.ilike.%${q}%`,
        `assigned_to.ilike.%${q}%`,
        `lead_source.ilike.%${q}%`,
        `import_source.ilike.%${q}%`,
        `phone.ilike.%${q}%`,
        `business_name.ilike.%${q}%`,
        `contact_name.ilike.%${q}%`,
        `email.ilike.%${q}%`,
        `city.ilike.%${q}%`,
        `industry.ilike.%${q}%`,
        `notes.ilike.%${q}%`,
        `latest_transcript.ilike.%${q}%`,
        `company.ilike.%${q}%`,
        `website.ilike.%${q}%`,
        `pin_code.ilike.%${q}%`,
        `location.ilike.%${q}%`,
        `country.ilike.%${q}%`,
        `owner_name.ilike.%${q}%`,
        `number_of_employees.ilike.%${q}%`,
        `company_size.ilike.%${q}%`,
        `lead_stage.ilike.%${q}%`,
        ...(q.toLowerCase() === "qualified" ? ["qualified.eq.true"] : []),
        ...(q.toLowerCase() === "l2 done" ||
        q.toLowerCase() === "l2" ||
        q.toLowerCase() === "l2_done"
          ? ["l2_done.eq.true"]
          : []),
        ...(q.toLowerCase() === "prospect" ? ["lead_stage.eq.prospect"] : []),
      ];

      const joolianOnlySearchFields =
        tableName === "joolian_leads"
          ? [
              `age_group.ilike.%${q}%`,
              `activity_category.ilike.%${q}%`,
              `sub_activity_category.ilike.%${q}%`,
              `type_of_business.ilike.%${q}%`,
              `pricing_approx.ilike.%${q}%`,
            ]
          : [];

      query = query.or(
        [...commonSearchFields, ...joolianOnlySearchFields].join(","),
      );
    }
    if (tableName === "rasset_leads") {
      if (industryFilter) {
        query = query.or(
          `industry.ilike.%${industryFilter}%,industry_primary.ilike.%${industryFilter}%,raw_industry.ilike.%${industryFilter}%`,
        );
      }

      if (capabilityFilter) {
        query = query.ilike(
          "manufacturing_capabilities",
          `%${capabilityFilter}%`,
        );
      }

      if (entityTypeFilter) {
        query = query.eq("entity_type", entityTypeFilter);
      }

      if (statusFilter) {
        query = query.eq("status", statusFilter);
      }

      if (cityFilter) {
        query = query.ilike("city", `%${cityFilter}%`);
      }

      if (stateFilter) {
        query = query.ilike("state", `%${stateFilter}%`);
      }

      if (hasCallTranscriptionFilter === "yes") {
        query = query
          .not("latest_transcript", "is", null)
          .neq("latest_transcript", "");
      }

      if (hasCallTranscriptionFilter === "no") {
        query = query.or("latest_transcript.is.null,latest_transcript.eq.");
      }

      if (qualifiedFilter === "yes") {
        query = query.eq("qualified", true);
      }

      if (qualifiedFilter === "no") {
        query = query.eq("qualified", false);
      }

      if (worthTalkingFilter === "yes") {
        query = query.eq("worth_talking", true);
      }

      if (worthTalkingFilter === "no") {
        query = query.eq("worth_talking", false);
      }
    }

    // assigned_to / location exist on every lead table (rasset/joolian/client_leads),
    // so these apply regardless of which table backs the business — unlike the
    // rasset-only filters above.
    if (assignedToFilter === "__unassigned__") {
      query = query.or("assigned_to.is.null,assigned_to.eq.");
    } else if (assignedToFilter) {
      query = query.ilike("assigned_to", `%${assignedToFilter}%`);
    }
    // "My leads only": a lead is mine when I'm named in any assignee role —
    // "Assign for Phone", "Assign for Email", or the overall owner. Navii's
    // owners were moved into phone_assigned_to, so matching assigned_to alone
    // would show them nothing; other businesses still use assigned_to only.
    // Lead tables without the per-channel columns fall back to the owner.
    if (mineNameFilter) {
      const mineColumns = tableHasClientLeadColumns(tableName)
        ? ["phone_assigned_to", "email_assigned_to", "assigned_to"]
        : ["assigned_to"];
      query = query.or(
        mineColumns.map((c) => `${c}.ilike.%${mineNameFilter}%`).join(","),
      );
    }
    // "__none__" = the lead has no location data at all — the same columns the
    // text match below searches must all be null/empty.
    if (locationFilter === "__none__") {
      query = query
        .or("city.is.null,city.eq.")
        .or("state.is.null,state.eq.")
        .or("country.is.null,country.eq.");
    } else if (locationFilter) {
      query = query.or(
        `city.ilike.%${locationFilter}%,state.ilike.%${locationFilter}%,country.ilike.%${locationFilter}%`,
      );
    }
    // "Lead with number": whether the phone column has a value. Applies to every
    // lead table (rasset/joolian/client_leads all carry `phone`).
    if (hasPhoneFilter === "yes") {
      query = query.not("phone", "is", null).neq("phone", "");
    } else if (hasPhoneFilter === "no") {
      query = query.or("phone.is.null,phone.eq.");
    }

    if (tableHasClientLeadColumns(tableName)) {
      // "Assign for Phone" / "Assign for Email" — same matching rules as the
      // assigned_to filter above (exact-ish name match, or "__unassigned__" for
      // rows where nobody was picked), on the client-lead-only columns.
      const applyAssigneeFilter = (column, value) => {
        if (!value) return;
        query =
          value === "__unassigned__"
            ? query.or(`${column}.is.null,${column}.eq.`)
            : query.ilike(column, `%${value}%`);
      };
      applyAssigneeFilter("phone_assigned_to", phoneAssignedToFilter);
      applyAssigneeFilter("email_assigned_to", emailAssignedToFilter);
      // pipeline_stage / demo_status fall back to their first option when the
      // column is null (imported leads leave demo unset), so filtering on that
      // default value also matches null rows. "__none__" is stricter: only
      // rows where the column was never set at all.
      const applyStatusFilter = (column, value, defaultKey) => {
        if (!value) return;
        if (value === "__none__") {
          query = query.is(column, null);
          return;
        }
        query =
          value === defaultKey
            ? query.or(`${column}.eq.${value},${column}.is.null`)
            : query.eq(column, value);
      };
      applyStatusFilter(
        "pipeline_stage",
        pipelineStageFilter,
        DEFAULT_CLIENT_LEAD_STAGE,
      );
      applyStatusFilter(
        "demo_status",
        demoStatusFilter,
        CLIENT_LEAD_DEMO_STATUSES[0].key,
      );
      // Reached-via channels (boolean columns). Multi-select: the filter value
      // is a comma-separated key list and a lead matches when reached via ANY
      // selected channel. "both" (also the legacy single-select value) still
      // requires LinkedIn + Email together, nested as an and() inside the or().
      const reachedViaOrParts = reachedViaFilter
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
        .map((key) => {
          if (key === "both") {
            return "and(reached_via_linkedin.eq.true,reached_via_email.eq.true)";
          }
          // "__none__" = not reached via any channel (every column false/null).
          if (key === "__none__") {
            return `and(${REACH_VIA_CHANNELS.map((c) => `${c.column}.not.is.true`).join(",")})`;
          }
          const ch = REACH_VIA_CHANNELS.find((c) => c.key === key);
          return ch ? `${ch.column}.eq.true` : null;
        })
        .filter(Boolean);
      if (reachedViaOrParts.length) {
        query = query.or(reachedViaOrParts.join(","));
      }
      // Category type — multi-select, comma-separated keys; a lead matches ANY
      // selected type. A single key (e.g. from the pill row) is a 1-item list.
      // "__none__" matches leads with no category type set.
      const categoryTypeKeys = categoryTypeFilter
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      if (categoryTypeKeys.length) {
        const namedKeys = categoryTypeKeys.filter((k) => k !== "__none__");
        const categoryOrParts = [];
        if (categoryTypeKeys.includes("__none__")) {
          categoryOrParts.push("category_type.is.null", "category_type.eq.");
        }
        if (namedKeys.length) {
          categoryOrParts.push(`category_type.in.(${namedKeys.join(",")})`);
        }
        query = query.or(categoryOrParts.join(","));
      }
      // Callback date range (inclusive, plain YYYY-MM-DD comparison — the
      // column carries no time/timezone component).
      if (callbackDateFromFilter) {
        query = query.gte("callback_date", callbackDateFromFilter);
      }
      if (callbackDateToFilter) {
        query = query.lte("callback_date", callbackDateToFilter);
      }
      // Missed callback: mirrors the red/green callback badge. A lead only shows
      // the badge when it has a callback_date, so both directions require one.
      // "yes" = overdue (past, red: callback_date < today); "no" = upcoming
      // (today or future, green: callback_date >= today).
      if (missedCallbackFilter === "yes" || missedCallbackFilter === "no") {
        const todayStr = getTodayDateStringInTimeZone(APP_TIMEZONE);
        query = query.not("callback_date", "is", null);
        query =
          missedCallbackFilter === "yes"
            ? query.lt("callback_date", todayStr)
            : query.gte("callback_date", todayStr);
      } else if (missedCallbackFilter === "none") {
        // "None" = no callback date set at all (no badge either way).
        query = query.is("callback_date", null);
      }
    }

    // Supabase caps a single response at ~1000 rows, so a client with several
    // thousand leads would otherwise show only the first 1000 (and a wrong
    // total). Page through the filtered result set in 1000-row batches to load
    // every row; the exact count comes back with each batch. A batch ceiling
    // guards against a bad count spinning the loop forever.
    const FETCH_BATCH = 1000;
    const MAX_FETCH_BATCHES = 50; // up to 50k leads
    let exactCount = null;
    for (let batchIdx = 0; batchIdx < MAX_FETCH_BATCHES; batchIdx += 1) {
      const fetchOffset = batchIdx * FETCH_BATCH;
      const { data, error, count } = await query.range(
        fetchOffset,
        fetchOffset + FETCH_BATCH - 1,
      );
      if (error) throw error;
      if (exactCount === null) exactCount = count;
      const batch = data || [];
      businessRows = businessRows.concat(batch);
      if (batch.length < FETCH_BATCH) break;
      if (exactCount != null && fetchOffset + FETCH_BATCH >= exactCount) break;
    }
    totalBusinessCount =
      exactCount != null ? exactCount : businessRows.length;

    // Notes filters need the JSON notes history parsed, so they run in JS here
    // (not in the DB query) and the count is recomputed to match.
    if (
      tableHasClientLeadColumns(tableName) &&
      (notesFilter || notesByFilter || noteAudioFilter)
    ) {
      const byNeedle = notesByFilter.toLowerCase();
      businessRows = businessRows.filter((row) => {
        const history = parseLeadNotesHistory(row.notes);
        if (notesFilter === "added" && history.length < 1) return false;
        if (notesFilter === "multiple" && history.length < 2) return false;
        if (notesFilter === "none" && history.length > 0) return false;
        if (notesByFilter === "__none__") {
          if (history.length > 0) return false;
        } else if (
          notesByFilter &&
          !history.some(
            (n) => String(n.by || "").trim().toLowerCase() === byNeedle,
          )
        ) {
          return false;
        }
        if (noteAudioFilter) {
          const hasAudio = history.some((n) => n && n.audio_url);
          if (noteAudioFilter === "yes" && !hasAudio) return false;
          if (noteAudioFilter === "no" && hasAudio) return false;
        }
        return true;
      });
      totalBusinessCount = businessRows.length;
    }

    // Updated-at date range: compare the row's IST calendar date against the
    // from/to bounds (inclusive). Rows with no updated_at are excluded once a
    // bound is set. Matches the client-view leads filter behaviour.
    if (updatedFromFilter || updatedToFilter) {
      businessRows = businessRows.filter((row) => {
        if (!row.updated_at) return false;
        const ud = getDateStringInTimeZone(
          new Date(row.updated_at),
          APP_TIMEZONE,
        );
        if (updatedFromFilter && ud < updatedFromFilter) return false;
        if (updatedToFilter && ud > updatedToFilter) return false;
        return true;
      });
      totalBusinessCount = businessRows.length;
    }
  }

  if (tableName === "rasset_leads") {
    const { count: b2bCount } = await supabase
      .from(tableName)
      .select("id", { count: "exact", head: true })
      .eq("org_id", orgId)
      .or("is_deleted.is.null,is_deleted.eq.false")
      .eq("lead_category", "b2b");

    const { count: b2cCount } = await supabase
      .from(tableName)
      .select("id", { count: "exact", head: true })
      .eq("org_id", orgId)
      .or("is_deleted.is.null,is_deleted.eq.false")
      .eq("lead_category", "b2c");

    b2bBusinessCount = b2bCount || 0;
    b2cBusinessCount = b2cCount || 0;
  } else {
    b2bBusinessCount = businessRows.filter(
      (x) => x.lead_category === "b2b",
    ).length;
    b2cBusinessCount = businessRows.filter(
      (x) => x.lead_category === "b2c",
    ).length;
  }

  const voice = voiceRows || [];

  const voiceInboxRows = voice.filter((x) =>
    [
      "pending_transcription",
      "transcribing",
      "pending_review",
      "rejected",
    ].includes(x.status),
  );

  const filteredBusinessRows = businessRows.filter((x) => {
    if (selectedTab === "b2b") return x.lead_category === "b2b";
    if (selectedTab === "b2c") return x.lead_category === "b2c";
    if (selectedTab === "in_progress") return x.status === "in_progress";
    if (selectedTab === "completed") return x.status === "completed";
    // Pipeline-stage tabs (client_leads only).
    if (CLIENT_LEAD_PIPELINE_STAGES.some((s) => s.key === selectedTab)) {
      return (x.pipeline_stage || DEFAULT_CLIENT_LEAD_STAGE) === selectedTab;
    }
    return true;
  });

  // Column sort. The DB already returns updated_at desc, so we only re-sort when
  // an explicit sort field is requested. Stage/demo sort by their pipeline
  // order (not alphabetically); notes by history length; updated by timestamp.
  if (sortField) {
    const dirMul = sortDir === "asc" ? 1 : -1;
    const sortValue = (row) => {
      switch (sortField) {
        case "name":
          return String(row.company || row.business_name || "").toLowerCase();
        case "stage":
          return CLIENT_LEAD_PIPELINE_STAGES.findIndex(
            (s) => s.key === (row.pipeline_stage || DEFAULT_CLIENT_LEAD_STAGE),
          );
        case "demo":
          return CLIENT_LEAD_DEMO_STATUSES.findIndex(
            (s) => s.key === (row.demo_status || CLIENT_LEAD_DEMO_STATUSES[0].key),
          );
        case "notes":
          return parseLeadNotesHistory(row.notes).length;
        case "updated":
          return row.updated_at ? new Date(row.updated_at).getTime() : 0;
        default:
          return 0;
      }
    };
    filteredBusinessRows.sort((a, b) => {
      const av = sortValue(a);
      const bv = sortValue(b);
      if (av < bv) return -1 * dirMul;
      if (av > bv) return 1 * dirMul;
      return 0;
    });
  }

  const pagedBusinessRows = filteredBusinessRows.slice(from, to + 1);

  const rows =
    selectedTab === "voice_inbox" ? voiceInboxRows : pagedBusinessRows;

  return {
    business: normalizedBusiness,
    selectedTab,
    search: q,
    page: safePage,
    pageSize,
    rows,
    voiceRows: voice,
    businessRows,
    tableName,
    counts: {
      all: totalBusinessCount,
      b2b: b2bBusinessCount,
      b2c: b2cBusinessCount,
      in_progress: businessRows.filter((x) => x.status === "in_progress")
        .length,
      completed: businessRows.filter((x) => x.status === "completed").length,
      qualified: businessRows.filter((x) =>
        [
          "qualified_opportunity",
          "pilot_evaluation",
          "commercial_discussion",
          "converted",
        ].includes(x.pipeline_stage),
      ).length,
      meeting_completed: businessRows.filter(
        (x) => x.pipeline_stage === "meeting_completed",
      ).length,
      converted: businessRows.filter(
        (x) => x.pipeline_stage === "converted",
      ).length,
      voice_inbox: voiceInboxRows.length,
      total: totalBusinessCount,
      pending_review: voice.filter((x) => x.status === "pending_review").length,
    },
    // Ids of every row matching the current tab/search/filters across all
    // pages — powers the "Select all N leads" bulk option on the Leads tab.
    filteredIds: filteredBusinessRows.map((r) => Number(r.id)).filter(Boolean),
    pagination: {
      total: filteredBusinessRows.length,
      page: safePage,
      pageSize,
      hasPrev: safePage > 1,
      hasNext: to + 1 < filteredBusinessRows.length,
    },
    filters: {
      industry: industryFilter,
      capability: capabilityFilter,
      entity_type: entityTypeFilter,
      status: statusFilter,
      city: cityFilter,
      state: stateFilter,
      assigned_to: assignedToFilter,
      qualified: qualifiedFilter,
      worth_talking: worthTalkingFilter,
      has_call_transcription: hasCallTranscriptionFilter,
      pipeline_stage: pipelineStageFilter,
      demo_status: demoStatusFilter,
      category_type: categoryTypeFilter,
      location: locationFilter,
      reached_via: reachedViaFilter,
      notes: notesFilter,
      notes_by: notesByFilter,
      has_note_audio: noteAudioFilter,
      updated_from: updatedFromFilter,
      updated_to: updatedToFilter,
      callback_date_from: callbackDateFromFilter,
      callback_date_to: callbackDateToFilter,
      sort: sortField,
      sort_dir: sortDir,
    },
  };
}

function parseLeadNotesHistory(raw) {
  if (Array.isArray(raw)) {
    return raw.filter((n) => n && typeof n === "object" && n.text != null);
  }
  if (typeof raw !== "string") return [];
  const trimmed = raw.trim();
  if (!trimmed) return [];
  if (trimmed[0] === "[") {
    try {
      const arr = JSON.parse(trimmed);
      if (Array.isArray(arr)) {
        return arr.filter((n) => n && typeof n === "object" && n.text != null);
      }
    } catch (e) {
      /* fall through to legacy single-note handling */
    }
  }
  return [{ text: trimmed, at: null, by: null }];
}

async function getLeadsOverviewData(orgId) {
  const { data: voiceRows, error: voiceError } = await supabase
    .from("lead_voice_uploads")
    .select(
      "id, business, lead_phone, sender_phone, status, media_content_type, created_at",
    )
    .eq("org_id", orgId)
    .order("created_at", { ascending: false });

  if (voiceError) throw voiceError;

  const businessTables = getActiveLeadBusinesses();

  const businesses = [];

  for (const item of businessTables) {
    let query = supabase
      .from(item.table)
      .select("id, status", { count: "exact" })
      .eq("org_id", orgId);

    if (item.table === "rasset_leads") {
      query = query.or("is_deleted.is.null,is_deleted.eq.false");
    }

    const { data, error, count } = await query;

    if (!error) {
      const rows = data || [];
      businesses.push({
        business: item.business,
        label: item.label || item.business,
        total: count || rows.length,
        leads: rows.filter(
          (x) => !["in_progress", "completed"].includes(x.status),
        ).length,
        in_progress: rows.filter((x) => x.status === "in_progress").length,
        completed: rows.filter((x) => x.status === "completed").length,
        voice_uploads: (voiceRows || []).filter(
          (x) => x.business === item.business,
        ).length,
      });
    }
  }

  return {
    summary: {
      total: businesses.reduce((sum, x) => sum + x.total, 0),
      leads: businesses.reduce((sum, x) => sum + x.leads, 0),
      in_progress: businesses.reduce((sum, x) => sum + x.in_progress, 0),
      completed: businesses.reduce((sum, x) => sum + x.completed, 0),
      voice_uploads: (voiceRows || []).length,
    },
    businesses,
    recent: (voiceRows || []).slice(0, 20),
  };
}

function getBusinessCanonicalName(input) {
  const key = String(input || "")
    .trim()
    .toLowerCase();

  const aliases = {
    rasset: "rasset",
    rassetai: "rasset",
    "rasset.ai": "rasset",
    joolian: "joolian",
    joolianai: "joolian",
    "joolian.ai": "joolian",
    rebus: "rebus",
    rebusai: "rebus",
    "rebus ai": "rebus",
    "rebus.ai": "rebus",
  };

  return aliases[key] || key;
}

function getBusinessLeadTableName(business) {
  const normalized = getBusinessCanonicalName(business);
  return getBusinessConfig(normalized)?.table || null;
}

function parseClientLeadBusiness(business) {
  const m = String(business || "").match(/^client:(\d+)$/);
  return m ? Number(m[1]) : null;
}

function resolveLeadSource(business) {
  const clientId = parseClientLeadBusiness(business);
  if (clientId) return { tableName: CLIENT_LEADS_TABLE, clientId };
  return { tableName: getBusinessLeadTableName(business), clientId: null };
}

function tableHasClientLeadColumns(tableName) {
  if (tableName === CLIENT_LEADS_TABLE) return true;
  for (const business of INLINE_CLIENT_LEADS_BUSINESSES) {
    if (getBusinessLeadTableName(business) === tableName) return true;
  }
  return false;
}

export {
  getBusinessCanonicalName,
  getBusinessLeadTableName,
  getBusinessLeadsData,
  getLeadsOverviewData,
  parseClientLeadBusiness,
  resolveLeadSource,
};
