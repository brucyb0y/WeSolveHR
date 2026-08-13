// /leads/:business — replaces renderBusinessLeadsPage() + its route.
//
// ?embed=1 is preserved: it hides the top nav, header and stat cards so the
// page reads as a panel. /clients/[id] renders this URL in a same-origin
// iframe for businesses in INLINE_CLIENT_LEADS_BUSINESSES, so that contract
// has to keep working while that page is still on the Express adapter.

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import {
  DASHBOARD_ORG_ID,
  getBusinessLeadsData,
  getBusinessCanonicalName,
  formatDateTime,
  badgeClass,
  RASSET_INDUSTRY_OPTIONS,
  RASSET_CAPABILITY_OPTIONS,
} from "@/lib/server/app.js";
import LeadsBoard from "./LeadsBoard";
import VoiceInbox from "./VoiceInbox";
import UploadBox from "./UploadBox";
import styles from "./leads.module.css";

export const dynamic = "force-dynamic";

const TABS = [
  ["all", "All Leads"],
  ["b2b", "B2B"],
  ["b2c", "B2C"],
  ["in_progress", "In Progress"],
  ["completed", "Completed"],
  ["voice_inbox", "Voice Inbox"],
];

const ALLOWED_TABS = TABS.map(([k]) => k);

const ENTITY_TYPES = [
  "Factory",
  "Service Provider",
  "Trading Company",
  "Supplier",
  "Training Institute",
];

const STATUSES = [
  "new",
  "working",
  "busy",
  "unreachable",
  "invalid",
  "unsure",
  "in_progress",
  "completed",
];

const FILTER_KEYS = [
  "industry",
  "capability",
  "entity_type",
  "status",
  "city",
  "state",
  "assigned_to",
  "qualified",
  "worth_talking",
  "has_call_transcription",
];

export async function generateMetadata({ params }) {
  const { business } = await params;
  return { title: `${getBusinessCanonicalName(business)} Leads | WeSolveHR` };
}

export default async function BusinessLeadsPage({ params, searchParams }) {
  const user = await requireDashboardUser();
  const { business: businessParam } = await params;
  const sp = await searchParams;

  const business = businessParam;
  const embed = sp?.embed === "1";
  const selectedTab = ALLOWED_TABS.includes(sp?.tab) ? sp.tab : "all";
  const search = String(sp?.search || "").trim();
  const page = Number(sp?.page || 1);

  const filters = Object.fromEntries(
    FILTER_KEYS.map((k) => [k, sp?.[k] || ""]),
  );

  // The original read org_id off req.session.user, which the session never
  // populates, so this always resolved to DASHBOARD_ORG_ID. Kept identical.
  const data = await getBusinessLeadsData(
    DASHBOARD_ORG_ID,
    business,
    selectedTab,
    search,
    page,
    filters,
  );

  const rows = data.rows || [];
  const counts = data.counts || {};
  const pagination = data.pagination || {};
  const isVoiceInbox = selectedTab === "voice_inbox";

  const filterQuery = new URLSearchParams({
    tab: selectedTab,
    search: search || "",
    ...filters,
  }).toString();

  const base = `/leads/${encodeURIComponent(business)}`;

  const header = (
    <div>
      <div className={styles.eyebrow}>Business Lead CRM</div>
      <h1>{business} Leads</h1>
      <div className={styles.subtitle}>
        All leads, B2B/B2C split, manual onboarding, search, and voice inbox.
      </div>
    </div>
  );

  const headerLinks = (
    <>
      <a className={styles.btn} href="/leads">
        ← Leads Overview
      </a>
      <a className={styles.btn} href={`${base}/intelligence`}>
        Intelligence
      </a>
    </>
  );

  const stats = (
    <div className={styles.stats}>
      {[
        ["All Leads", counts.all],
        ["B2B", counts.b2b],
        ["B2C", counts.b2c],
        ["Voice Inbox", counts.voice_inbox],
      ].map(([label, value]) => (
        <div className={styles.statCard} key={label}>
          <div className={styles.statLabel}>{label}</div>
          <div className={styles.statValue}>{value || 0}</div>
        </div>
      ))}
    </div>
  );

  const tabs = (
    <div className={styles.tabs}>
      {TABS.map(([key, label]) => (
        <a
          key={key}
          className={`${styles.tab} ${selectedTab === key ? styles.active : ""}`}
          href={`${base}?tab=${key}&search=${encodeURIComponent(search)}`}
        >
          {label} ({counts[key] || 0})
        </a>
      ))}
    </div>
  );

  const select = (name, placeholder, options) => (
    <select name={name} defaultValue={filters[name]}>
      <option value="">{placeholder}</option>
      {options.map((x) =>
        Array.isArray(x) ? (
          <option value={x[0]} key={x[0]}>
            {x[1]}
          </option>
        ) : (
          <option value={x} key={x}>
            {x}
          </option>
        ),
      )}
    </select>
  );

  const filterPanel = isVoiceInbox ? null : (
    <div className={styles.panel}>
      <form method="GET" action={base}>
        <input type="hidden" name="tab" value={selectedTab} />

        {business === "rasset" ? (
          <>
            <div className={styles.advancedFilterGrid}>
              <input
                name="search"
                defaultValue={search}
                placeholder="Search company, phone, city, CNC, laser, owner, notes..."
              />
              {select("industry", "All Industries", RASSET_INDUSTRY_OPTIONS)}
              {select("capability", "All Capabilities", RASSET_CAPABILITY_OPTIONS)}
              {select("entity_type", "All Entity Types", ENTITY_TYPES)}
              {select("status", "All Status", STATUSES)}
              <input name="city" defaultValue={filters.city} placeholder="City" />
              <input
                name="state"
                defaultValue={filters.state}
                placeholder="State"
              />
              <input
                name="assigned_to"
                defaultValue={filters.assigned_to}
                placeholder="Assigned to"
              />
              {select("qualified", "Qualified?", [
                ["yes", "Qualified"],
                ["no", "Not Qualified"],
              ])}
              {select("worth_talking", "Worth Talking?", [
                ["yes", "Worth Talking"],
                ["no", "Not Worth Talking"],
              ])}
              {select("has_call_transcription", "Call Transcription?", [
                ["yes", "Has transcription"],
                ["no", "No transcription"],
              ])}
            </div>

            <div className={styles.filterActions}>
              <button
                className={`${styles.btn} ${styles.btnPrimary}`}
                type="submit"
              >
                Search / Filter
              </button>
              <a className={styles.btn} href={`${base}?tab=${selectedTab}`}>
                Clear
              </a>
            </div>
          </>
        ) : (
          <div className={styles.searchRow}>
            <input
              name="search"
              defaultValue={search}
              placeholder="Search phone, business, contact, city, notes..."
            />
            <button
              className={`${styles.btn} ${styles.btnPrimary}`}
              type="submit"
            >
              Search
            </button>
            <a className={styles.btn} href={`${base}?tab=${selectedTab}`}>
              Clear
            </a>
          </div>
        )}
      </form>
    </div>
  );

  const uploads = isVoiceInbox ? null : (
    <>
      {business === "rasset" ? (
        <UploadBox
          label="Import Rasset Excel"
          title="Upload Excel with Company, Email, Phone, Industry, Location, etc."
          endpoint="/api/rasset-leads/import-excel"
          note="Supports: Company, Website, Email, Industry, City, Phone, Owner, Employees, Size, Country"
          extraLink={{ href: "/leads/rasset/imports", label: "Import Logs" }}
        />
      ) : null}

      {business === "joolian" ? (
        <UploadBox
          label="Import Joolian Excel"
          title="Upload Excel with AP details, category, pricing, etc."
          endpoint="/api/joolian-leads/import-b2b-excel"
          note="Supports: AP Name, Phone, Email, City, Category, Pricing, Owner, etc."
        />
      ) : null}
    </>
  );

  const paginationBar = (
    <div className={styles.pagination}>
      {pagination.hasPrev ? (
        <a
          className={styles.btn}
          href={`${base}?${filterQuery}&page=${Number(pagination.page) - 1}`}
        >
          ← Previous
        </a>
      ) : null}
      <span className={styles.btn}>Page {pagination.page || 1}</span>
      {pagination.hasNext ? (
        <a
          className={styles.btn}
          href={`${base}?${filterQuery}&page=${Number(pagination.page) + 1}`}
        >
          Next →
        </a>
      ) : null}
    </div>
  );

  // Voice inbox rows carry server-formatted values so the client component
  // needs nothing from lib/server/app.js.
  const voiceRows = isVoiceInbox
    ? rows.map((lead) => ({
        ...lead,
        created_at_text: formatDateTime(lead.created_at),
        statusBadgeClass: badgeClass(lead.status),
      }))
    : [];

  return (
    <>
      {embed ? null : <TopNav active="leads" user={user} />}

      <LeadsBoard
        business={business}
        rows={rows}
        industryOptions={RASSET_INDUSTRY_OPTIONS}
        capabilityOptions={RASSET_CAPABILITY_OPTIONS}
        embed={embed}
        {...(embed ? {} : { header, headerLinks, stats })}
        tabs={tabs}
        filters={filterPanel}
        uploads={uploads}
        pagination={paginationBar}
        table={
          isVoiceInbox ? (
            <div className={styles.leadList}>
              <VoiceInbox rows={voiceRows} />
            </div>
          ) : null
        }
      />
    </>
  );
}
