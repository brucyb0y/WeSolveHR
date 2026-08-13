// AI summary + goals panel — the React version of renderSummaryWithGoals /
// renderReportSummaryPanel / renderReportSummaryBody / renderClientGoalsPanel.
//
// Shared by /clients/[id] (editable: staff can regenerate and edit goals) and
// /client-view/[token] (read-only). `editable` is the only difference, and it
// is passed in rather than inferred — the customer page must never render the
// regenerate or edit-goals controls.
//
// THREE SUMMARY SHAPES have to keep working, because rows written at different
// times are stored differently and none are migrated:
//   1. structured  — summary_json with {headline, sections[{title, description,
//                    stats[]}]}, the current format;
//   2. legacy list — summary_json sections carrying `items[]` bullets instead
//                    of description/stats;
//   3. plain text  — summary_text only, the oldest rows.
// Falling through to "no summary yet" on shapes 2 or 3 would silently blank
// historical weeks that do have content.
//
// The Reached-Via breakdown is rendered from row.stats, NOT from the AI text,
// so the per-channel numbers are exact rather than whatever the model wrote.

// Bolds a leading "Label:" and any **...** spans. Returns React nodes, so
// nothing is injected as HTML.
function Headline({ text }) {
  const str = String(text);
  const idx = str.indexOf(":");
  // A colon far into the string is punctuation, not a label.
  const hasLabel = idx > 0 && idx < 40;
  const label = hasLabel ? str.slice(0, idx + 1) : "";
  const rest = hasLabel ? str.slice(idx + 1) : str;

  const parts = String(rest).split(/\*\*([^*]+)\*\*/g);

  return (
    <div style={{ fontSize: 14, lineHeight: 1.6, marginBottom: 4 }}>
      {label ? <span style={{ fontWeight: 700 }}>{label}</span> : null}
      {/* Odd indices are the captured **bold** groups. */}
      {parts.map((p, i) =>
        i % 2 === 1 ? (
          <span style={{ fontWeight: 700 }} key={i}>
            {p}
          </span>
        ) : (
          p
        ),
      )}
    </div>
  );
}

function Stats({ stats }) {
  const visible = (stats || []).filter(
    (s) => s && (s.value != null || s.label),
  );
  return (
    <>
      {visible.map((s, i) => (
        <span key={i}>
          {i > 0 ? <span style={{ opacity: 0.4 }}> | </span> : null}
          <span style={{ fontWeight: 700 }}>
            {String(s.value != null ? s.value : "")}
          </span>
          {s.label ? ` ${s.label}` : ""}
        </span>
      ))}
    </>
  );
}

function ReachBreakdown({ breakdown }) {
  return (
    <ul
      style={{
        margin: "6px 0 0",
        paddingLeft: 20,
        listStyle: "disc",
        fontSize: 13,
        lineHeight: 1.5,
      }}
    >
      {breakdown
        .filter((b) => b && b.channel)
        .map((b, i) => {
          const statuses = (b.statuses || []).filter(
            (s) => s && (s.count != null || s.status),
          );
          return (
            <li style={{ margin: "2px 0" }} key={i}>
              <span style={{ fontWeight: 700 }}>{String(b.channel)}</span> —{" "}
              <span style={{ fontWeight: 700 }}>
                {String(b.count != null ? b.count : "")}
              </span>{" "}
              reached
              {statuses.length ? ": " : ""}
              {statuses.map((s, j) => (
                <span key={j}>
                  {j > 0 ? " · " : ""}
                  <span style={{ fontWeight: 700 }}>
                    {String(s.count != null ? s.count : "")}
                  </span>{" "}
                  {String(s.status || "")}
                </span>
              ))}
            </li>
          );
        })}
    </ul>
  );
}

// Legacy bullet item: either a plain string or {label, items[]} with a nested
// list.
function LegacyItem({ item }) {
  if (item && typeof item === "object" && Array.isArray(item.items)) {
    return (
      <li style={{ margin: "4px 0" }}>
        {item.label ? (
          <span style={{ fontWeight: 700 }}>{String(item.label)}</span>
        ) : null}
        <ul style={{ margin: "4px 0 0", paddingLeft: 20, listStyle: "circle" }}>
          {item.items.map((s, i) => (
            <li style={{ margin: "2px 0" }} key={i}>
              {String(s)}
            </li>
          ))}
        </ul>
      </li>
    );
  }
  return <li style={{ margin: "3px 0" }}>{String(item)}</li>;
}

export function ReportSummaryBody({ row }) {
  const json = row && row.summary_json;
  const reachBreakdown = row?.stats?.outreach?.reach_breakdown || [];

  if (
    json &&
    Array.isArray(json.sections) &&
    (json.sections.length || json.headline)
  ) {
    return (
      <>
        {json.headline ? <Headline text={json.headline} /> : null}
        {(json.sections || []).map((sec, i) => {
          const hasStats = Array.isArray(sec.stats) && sec.stats.length;
          const hasDesc =
            sec.description != null && String(sec.description).trim();

          const title = (
            <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 2 }}>
              {String(sec.title || "")}
            </div>
          );

          if (hasStats || hasDesc) {
            // The channel breakdown belongs under Outreach Execution only.
            const showBreakdown =
              String(sec.title || "").trim() === "Outreach Execution" &&
              reachBreakdown.length > 0;

            return (
              <div style={{ marginTop: 16 }} key={i}>
                {title}
                {hasDesc ? (
                  <div style={{ fontSize: 14, lineHeight: 1.5, marginTop: 2 }}>
                    {String(sec.description)}
                  </div>
                ) : null}
                {hasStats ? (
                  <div style={{ fontSize: 14, lineHeight: 1.5, marginTop: 4 }}>
                    <Stats stats={sec.stats} />
                  </div>
                ) : null}
                {showBreakdown ? (
                  <ReachBreakdown breakdown={reachBreakdown} />
                ) : null}
              </div>
            );
          }

          return (
            <div style={{ marginTop: 16 }} key={i}>
              {title}
              <ul
                style={{
                  margin: "6px 0 0",
                  paddingLeft: 20,
                  listStyle: "disc",
                  fontSize: 14,
                  lineHeight: 1.5,
                }}
              >
                {(sec.items || []).map((it, j) => (
                  <LegacyItem item={it} key={j} />
                ))}
              </ul>
            </div>
          );
        })}
      </>
    );
  }

  if (row && row.summary_text) {
    return (
      <div style={{ fontSize: 14, lineHeight: 1.65, whiteSpace: "pre-wrap" }}>
        {row.summary_text}
      </div>
    );
  }

  return null;
}

// True when the row holds anything worth showing — used to pick between the
// body and the "generated at 9 PM" placeholder, and to label the button
// Regenerate vs Generate now.
export function hasSummaryContent(row) {
  const json = row && row.summary_json;
  if (
    json &&
    Array.isArray(json.sections) &&
    (json.sections.length || json.headline)
  ) {
    return true;
  }
  return !!(row && row.summary_text);
}

export function ReportSummaryPanel({
  period,
  row,
  editable,
  weekLabel = "",
  rangeLabel = "",
  generatedText = "",
  regenButton = null,
}) {
  const isWeekly = period === "weekly";
  const title = isWeekly
    ? weekLabel
      ? `${weekLabel} Summary`
      : "Weekly Summary"
    : "Daily Summary";
  const sub = isWeekly
    ? `${rangeLabel || "this week (since Monday)"} · auto-generated daily at 9 PM PST`
    : "last 24 hours · auto-generated daily at 9 PM PST";

  const hasContent = hasSummaryContent(row);

  return (
    <div className="panel" data-ai-sum={period} style={{ marginBottom: 16 }}>
      <div
        className="panel-head"
        style={{
          display: "flex",
          alignItems: "flex-start",
          gap: 12,
          marginBottom: 12,
        }}
      >
        <div>
          <h2
            style={{
              margin: 0,
              display: "flex",
              alignItems: "center",
              flexWrap: "wrap",
              gap: 10,
            }}
          >
            ✨ {title}
            {regenButton}
            {generatedText ? (
              <span
                className="meta"
                style={{
                  fontSize: 12,
                  fontWeight: 400,
                  whiteSpace: "nowrap",
                }}
              >
                Generated {generatedText}
              </span>
            ) : null}
          </h2>
          <div className="meta" style={{ fontSize: 12 }}>
            {sub}
          </div>
        </div>
      </div>

      {hasContent ? (
        <ReportSummaryBody row={row} />
      ) : (
        <div className="meta" style={{ fontSize: 13, lineHeight: 1.65 }}>
          {`🕘 Your ${isWeekly ? "weekly" : "daily"} AI summary is generated automatically every day at 9 PM PST. Check back then${editable ? ", or generate it now." : "."}`}
        </div>
      )}
    </div>
  );
}

// Curated goals shown beside the AI summary, on both the daily and weekly
// views, so the client sees the targets next to either.
export function ClientGoalsPanel({
  clientId,
  goals,
  editable,
  updatedText = "",
  updatedByName = "",
  editButton = null,
}) {
  const items = goals?.items || [];
  const notes = (goals?.notes || "").trim();
  const hasText = items.length > 0 || !!notes;

  return (
    <div
      className="panel"
      data-client-goals={Number(clientId)}
      // height:97% makes the goals card match the summary card beside it
      // rather than shrink to its content.
      style={{ marginBottom: 16, height: "97%" }}
    >
      <div
        className="panel-head"
        style={{
          display: "flex",
          alignItems: "flex-start",
          gap: 12,
          marginBottom: 12,
        }}
      >
        <div>
          <h2
            style={{
              margin: 0,
              display: "flex",
              alignItems: "center",
              flexWrap: "wrap",
              gap: 10,
            }}
          >
            🎯 Weekly Goals
            {editable ? editButton : null}
          </h2>
          <div className="meta" style={{ fontSize: 12 }}>
            Manually curated · visible to the client
          </div>
        </div>
      </div>

      {hasText ? (
        <>
          {items.length ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {items.map((g, i) => (
                <div
                  key={i}
                  style={{
                    display: "flex",
                    alignItems: "baseline",
                    justifyContent: "space-between",
                    gap: 12,
                  }}
                >
                  <span style={{ fontWeight: 700, fontSize: 14 }}>
                    {g.title}
                  </span>
                  <span
                    style={{
                      fontWeight: 700,
                      fontSize: 14,
                      whiteSpace: "nowrap",
                    }}
                  >
                    {g.value}
                  </span>
                </div>
              ))}
            </div>
          ) : null}
          {notes ? (
            <div
              style={{
                marginTop: items.length ? 14 : 0,
                fontSize: 14,
                lineHeight: 1.6,
                whiteSpace: "pre-wrap",
              }}
            >
              {notes}
            </div>
          ) : null}
        </>
      ) : (
        <div className="meta" style={{ fontSize: 13, lineHeight: 1.65 }}>
          {`🎯 No goals set yet.${editable ? " Use \u201CAdd goals\u201D to capture this client\u2019s goals." : ""}`}
        </div>
      )}

      {/* The byline only appears once there is something to attribute. */}
      {hasText && updatedText ? (
        <div className="meta" style={{ fontSize: 12, marginTop: 10 }}>
          Last updated {updatedText}
          {updatedByName ? ` by ${updatedByName}` : ""}
        </div>
      ) : null}
    </div>
  );
}

// Summary and goals side by side; both wrap to full width on narrow screens.
export function SummaryWithGoals({ summary, goals }) {
  return (
    <div
      style={{
        display: "flex",
        gap: 16,
        alignItems: "stretch",
        flexWrap: "wrap",
      }}
    >
      <div style={{ flex: "1 1 380px", minWidth: 300 }}>{summary}</div>
      <div style={{ flex: "1 1 320px", minWidth: 280 }}>{goals}</div>
    </div>
  );
}
