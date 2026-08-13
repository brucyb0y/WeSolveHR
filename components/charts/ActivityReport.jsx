// Activity report panel — the React version of buildActivityReport().
//
// Renders one time window (today, or a given week) from an aggregate produced
// by buildClientAutoReportSections: KPI tiles, a team-contribution stacked bar
// chart, an activity-mix donut, and a per-member highlight table.
//
// It takes ALREADY-AGGREGATED totals and rows. The aggregation itself is ~400
// lines of window/attribution logic in lib/server/app.js and stays there —
// re-implementing it here to feed a different renderer is exactly how the staff
// and customer reports would start disagreeing about the same week.

import {
  ArKpiCard,
  ArStackedBars,
  ArDonut,
  ArLegend,
  ArHighlightTable,
  ArCard,
  ArIcon,
} from "./AutoReport";

export default function ActivityReport({
  title,
  eyebrow,
  rangeLabel,
  totals,
  rows,
  periodWord,
  live,
  metrics,
}) {
  const mixTotal = metrics.reduce((s, m) => s + (Number(totals[m.key]) || 0), 0);

  const donutSegments = metrics.map((m) => ({
    label: m.label,
    color: m.color,
    value: Number(totals[m.key]) || 0,
  }));

  const tableCols = [
    { key: "name", label: "Team Member", isLabel: true },
    ...metrics.map((m) => ({ key: m.key, label: m.label, color: m.color })),
    {
      key: "incentive",
      label: "Incentives ₹",
      color: "#2dd4bf",
      fmt: (v) => `₹${v}`,
    },
  ];

  return (
    <div className="panel ar-wrap">
      <div className="ar-head">
        <div>
          <div className="ar-eyebrow">
            <ArIcon name="flag" /> {eyebrow}
          </div>
          <h2>{title}</h2>
          <div className="ar-sub">{rangeLabel}</div>
        </div>
        {/* "Live today" only on the daily view — a past week is settled, and
            a pulsing dot on it would imply it is still moving. */}
        {live ? (
          <span className="ar-chip">
            <span className="ar-live-dot" /> Live today
          </span>
        ) : (
          <span className="ar-chip">{rangeLabel}</span>
        )}
      </div>

      <div className="ar-kpis">
        <ArKpiCard
          label="Campaigns"
          value={totals.campaigns}
          color="#8b7cf6"
          icon="megaphone"
        />
        <ArKpiCard
          label="Leads converted"
          value={totals.converted}
          color="#58c98a"
          icon="check"
        />
        <ArKpiCard
          label="Meetings"
          value={totals.meetings}
          color="#6ea8ff"
          icon="calendar"
        />
        <ArKpiCard
          label="MOMs recorded"
          value={totals.moms}
          color="#f3b562"
          icon="doc"
        />
        <ArKpiCard
          label="Blockers raised"
          value={totals.blockers}
          color="#ef6b73"
          icon="alert"
        />
        <ArKpiCard
          label="Incentives"
          value={`₹${totals.incentive || 0}`}
          color="#2dd4bf"
          icon="rupee"
        />
      </div>

      <div className="ar-charts">
        <ArCard title="Team contribution" sub={`activity ${periodWord}`}>
          <ArStackedBars
            rows={rows}
            series={metrics}
            emptyLabel={`No tracked activity ${periodWord} yet.`}
          />
          <ArLegend items={metrics} />
        </ArCard>

        <div className="ar-card ar-card-center">
          <div className="ar-card-head">
            <span className="ar-card-title">Activity mix</span>
          </div>
          <ArDonut
            segments={donutSegments}
            centerNum={mixTotal}
            centerSub="actions"
          />
          <ArLegend items={donutSegments} />
        </div>
      </div>

      <ArCard title="Per-member detail" sub={periodWord}>
        <ArHighlightTable
          columns={tableCols}
          rows={rows}
          totals={totals}
          emptyLabel={`No tracked activity ${periodWord} yet.`}
        />
      </ArCard>
    </div>
  );
}
