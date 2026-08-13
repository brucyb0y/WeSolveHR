// Pipeline funnel panel — the React version of buildFunnelReport().
//
// Mixes two different time bases on purpose, and the sub-labels say which is
// which:
//   * The funnel itself and the "Total leads" / "Converted" / "Conversion rate"
//     tiles are a SNAPSHOT — where every lead sits right now, all-time.
//   * "Leads added" and "Status moves", and all three movement charts, are
//     MOVEMENT within the selected window (today, or one week).
// Reading a movement number as a snapshot (or the reverse) is the easy mistake
// here, which is why every tile carries a `sub`.

import {
  ArKpiCard,
  ArBars,
  ArFunnelChart,
  ArHighlightTable,
  ArCard,
  ArIcon,
} from "./AutoReport";

const MEMBER_COLS = [
  { key: "name", label: "Team Member", isLabel: true },
  { key: "stageMoves", label: "Stage Moves", color: "#6ea8ff" },
  { key: "outreachMoves", label: "Outreach Moves", color: "#f3b562" },
  { key: "demoMoves", label: "Demo Moves", color: "#2dd4bf" },
  { key: "converted", label: "Converted", color: "#58c98a" },
];

export default function FunnelReport({
  colLabel,
  rangeLabel,
  memberPeriodLabel,
  agg,
  funnelStages,
  totalLeads,
  convertedNow,
  conversionRate,
}) {
  const pipelineMoveRows = [
    { label: "Leads added", value: agg.leadsAdded, color: "#8b7cf6" },
    ...agg.consecutiveTransitions.map((t) => ({
      label: t.label,
      value: t.value,
      color: "#6ea8ff",
    })),
  ];

  const outreachRows = agg.outreachRows.map((r) => ({
    label: r.label,
    value: r.value,
    color: "#f3b562",
  }));

  const demoRows = agg.demoRows.map((r) => ({
    label: r.label,
    value: r.value,
    color: "#2dd4bf",
  }));

  return (
    <div className="panel ar-wrap">
      <div className="ar-head">
        <div>
          <div className="ar-eyebrow">
            <ArIcon name="layers" /> Lead funnel
          </div>
          <h2>Pipeline Funnel</h2>
          <div className="ar-sub">
            Movement for {colLabel.toLowerCase()} · {rangeLabel}
          </div>
        </div>
        <span className="ar-chip">
          <span className="ar-live-dot" /> {totalLeads} leads live
        </span>
      </div>

      <div className="ar-kpis">
        <ArKpiCard
          label="Leads added"
          value={agg.leadsAdded}
          color="#8b7cf6"
          icon="userplus"
          sub={colLabel}
        />
        <ArKpiCard
          label="Status moves"
          value={agg.totalMoves}
          color="#6ea8ff"
          icon="moves"
          sub={colLabel}
        />
        <ArKpiCard
          label="Total leads"
          value={totalLeads}
          color="#f3b562"
          icon="layers"
          sub="in pipeline"
        />
        <ArKpiCard
          label="Converted"
          value={convertedNow}
          color="#58c98a"
          icon="check"
          sub="all-time"
        />
        <ArKpiCard
          label="Conversion rate"
          value={`${conversionRate}%`}
          color="#2dd4bf"
          icon="percent"
          sub="of all leads"
        />
      </div>

      <div className="ar-card ar-card-wide">
        <div className="ar-card-head">
          <span className="ar-card-title">Where leads sit now</span>
          <span className="ar-card-sub">current pipeline distribution</span>
        </div>
        <ArFunnelChart stages={funnelStages} total={totalLeads} />
      </div>

      {/* hideZero on all three: a movement chart listing every possible
          transition at zero buries the handful that actually happened. */}
      <div className="ar-charts3">
        <ArCard title="Pipeline movement" sub={colLabel}>
          <ArBars
            rows={pipelineMoveRows}
            emptyLabel="No pipeline movement tracked yet."
            hideZero
          />
        </ArCard>
        <ArCard title="Outreach movement" sub={colLabel}>
          <ArBars
            rows={outreachRows}
            emptyLabel="No outreach movement tracked yet."
            hideZero
          />
        </ArCard>
        <ArCard title="Demo movement" sub={colLabel}>
          <ArBars
            rows={demoRows}
            emptyLabel="No demo movement tracked yet."
            hideZero
          />
        </ArCard>
      </div>

      <ArCard title="By team member" sub={memberPeriodLabel}>
        <ArHighlightTable
          columns={MEMBER_COLS}
          rows={agg.memberRows}
          totals={agg.memberTotals}
          emptyLabel={`No per-member activity ${memberPeriodLabel} yet.`}
        />
      </ArCard>
    </div>
  );
}
