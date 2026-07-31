// Markup for GET /clients/:id.
//
// Body markup extracted verbatim from renderClientWorkspacePage() (lib/server/app.js
// lines 4843-11433). The document shell now comes from
// app/layout.jsx, the <style> block from ./client-workspace.css, and the inline
// <script> from public/js/.

import { CLIENT_REPORT_MAX_WEEKS } from "@/lib/data/client-reports.js";
import { CLIENT_LEAD_CATEGORY_TYPES, CLIENT_LEAD_CATEGORY_TYPE_LABELS, CLIENT_LEAD_DEMO_STATUSES, CLIENT_LEAD_OUTREACH_STATUSES, CLIENT_LEAD_PIPELINE_STAGES, REACH_VIA_CHANNELS } from "@/lib/server/constants.js";
import { APP_TIMEZONE } from "@/lib/server/runtime.js";
import { getDateStringInTimeZone, getTodayDateStringInTimeZone } from "@/lib/server/time.js";
import { escapeHtml, formatDateOnly, formatDateTime, formatDateTimeNoTz } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

const CLIENT_REPORT_METRICS = [
  { key: "campaigns", label: "Campaigns", color: "#8b7cf6" },
  { key: "converted", label: "Converted", color: "#58c98a" },
  { key: "meetings", label: "Meetings", color: "#6ea8ff" },
  { key: "moms", label: "MOMs", color: "#f3b562" },
  { key: "blockers", label: "Blockers", color: "#ef6b73" },
];

const CLIENT_REPORT_NEGATIVE_STAGES = new Set([
  "lost",
  "not_interested",
  "no_response",
]);

const CLIENT_REPORT_ICONS = {
  megaphone:
    '<path d="M3 8.5 13 5v9L3 11.5z"/><path d="M13 6.5 16 6v6l-3-.5z"/><path d="M5.5 11.5V14a2 2 0 0 0 3.6 1.2"/>',
  check: '<circle cx="9" cy="9" r="6.5"/><path d="M6 9.2 8.2 11.4 12 7"/>',
  calendar:
    '<rect x="3" y="4" width="12" height="11" rx="2"/><path d="M3 7.5h12M6.5 2.5V5M11.5 2.5V5"/>',
  doc: '<path d="M5 2.5h5l4 4V15a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V3.5a1 1 0 0 1 1-1z"/><path d="M10 2.5V6h4"/>',
  alert: '<path d="M9 3 16 15H2z"/><path d="M9 7.5v3.5M9 13h.01"/>',
  rupee: '<path d="M6 4h6M6 7h6M6 4.4c4 0 4.4 5.6.4 5.6H7l4.6 5.6"/>',
  userplus:
    '<circle cx="7.5" cy="6.5" r="2.8"/><path d="M3 15.5a4.5 4.5 0 0 1 9 0"/><path d="M14 6v4M12 8h4"/>',
  moves:
    '<path d="M3 5.5h3.3L13 13h2.7"/><path d="M13.2 11.3 15.7 13l-2.5 1.7"/><path d="M3 14.5h3.3L9 11.2"/>',
  layers: '<path d="M9 2.5 16 6l-7 3.5L2 6z"/><path d="M2 10.5 9 14l7-3.5"/>',
  percent:
    '<path d="M5 14 14 5"/><circle cx="6.2" cy="6.2" r="1.8"/><circle cx="12.8" cy="12.8" r="1.8"/>',
  flag: '<path d="M5 16V3"/><path d="M5 3.5h8l-2 3 2 3H5"/>',
};

function arIcon(name) {
  return `<svg viewBox="0 0 18 18" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${CLIENT_REPORT_ICONS[name] || ""}</svg>`;
}

function arKpiCard({ label, value, sub = "", color = "#8b7cf6", icon = "" }) {
  return `
    <div class="ar-kpi" style="--c:${color};">
      <div class="ar-kpi-top">
        <span class="ar-kpi-ico">${icon ? arIcon(icon) : ""}</span>
        <span class="ar-kpi-label">${escapeHtml(label)}</span>
      </div>
      <div class="ar-kpi-value">${escapeHtml(String(value))}</div>
      ${sub ? `<div class="ar-kpi-sub">${escapeHtml(sub)}</div>` : ""}
    </div>`;
}

function arLegend(items) {
  return `<div class="ar-legend">${items
    .map(
      (it) =>
        `<span class="ar-legend-item" style="--c:${it.color};"><span class="ar-dot"></span>${escapeHtml(it.label)}${
          it.value !== undefined && it.value !== null
            ? ` <b>${escapeHtml(String(it.value))}</b>`
            : ""
        }</span>`,
    )
    .join("")}</div>`;
}

function arBars(
  rows,
  { color = "#8b7cf6", emptyLabel = "No data yet.", hideZero = false } = {},
) {
  const visible = hideZero ? rows.filter((r) => Number(r.value) > 0) : rows;
  if (!visible.some((r) => Number(r.value) > 0)) {
    return `<div class="ar-empty">${escapeHtml(emptyLabel)}</div>`;
  }
  const max = Math.max(1, ...visible.map((r) => Number(r.value) || 0));
  return `<div class="ar-bars">${visible
    .map((r) => {
      const v = Number(r.value) || 0;
      const pct = v > 0 ? Math.max(Math.round((v / max) * 100), 3) : 0;
      return `
      <div class="ar-bar-row">
        <div class="ar-bar-label" title="${escapeHtml(r.label)}">${escapeHtml(r.label)}</div>
        <div class="ar-bar-track"><div class="ar-bar-fill" style="width:${pct}%; --c:${r.color || color};"></div></div>
        <div class="ar-bar-val">${escapeHtml(String(v))}</div>
      </div>`;
    })
    .join("")}</div>`;
}

function arStackedBars(rows, series, { emptyLabel = "No data yet." } = {}) {
  const totalOf = (r) => series.reduce((s, m) => s + (Number(r[m.key]) || 0), 0);
  if (!rows.length || rows.every((r) => totalOf(r) === 0)) {
    return `<div class="ar-empty">${escapeHtml(emptyLabel)}</div>`;
  }
  const maxTotal = Math.max(1, ...rows.map(totalOf));
  return `<div class="ar-bars">${rows
    .map((r) => {
      const t = totalOf(r);
      const widthPct = t > 0 ? Math.max(Math.round((t / maxTotal) * 100), 3) : 0;
      const segs = series
        .filter((m) => Number(r[m.key]) > 0)
        .map(
          (m) =>
            `<span class="ar-seg" style="flex:${Number(r[m.key])}; --c:${m.color};" title="${escapeHtml(m.label)}: ${Number(r[m.key])}"></span>`,
        )
        .join("");
      return `
      <div class="ar-bar-row">
        <div class="ar-bar-label" title="${escapeHtml(r.name)}">${escapeHtml(r.name)}</div>
        <div class="ar-bar-track"><div class="ar-stack" style="width:${widthPct}%;">${segs}</div></div>
        <div class="ar-bar-val">${t}</div>
      </div>`;
    })
    .join("")}</div>`;
}

function arDonut(
  segments,
  { size = 176, thickness = 24, centerNum = "", centerSub = "" } = {},
) {
  const total = segments.reduce((s, x) => s + (Number(x.value) || 0), 0);
  const r = (size - thickness) / 2;
  const c = size / 2;
  const circ = 2 * Math.PI * r;
  let offset = 0;
  const arcs =
    total > 0
      ? segments
          .filter((s) => Number(s.value) > 0)
          .map((s) => {
            const len = (Number(s.value) / total) * circ;
            const el = `<circle cx="${c}" cy="${c}" r="${r}" fill="none" stroke="${s.color}" stroke-width="${thickness}" stroke-dasharray="${len.toFixed(2)} ${(circ - len).toFixed(2)}" stroke-dashoffset="${(-offset).toFixed(2)}"/>`;
            offset += len;
            return el;
          })
          .join("")
      : "";
  return `
    <div class="ar-donut-wrap">
      <svg class="ar-donut" viewBox="0 0 ${size} ${size}" role="img" aria-label="Activity mix">
        <g transform="rotate(-90 ${c} ${c})">
          <circle cx="${c}" cy="${c}" r="${r}" fill="none" stroke="rgba(255,255,255,0.07)" stroke-width="${thickness}"/>
          ${arcs}
        </g>
        <text x="${c}" y="${c - 1}" text-anchor="middle" class="ar-donut-num" fill="var(--text-strong,#fff)">${escapeHtml(String(centerNum))}</text>
        <text x="${c}" y="${c + 17}" text-anchor="middle" class="ar-donut-sub" fill="var(--muted,#c4cce0)">${escapeHtml(String(centerSub))}</text>
      </svg>
    </div>`;
}

function arFunnelChart(stages, total) {
  if (!total) return `<div class="ar-empty">No leads in the pipeline yet.</div>`;
  const max = Math.max(1, ...stages.map((s) => Number(s.value) || 0));
  const positives = stages.filter((s) => !s.negative);
  const negatives = stages.filter((s) => s.negative);
  const posSpan = Math.max(1, positives.length - 1);
  const row = (s, i, span, isNeg) => {
    const v = Number(s.value) || 0;
    const pct = v > 0 ? Math.max(Math.round((v / max) * 100), 3) : 2;
    const share = total ? Math.round((v / total) * 100) : 0;
    const hue = isNeg ? 353 : Math.round(262 - 112 * (span > 1 ? i / span : 0));
    const color = `hsl(${hue} ${isNeg ? 74 : 66}% 61%)`;
    return `
      <div class="ar-funnel-row">
        <div class="ar-funnel-label" title="${escapeHtml(s.label)}">${escapeHtml(s.label)}</div>
        <div class="ar-funnel-bar-wrap"><div class="ar-funnel-bar" style="width:${pct}%; background:${color};">${v > 0 ? `<span>${v}</span>` : ""}</div></div>
        <div class="ar-funnel-meta">${v}<small> · ${share}%</small></div>
      </div>`;
  };
  return `<div class="ar-funnel">
    ${positives.map((s, i) => row(s, i, posSpan, false)).join("")}
    ${negatives.length ? `<div class="ar-funnel-divider"><span>Closed · negative</span></div>${negatives.map((s) => row(s, 0, 1, true)).join("")}` : ""}
  </div>`;
}

function arHighlightTable({ columns, rows, totals, emptyLabel = "No data yet." }) {
  const numCols = columns.filter((c) => !c.isLabel);
  const max = {};
  numCols.forEach((c) => {
    max[c.key] = Math.max(1, ...rows.map((r) => Number(r[c.key]) || 0));
  });
  const fmt = (c, v) => (c.fmt ? c.fmt(v) : String(v));
  const head = `<tr>${columns
    .map(
      (c) =>
        `<th class="${c.isLabel ? "ar-th-label" : "ar-th-num"}">${escapeHtml(c.label)}</th>`,
    )
    .join("")}</tr>`;
  const body = rows.length
    ? rows
        .map(
          (r) =>
            `<tr>${columns
              .map((c) => {
                if (c.isLabel)
                  return `<td class="ar-td-label">${escapeHtml(String(r[c.key] ?? ""))}</td>`;
                const v = Number(r[c.key]) || 0;
                const pct = v > 0 ? Math.max(Math.round((v / max[c.key]) * 100), 5) : 0;
                return `<td class="ar-td-num"><span class="ar-cell-num">${escapeHtml(fmt(c, v))}</span><span class="ar-cell-track"><span class="ar-cell-bar" style="width:${pct}%; --c:${c.color};"></span></span></td>`;
              })
              .join("")}</tr>`,
        )
        .join("")
    : `<tr><td colspan="${columns.length}" class="ar-empty-cell">${escapeHtml(emptyLabel)}</td></tr>`;
  const foot =
    rows.length && totals
      ? `<tr class="ar-total-row">${columns
          .map((c, i) =>
            i === 0
              ? `<td class="ar-td-label">Total</td>`
              : `<td class="ar-td-num"><span class="ar-cell-num">${escapeHtml(fmt(c, Number(totals[c.key]) || 0))}</span></td>`,
          )
          .join("")}</tr>`
      : "";
  return `<div class="ar-table-wrap"><table class="ar-table"><thead>${head}</thead><tbody>${body}</tbody>${foot ? `<tfoot>${foot}</tfoot>` : ""}</table></div>`;
}

function buildActivityReport({
  title,
  eyebrow,
  rangeLabel,
  totals,
  rows,
  periodWord,
  live,
}) {
  const kpiCards = [
    arKpiCard({ label: "Campaigns", value: totals.campaigns, color: "#8b7cf6", icon: "megaphone" }),
    arKpiCard({ label: "Leads converted", value: totals.converted, color: "#58c98a", icon: "check" }),
    arKpiCard({ label: "Meetings", value: totals.meetings, color: "#6ea8ff", icon: "calendar" }),
    arKpiCard({ label: "MOMs recorded", value: totals.moms, color: "#f3b562", icon: "doc" }),
    arKpiCard({ label: "Blockers raised", value: totals.blockers, color: "#ef6b73", icon: "alert" }),
    arKpiCard({ label: "Incentives", value: "₹" + (totals.incentive || 0), color: "#2dd4bf", icon: "rupee" }),
  ].join("");
  const mixTotal = CLIENT_REPORT_METRICS.reduce(
    (s, m) => s + (Number(totals[m.key]) || 0),
    0,
  );
  const donutSegments = CLIENT_REPORT_METRICS.map((m) => ({
    label: m.label,
    color: m.color,
    value: Number(totals[m.key]) || 0,
  }));
  const tableCols = [
    { key: "name", label: "Team Member", isLabel: true },
    ...CLIENT_REPORT_METRICS.map((m) => ({ key: m.key, label: m.label, color: m.color })),
    { key: "incentive", label: "Incentives ₹", color: "#2dd4bf", fmt: (v) => "₹" + v },
  ];
  return `
    <div class="panel ar-wrap">
      <div class="ar-head">
        <div>
          <div class="ar-eyebrow">${arIcon("flag")} ${escapeHtml(eyebrow)}</div>
          <h2>${escapeHtml(title)}</h2>
          <div class="ar-sub">${escapeHtml(rangeLabel)}</div>
        </div>
        ${live ? `<span class="ar-chip"><span class="ar-live-dot"></span> Live today</span>` : `<span class="ar-chip">${escapeHtml(rangeLabel)}</span>`}
      </div>

      <div class="ar-kpis">${kpiCards}</div>

      <div class="ar-charts">
        <div class="ar-card">
          <div class="ar-card-head"><span class="ar-card-title">Team contribution</span><span class="ar-card-sub">activity ${escapeHtml(periodWord)}</span></div>
          ${arStackedBars(rows, CLIENT_REPORT_METRICS, { emptyLabel: `No tracked activity ${periodWord} yet.` })}
          ${arLegend(CLIENT_REPORT_METRICS)}
        </div>
        <div class="ar-card ar-card-center">
          <div class="ar-card-head"><span class="ar-card-title">Activity mix</span></div>
          ${arDonut(donutSegments, { centerNum: mixTotal, centerSub: "actions" })}
          ${arLegend(donutSegments)}
        </div>
      </div>

      <div class="ar-card">
        <div class="ar-card-head"><span class="ar-card-title">Per-member detail</span><span class="ar-card-sub">${escapeHtml(periodWord)}</span></div>
        ${arHighlightTable({ columns: tableCols, rows, totals, emptyLabel: `No tracked activity ${periodWord} yet.` })}
      </div>
    </div>`;
}

const CLIENT_REPORT_STYLES = `<style id="ar-report-styles">
.ar-wrap{padding:20px;}
.ar-head{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:18px;}
.ar-head h2{margin:0;font-size:20px;font-weight:900;letter-spacing:-.01em;color:var(--text-strong,#fff);}
.ar-eyebrow{display:inline-flex;align-items:center;gap:7px;font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.09em;color:var(--primary,#8b7cf6);margin-bottom:7px;}
.ar-eyebrow svg{width:14px;height:14px;}
.ar-sub{color:var(--muted,#c4cce0);font-size:13px;margin-top:4px;}
.ar-chip{display:inline-flex;align-items:center;gap:7px;padding:7px 12px;border-radius:999px;font-size:12px;font-weight:800;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.11);color:var(--text,#e7ecf6);white-space:nowrap;}
.ar-live-dot{width:7px;height:7px;border-radius:50%;background:var(--success,#58c98a);animation:ar-pulse 2s infinite;}
@keyframes ar-pulse{0%{box-shadow:0 0 0 0 color-mix(in srgb,var(--success,#58c98a) 55%,transparent);}70%{box-shadow:0 0 0 7px transparent;}100%{box-shadow:0 0 0 0 transparent;}}

.ar-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(158px,1fr));gap:12px;margin-bottom:16px;}
.ar-kpi{position:relative;overflow:hidden;padding:15px 16px 16px;border-radius:16px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(150deg,color-mix(in srgb,var(--c) 15%,transparent),rgba(255,255,255,.02));transition:transform .18s ease,border-color .18s ease;}
.ar-kpi:hover{transform:translateY(-2px);border-color:color-mix(in srgb,var(--c) 45%,transparent);}
.ar-kpi::after{content:"";position:absolute;left:0;top:0;bottom:0;width:4px;background:var(--c);}
.ar-kpi-top{display:flex;align-items:center;gap:9px;}
.ar-kpi-ico{display:inline-grid;place-items:center;width:30px;height:30px;border-radius:10px;background:color-mix(in srgb,var(--c) 24%,transparent);color:var(--c);flex:none;}
.ar-kpi-ico svg{width:17px;height:17px;}
.ar-kpi-label{font-size:11.5px;font-weight:800;color:var(--muted,#c4cce0);text-transform:uppercase;letter-spacing:.04em;}
.ar-kpi-value{font-size:30px;font-weight:900;line-height:1.04;margin-top:12px;color:var(--text-strong,#fff);}
.ar-kpi-sub{margin-top:4px;font-size:11.5px;font-weight:700;color:var(--muted,#c4cce0);}

.ar-charts{display:grid;grid-template-columns:1.55fr 1fr;gap:14px;margin-bottom:14px;}
.ar-charts3{display:grid;grid-template-columns:repeat(auto-fit,minmax(228px,1fr));gap:14px;margin-bottom:14px;}
.ar-card{padding:16px 17px;border-radius:16px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.022);}
.ar-card-wide{margin-bottom:14px;}
.ar-card-center{display:flex;flex-direction:column;}
.ar-card-head{display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin-bottom:16px;}
.ar-card-title{font-size:12.5px;font-weight:800;color:var(--text-strong,#fff);text-transform:uppercase;letter-spacing:.06em;}
.ar-card-sub{font-size:11.5px;color:var(--muted,#c4cce0);font-weight:600;}

.ar-bars{display:grid;gap:11px;}
.ar-bar-row{display:grid;grid-template-columns:122px 1fr 46px;align-items:center;gap:11px;}
.ar-bar-label{font-size:12px;font-weight:700;color:var(--text,#e7ecf6);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.ar-bar-track{position:relative;height:13px;border-radius:999px;background:rgba(255,255,255,.06);overflow:hidden;}
.ar-bar-fill{height:100%;border-radius:999px;min-width:3px;background:linear-gradient(90deg,color-mix(in srgb,var(--c) 55%,transparent),var(--c));transition:width .55s cubic-bezier(.22,.61,.36,1);}
.ar-stack{display:flex;height:13px;border-radius:999px;overflow:hidden;min-width:3px;transition:width .55s cubic-bezier(.22,.61,.36,1);}
.ar-seg{height:100%;background:var(--c);}
.ar-bar-val{font-size:12.5px;font-weight:800;color:var(--text-strong,#fff);text-align:right;}

.ar-legend{display:flex;flex-wrap:wrap;gap:8px 14px;margin-top:15px;}
.ar-legend-item{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;font-weight:700;color:var(--muted,#c4cce0);}
.ar-legend-item b{color:var(--text-strong,#fff);font-weight:800;}
.ar-dot{width:10px;height:10px;border-radius:3px;background:var(--c);flex:none;}

.ar-donut-wrap{display:flex;align-items:center;justify-content:center;flex:1;padding:8px 0 4px;}
.ar-donut{width:176px;height:176px;}
.ar-donut-num{font-size:30px;font-weight:900;}
.ar-donut-sub{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;}

.ar-funnel{display:grid;gap:7px;}
.ar-funnel-row{display:grid;grid-template-columns:150px 1fr 84px;align-items:center;gap:12px;}
.ar-funnel-label{font-size:12px;font-weight:700;color:var(--text,#e7ecf6);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.ar-funnel-bar-wrap{display:flex;justify-content:center;}
.ar-funnel-bar{height:25px;border-radius:7px;min-width:5px;display:flex;align-items:center;justify-content:center;box-shadow:inset 0 -7px 11px rgba(0,0,0,.16),inset 0 7px 9px rgba(255,255,255,.10);transition:width .55s cubic-bezier(.22,.61,.36,1);}
.ar-funnel-bar span{font-size:11px;font-weight:800;color:rgba(8,12,25,.8);white-space:nowrap;}
.ar-funnel-meta{font-size:13px;font-weight:800;color:var(--text-strong,#fff);text-align:right;}
.ar-funnel-meta small{color:var(--muted,#c4cce0);font-weight:700;}
.ar-funnel-divider{display:flex;align-items:center;gap:10px;margin:7px 0 1px;color:var(--muted,#c4cce0);font-size:10.5px;font-weight:800;text-transform:uppercase;letter-spacing:.09em;}
.ar-funnel-divider span{flex:none;}
.ar-funnel-divider::after{content:"";flex:1;height:1px;background:rgba(255,255,255,.12);}

.ar-table-wrap{overflow-x:auto;}
.ar-table{width:100%;border-collapse:collapse;}
.ar-table th{font-size:10.5px;font-weight:800;text-transform:uppercase;letter-spacing:.07em;color:var(--muted,#c4cce0);padding:0 12px 11px;border-bottom:1px solid rgba(255,255,255,.10);}
.ar-th-label{text-align:left;}
.ar-th-num{text-align:right;}
.ar-table td{padding:12px;border-bottom:1px solid rgba(255,255,255,.055);vertical-align:middle;}
.ar-td-label{font-weight:800;color:var(--text-strong,#fff);font-size:13px;white-space:nowrap;}
.ar-td-num{text-align:right;min-width:78px;}
.ar-cell-num{display:block;text-align:right;font-size:13px;font-weight:800;color:var(--text-strong,#fff);}
.ar-cell-track{display:block;height:4px;border-radius:3px;background:rgba(255,255,255,.06);margin-top:6px;overflow:hidden;}
.ar-cell-bar{display:block;height:100%;border-radius:3px;background:var(--c);transition:width .55s cubic-bezier(.22,.61,.36,1);}
.ar-total-row td{border-top:2px solid rgba(255,255,255,.18);border-bottom:none;padding-top:13px;}
.ar-total-row .ar-cell-num,.ar-total-row .ar-td-label{font-weight:900;}
.ar-empty-cell{text-align:center;color:var(--muted,#c4cce0);font-style:italic;font-size:12.5px;padding:22px 12px;}
.ar-empty{padding:20px;text-align:center;color:var(--muted,#c4cce0);font-size:12.5px;font-style:italic;border:1px dashed rgba(255,255,255,.13);border-radius:12px;}

@media (max-width:900px){.ar-charts{grid-template-columns:1fr;}}
@media (max-width:560px){
  .ar-wrap{padding:15px;}
  .ar-bar-row{grid-template-columns:90px 1fr 38px;gap:8px;}
  .ar-funnel-row{grid-template-columns:100px 1fr 64px;gap:8px;}
}
</style>`;

function mondayStartOfUtcMs(ms) {
  const dayMs = 24 * 60 * 60 * 1000;
  const midnight = Math.floor(ms / dayMs) * dayMs;
  const daysSinceMonday = (new Date(midnight).getUTCDay() + 6) % 7;
  return midnight - daysSinceMonday * dayMs;
}

const NAVII_WEEK_ANCHOR_MS = Date.parse("2026-05-25T00:00:00Z");

function clientWeeklyReportNumbering(client) {
  const id = String(client?.name || client?.slug || client?.company_name || "")
    .trim()
    .toLowerCase();
  if (id === "navii") {
    return { anchorMs: NAVII_WEEK_ANCHOR_MS, minWeekNum: 4 };
  }
  return null;
}

function buildClientAutoReportSections({
  leadAllRows = [],
  campaigns = [],
  meetings = [],
  blockers = [],
  incentives = [],
  leadStageEvents = [],
  users = [],
  weekNumbering = null,
}) {
  const getUserName = (userId) =>
    users.find((u) => String(u.id) === String(userId))?.name || "-";
  const momFilled = (m) =>
    !!(
      m.summary ||
      m.discussion_points ||
      m.decisions ||
      m.deliverables ||
      m.action_items ||
      m.follow_ups ||
      m.next_steps
    );

  const dayMs = 24 * 60 * 60 * 1000;
  const weekWindowMs = 7 * dayMs;
  const weeklyNowMs = Date.now();
  const tsOf = (d) => (d ? new Date(d).getTime() : 0);

  const mondayStartOf = mondayStartOfUtcMs;
  const currentWeekStartMs = mondayStartOf(weeklyNowMs);

  // Map a user's display name -> id so free-text lead assignment can attribute.
  const userIdByName = {};
  users.forEach((u) => {
    if (u.name) userIdByName[String(u.name).trim().toLowerCase()] = String(u.id);
  });

  const transitionKey = (from, to) => `${from || "?"}->${to || "?"}`;
  const bump = (obj, key) => {
    obj[key] = (obj[key] || 0) + 1;
  };
  const sumValues = (obj) => Object.values(obj).reduce((a, b) => a + b, 0);
  const blankMember = () => ({
    leadsAdded: 0,
    stageMoves: 0,
    outreachMoves: 0,
    demoMoves: 0,
    converted: 0,
  });
  const buildMemberRows = (store) =>
    Object.keys(store)
      .map((key) => ({
        key,
        name: key === "unattributed" ? "Unattributed" : getUserName(key),
        ...store[key],
      }))
      .filter(
        (r) =>
          r.leadsAdded ||
          r.stageMoves ||
          r.outreachMoves ||
          r.demoMoves ||
          r.converted,
      )
      .sort((a, b) => {
        const score = (r) =>
          r.leadsAdded + r.stageMoves + r.outreachMoves + r.demoMoves + r.converted;
        return score(b) - score(a);
      });
  const sumMemberTotals = (rows) =>
    rows.reduce(
      (acc, r) => {
        acc.leadsAdded += r.leadsAdded;
        acc.stageMoves += r.stageMoves;
        acc.outreachMoves += r.outreachMoves;
        acc.demoMoves += r.demoMoves;
        acc.converted += r.converted;
        return acc;
      },
      { leadsAdded: 0, stageMoves: 0, outreachMoves: 0, demoMoves: 0, converted: 0 },
    );

  // Aggregate every report metric for a single time window, expressed as an
  // `inWindow(timestamp)` predicate. Reused for the daily view and for each
  // rolling weekly window so every report view shares identical attribution.
  const aggregateWindow = (inWindow) => {
    // Activity stats: campaigns / converted / meetings / MOMs / blockers / ₹.
    const stats = {};
    const ensure = (userId) => {
      const key = userId ? String(userId) : "unattributed";
      if (!stats[key]) {
        stats[key] = {
          campaigns: 0,
          converted: 0,
          meetings: 0,
          moms: 0,
          blockers: 0,
          incentive: 0,
        };
      }
      return stats[key];
    };
    campaigns.forEach((c) => {
      if (inWindow(c.created_at)) ensure(c.created_by_user_id).campaigns += 1;
    });
    leadAllRows.forEach((l) => {
      if (l.pipeline_stage === "converted" && inWindow(l.updated_at)) {
        const uid =
          userIdByName[String(l.assigned_to || "").trim().toLowerCase()] || null;
        ensure(uid).converted += 1;
      }
    });
    meetings.forEach((m) => {
      const when = m.meeting_date || m.created_at;
      if (inWindow(when)) {
        const s = ensure(m.created_by_user_id);
        s.meetings += 1;
        if (momFilled(m)) s.moms += 1;
      }
    });
    blockers.forEach((b) => {
      if (inWindow(b.created_at)) ensure(b.owner_user_id).blockers += 1;
    });
    incentives.forEach((i) => {
      if (inWindow(i.created_at))
        ensure(i.gtm_user_id).incentive += Number(i.amount) || 0;
    });

    const totals = Object.values(stats).reduce(
      (acc, s) => {
        acc.campaigns += s.campaigns;
        acc.converted += s.converted;
        acc.meetings += s.meetings;
        acc.moms += s.moms;
        acc.blockers += s.blockers;
        acc.incentive += s.incentive;
        return acc;
      },
      { campaigns: 0, converted: 0, meetings: 0, moms: 0, blockers: 0, incentive: 0 },
    );
    const rows = Object.keys(stats)
      .map((key) => ({
        key,
        name: key === "unattributed" ? "Unattributed" : getUserName(key),
        ...stats[key],
      }))
      .filter(
        (r) =>
          r.campaigns ||
          r.converted ||
          r.meetings ||
          r.moms ||
          r.blockers ||
          r.incentive,
      )
      .sort((a, b) => {
        const score = (r) =>
          r.campaigns + r.converted + r.meetings + r.moms + r.blockers;
        return score(b) - score(a);
      });

    // Funnel movement: pipeline / outreach / demo transitions + per-member.
    const stageTrans = {};
    const outreachTo = {};
    const demoTo = {};
    const memberStore = {};
    const ensureMember = (key) => {
      if (!memberStore[key]) memberStore[key] = blankMember();
      return memberStore[key];
    };
    leadStageEvents.forEach((ev) => {
      const nv = ev.new_value || {};
      const field = nv.field;
      const to = nv.to;
      if (!field || !to) return;
      if (!inWindow(ev.created_at)) return;
      const actorKey = ev.actor_user_id
        ? String(ev.actor_user_id)
        : "unattributed";
      if (field === "pipeline_stage") {
        bump(stageTrans, transitionKey(nv.from, to));
        ensureMember(actorKey).stageMoves += 1;
      } else if (field === "outreach_status") {
        bump(outreachTo, to);
        ensureMember(actorKey).outreachMoves += 1;
      } else if (field === "demo_status") {
        bump(demoTo, to);
        ensureMember(actorKey).demoMoves += 1;
      }
    });
    leadAllRows.forEach((l) => {
      const uid =
        userIdByName[String(l.assigned_to || "").trim().toLowerCase()] ||
        "unattributed";
      if (inWindow(l.created_at)) ensureMember(uid).leadsAdded += 1;
      if (l.pipeline_stage === "converted" && inWindow(l.updated_at))
        ensureMember(uid).converted += 1;
    });

    const consecutiveTransitions = CLIENT_LEAD_PIPELINE_STAGES.slice(0, -1).map(
      (from, i) => {
        const to = CLIENT_LEAD_PIPELINE_STAGES[i + 1];
        return {
          label: `${from.label} → ${to.label}`,
          value: stageTrans[transitionKey(from.key, to.key)] || 0,
        };
      },
    );
    const outreachRows = CLIENT_LEAD_OUTREACH_STATUSES.map((s) => ({
      label: s.label,
      value: outreachTo[s.key] || 0,
    }));
    const demoRows = CLIENT_LEAD_DEMO_STATUSES.map((s) => ({
      label: s.label,
      value: demoTo[s.key] || 0,
    }));
    const leadsAdded = leadAllRows.filter((l) => inWindow(l.created_at)).length;
    const totalMoves =
      sumValues(stageTrans) + sumValues(outreachTo) + sumValues(demoTo);
    const memberRows = buildMemberRows(memberStore);
    const memberTotals = sumMemberTotals(memberRows);

    return {
      totals,
      rows,
      leadsAdded,
      totalMoves,
      memberRows,
      memberTotals,
      consecutiveTransitions,
      outreachRows,
      demoRows,
    };
  };

  // Live pipeline snapshot — identical for every window, so computed once and
  // reused by each funnel panel.
  const funnelSnapshot = {};
  CLIENT_LEAD_PIPELINE_STAGES.forEach((s) => {
    funnelSnapshot[s.key] = 0;
  });
  leadAllRows.forEach((l) => {
    const k = l.pipeline_stage || "prospect_identified";
    funnelSnapshot[k] = (funnelSnapshot[k] || 0) + 1;
  });
  const totalLeads = leadAllRows.length;
  const convertedNow = funnelSnapshot["converted"] || 0;
  const conversionRate = totalLeads
    ? Math.round((convertedNow / totalLeads) * 100)
    : 0;
  const funnelStages = CLIENT_LEAD_PIPELINE_STAGES.map((s) => ({
    label: s.label,
    value: funnelSnapshot[s.key] || 0,
    negative: CLIENT_REPORT_NEGATIVE_STAGES.has(s.key),
  }));
  const funnelChartHtml = arFunnelChart(funnelStages, totalLeads);

  // Builds the full lead-funnel panel for a single window from its aggregate:
  // KPI tiles, the live pipeline funnel, movement bar charts and a member table.
  const buildFunnelReport = ({ colLabel, rangeLabel, agg, memberPeriodLabel }) => {
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
    const memberCols = [
      { key: "name", label: "Team Member", isLabel: true },
      { key: "stageMoves", label: "Stage Moves", color: "#6ea8ff" },
      { key: "outreachMoves", label: "Outreach Moves", color: "#f3b562" },
      { key: "demoMoves", label: "Demo Moves", color: "#2dd4bf" },
      { key: "converted", label: "Converted", color: "#58c98a" },
    ];
    return `
      <div class="panel ar-wrap">
        <div class="ar-head">
          <div>
            <div class="ar-eyebrow">${arIcon("layers")} Lead funnel</div>
            <h2>Pipeline Funnel</h2>
            <div class="ar-sub">Movement for ${escapeHtml(colLabel.toLowerCase())} · ${escapeHtml(rangeLabel)}</div>
          </div>
          <span class="ar-chip"><span class="ar-live-dot"></span> ${totalLeads} leads live</span>
        </div>

        <div class="ar-kpis">
          ${arKpiCard({ label: "Leads added", value: agg.leadsAdded, color: "#8b7cf6", icon: "userplus", sub: colLabel })}
          ${arKpiCard({ label: "Status moves", value: agg.totalMoves, color: "#6ea8ff", icon: "moves", sub: colLabel })}
          ${arKpiCard({ label: "Total leads", value: totalLeads, color: "#f3b562", icon: "layers", sub: "in pipeline" })}
          ${arKpiCard({ label: "Converted", value: convertedNow, color: "#58c98a", icon: "check", sub: "all-time" })}
          ${arKpiCard({ label: "Conversion rate", value: conversionRate + "%", color: "#2dd4bf", icon: "percent", sub: "of all leads" })}
        </div>

        <div class="ar-card ar-card-wide">
          <div class="ar-card-head"><span class="ar-card-title">Where leads sit now</span><span class="ar-card-sub">current pipeline distribution</span></div>
          ${funnelChartHtml}
        </div>

        <div class="ar-charts3">
          <div class="ar-card">
            <div class="ar-card-head"><span class="ar-card-title">Pipeline movement</span><span class="ar-card-sub">${escapeHtml(colLabel)}</span></div>
            ${arBars(pipelineMoveRows, { emptyLabel: "No pipeline movement tracked yet.", hideZero: true })}
          </div>
          <div class="ar-card">
            <div class="ar-card-head"><span class="ar-card-title">Outreach movement</span><span class="ar-card-sub">${escapeHtml(colLabel)}</span></div>
            ${arBars(outreachRows, { emptyLabel: "No outreach movement tracked yet.", hideZero: true })}
          </div>
          <div class="ar-card">
            <div class="ar-card-head"><span class="ar-card-title">Demo movement</span><span class="ar-card-sub">${escapeHtml(colLabel)}</span></div>
            ${arBars(demoRows, { emptyLabel: "No demo movement tracked yet.", hideZero: true })}
          </div>
        </div>

        <div class="ar-card">
          <div class="ar-card-head"><span class="ar-card-title">By team member</span><span class="ar-card-sub">${escapeHtml(memberPeriodLabel)}</span></div>
          ${arHighlightTable({ columns: memberCols, rows: agg.memberRows, totals: agg.memberTotals, emptyLabel: `No per-member activity ${memberPeriodLabel} yet.` })}
        </div>
      </div>`;
  };

  // ----------------------------------------------------------------------
  // Daily view — activity since the start of today (UTC).
  // ----------------------------------------------------------------------
  const dailyDateStr = new Date(weeklyNowMs).toISOString().slice(0, 10);
  const dayStartMs = new Date(dailyDateStr + "T00:00:00Z").getTime();
  const inToday = (d) => {
    const t = tsOf(d);
    return t > 0 && t >= dayStartMs;
  };
  const dailyRangeLabel = formatDateOnly(dailyDateStr);
  const dailyAgg = aggregateWindow(inToday);

  const dailyAutoReportHtml =
    CLIENT_REPORT_STYLES +
    buildActivityReport({
      title: "Today's Report",
      eyebrow: "Daily snapshot",
      rangeLabel: dailyRangeLabel,
      totals: dailyAgg.totals,
      rows: dailyAgg.rows,
      periodWord: "today",
      live: true,
    });
  const leadFunnelReportDailyHtml = buildFunnelReport({
    colLabel: "Today",
    rangeLabel: dailyRangeLabel,
    agg: dailyAgg,
    memberPeriodLabel: "today",
  });

  // ----------------------------------------------------------------------
  // Weekly views — one calendar week (Mon–Sat) per tab. Week 1 = the current
  // week, Week 2 = the previous week, and so on, back to the earliest tracked
  // activity (capped at CLIENT_REPORT_MAX_WEEKS).
  // ----------------------------------------------------------------------
  let earliestMs = weeklyNowMs;
  const considerTs = (d) => {
    const t = tsOf(d);
    if (t > 0 && t < earliestMs) earliestMs = t;
  };
  leadAllRows.forEach((l) => considerTs(l.created_at));
  campaigns.forEach((c) => considerTs(c.created_at));
  meetings.forEach((m) => considerTs(m.meeting_date || m.created_at));
  blockers.forEach((b) => considerTs(b.created_at));
  incentives.forEach((i) => considerTs(i.created_at));
  leadStageEvents.forEach((ev) => considerTs(ev.created_at));

  // Number of calendar weeks from the earliest activity's week up to this week.
  const spanWeeks =
    Math.round((currentWeekStartMs - mondayStartOf(earliestMs)) / weekWindowMs) +
    1;
  const weekCount = Math.min(
    CLIENT_REPORT_MAX_WEEKS,
    Math.max(1, spanWeeks),
  );

  // `num` (k) is the stable internal id: 1 = current calendar week, ascending
  // into the past. `displayNum` is the user-facing label. By default it matches
  // num (Week 1 = current week). When weekNumbering is set (e.g. Navii), the
  // latest week gets an absolute number that auto-advances from the anchor week,
  // labels count DOWN into the past, and any week older than minWeekNum is
  // dropped — so the dropdown shows the latest week on top and minWeekNum as the
  // oldest.
  const latestDisplayNum =
    weekNumbering != null
      ? Math.round(
          (currentWeekStartMs - mondayStartOf(weekNumbering.anchorMs)) /
            weekWindowMs,
        ) + 1
      : null;

  const weeklyReports = [];
  for (let k = 1; k <= weekCount; k++) {
    const displayNum = latestDisplayNum != null ? latestDisplayNum - (k - 1) : k;
    // Stop once we reach the pinned oldest week (weeks only get older as k grows).
    if (weekNumbering != null && displayNum < weekNumbering.minWeekNum) break;
    // Calendar week: Monday 00:00 → Saturday end (Sunday excluded). `startMs` is
    // this week's Monday for k=1, stepping back one week per k.
    const startMs = currentWeekStartMs - (k - 1) * weekWindowMs;
    const endMs = startMs + 6 * dayMs; // exclusive: Sunday 00:00, so Mon–Sat
    const inWeek = (d) => {
      const t = tsOf(d);
      return t > 0 && t >= startMs && t < endMs;
    };
    const agg = aggregateWindow(inWeek);
    // Label spans Monday → Saturday, but the current (in-progress) week is capped
    // at today so it reads "just the days elapsed so far".
    const labelEndMs = Math.min(startMs + 5 * dayMs, weeklyNowMs);
    const rangeLabel = `${formatDateOnly(new Date(startMs).toISOString().slice(0, 10))} – ${formatDateOnly(new Date(labelEndMs).toISOString().slice(0, 10))}`;
    weeklyReports.push({
      num: k,
      displayNum,
      rangeLabel,
      // Monday (UTC) date string — the storage key for this week's AI summary.
      weekStart: new Date(startMs).toISOString().slice(0, 10),
      activityHtml: buildActivityReport({
        title: `Week ${displayNum} Report`,
        eyebrow: "Mon–Sat snapshot",
        rangeLabel,
        totals: agg.totals,
        rows: agg.rows,
        periodWord: `in week ${displayNum}`,
        live: false,
      }),
      funnelHtml: buildFunnelReport({
        colLabel: `Week ${displayNum}`,
        rangeLabel,
        agg,
        memberPeriodLabel: `in week ${displayNum}`,
      }),
    });
  }

  return {
    dailyAutoReportHtml,
    leadFunnelReportDailyHtml,
    weeklyReports,
  };
}

function renderClientWorkspacePage({
  client,
  contacts = [],
  services = [],
  workItems = [],
  updates = [],
  actions = [],
  contributors = [],
  milestones = [],
  documents = [],
  users = [],
  linkedTasks = [],
  selectedTab = "overview",
  activityLogs = [],
  blockers = [],
  meetings = [],
  campaigns = [],
  incentives = [],
  reports = [],
  leads = [],
  leadAllRows = [],
  leadFilteredIds = [],
  leadStageEvents = [],
  leadCounts = {},
  leadPagination = null,
  selectedLeadTab = "all",
  leadSearch = "",
  leadFilters = {},
  leadMineOnly = false,
  leadCategoryTypeCounts = [],
  staticLeadBusiness = null,
  reportSummaries = { daily: null, weekly: null, weeklyByDate: {} },
  clientGoals = null,
}) {
  const activeTab = [
    "overview",
    "task",
    "leads",
    "campaigns",
    "meetings",
    "blockers",
    "team",
    "performance",
    "incentives",
    "report",
    "updates",
    "actions",
    "milestones",
    "documents",
  ].includes(selectedTab)
    ? selectedTab
    : "overview";

  // count: number|null → renders an at-a-glance badge on the tab.
  // tone: "" (neutral) | "attention" (needs action, highlighted).
  const tabLink = (key, label, icon = "", count = null, tone = "") => {
    const showCount = typeof count === "number" && count > 0;
    const countCls =
      "tab-count" + (tone === "attention" ? " tab-count-attention" : "");
    return `
    <a class="tab ${activeTab === key ? "active" : ""}" href="/clients/${Number(client.id)}?tab=${key}" aria-current="${activeTab === key ? "page" : "false"}">
      ${icon ? `<span class="tab-ico" aria-hidden="true">${icon}</span>` : ""}<span class="tab-label">${label}</span>${showCount ? `<span class="${countCls}">${count > 99 ? "99+" : count}</span>` : ""}
    </a>
  `;
  };

  const getUserName = (userId) =>
    users.find((u) => String(u.id) === String(userId))?.name || "-";

  const getWorkItemTitle = (workItemId) =>
    workItems.find((w) => String(w.id) === String(workItemId))?.title || "";
  const getMilestoneTitle = (milestoneId) =>
    milestones.find((m) => String(m.id) === String(milestoneId))?.title || "";
  const manualUpdateEvents = updates.map((u) => ({
    type: "manual_update",
    at: u.created_at,
    title: u.title || "Client update",
    text: u.update_text || "",
    by: getUserName(u.created_by_user_id),
    relatedWorkItemTitle: u.related_work_item_id
      ? getWorkItemTitle(u.related_work_item_id)
      : "",
  }));

  const activityEvents = activityLogs.map((log) => {
    const actionLabel = String(log.action || "").replaceAll("_", " ");
    const newValue = log.new_value || {};
    const oldValue = log.old_value || {};

    let text = actionLabel;

    if (log.action === "work_item_created") {
      text = `Created work item: ${newValue.title || "-"}`;
    }

    if (log.action === "work_item_updated") {
      if (oldValue.status !== newValue.status) {
        text = `Status changed: ${oldValue.status || "-"} → ${newValue.status || "-"}`;
      } else {
        text = `Updated work item: ${newValue.title || "-"}`;
      }
    }

    if (log.action === "work_item_archived") {
      text = `Archived work item: ${oldValue.title || newValue.title || "-"}`;
    }

    return {
      type: "activity",
      at: log.created_at,
      title: actionLabel,
      text,
      by: getUserName(log.actor_user_id),
      relatedWorkItemTitle:
        log.entity_type === "client_work_items"
          ? getWorkItemTitle(log.entity_id)
          : "",
    };
  });

  const timelineEvents = [...manualUpdateEvents, ...activityEvents]
    .filter((x) => x.at)
    .sort((a, b) => new Date(b.at).getTime() - new Date(a.at).getTime());

  const clientIdNum = Number(client.id);

  // Visibility chips (the PDF flow's internal-only vs client-visible markers).
  const VIS_CHIP = {
    internal: `<span class="vis-chip vis-internal">INTERNAL</span>`,
    client: `<span class="vis-chip vis-client">CLIENT</span>`,
  };

  // ----------------------------------------------------------------------
  // Leads tab (reuses the rasset/joolian leads engine via "client:<id>")
  // ----------------------------------------------------------------------
  const stageOptions = (current) =>
    CLIENT_LEAD_PIPELINE_STAGES.map(
      (s) =>
        `<option value="${s.key}" ${(current || "prospect_identified") === s.key ? "selected" : ""}>${escapeHtml(s.label)}</option>`,
    ).join("");

  // ---- Leads "Filter" popup state (Status / Demo / Reached-via / Notes) ----
  const lf = leadFilters || {};
  const lfPipeline = String(lf.pipeline_stage || "");
  const lfDemo = String(lf.demo_status || "");
  // Multi-select: comma-separated category-type keys (e.g. "agency_in,micro").
  const lfCategoryType = String(lf.category_type || "");
  const lfCategoryTypeList = lfCategoryType
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const lfLocation = String(lf.location || "");
  const lfAssignee = String(lf.assignee || "");
  // "Assign for Phone" / "Assign for Email" — same team-member option list as
  // ASSIGNED TO, filtered independently of it.
  const lfPhoneAssignee = String(lf.phone_assignee || "");
  const lfEmailAssignee = String(lf.email_assignee || "");
  // Multi-select: comma-separated channel keys (e.g. "linkedin,email").
  const lfReached = String(lf.reached_via || "");
  const lfReachedList = lfReached
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const lfNotes = String(lf.notes || "");
  const lfNotesBy = String(lf.notes_by || "");
  const lfNoteAudio = String(lf.has_note_audio || "");
  const lfHasPhone = String(lf.has_phone || "");
  const lfUpdatedFrom = String(lf.updated_from || "");
  const lfUpdatedTo = String(lf.updated_to || "");
  const lfCallbackFrom = String(lf.callback_date_from || "");
  const lfCallbackTo = String(lf.callback_date_to || "");
  const lfMissedCallback = String(lf.missed_callback || "");
  // Active column sort (persisted across search / filter / pagination). Not a
  // "filter" so it doesn't count toward the filter badge.
  const lfSort = String(lf.sort || "");
  const lfSortDir =
    String(lf.sort_dir || "").toLowerCase() === "asc" ? "asc" : "desc";
  const activeLeadFilterEntries = [
    ["pipeline_stage", lfPipeline],
    ["demo_status", lfDemo],
    ["category_type", lfCategoryType],
    ["location", lfLocation],
    ["assignee", lfAssignee],
    ["phone_assignee", lfPhoneAssignee],
    ["email_assignee", lfEmailAssignee],
    ["reached_via", lfReached],
    ["notes", lfNotes],
    ["notes_by", lfNotesBy],
    ["has_note_audio", lfNoteAudio],
    ["has_phone", lfHasPhone],
    ["updated_from", lfUpdatedFrom],
    ["updated_to", lfUpdatedTo],
    ["callback_date_from", lfCallbackFrom],
    ["callback_date_to", lfCallbackTo],
    ["missed_callback", lfMissedCallback],
  ].filter(([, v]) => v);
  // The updated-at and callback-date ranges are each a single control (from +
  // to), so count each pair once.
  const activeLeadFilterCount =
    activeLeadFilterEntries.filter(
      ([k]) =>
        !["updated_from", "updated_to", "callback_date_from", "callback_date_to"].includes(k),
    ).length +
    (lfUpdatedFrom || lfUpdatedTo ? 1 : 0) +
    (lfCallbackFrom || lfCallbackTo ? 1 : 0);
  const hasActiveLeadQuery = Boolean(leadSearch) || activeLeadFilterCount > 0;
  // Hidden inputs let the search form preserve active filters on submit.
  const leadFilterHiddenInputs = activeLeadFilterEntries
    .map(
      ([k, v]) => `<input type="hidden" name="${k}" value="${escapeHtml(v)}" />`,
    )
    .join("");
  // The active sort travels with search, filters and pagination so it isn't
  // lost when any of those change. Kept out of `leadFilterQs` so the "Clear
  // filters" link can drop filters while preserving the chosen sort.
  const leadSortQs = lfSort
    ? `&sort=${encodeURIComponent(lfSort)}&sort_dir=${encodeURIComponent(lfSortDir)}`
    : "";
  const leadSortHiddenInputs = lfSort
    ? `<input type="hidden" name="sort" value="${escapeHtml(lfSort)}" /><input type="hidden" name="sort_dir" value="${escapeHtml(lfSortDir)}" />`
    : "";
  // Clear links each reset only their own concern, preserving the other.
  const leadFilterQs = activeLeadFilterEntries
    .map(([k, v]) => `&${k}=${encodeURIComponent(v)}`)
    .join("");
  const leadMineQs = leadMineOnly ? "&mine=1" : "";
  const leadMineHiddenInput = leadMineOnly
    ? `<input type="hidden" name="mine" value="1" />`
    : "";
  const clearLeadSearchHref = `/clients/${clientIdNum}?tab=leads${leadFilterQs}${leadSortQs}${leadMineQs}`;
  const clearLeadFiltersHref = `/clients/${clientIdNum}?tab=leads${
    leadSearch ? `&search=${encodeURIComponent(leadSearch)}` : ""
  }${leadSortQs}${leadMineQs}`;
  // ---- Leads pagination controls (rendered above and below the table) ----
  // The backend serves 25 leads per page via getBusinessLeadsData; these links
  // preserve the active search / filters / lead-tab while changing only `page`.
  const leadPageQsParts = [];
  if (leadSearch)
    leadPageQsParts.push(`search=${encodeURIComponent(leadSearch)}`);
  activeLeadFilterEntries.forEach(([k, v]) =>
    leadPageQsParts.push(`${k}=${encodeURIComponent(v)}`),
  );
  if (selectedLeadTab && selectedLeadTab !== "all")
    leadPageQsParts.push(`leadTab=${encodeURIComponent(selectedLeadTab)}`);
  // "My leads only" defaults to off, so only the on state needs to travel
  // with search/filter/sort/pagination links.
  if (leadMineOnly) leadPageQsParts.push("mine=1");
  // Base query for filters + search + lead-tab (no sort, no page). Sort headers
  // build on this to swap only the sort; pagination adds sort + page.
  const leadBaseQs = leadPageQsParts.length
    ? `&${leadPageQsParts.join("&")}`
    : "";
  const leadPageHref = (p) =>
    `/clients/${clientIdNum}?tab=leads${leadBaseQs}${leadSortQs}&page=${p}`;
  // "My leads only" toggle — preserves search/filters/sort, resets to page 1
  // since the result set changes. Drops every explicit assignee pick (so the
  // assigned-to controls don't fight the toggle — it is its own mutually-
  // exclusive "just show mine" shortcut) and any baked-in "mine=0" (added to
  // leadPageQsParts above), since the href appends its own "mine=" value and
  // a duplicate query key would otherwise reach the server as an array.
  const leadMineToggleQsParts = leadPageQsParts.filter(
    (p) =>
      !p.startsWith("assignee=") &&
      !p.startsWith("phone_assignee=") &&
      !p.startsWith("email_assignee=") &&
      !p.startsWith("mine="),
  );
  const leadMineToggleBaseQs = leadMineToggleQsParts.length
    ? `&${leadMineToggleQsParts.join("&")}`
    : "";
  const leadMineToggleHref = (mineOn) =>
    `/clients/${clientIdNum}?tab=leads${leadMineToggleBaseQs}${leadSortQs}&mine=${mineOn ? "1" : "0"}`;
  // Category Type pill row — same base as leadBaseQs but with category_type
  // excluded so each pill can set (or clear) it independently.
  const leadCategoryPillQsParts = [];
  if (leadSearch)
    leadCategoryPillQsParts.push(`search=${encodeURIComponent(leadSearch)}`);
  activeLeadFilterEntries
    .filter(([k]) => k !== "category_type")
    .forEach(([k, v]) =>
      leadCategoryPillQsParts.push(`${k}=${encodeURIComponent(v)}`),
    );
  if (selectedLeadTab && selectedLeadTab !== "all")
    leadCategoryPillQsParts.push(`leadTab=${encodeURIComponent(selectedLeadTab)}`);
  if (leadMineOnly) leadCategoryPillQsParts.push("mine=1");
  const leadCategoryPillBaseQs = leadCategoryPillQsParts.length
    ? `&${leadCategoryPillQsParts.join("&")}`
    : "";
  const leadCategoryPillHref = (key) =>
    `/clients/${clientIdNum}?tab=leads${leadCategoryPillBaseQs}${leadSortQs}${
      key ? `&category_type=${encodeURIComponent(key)}` : ""
    }`;
  // Sortable column header links. Numeric-ish columns open descending on first
  // click; the name column opens ascending. Clicking the active column flips it.
  const NUMERIC_LEAD_SORTS = { stage: 1, demo: 1, notes: 1, updated: 1 };
  const leadSortHref = (field) => {
    const dir =
      lfSort === field
        ? lfSortDir === "asc"
          ? "desc"
          : "asc"
        : NUMERIC_LEAD_SORTS[field]
          ? "desc"
          : "asc";
    return `/clients/${clientIdNum}?tab=leads${leadBaseQs}&sort=${encodeURIComponent(field)}&sort_dir=${dir}`;
  };
  const leadSortArrow = (field) =>
    lfSort === field
      ? lfSortDir === "asc"
        ? " ↑"
        : " ↓"
      : ` <span style="opacity:.4;">↕</span>`;
  const leadSortTh = (field, label, extraStyle = "") =>
    `<th style="text-align:left; ${extraStyle}"><a href="${leadSortHref(field)}" style="color:inherit; text-decoration:none; white-space:nowrap; cursor:pointer;" title="Sort by ${escapeHtml(label)}">${escapeHtml(label)}${leadSortArrow(field)}</a></th>`;
  const renderLeadsPagination = (position) => {
    const lp = leadPagination;
    if (!lp || (!lp.hasPrev && !lp.hasNext)) return "";
    const pageSize = lp.pageSize || 25;
    const total = lp.total || 0;
    const cur = lp.page || 1;
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const start = total ? (cur - 1) * pageSize + 1 : 0;
    const end = Math.min(cur * pageSize, total);
    // The top bar is rendered in a tighter, smaller style so it doesn't
    // dominate the space above the table; the bottom bar keeps full size.
    const compact = position === "top";
    const btnStyle = compact
      ? "padding:4px 10px; font-size:12px;"
      : "";
    const disabled =
      `opacity:0.4; pointer-events:none; cursor:default; ${btnStyle}`;
    const metaStyle = compact ? "font-size:12px;" : "";
    const btn = (extra = "") => `style="${btnStyle}${extra}"`;
    return `
      <div class="leads-pagination" data-pos="${position}" style="display:flex; justify-content:space-between; align-items:center; gap:${compact ? "8px" : "12px"}; flex-wrap:wrap; padding:${compact ? "4px 2px" : "10px 2px"};">
        <div class="meta" style="${metaStyle}">Showing ${start}–${end} of ${total}</div>
        <div style="display:flex; gap:6px; align-items:center;">
          ${
            lp.hasPrev
              ? `<a class="btn" ${btn()} href="${leadPageHref(cur - 1)}">← Prev</a>`
              : `<span class="btn" style="${disabled}">← Prev</span>`
          }
          <span class="btn" style="${disabled}">Page ${cur} of ${totalPages}</span>
          ${
            lp.hasNext
              ? `<a class="btn" ${btn()} href="${leadPageHref(cur + 1)}">Next →</a>`
              : `<span class="btn" style="${disabled}">Next →</span>`
          }
        </div>
      </div>`;
  };
  // Client-lead filter option sets.
  const REACHED_VIA_FILTER_OPTIONS = [
    { key: "__none__", label: "None (not reached)" },
    ...REACH_VIA_CHANNELS.map((c) => ({ key: c.key, label: c.label })),
    { key: "both", label: "LinkedIn + Email" },
  ];
  const NOTES_FILTER_OPTIONS = [
    { key: "none", label: "No notes" },
    { key: "added", label: "Has notes" },
    { key: "multiple", label: "Multiple notes" },
  ];
  const NOTE_AUDIO_FILTER_OPTIONS = [
    { key: "yes", label: "Has audio" },
    { key: "no", label: "No audio" },
  ];
  const HAS_PHONE_FILTER_OPTIONS = [
    { key: "yes", label: "Yes" },
    { key: "no", label: "No" },
  ];
  const MISSED_CALLBACK_FILTER_OPTIONS = [
    { key: "yes", label: "Yes — overdue (past)" },
    { key: "no", label: "No — upcoming (future)" },
    { key: "none", label: "No callback date set" },
  ];
  // Note authors come from the org's users (notes are attributed by name).
  const noteAuthorOptions = Array.from(
    new Set((users || []).map((u) => u && u.name).filter(Boolean)),
  ).map((n) => ({ key: n, label: n }));
  // Assigned To filter options: team members plus a literal "Unassigned".
  const assignedToFilterOptions = [
    { key: "__unassigned__", label: "Unassigned" },
    ...noteAuthorOptions,
  ];
  const leadFilterLabelStyle =
    "display:flex; flex-direction:column; gap:4px; font-size:12px; font-weight:700; letter-spacing:0.02em; color:var(--muted, #9aa3c0);";
  // A <select> for the filter popup, pre-selecting the active value.
  const leadFilterSelect = (name, options, current) =>
    `<select name="${name}" style="width:100%; padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);"><option value="">All</option>${options
      .map(
        (s) =>
          `<option value="${escapeHtml(s.key)}" ${current === s.key ? "selected" : ""}>${escapeHtml(s.label)}</option>`,
      )
      .join("")}</select>`;
  // A collapsed multi-select for the filter popup: a select-like button that
  // summarizes the selection ("All" / one label / "N selected") and expands an
  // inline checkbox list. Checkboxes submit as repeated `name` query keys.
  const leadFilterMultiSelect = (name, options, selectedKeys) => {
    const picked = options.filter((o) => selectedKeys.includes(o.key));
    const summary =
      picked.length === 0
        ? "All"
        : picked.length === 1
          ? picked[0].label
          : `${picked.length} selected`;
    return `<div>
      <button type="button" class="lead-filter-ms-btn" onclick="toggleLeadFilterMs(this)" style="width:100%; padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text); display:flex; justify-content:space-between; align-items:center; gap:6px; cursor:pointer;"><span>${escapeHtml(summary)}</span><span style="opacity:.6;">▾</span></button>
      <div class="lead-filter-ms-panel" style="display:none; flex-direction:column; gap:2px; padding:8px; border:1px solid var(--line); border-radius:8px; background:rgba(255,255,255,0.04); max-height:160px; overflow:auto; margin-top:4px;">
        ${options
          .map(
            (o) =>
              `<label style="display:flex; align-items:center; gap:8px; font-size:12px; font-weight:400; letter-spacing:0; color:var(--text); cursor:pointer;"><input type="checkbox" name="${name}" value="${escapeHtml(o.key)}" ${selectedKeys.includes(o.key) ? "checked" : ""} onchange="updateLeadFilterMsSummary(this)" /> ${escapeHtml(o.label)}</label>`,
          )
          .join("")}
      </div>
    </div>`;
  };
  // A free-text <input> for the filter popup (city/state/country match).
  const leadFilterTextInput = (name, current, placeholder) =>
    `<input type="text" name="${name}" value="${escapeHtml(current)}" placeholder="${escapeHtml(placeholder)}" style="width:100%; padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);" />`;

  // Pipeline-stage filter tabs removed; the leads table now lists every lead.
  const leadsTodayStr = getTodayDateStringInTimeZone(APP_TIMEZONE);
  const leadsRowsHtml = leads.length
    ? leads
        .map((l) => {
          const stage = l.pipeline_stage || "prospect_identified";
          const outreach = l.outreach_status || "not_started";
          const demo = l.demo_status || "not_scheduled";
          const company = l.company || l.business_name || "(no name)";
          const loc = [l.city, l.state].filter(Boolean).join(", ");
          const noteHistory = parseLeadNotesHistory(l.notes);
          const latestNote = noteHistory.length
            ? noteHistory[noteHistory.length - 1]
            : null;
          const reachChannels = REACH_VIA_CHANNELS.filter((c) => l[c.column]).map(
            (c) => c.label,
          );
          const reachLabel = reachChannels.length
            ? reachChannels.join(", ")
            : "Select";
          const reachKeysCsv = REACH_VIA_CHANNELS.filter((c) => l[c.column])
            .map((c) => c.key)
            .join(",");
          const latestNoteByline = latestNote
            ? [
                latestNote.by ? escapeHtml(latestNote.by) : "",
                latestNote.at ? formatDateTimeNoTz(latestNote.at) : "",
              ]
                .filter(Boolean)
                .join(" · ")
            : "";
          const outreachOptions = CLIENT_LEAD_OUTREACH_STATUSES.map(
            (s) =>
              `<option value="${s.key}" ${s.key === outreach ? "selected" : ""}>${escapeHtml(s.label)}</option>`,
          ).join("");
          const demoOptions = CLIENT_LEAD_DEMO_STATUSES.map(
            (s) =>
              `<option value="${s.key}" ${s.key === demo ? "selected" : ""}>${escapeHtml(s.label)}</option>`,
          ).join("");
          return `
          <tr class="client-lead-row" data-stage="${stage}">
            <td style="text-align:center; vertical-align:top; padding-top:12px;">
              <input type="checkbox" class="lead-select" value="${Number(l.id)}" onchange="onLeadSelectChange()" aria-label="Select ${escapeHtml(company)}" />
            </td>
            <td>
              <div style="display:flex; align-items:center; gap:6px;">
                <input type="checkbox" title="Client visible" ${l.is_client_visible ? "checked" : ""} onchange="toggleLeadVisible(${clientIdNum}, ${Number(l.id)}, this.checked)" />
                <span style="font-weight:800; cursor:pointer; text-decoration:underline;" title="Open / Edit" onclick="openClientLeadDetail(${clientIdNum}, ${Number(l.id)})">${escapeHtml(company)}</span>
                ${
                  l.is_call_made
                    ? `<span class="lead-call-icon" aria-disabled="true" title="Call made${l.call_made_by ? ` by ${escapeHtml(l.call_made_by)}` : ""}${l.call_time ? ` · ${escapeHtml(formatDateTimeNoTz(l.call_time))}` : ""}" aria-label="Call already made" style="display:inline-flex; align-items:center; color:#ef4444; flex-shrink:0; cursor:not-allowed;"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/></svg></span>`
                    : `<span class="lead-call-icon" role="button" tabindex="0" title="Log a call — status, demo, reached via & note for ${escapeHtml(company)}" aria-label="Log call to ${escapeHtml(company)}" data-client="${clientIdNum}" data-lead="${Number(l.id)}" data-stage="${escapeHtml(stage)}" data-demo="${escapeHtml(demo)}" data-reach="${escapeHtml(reachKeysCsv)}" data-company="${escapeHtml(company)}" data-callback="${escapeHtml(l.callback_date || "")}" onclick="openLeadQuickUpdate(this)" style="display:inline-flex; align-items:center; color:var(--muted); flex-shrink:0; cursor:pointer;"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/></svg></span>`
                }
              </div>
              ${loc ? `<div class="meta">${escapeHtml(loc)}</div>` : ""}
              <div class="meta">${escapeHtml(l.contact_name || "")}</div>
              <div class="meta" style="font-size:11px;"><a href="${leadSortHref("updated")}" style="color:inherit; text-decoration:none; cursor:pointer;" title="Sort by Updated">Updated ${l.updated_at ? escapeHtml(getDateStringInTimeZone(new Date(l.updated_at), APP_TIMEZONE)) : "-"}${leadSortArrow("updated")}</a></div>
              ${l.callback_date ? `<div style="font-size:11px; margin-top:2px; font-weight:700; color:${l.callback_date < leadsTodayStr ? "#ef4444" : "#22c55e"};">Callback: ${escapeHtml(formatDateOnly(l.callback_date))}</div>` : ""}
            </td>
            <td style="width:130px; font-size:12px; word-break:break-word;">
              <div>${escapeHtml(l.phone || "-")}</div>
              ${l.phone_assigned_to ? `<div class="meta" style="font-size:11px;" title="Assigned for phone">☎ ${escapeHtml(l.phone_assigned_to)}</div>` : ""}
              <div class="meta">${escapeHtml(l.email || "-")}</div>
              ${l.email_assigned_to ? `<div class="meta" style="font-size:11px;" title="Assigned for email">✉ ${escapeHtml(l.email_assigned_to)}</div>` : ""}
              <div class="meta">${escapeHtml(l.lead_source || "-")}</div>
              ${l.verified_by ? `<div class="meta" style="font-size:11px; margin-top:2px; color:#22c55e;" title="Verified by">✓ ${escapeHtml(l.verified_by)}</div>` : ""}
            </td>
            <td>
              <select class="stage-select" onfocus="this.dataset.prev=this.value" onchange="updateLeadStage(${clientIdNum}, ${Number(l.id)}, this.value, this)">
                ${stageOptions(stage)}
              </select>
            </td>
            <td>
              <select class="stage-select" onfocus="this.dataset.prev=this.value" onchange="updateLeadDemo(${clientIdNum}, ${Number(l.id)}, this.value, this)">
                ${demoOptions}
              </select>
            </td>
            <td style="width:360px;">
              <div style="display:flex; align-items:flex-start; gap:6px;">
                <div style="flex:1; min-width:0;">
                  ${
                    latestNote
                      ? `<div style="font-size:12px; white-space:pre-wrap; word-break:break-word;">${escapeHtml(latestNote.text)}</div>
                         ${latestNote.audio_url ? `<audio controls preload="none" style="margin-top:4px; width:100%; max-width:240px; height:30px;" src="${escapeHtml(latestNote.audio_url)}"></audio>` : ""}
                         ${latestNoteByline ? `<div class="meta" style="font-size:11px;">${latestNoteByline}</div>` : ""}
                         ${noteHistory.length > 1 ? `<div class="meta" style="font-size:11px; cursor:pointer; text-decoration:underline;" onclick="openLeadNotesHistory(${clientIdNum}, ${Number(l.id)})">+${noteHistory.length - 1} earlier note${noteHistory.length - 1 === 1 ? "" : "s"}</div>` : ""}`
                      : `<div class="meta" style="font-size:12px;">No notes yet</div>`
                  }
                </div>
                <button class="btn" type="button" title="Add note" aria-label="Add note" style="padding:0; width:20px; height:20px; min-width:20px; font-size:14px; line-height:1; flex-shrink:0; display:flex; align-items:center; justify-content:center;" onclick="openLeadNoteModal(${clientIdNum}, ${Number(l.id)})">+</button>
              </div>
            </td>
            <td>
              <div class="reach-ms" style="position:relative; display:inline-block;">
                <button type="button" class="btn" style="padding:4px 10px; font-size:12px; min-width:120px; text-align:left; display:flex; justify-content:space-between; gap:6px; align-items:center;" onclick="toggleReachDropdown(this)">
                  <span>${reachLabel}</span><span style="opacity:.6;">▾</span>
                </button>
                <div class="reach-ms-panel" style="display:none; position:absolute; z-index:50; top:calc(100% + 4px); left:0; background:var(--card, #1e1e2e); border:1px solid var(--line); border-radius:8px; padding:6px; min-width:140px; box-shadow:0 8px 24px rgba(0,0,0,0.25);">
                  ${REACH_VIA_CHANNELS.map(
                    (c) =>
                      `<label style="display:block; font-size:12px; white-space:nowrap; padding:3px 4px;"><input type="checkbox" value="${c.key}" ${l[c.column] ? "checked" : ""} onchange="updateLeadReached(${clientIdNum}, ${Number(l.id)}, this)" /> ${escapeHtml(c.label)}</label>`,
                  ).join("")}
                </div>
              </div>
            </td>
            <td style="text-align:center; width:40px;">
              <button type="button" title="Delete lead" aria-label="Delete lead" onclick="deleteClientLead(${clientIdNum}, ${Number(l.id)})" style="background:transparent; border:none; color:#ef4444; cursor:pointer; font-size:15px; line-height:1; padding:4px 6px; border-radius:6px;" onmouseover="this.style.background='rgba(239,68,68,0.15)'" onmouseout="this.style.background='transparent'">✕</button>
            </td>
          </tr>`;
        })
        .join("")
    : hasActiveLeadQuery
    ? `<tr><td colspan="9" class="meta">No leads match ${leadSearch ? `“${escapeHtml(leadSearch)}”` : "the selected filters"}. <a href="/clients/${clientIdNum}?tab=leads">Clear</a></td></tr>`
    : `<tr><td colspan="9" class="meta">No leads yet for this client. Add the first lead.</td></tr>`;

  // When the client maps to a static lead business (e.g. Joolian -> joolian_leads),
  // embed the full /leads/<business> page (same design) via a same-origin iframe.
  const leadsTabHtml =
    activeTab === "leads" && staticLeadBusiness
      ? `
      <iframe
        id="clientLeadsFrame"
        src="/leads/${encodeURIComponent(staticLeadBusiness)}?embed=1"
        title="${escapeHtml(staticLeadBusiness)} leads"
        style="width:100%; height:82vh; border:1px solid var(--line); border-radius:var(--radius-lg); background:transparent;"
      ></iframe>`
      : activeTab === "leads"
      ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px; flex-wrap:wrap;">
          <div style="display:flex; align-items:center; gap:12px;">
          <h2 style="margin:0;">Leads</h2>
          <div>
          <div class="meta">${leadAllRows.length}${
            hasActiveLeadQuery
              ? ` result${leadAllRows.length === 1 ? "" : "s"}${leadSearch ? ` for “${escapeHtml(leadSearch)}”` : ""}`
              : " total"
          }</div>
          </div>
            <label style="display:inline-flex; align-items:center; gap:8px; font-size:12px; font-weight:700; letter-spacing:0.02em; color:var(--muted, #9aa3c0); cursor:pointer; user-select:none;" title="Show only leads where you are assigned for phone, assigned for email, or the overall owner">
              <span style="position:relative; display:inline-block; width:34px; height:19px; flex-shrink:0;">
                <input type="checkbox" ${leadMineOnly ? "checked" : ""} onchange="window.location.href = this.checked ? '${leadMineToggleHref(true)}' : '${leadMineToggleHref(false)}';" style="opacity:0; width:100%; height:100%; margin:0; position:absolute; cursor:pointer;" />
                <span style="position:absolute; inset:0; background:${leadMineOnly ? "#8b7cf6" : "rgba(255,255,255,0.15)"}; border-radius:999px; transition:background .15s; pointer-events:none;"></span>
                <span style="position:absolute; top:2px; left:${leadMineOnly ? "17px" : "2px"}; width:15px; height:15px; background:#fff; border-radius:50%; transition:left .15s; pointer-events:none;"></span>
              </span>
              My leads only
            </label>
          </div>
          <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
            <form method="GET" action="/clients/${clientIdNum}" style="display:flex; gap:6px; align-items:center; margin:0;">
              <input type="hidden" name="tab" value="leads" />
              ${leadFilterHiddenInputs}
              ${leadSortHiddenInputs}
              ${leadMineHiddenInput}
              <input type="search" name="search" value="${escapeHtml(leadSearch)}" placeholder="Search company, phone, or emails…" aria-label="Search leads by company, phone, or a pasted list of emails" onpaste="normalizeLeadSearchPaste(event)" style="padding:8px 10px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text); min-width:220px;" />
              <button class="btn" type="submit">Search</button>
              ${leadSearch ? `<a class="btn" href="${clearLeadSearchHref}">Clear</a>` : ""}
            </form>
            <div id="clientLeadFilterWrap" style="position:relative;">
              <button class="btn ${activeLeadFilterCount ? "btn-primary" : ""}" type="button" onclick="toggleClientLeadFilterPopup(event)" aria-haspopup="true" aria-expanded="false">Filter${activeLeadFilterCount ? ` (${activeLeadFilterCount})` : ""} ▾</button>
              <form id="clientLeadFilterPopup" method="GET" action="/clients/${clientIdNum}" style="display:none; position:absolute; right:0; top:calc(100% + 6px); z-index:60; width:240px; max-height:72vh; overflow:auto; flex-direction:column; gap:10px; padding:14px; background:var(--panel-strong, #11162a); border:1px solid var(--line); border-radius:12px; box-shadow:0 12px 32px rgba(0,0,0,0.45);">
                <input type="hidden" name="tab" value="leads" />
                <input type="hidden" name="search" value="${escapeHtml(leadSearch)}" />
                ${leadSortHiddenInputs}
                ${leadMineHiddenInput}
                <label style="${leadFilterLabelStyle}">STATUS${leadFilterSelect("pipeline_stage", [{ key: "__none__", label: "None (never set)" }, ...CLIENT_LEAD_PIPELINE_STAGES], lfPipeline)}</label>
                <label style="${leadFilterLabelStyle}">DEMO${leadFilterSelect("demo_status", [{ key: "__none__", label: "None (never set)" }, ...CLIENT_LEAD_DEMO_STATUSES], lfDemo)}</label>
                <div style="${leadFilterLabelStyle}">CATEGORY TYPE${leadFilterMultiSelect("category_type", [{ key: "__none__", label: "None (no category)" }, ...CLIENT_LEAD_CATEGORY_TYPES], lfCategoryTypeList)}</div>
                <div style="${leadFilterLabelStyle}">LOCATION OF LEAD${leadFilterTextInput("location", lfLocation === "__none__" ? "" : lfLocation, "City, state, or country")}
                  <label style="display:flex; align-items:center; gap:8px; font-size:12px; font-weight:400; letter-spacing:0; color:var(--text); cursor:pointer;"><input type="checkbox" name="location" value="__none__" ${lfLocation === "__none__" ? "checked" : ""} /> None (no location data)</label>
                </div>
                <label style="${leadFilterLabelStyle}">ASSIGNED TO${leadFilterSelect("assignee", assignedToFilterOptions, lfAssignee)}</label>
                <label style="${leadFilterLabelStyle}">ASSIGNED FOR PHONE${leadFilterSelect("phone_assignee", assignedToFilterOptions, lfPhoneAssignee)}</label>
                <label style="${leadFilterLabelStyle}">ASSIGNED FOR EMAIL${leadFilterSelect("email_assignee", assignedToFilterOptions, lfEmailAssignee)}</label>
                <label style="${leadFilterLabelStyle}">LEAD WITH NUMBER${leadFilterSelect("has_phone", HAS_PHONE_FILTER_OPTIONS, lfHasPhone)}</label>
                <div style="${leadFilterLabelStyle}">REACHED VIA${leadFilterMultiSelect("reached_via", REACHED_VIA_FILTER_OPTIONS, lfReachedList)}</div>
                <label style="${leadFilterLabelStyle}">NOTES${leadFilterSelect("notes", NOTES_FILTER_OPTIONS, lfNotes)}</label>
                <label style="${leadFilterLabelStyle}">NOTES AUDIO${leadFilterSelect("has_note_audio", NOTE_AUDIO_FILTER_OPTIONS, lfNoteAudio)}</label>
                ${noteAuthorOptions.length ? `<label style="${leadFilterLabelStyle}">NOTES BY${leadFilterSelect("notes_by", [{ key: "__none__", label: "No notes" }, ...noteAuthorOptions], lfNotesBy)}</label>` : ""}
                <label style="${leadFilterLabelStyle}">UPDATED FROM<input type="date" name="updated_from" value="${escapeHtml(lfUpdatedFrom)}" style="width:100%; padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text); color-scheme:dark;" /></label>
                <label style="${leadFilterLabelStyle}">UPDATED TO<input type="date" name="updated_to" value="${escapeHtml(lfUpdatedTo)}" style="width:100%; padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text); color-scheme:dark;" /></label>
                <label style="${leadFilterLabelStyle}">CALLBACK DATE FROM<input type="date" name="callback_date_from" value="${escapeHtml(lfCallbackFrom)}" style="width:100%; padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text); color-scheme:dark;" /></label>
                <label style="${leadFilterLabelStyle}">CALLBACK DATE TO<input type="date" name="callback_date_to" value="${escapeHtml(lfCallbackTo)}" style="width:100%; padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text); color-scheme:dark;" /></label>
                <label style="${leadFilterLabelStyle}">MISSED CALLBACK${leadFilterSelect("missed_callback", MISSED_CALLBACK_FILTER_OPTIONS, lfMissedCallback)}</label>
                <div style="display:flex; gap:6px; margin-top:4px;">
                  <button class="btn btn-primary" type="submit" style="flex:1;">Apply</button>
                  ${activeLeadFilterCount ? `<a class="btn" href="${clearLeadFiltersHref}">Clear</a>` : ""}
                </div>
              </form>
            </div>
            <button class="btn" type="button" onclick="openLeadImportModal()">⬆ Import from Excel</button>
            <button class="btn btn-primary" type="button" onclick="openClientLeadModal()">+ Add Lead</button>
          </div>
        </div>

        ${
          leadCategoryTypeCounts.length
            ? `<div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin:0 0 14px;">
                <a href="${leadCategoryPillHref("")}" style="padding:4px 12px; border-radius:999px; font-size:12px; font-weight:700; text-decoration:none; border:1px solid var(--line); ${!lfCategoryType ? "background:#8b7cf6; color:#fff; border-color:#8b7cf6;" : "background:rgba(255,255,255,0.04); color:var(--text);"}">All</a>
                ${leadCategoryTypeCounts
                  .map((c) => {
                    const label = CLIENT_LEAD_CATEGORY_TYPE_LABELS[c.key] || c.key;
                    const active = lfCategoryTypeList.includes(c.key);
                    return `<a href="${leadCategoryPillHref(c.key)}" title="Filter leads by ${escapeHtml(label)}" style="padding:4px 12px; border-radius:999px; font-size:12px; font-weight:700; text-decoration:none; border:1px solid var(--line); ${active ? "background:#8b7cf6; color:#fff; border-color:#8b7cf6;" : "background:rgba(255,255,255,0.04); color:var(--text);"}">${escapeHtml(label)} <span style="opacity:.7; font-weight:600;">${c.count}</span></a>`;
                  })
                  .join("")}
              </div>`
            : ""
        }

        ${renderLeadsPagination("top")}

        <div id="leadBulkBar" style="display:none; align-items:center; gap:10px; flex-wrap:wrap; margin:0 0 12px; padding:10px 14px; background:var(--panel-strong, #11162a); border:1px solid var(--line); border-radius:12px;">
          <strong id="leadBulkCount" style="font-size:13px;">0 selected</strong>
          <button class="btn" type="button" id="leadSelectAllMatchingBtn" onclick="selectAllMatchingLeads()" style="display:none; padding:4px 10px; font-size:12px; color:#8b7cf6; border-color:rgba(139,124,246,0.5);">Select all ${leadFilteredIds.length} leads</button>
          <select id="leadBulkStage" class="stage-select" onchange="bulkSetStage(${clientIdNum})">
            <option value="">Set status…</option>
            ${CLIENT_LEAD_PIPELINE_STAGES.map((s) => `<option value="${escapeHtml(s.key)}">${escapeHtml(s.label)}</option>`).join("")}
          </select>
          <select id="leadBulkDemo" class="stage-select" onchange="bulkSetDemo(${clientIdNum})">
            <option value="">Set demo…</option>
            ${CLIENT_LEAD_DEMO_STATUSES.map((s) => `<option value="${escapeHtml(s.key)}">${escapeHtml(s.label)}</option>`).join("")}
          </select>
          <select id="leadBulkReached" class="stage-select" onchange="bulkSetReached(${clientIdNum})">
            <option value="">Set reached via…</option>
            ${REACH_VIA_CHANNELS.map(
              (c) => `<option value="${c.key}">Mark ${escapeHtml(c.label)}</option>`,
            ).join("")}
            <option value="none">Clear reached via</option>
          </select>
          <select id="leadBulkAssign" class="stage-select" onchange="bulkSetAssigned(${clientIdNum})">
            <option value="">Assign to…</option>
            <option value="__unassigned__">Unassigned</option>
            ${Array.from(new Set((users || []).map((u) => u && u.name).filter(Boolean)))
              .map((n) => `<option value="${escapeHtml(n)}">${escapeHtml(n)}</option>`)
              .join("")}
          </select>
          <select id="leadBulkCategoryType" class="stage-select" onchange="bulkSetCategoryType(${clientIdNum})">
            <option value="">Set category type…</option>
            <option value="__clear__">Clear category type</option>
            ${CLIENT_LEAD_CATEGORY_TYPES.map((c) => `<option value="${escapeHtml(c.key)}">${escapeHtml(c.label)}</option>`).join("")}
          </select>
          <button class="btn" type="button" onclick="bulkDeleteLeads(${clientIdNum})" style="color:#ef4444; border-color:rgba(239,68,68,0.4);">Delete selected</button>
          <button class="btn" type="button" onclick="clearLeadSelection()">Clear</button>
        </div>

        <div style="overflow-x:auto;">
          <table class="work-table" style="width:100%; border-collapse:collapse;">
            <thead>
              <tr>
                <th style="width:34px; text-align:center;"><input type="checkbox" id="leadSelectAll" title="Select all on this page" onchange="toggleSelectAllLeads(this)" /></th>
                ${leadSortTh("name", "Company", "width:200px;")}
                <th style="text-align:left; width:200px;">Phone / Email / Source</th>
                ${leadSortTh("stage", "Status")}
                ${leadSortTh("demo", "Demo")}
                ${leadSortTh("notes", "Notes", "width:360px;")}
                <th style="text-align:left;">Reached Via</th>
                <th></th>
              </tr>
            </thead>
            <tbody>${leadsRowsHtml}</tbody>
          </table>
        </div>

        ${renderLeadsPagination("bottom")}
      </div>`
      : "";

  // ----------------------------------------------------------------------
  // Tasks & Blockers tab (enriched work items + blockers)
  // ----------------------------------------------------------------------
  const todayStr = new Date().toISOString().slice(0, 10);
  const isOverdue = (w) =>
    w.due_date &&
    String(w.due_date).slice(0, 10) < todayStr &&
    w.status !== "done";
  const priorityBadgeClass = (p) =>
    p === "high"
      ? "badge badge-danger"
      : p === "medium"
        ? "badge badge-warn"
        : "badge badge-muted";

  const overdueCount = workItems.filter(isOverdue).length;
  const highPriorityCount = workItems.filter(
    (w) => w.priority === "high" && w.status !== "done",
  ).length;
  const openBlockerCount = blockers.filter(
    (b) => b.resolution_status !== "resolved",
  ).length;

  // Alert strip for the Work Progress tab (overdue + open high-priority).
  const workAlertStrip =
    overdueCount || highPriorityCount
      ? `<div class="alert-strip">
           ${overdueCount ? `<span>⚠️ ${overdueCount} overdue task${overdueCount === 1 ? "" : "s"}</span>` : ""}
           ${highPriorityCount ? `<span>🔴 ${highPriorityCount} open high-priority task${highPriorityCount === 1 ? "" : "s"}</span>` : ""}
         </div>`
      : "";

  // Linked tasks: general tasks whose free-text `business` field names this
  // client. Editable inline (status / priority / progress) — they still live in
  // the org-wide task system, so edits write straight back to it via the API.
  const taskStatusClass = (s) =>
    s === "done"
      ? "badge badge-ok"
      : s === "in_progress"
        ? "badge badge-info"
        : s === "blocked"
          ? "badge badge-warn"
          : "badge badge-muted";

  const linkedTasksHtml = linkedTasks.length
    ? linkedTasks
        .map((t) => {
          const ownerName =
            users.find((u) => String(u.id) === String(t.assigned_to_user_id))
              ?.name || "-";
          const taskRefNo = t.task_no || t.id;
          const openHref = t.assigned_to_user_id
            ? `/tasks/user/${Number(t.assigned_to_user_id)}`
            : "";
          const titleHtml = openHref
            ? `<a class="work-card-title" href="${openHref}" style="text-decoration:none;">#${escapeHtml(taskRefNo)} · ${escapeHtml(t.title || "Untitled")}</a>`
            : `<div class="work-card-title">#${escapeHtml(taskRefNo)} · ${escapeHtml(t.title || "Untitled")}</div>`;
          const statusOptions = ["open", "in_progress", "blocked", "done"]
            .map(
              (s) =>
                `<option value="${s}" ${(t.status || "open") === s ? "selected" : ""}>${s.replace("_", " ")}</option>`,
            )
            .join("");
          const priorityOptions = ["low", "medium", "high", "urgent"]
            .map(
              (p) =>
                `<option value="${p}" ${(t.priority || "medium") === p ? "selected" : ""}>${p}</option>`,
            )
            .join("");
          return `
          <div class="work-card">
            <div class="work-card-top">
              <div>${titleHtml}</div>
              <div style="display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; align-items:center;">
                <select class="stage-select" title="Status" onchange="updateLinkedTask(${clientIdNum}, ${Number(t.id)}, 'status', this.value)">${statusOptions}</select>
                <select class="stage-select" title="Priority" onchange="updateLinkedTask(${clientIdNum}, ${Number(t.id)}, 'priority', this.value)">${priorityOptions}</select>
                <label style="font-size:12px; white-space:nowrap; display:inline-flex; align-items:center; gap:4px;" title="Show this task on the client's external dashboard"><input type="checkbox" ${t.is_client_visible ? "checked" : ""} onchange="updateLinkedTask(${clientIdNum}, ${Number(t.id)}, 'is_client_visible', this.checked)" /> Client</label>
              </div>
            </div>
            <div class="work-card-meta">
              <div><strong>Owner:</strong> ${escapeHtml(ownerName)}</div>
              <div><strong>Area:</strong> ${escapeHtml(t.area || "-")}</div>
              <div><strong>Progress:</strong> <input type="number" min="0" max="100" step="5" value="${Number(t.progress) || 0}" style="width:62px; padding:4px 6px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);" onchange="updateLinkedTask(${clientIdNum}, ${Number(t.id)}, 'progress', this.value)" />%</div>
              <div><strong>Due:</strong> ${escapeHtml(t.deadline || "-")}</div>
              <div><strong>Last updated:</strong> ${escapeHtml(t.updated_at ? formatDateTime(t.updated_at) : "-")}</div>
            </div>
          </div>`;
        })
        .join("")
    : "";

  const linkedTasksTabHtml =
    activeTab === "task" && linkedTasks.length
      ? `
      <div class="panel">
        <div style="margin-bottom:14px;">
          <h2 style="margin:0;">Linked Tasks</h2>
          <div class="section-subtitle">Tasks from the task system where this client is set as the business · update status, priority &amp; progress inline</div>
        </div>
        <div class="work-card-list">${linkedTasksHtml}</div>
      </div>`
      : "";

  const blockerSideLabel = (s) =>
    s === "client_side" ? "Client-side" : "Internal";
  const blockerStatusClass = (s) =>
    s === "resolved"
      ? "badge badge-ok"
      : s === "in_progress"
        ? "badge badge-info"
        : "badge badge-warn";

  const blockersCardsHtml = blockers.length
    ? blockers
        .map((b) => {
          const relatedTitle = b.related_work_item_id
            ? getWorkItemTitle(b.related_work_item_id) ||
              `#${b.related_work_item_id}`
            : "-";
          return `
          <div class="standard-card">
            <div class="standard-card-top">
              <div>
                <div class="standard-card-title">${escapeHtml(b.title || "Untitled blocker")}</div>
                <div class="meta">${escapeHtml(b.description || "No description")}</div>
              </div>
              <div style="display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end;">
                <span class="badge badge-muted">${escapeHtml(blockerSideLabel(b.blocker_side))}</span>
                <span class="${priorityBadgeClass(b.priority)}">${escapeHtml(b.priority || "medium")}</span>
                <span class="${blockerStatusClass(b.resolution_status)}">${escapeHtml(String(b.resolution_status || "open").replaceAll("_", " "))}</span>
              </div>
            </div>
            <div class="work-card-meta">
              <div><strong>Owner:</strong> ${escapeHtml(getUserName(b.owner_user_id))}</div>
              <div><strong>Related work item:</strong> ${escapeHtml(relatedTitle)}</div>
              <div><strong>Created:</strong> ${escapeHtml(b.created_at ? formatDateTime(b.created_at) : "-")}</div>
            </div>
            <div class="work-card-actions">
              ${
                b.resolution_status === "open"
                  ? `<button class="btn" type="button" onclick="updateBlocker(${clientIdNum}, ${Number(b.id)}, { resolution_status: 'in_progress' })">Start</button>`
                  : ""
              }
              ${
                b.resolution_status !== "resolved"
                  ? `<button class="btn" type="button" onclick="updateBlocker(${clientIdNum}, ${Number(b.id)}, { resolution_status: 'resolved' })">Resolve</button>`
                  : ""
              }
              <button class="btn" type="button" onclick="openBlockerDetail(${Number(b.id)})">Edit</button>
              <button class="btn" type="button" onclick="archiveBlocker(${clientIdNum}, ${Number(b.id)})">Archive</button>
            </div>
          </div>`;
        })
        .join("")
    : `<div class="meta">No blockers logged. Add one when something is blocking progress.</div>`;

  const blockerAlertStrip = openBlockerCount
    ? `<div class="alert-strip"><span>⛔ ${openBlockerCount} open blocker${openBlockerCount === 1 ? "" : "s"}</span></div>`
    : "";

  const blockersTabHtml =
    activeTab === "blockers"
      ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Blockers</h2>
            <div class="section-subtitle">Internal &amp; client-side blockers · ownership · resolution status</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openBlockerModal()">+ Add Blocker</button>
        </div>

        ${blockerAlertStrip}

        <div class="work-summary-chips" style="margin-bottom:12px;">
          <span class="summary-chip">Total ${blockers.length}</span>
          <span class="summary-chip">Open ${blockers.filter((b) => b.resolution_status === "open").length}</span>
          <span class="summary-chip">In progress ${blockers.filter((b) => b.resolution_status === "in_progress").length}</span>
          <span class="summary-chip">Resolved ${blockers.filter((b) => b.resolution_status === "resolved").length}</span>
        </div>

        <div class="standard-list">${blockersCardsHtml}</div>
      </div>`
      : "";

  // Blocker data for the edit modal (avoids an extra fetch endpoint).
  const blockersJson = JSON.stringify(
    blockers.map((b) => ({
      id: b.id,
      title: b.title || "",
      description: b.description || "",
      blocker_side: b.blocker_side || "internal",
      priority: b.priority || "medium",
      resolution_status: b.resolution_status || "open",
      owner_user_id: b.owner_user_id || "",
      related_work_item_id: b.related_work_item_id || "",
    })),
  ).replace(/</g, "\\u003c");

  // ----------------------------------------------------------------------
  // Meetings & MOMs tab
  // ----------------------------------------------------------------------
  const meetingTypeLabel = (t) =>
    ({
      sync_call: "Sync Call",
      internal: "Internal",
      review: "Review",
      adhoc: "Ad-hoc",
    })[t] || "Sync Call";

  // Sync-call compliance + frequency stats (last 7 days vs total).
  const nowMs = Date.now();
  const weekAgoMs = nowMs - 7 * 24 * 60 * 60 * 1000;
  const meetingTime = (m) =>
    m.meeting_date
      ? new Date(m.meeting_date).getTime()
      : m.created_at
        ? new Date(m.created_at).getTime()
        : 0;
  const meetingsThisWeek = meetings.filter(
    (m) => meetingTime(m) >= weekAgoMs,
  ).length;
  const syncCallsThisWeek = meetings.filter(
    (m) => m.meeting_type === "sync_call" && meetingTime(m) >= weekAgoMs,
  ).length;

  // A meeting's MOM is considered captured once any minutes field is filled.
  const momFilled = (m) =>
    !!(
      m.summary ||
      m.discussion_points ||
      m.decisions ||
      m.deliverables ||
      m.action_items ||
      m.follow_ups ||
      m.next_steps
    );
  const momPendingCount = meetings.filter((m) => !momFilled(m)).length;
  // Sync-call compliance: at least one sync call logged in the last 7 days.
  const syncCompliant = syncCallsThisWeek >= 1;
  // Next meeting = earliest meeting with a date in the future (follow-up date).
  const nextMeeting = meetings
    .filter((m) => m.meeting_date && new Date(m.meeting_date).getTime() > nowMs)
    .sort(
      (a, b) =>
        new Date(a.meeting_date).getTime() - new Date(b.meeting_date).getTime(),
    )[0];
  const nextMeetingDate = nextMeeting ? nextMeeting.meeting_date : null;

  const meetingClip = (text, limit) => {
    const value = String(text || "").trim();
    return value.length <= limit ? value : value.slice(0, limit) + "…";
  };

  const meetingsRowsHtml = meetings.length
    ? meetings
        .map((m) => {
          const filled = momFilled(m);
          return `
          <tr style="cursor:pointer;" onclick="openMeetingDetail(${Number(m.id)})">
            <td>
              <div style="font-weight:800;">${escapeHtml(m.meeting_date || "No date")}</div>
              <div class="meta">${escapeHtml(m.title || "Meeting")}</div>
            </td>
            <td>${m.participants ? escapeHtml(meetingClip(m.participants, 60)) : `<span class="meta">—</span>`}</td>
            <td>${m.summary ? escapeHtml(meetingClip(m.summary, 90)) : `<span class="meta">—</span>`}</td>
            <td><span class="badge badge-info">${escapeHtml(meetingTypeLabel(m.meeting_type))}</span></td>
            <td><span class="badge ${filled ? "badge-ok" : "badge-warn"}">${filled ? "Done" : "Pending"}</span></td>
            <td onclick="event.stopPropagation()">
              <button class="btn" type="button" onclick="openMeetingDetail(${Number(m.id)})">Edit</button>
              <button class="btn" type="button" onclick="archiveMeeting(${clientIdNum}, ${Number(m.id)})">Archive</button>
            </td>
          </tr>`;
        })
        .join("")
    : `<tr><td colspan="6" class="meta">No meetings logged yet. Record the first client meeting or sync call.</td></tr>`;

  const meetingsTabHtml =
    activeTab === "meetings"
      ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Meetings &amp; MOMs</h2>
            <div class="section-subtitle">Call log, minutes of meeting, and sync-call compliance</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openMeetingModal()">+ Log Meeting</button>
        </div>

        <div class="kpi-grid">
          <div class="kpi-card"><div class="kpi-label">Total meetings</div><div class="kpi-value">${meetings.length}</div></div>
          <div class="kpi-card"><div class="kpi-label">This week</div><div class="kpi-value">${meetingsThisWeek}</div></div>
          <div class="kpi-card"><div class="kpi-label">Sync compliance</div><div class="kpi-value" style="margin-top:6px;"><span class="badge ${syncCompliant ? "badge-ok" : "badge-warn"}">${syncCompliant ? "On track" : "Overdue"}</span></div></div>
          <div class="kpi-card"><div class="kpi-label">MOM pending</div><div class="kpi-value">${momPendingCount}</div></div>
          <div class="kpi-card"><div class="kpi-label">Next meeting</div><div class="kpi-value">${nextMeetingDate ? escapeHtml(nextMeetingDate) : "—"}</div></div>
        </div>

        <div style="overflow-x:auto;">
          <table class="work-table" style="width:100%; border-collapse:collapse;">
            <thead><tr>
              <th style="text-align:left;">Date</th>
              <th style="text-align:left;">Participants</th>
              <th style="text-align:left;">Summary</th>
              <th style="text-align:left;">Status</th>
              <th style="text-align:left;">MOM</th>
              <th></th>
            </tr></thead>
            <tbody>${meetingsRowsHtml}</tbody>
          </table>
        </div>
      </div>`
      : "";

  // Meeting data for the edit modal (avoids an extra fetch round-trip).
  const meetingsJson = JSON.stringify(
    meetings.map((m) => ({
      id: m.id,
      title: m.title || "",
      meeting_date: m.meeting_date || "",
      meeting_type: m.meeting_type || "sync_call",
      participants: m.participants || "",
      summary: m.summary || "",
      action_items: m.action_items || "",
      next_steps: m.next_steps || "",
      discussion_points: m.discussion_points || "",
      decisions: m.decisions || "",
      deliverables: m.deliverables || "",
      follow_ups: m.follow_ups || "",
    })),
  ).replace(/</g, "\\u003c");

  // ----------------------------------------------------------------------
  // Campaigns tab (Email / Calling / LinkedIn / WhatsApp)
  // ----------------------------------------------------------------------
  const campaignTypeLabel = (t) =>
    ({
      email: "Email",
      calling: "Calling",
      linkedin: "LinkedIn",
      whatsapp: "WhatsApp",
      sms: "SMS",
      events: "Events / Webinar",
      ads: "Paid Ads",
      content: "Content / SEO",
      referral: "Referral",
      reddit: "Reddit",
      other: "Other",
    })[t] || "Email";
  const campaignStatusClass = (s) =>
    s === "completed"
      ? "badge badge-ok"
      : s === "active"
        ? "badge badge-info"
        : s === "paused"
          ? "badge badge-warn"
          : "badge badge-muted";

  const totalSent = campaigns.reduce((n, c) => n + (Number(c.sent_count) || 0), 0);
  const totalResponses = campaigns.reduce(
    (n, c) => n + (Number(c.response_count) || 0),
    0,
  );
  const totalPositiveReplies = campaigns.reduce(
    (n, c) => n + (Number(c.positive_replies) || 0),
    0,
  );

  const campaignsRowsHtml = campaigns.length
    ? campaigns
        .map((c) => {
          const sent = Number(c.sent_count) || 0;
          const responses = Number(c.response_count) || 0;
          const positive = Number(c.positive_replies) || 0;
          const rate = sent ? Math.round((responses / sent) * 100) : 0;
          return `
          <tr>
            <td><div style="font-weight:800;">${escapeHtml(c.name || "Untitled")}</div></td>
            <td><span class="badge badge-muted">${escapeHtml(campaignTypeLabel(c.campaign_type))}</span></td>
            <td>${escapeHtml(c.channel || "-")}</td>
            <td><span class="${campaignStatusClass(c.status)}">${escapeHtml(c.status || "planned")}</span></td>
            <td>${sent}</td>
            <td>${responses}${sent ? ` (${rate}%)` : ""}</td>
            <td>${positive}</td>
            <td><button class="btn" type="button" onclick="openCampaignDetail(${Number(c.id)})">Edit</button>
                <button class="btn" type="button" onclick="archiveCampaign(${clientIdNum}, ${Number(c.id)})">Archive</button></td>
          </tr>`;
        })
        .join("")
    : `<tr><td colspan="8" class="meta">No campaigns yet. Add an email, calling, LinkedIn, or WhatsApp campaign.</td></tr>`;

  const campaignsTabHtml =
    activeTab === "campaigns"
      ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Campaigns</h2>
            <div class="section-subtitle">Email · Calling · LinkedIn · WhatsApp outreach</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openCampaignModal()">+ Add Campaign</button>
        </div>
        <div class="work-summary-chips" style="margin-bottom:12px;">
          <span class="summary-chip">Campaigns ${campaigns.length}</span>
          <span class="summary-chip">Active ${campaigns.filter((c) => c.status === "active").length}</span>
          <span class="summary-chip">Total sent ${totalSent}</span>
          <span class="summary-chip">Total responses ${totalResponses}</span>
          <span class="summary-chip">Positive replies ${totalPositiveReplies}</span>
        </div>
        <div style="overflow-x:auto;">
          <table class="work-table" style="width:100%; border-collapse:collapse;">
            <thead><tr>
              <th style="text-align:left;">Campaign</th>
              <th style="text-align:left;">Type</th>
              <th style="text-align:left;">Channel</th>
              <th style="text-align:left;">Status</th>
              <th style="text-align:left;">Sent</th>
              <th style="text-align:left;">Responses</th>
              <th style="text-align:left;">Positive</th>
              <th></th>
            </tr></thead>
            <tbody>${campaignsRowsHtml}</tbody>
          </table>
        </div>
      </div>`
      : "";

  const campaignsJson = JSON.stringify(
    campaigns.map((c) => ({
      id: c.id,
      name: c.name || "",
      campaign_type: c.campaign_type || "email",
      channel: c.channel || "",
      status: c.status || "planned",
      sent_count: c.sent_count || 0,
      response_count: c.response_count || 0,
      positive_replies: c.positive_replies || 0,
      notes: c.notes || "",
    })),
  ).replace(/</g, "\\u003c");

  // ----------------------------------------------------------------------
  // Team tab (assigned employees + open task counts)
  // ----------------------------------------------------------------------
  const openTaskCountByUser = {};
  workItems.forEach((w) => {
    if (w.status !== "done" && w.owner_user_id) {
      const k = String(w.owner_user_id);
      openTaskCountByUser[k] = (openTaskCountByUser[k] || 0) + 1;
    }
  });

  // Only people actually associated with THIS client: the account manager,
  // project manager, and anyone owning a work item or blocker for the client.
  const associatedUserIds = new Set();
  if (client.account_manager_user_id)
    associatedUserIds.add(String(client.account_manager_user_id));
  if (client.project_manager_user_id)
    associatedUserIds.add(String(client.project_manager_user_id));
  workItems.forEach((w) => {
    if (w.owner_user_id) associatedUserIds.add(String(w.owner_user_id));
  });
  blockers.forEach((b) => {
    if (b.owner_user_id) associatedUserIds.add(String(b.owner_user_id));
  });

  const teamRoleLabel = (u) => {
    const roles = [];
    if (String(u.id) === String(client.account_manager_user_id))
      roles.push("Account Manager");
    if (String(u.id) === String(client.project_manager_user_id))
      roles.push("Project Manager");
    if (!roles.length) roles.push("Contributor");
    return roles.join(" · ");
  };

  const teamMembers = users.filter((u) => associatedUserIds.has(String(u.id)));
  const teamTaskRowsHtml = teamMembers.length
    ? teamMembers
        .map((u) => {
          const count = openTaskCountByUser[String(u.id)] || 0;
          return `
          <tr>
            <td style="font-weight:700;">${escapeHtml(u.name || "-")}</td>
            <td>${escapeHtml(teamRoleLabel(u))}</td>
            <td>${count}</td>
          </tr>`;
        })
        .join("")
    : `<tr><td colspan="3" class="meta">No team members assigned yet. Set an account/project manager or assign work items.</td></tr>`;

  // ----------------------------------------------------------------------
  // Employee workload (WeSolve-internal): for each assigned employee, the
  // tasks they own on THIS project with progress, deadlines, and current
  // work status. Drives the dedicated employee section under the Team tab.
  // ----------------------------------------------------------------------
  const dateOnly = (d) => (d ? String(d).slice(0, 10) : "");
  const taskStatusLabel = (w) =>
    w.status === "done"
      ? "Done"
      : w.status === "in_progress"
        ? "In Progress"
        : "To Do";
  const taskStatusBadgeClass = (w) =>
    w.status === "done"
      ? "badge badge-ok"
      : w.status === "in_progress"
        ? "badge badge-info"
        : "badge badge-muted";

  const employeeCardsHtml = teamMembers.length
    ? teamMembers
        .map((u) => {
          const tasks = workItems
            .filter((w) => String(w.owner_user_id) === String(u.id))
            .sort((a, b) => {
              // Open tasks first, then earliest deadline first.
              const ad = a.status === "done" ? 1 : 0;
              const bd = b.status === "done" ? 1 : 0;
              if (ad !== bd) return ad - bd;
              return String(a.due_date || "9999-12-31").localeCompare(
                String(b.due_date || "9999-12-31"),
              );
            });
          const total = tasks.length;
          const doneCount = tasks.filter((t) => t.status === "done").length;
          const inProgressCount = tasks.filter(
            (t) => t.status === "in_progress",
          ).length;
          const overdueTasks = tasks.filter(isOverdue);
          const avgProgress = total
            ? Math.round(
                tasks.reduce((s, t) => s + (Number(t.progress) || 0), 0) /
                  total,
              )
            : 0;
          const nextDeadline = tasks
            .filter((t) => t.status !== "done" && t.due_date)
            .sort((a, b) =>
              String(a.due_date).localeCompare(String(b.due_date)),
            )[0]?.due_date;

          // Employee's current overall work status.
          let workState;
          if (total === 0) workState = { label: "No tasks", cls: "badge badge-muted" };
          else if (overdueTasks.length)
            workState = { label: "Behind schedule", cls: "badge badge-danger" };
          else if (inProgressCount)
            workState = { label: "Working", cls: "badge badge-info" };
          else if (doneCount === total)
            workState = { label: "All clear", cls: "badge badge-ok" };
          else workState = { label: "Not started", cls: "badge badge-warn" };

          const taskRows = total
            ? tasks
                .map((t) => {
                  const prog = Math.max(
                    0,
                    Math.min(100, Number(t.progress) || 0),
                  );
                  const over = isOverdue(t);
                  return `
                  <tr>
                    <td>
                      <div style="font-weight:700;">${escapeHtml(t.title || "Untitled")}</div>
                      <div class="meta">${escapeHtml((t.priority || "medium") + " priority")}</div>
                    </td>
                    <td><span class="${taskStatusBadgeClass(t)}">${escapeHtml(taskStatusLabel(t))}</span></td>
                    <td style="min-width:150px;">
                      <div class="emp-prog"><span class="emp-prog-fill" style="width:${prog}%;"></span></div>
                      <div class="meta" style="margin-top:4px;">${prog}%</div>
                    </td>
                    <td>${
                      t.due_date
                        ? `<span class="${over ? "overdue-pill" : "meta"}">${escapeHtml(dateOnly(t.due_date))}${over ? " · overdue" : ""}</span>`
                        : `<span class="meta">No deadline</span>`
                    }</td>
                  </tr>`;
                })
                .join("")
            : `<tr><td colspan="4" class="meta">No tasks assigned to this employee yet.</td></tr>`;

          return `
          <div class="work-card emp-card">
            <div class="work-card-top">
              <div>
                <div class="work-card-title">${escapeHtml(u.name || "-")} <span class="vis-chip vis-internal">INTERNAL</span></div>
                <div class="meta">${escapeHtml(teamRoleLabel(u))}</div>
              </div>
              <span class="${workState.cls}">${escapeHtml(workState.label)}</span>
            </div>

            <div class="emp-stat-row">
              <div class="emp-stat"><div class="emp-stat-val">${total}</div><div class="emp-stat-label">Assigned</div></div>
              <div class="emp-stat"><div class="emp-stat-val">${inProgressCount}</div><div class="emp-stat-label">In Progress</div></div>
              <div class="emp-stat"><div class="emp-stat-val">${doneCount}</div><div class="emp-stat-label">Completed</div></div>
              <div class="emp-stat"><div class="emp-stat-val"${overdueTasks.length ? ' style="color:#ffd7da;"' : ""}>${overdueTasks.length}</div><div class="emp-stat-label">Overdue</div></div>
              <div class="emp-stat"><div class="emp-stat-val">${nextDeadline ? escapeHtml(dateOnly(nextDeadline)) : "—"}</div><div class="emp-stat-label">Next Deadline</div></div>
            </div>

            <div class="emp-overall">
              <div style="display:flex; justify-content:space-between; font-size:12px; margin-bottom:6px;">
                <span class="meta">Overall progress</span>
                <strong>${avgProgress}%</strong>
              </div>
              <div class="emp-prog"><span class="emp-prog-fill" style="width:${avgProgress}%;"></span></div>
            </div>

            <div style="overflow-x:auto; margin-top:14px;">
              <table class="work-table" style="width:100%; border-collapse:collapse;">
                <thead><tr>
                  <th style="text-align:left;">Assigned task</th>
                  <th style="text-align:left;">Status</th>
                  <th style="text-align:left;">Progress</th>
                  <th style="text-align:left;">Deadline</th>
                </tr></thead>
                <tbody>${taskRows}</tbody>
              </table>
            </div>
          </div>`;
        })
        .join("")
    : `<div class="meta">No WeSolve employees assigned to this project yet. Set an account/project manager or assign work items to populate this section.</div>`;

  // ----------------------------------------------------------------------
  // Performance tab (GTM velocity + inactivity alerts) — computed from leads
  // ----------------------------------------------------------------------
  const perfNowMs = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;
  const leadCreatedMs = (l) =>
    l.created_at ? new Date(l.created_at).getTime() : 0;
  const leadsLast3 = leadAllRows.filter(
    (l) => leadCreatedMs(l) >= perfNowMs - 3 * dayMs,
  ).length;
  const leadsLast7 = leadAllRows.filter(
    (l) => leadCreatedMs(l) >= perfNowMs - 7 * dayMs,
  ).length;
  const convertedCount = leadAllRows.filter(
    (l) => l.pipeline_stage === "converted",
  ).length;
  const lastLeadMs = leadAllRows.reduce(
    (max, l) => Math.max(max, leadCreatedMs(l)),
    0,
  );
  const daysSinceLastLead = lastLeadMs
    ? Math.floor((perfNowMs - lastLeadMs) / dayMs)
    : null;
  const lastMeetingMs = meetings.reduce((max, m) => {
    const t = m.meeting_date
      ? new Date(m.meeting_date).getTime()
      : m.created_at
        ? new Date(m.created_at).getTime()
        : 0;
    return Math.max(max, t);
  }, 0);
  const daysSinceLastMeeting = lastMeetingMs
    ? Math.floor((perfNowMs - lastMeetingMs) / dayMs)
    : null;

  const perfAlerts = [];
  if (daysSinceLastLead === null || daysSinceLastLead >= 3) {
    perfAlerts.push(
      `No new leads in ${daysSinceLastLead === null ? "the recorded period" : daysSinceLastLead + " day" + (daysSinceLastLead === 1 ? "" : "s")} — GTM inactivity`,
    );
  }
  if (leadsLast7 === 0) {
    perfAlerts.push("No weekly progress: 0 leads added in the last 7 days");
  }
  const perfAlertStrip = perfAlerts.length
    ? `<div class="alert-strip">${perfAlerts.map((a) => `<span>⚠️ ${escapeHtml(a)}</span>`).join("")}</div>`
    : "";

  const performanceTabHtml =
    activeTab === "performance"
      ? `
      <div class="panel">
        <div style="margin-bottom:14px;">
          <h2 style="margin:0;">Performance ${VIS_CHIP.internal}</h2>
          <div class="section-subtitle">GTM velocity &amp; inactivity alerts (internal only)</div>
        </div>
        ${perfAlertStrip}
        <div class="kpi-grid">
          <div class="kpi-card"><div class="kpi-label">Leads · last 3 days</div><div class="kpi-value">${leadsLast3}</div></div>
          <div class="kpi-card"><div class="kpi-label">Leads · last 7 days</div><div class="kpi-value">${leadsLast7}</div></div>
          <div class="kpi-card"><div class="kpi-label">Converted (total)</div><div class="kpi-value">${convertedCount}</div></div>
          <div class="kpi-card"><div class="kpi-label">Days since last lead</div><div class="kpi-value">${daysSinceLastLead === null ? "—" : daysSinceLastLead}</div></div>
          <div class="kpi-card"><div class="kpi-label">Days since last demo</div><div class="kpi-value">${daysSinceLastMeeting === null ? "—" : daysSinceLastMeeting}</div></div>
          <div class="kpi-card"><div class="kpi-label">Total leads</div><div class="kpi-value">${leadAllRows.length}</div></div>
        </div>
      </div>`
      : "";

  // ----------------------------------------------------------------------
  // Incentives tab (attribution / commission / credit log)
  // ----------------------------------------------------------------------
  const incentiveStatusClass = (s) =>
    s === "paid"
      ? "badge badge-ok"
      : s === "approved"
        ? "badge badge-info"
        : "badge badge-warn";
  const leadLabelById = {};
  leadAllRows.forEach((l) => {
    leadLabelById[String(l.id)] =
      l.company || l.business_name || l.contact_name || `Lead #${l.id}`;
  });
  const totalIncentive = incentives.reduce(
    (n, i) => n + (Number(i.amount) || 0),
    0,
  );
  const incentivesRowsHtml = incentives.length
    ? incentives
        .map((i) => {
          return `
          <tr>
            <td style="font-weight:700;">${escapeHtml(i.title || "-")}</td>
            <td>${escapeHtml(getUserName(i.gtm_user_id))}</td>
            <td>${escapeHtml(i.related_lead_id ? leadLabelById[String(i.related_lead_id)] || `Lead #${i.related_lead_id}` : "-")}</td>
            <td>${Number(i.amount) || 0}</td>
            <td><span class="${incentiveStatusClass(i.status)}">${escapeHtml(i.status || "pending")}</span></td>
            <td><button class="btn" type="button" onclick="openIncentiveDetail(${Number(i.id)})">Edit</button>
                <button class="btn" type="button" onclick="archiveIncentive(${clientIdNum}, ${Number(i.id)})">Archive</button></td>
          </tr>`;
        })
        .join("")
    : `<tr><td colspan="6" class="meta">No incentives logged yet.</td></tr>`;

  const incentivesTabHtml =
    activeTab === "incentives"
      ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Incentives ${VIS_CHIP.internal}</h2>
            <div class="section-subtitle">Attribution · commission · credit log (internal only)</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openIncentiveModal()">+ Add Incentive</button>
        </div>
        <div class="work-summary-chips" style="margin-bottom:12px;">
          <span class="summary-chip">Entries ${incentives.length}</span>
          <span class="summary-chip">Paid ${incentives.filter((i) => i.status === "paid").length}</span>
          <span class="summary-chip">Total amount ${totalIncentive}</span>
        </div>
        <div style="overflow-x:auto;">
          <table class="work-table" style="width:100%; border-collapse:collapse;">
            <thead><tr>
              <th style="text-align:left;">Title</th>
              <th style="text-align:left;">GTM (attribution)</th>
              <th style="text-align:left;">Lead</th>
              <th style="text-align:left;">Amount</th>
              <th style="text-align:left;">Status</th>
              <th></th>
            </tr></thead>
            <tbody>${incentivesRowsHtml}</tbody>
          </table>
        </div>
      </div>`
      : "";

  const incentivesJson = JSON.stringify(
    incentives.map((i) => ({
      id: i.id,
      title: i.title || "",
      gtm_user_id: i.gtm_user_id || "",
      related_lead_id: i.related_lead_id || "",
      amount: i.amount || 0,
      status: i.status || "pending",
      notes: i.notes || "",
    })),
  ).replace(/</g, "\\u003c");

  // Lead options for the incentive attribution dropdown.
  const incentiveLeadOptions = leadAllRows
    .map(
      (l) =>
        `<option value="${Number(l.id)}">${escapeHtml(l.company || l.business_name || l.contact_name || `Lead #${l.id}`)}</option>`,
    )
    .join("");

  // ----------------------------------------------------------------------
  // Weekly Report tab (PM publishes · client visible)
  // ----------------------------------------------------------------------
  const reportSection = (label, value) =>
    value
      ? `<div style="margin-top:8px;"><strong>${escapeHtml(label)}:</strong><div class="meta" style="white-space:pre-wrap;">${escapeHtml(value)}</div></div>`
      : "";
  const reportsCardsHtml = reports.length
    ? reports
        .map((r) => {
          const period =
            r.period_label || (r.week_start ? `Week of ${r.week_start}` : "Report");
          return `
          <div class="standard-card">
            <div class="standard-card-top">
              <div>
                <div class="standard-card-title">${escapeHtml(period)}</div>
                <div class="meta">${escapeHtml(r.created_at ? formatDateTime(r.created_at) : "")}</div>
              </div>
              <div style="display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end;">
                <span class="${r.is_published ? "badge badge-ok" : "badge badge-muted"}">${r.is_published ? "Published" : "Draft"}</span>
                ${r.is_client_visible ? VIS_CHIP.client : VIS_CHIP.internal}
              </div>
            </div>
            ${reportSection("Summary", r.summary)}
            ${reportSection("Highlights", r.highlights)}
            ${reportSection("Lowlights / Risks", r.lowlights)}
            ${reportSection("Next Week Plan", r.next_week_plan)}
            <div class="work-card-actions" style="margin-top:12px;">
              ${
                r.is_published
                  ? `<button class="btn" type="button" onclick="updateReport(${clientIdNum}, ${Number(r.id)}, { unpublish: true })">Unpublish</button>`
                  : `<button class="btn" type="button" onclick="updateReport(${clientIdNum}, ${Number(r.id)}, { publish: true })">Publish</button>`
              }
              <button class="btn" type="button" onclick="openReportDetail(${Number(r.id)})">Edit</button>
              <button class="btn" type="button" onclick="archiveReport(${clientIdNum}, ${Number(r.id)})">Archive</button>
            </div>
          </div>`;
        })
        .join("")
    : `<div class="meta">No weekly reports yet. Publish the first weekly update for this client.</div>`;

  // Auto-computed daily / weekly / funnel report sections. Built by the shared
  // buildClientAutoReportSections helper so the public client-view page renders
  // an identical report.
  const {
    dailyAutoReportHtml,
    leadFunnelReportDailyHtml,
    weeklyReports,
  } = buildClientAutoReportSections({
    leadAllRows,
    campaigns,
    meetings,
    blockers,
    incentives,
    leadStageEvents,
    users,
    weekNumbering: clientWeeklyReportNumbering(client),
  });

  const reportTabHtml =
    activeTab === "report"
      ? `
      <div class="report-subtabs" role="tablist">
        <button class="report-subtab active" type="button" data-view="daily" role="tab" onclick="setReportView('daily')">Daily Report</button>
        ${
          weeklyReports.length
            ? `<div class="report-week-dd">
          <button type="button" class="report-subtab report-week-btn" aria-haspopup="true" onclick="toggleWeekMenu(event)"><span class="report-week-label">Week</span><span class="report-week-caret">▾</span></button>
          <div class="report-week-menu" role="menu">
            ${weeklyReports
              .map(
                (w) =>
                  `<button type="button" class="report-week-item" role="menuitem" data-view="week${w.num}" data-label="Week ${w.displayNum}" onclick="setReportView('week${w.num}')">Week ${w.displayNum} Report</button>`,
              )
              .join("")}
          </div>
        </div>`
            : ""
        }
      </div>
      <div id="reportView-daily" class="report-subview">${renderSummaryWithGoals({ period: "daily", summaryRow: reportSummaries.daily, goalsRow: clientGoals, editable: true, clientId: client.id, users })}${dailyAutoReportHtml}${leadFunnelReportDailyHtml}</div>
      ${weeklyReports
        .map(
          (w) =>
            `<div id="reportView-week${w.num}" class="report-subview" style="display:none;">${renderSummaryWithGoals({ period: "weekly", summaryRow: (reportSummaries.weeklyByDate || {})[w.weekStart] || null, goalsRow: clientGoals, editable: true, clientId: client.id, users, weekStart: w.weekStart, weekLabel: `Week ${w.displayNum}`, rangeLabel: w.rangeLabel })}${w.activityHtml}${w.funnelHtml}</div>`,
        )
        .join("")}
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Weekly Report ${VIS_CHIP.client}</h2>
            <div class="section-subtitle">PM publishes · visible to client when published</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openReportModal()">+ New Report</button>
        </div>
        <div class="standard-list">${reportsCardsHtml}</div>
      </div>`
      : "";

  const reportsJson = JSON.stringify(
    reports.map((r) => ({
      id: r.id,
      period_label: r.period_label || "",
      week_start: r.week_start || "",
      summary: r.summary || "",
      highlights: r.highlights || "",
      lowlights: r.lowlights || "",
      next_week_plan: r.next_week_plan || "",
      is_client_visible: r.is_client_visible !== false,
    })),
  ).replace(/</g, "\\u003c");

  // SweetAlert is pulled in as part of the server-rendered markup, the way the
  // original document loaded it from <head>: the browser runs it during
  // document parse, ahead of this page's own script at the end of the body.
  // Rendering it as a JSX element instead would not execute it, and next/script
  // would hold it back until after hydration.
  return `
        <script src="https://cdn.jsdelivr.net/npm/sweetalert2@11"></script>
            ${renderTopNav("clients")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Client Workspace internal</div>
              <h1>${escapeHtml(client.name)}</h1>
              <div class="subtitle">
                ${escapeHtml(client.company_name || "-")} · ${escapeHtml(client.status || "-")} · ${escapeHtml(client.health_status || "-")}
              </div>
            </div>

<div style="display:flex; gap:10px; flex-wrap:wrap;">
  <a class="btn" href="/clients">← Clients</a>
  ${
    client.google_drive_folder_url
      ? `<a class="btn" href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer">Drive</a>`
      : ""
  }
  <a class="btn" href="https://notebooklm.google.com/notebook/76c66777-16e6-447f-b6a7-d40befa08590" target="_blank" rel="noopener noreferrer">Notebook</a>
  <button class="btn" type="button" onclick="generateClientViewLink()">External Link</button>
  <a class="btn btn-primary" href="/clients/${client.id}/edit">Edit Client</a>
  <a class="btn" href="/clients/${client.id}/reset">Reset</a>
</div>
</div>

${(() => {
  // Stats row is contextual: it reflects whichever tab is active.
  const num = (v) => (typeof v === "number" && !Number.isNaN(v) ? v : 0);
  const openWork = workItems.filter((w) => w.status !== "done").length;
  const doneWork = workItems.filter((w) => w.status === "done").length;
  const openActions = actions.filter(
    (a) =>
      !["done", "completed", "resolved"].includes(
        String(a.status || "").toLowerCase(),
      ),
  ).length;
  const doneActions = actions.length - openActions;
  const resolvedBlockers = blockers.filter(
    (b) => b.resolution_status === "resolved",
  ).length;
  const nowMs = Date.now();
  const upcomingMeetings = meetings.filter(
    (m) => m.meeting_date && new Date(m.meeting_date).getTime() > nowMs,
  ).length;
  const activeCampaigns = campaigns.filter((c) => c.status === "active").length;
  const completedCampaigns = campaigns.filter(
    (c) => c.status === "completed",
  ).length;
  const doneMilestones = milestones.filter((m) =>
    ["done", "completed"].includes(String(m.status || "").toLowerCase()),
  ).length;
  const incentiveTotal = incentives.reduce(
    (sum, i) => sum + (Number(i.amount) || 0),
    0,
  );

  const statsByTab = {
    task: [
      ["Total Tasks", workItems.length],
      ["Open", openWork],
      ["Done", doneWork],
      ["Overdue", overdueCount],
    ],
    leads: [
      ["Total Leads", num(leadCounts.all)],
      ["Qualified", num(leadCounts.qualified)],
      ["Meetings Completed", num(leadCounts.meeting_completed)],
      ["Converted", num(leadCounts.converted)],
    ],
    campaigns: [
      ["Campaigns", campaigns.length],
      ["Active", activeCampaigns],
      ["Completed", completedCampaigns],
    ],
    meetings: [
      ["Meetings", meetings.length],
      ["Upcoming", upcomingMeetings],
    ],
    blockers: [
      ["Blockers", blockers.length],
      ["Open", openBlockerCount],
      ["Resolved", resolvedBlockers],
    ],
    team: [["Team Members", contributors.length]],
    performance: [
      ["Tasks Done", doneWork],
      ["Open Work", openWork],
      ["Overdue", overdueCount],
    ],
    incentives: [
      ["Records", incentives.length],
      ["Total ₹", incentiveTotal],
    ],
    report: [["Weekly Reports", reports.length]],
    actions: [
      ["Total", actions.length],
      ["Open", openActions],
      ["Done", doneActions],
    ],
    milestones: [
      ["Milestones", milestones.length],
      ["Completed", doneMilestones],
    ],
    updates: [["Updates", updates.length]],
    documents: [["Documents", documents.length]],
  };

  const cards = statsByTab[activeTab];
  if (!cards || !cards.length) return "";
  return `
          <div class="stats">
            ${cards
              .map(
                ([label, value]) =>
                  `<div class="stat-card"><div class="stat-label">${escapeHtml(label)}</div><div class="stat-value">${escapeHtml(String(value))}</div></div>`,
              )
              .join("")}
          </div>`;
})()}

${(() => {
  // At-a-glance tab counts. Only show numbers that are reliable on every page
  // load — leads are fetched lazily, so their count is omitted unless present.
  const openWorkCount = workItems.filter((w) => w.status !== "done").length;
  const openActionsCount = actions.filter(
    (a) => !["done", "completed", "resolved"].includes(
      String(a.status || "").toLowerCase(),
    ),
  ).length;
  const leadsBadge =
    typeof leadCounts?.all === "number" ? leadCounts.all : null;
  return `
<div class="tabs" role="tablist">
  ${tabLink("overview", "Overview")}
  ${tabLink("task", "Task", openWorkCount, overdueCount > 0 ? "attention" : "")}
  ${tabLink("leads", "Leads", leadsBadge)}
  ${tabLink("campaigns", "Campaigns", campaigns.length)}
  ${tabLink("meetings", "Meetings &amp; MOMs")}
  ${tabLink("blockers", "Blockers", openBlockerCount, openBlockerCount > 0 ? "attention" : "")}
  ${tabLink("team", `Team`)}
  ${tabLink("performance", `Performance`)}
  ${tabLink("incentives", `Incentives`)}
  ${(() => {
    const reportActive = activeTab === "report";
    const base = `/clients/${Number(client.id)}?tab=report`;
    return `
  <div class="tab-flyout-wrap">
    <a class="tab ${reportActive ? "active" : ""}" href="${base}" aria-current="${reportActive ? "page" : "false"}">
      <span class="tab-label">Report</span>
    </a>
    <div class="tab-flyout" role="menu">
      <a class="tab-flyout-item" role="menuitem" href="${base}#daily" onclick="return setReportView('daily')">Daily Report</a>
      <a class="tab-flyout-item" role="menuitem" href="${base}#week1" onclick="return setReportView('week1')">Week ${weeklyReports.find((w) => w.num === 1)?.displayNum || 1} Report</a>
    </div>
  </div>`;
  })()}
  ${tabLink("actions", "Actions Needed", openActionsCount, openActionsCount > 0 ? "attention" : "")}
  </div>`;
})()}
<div class="tab-content-wrap">
${
  activeTab === "overview"
    ? `
      <div class="grid-2">
        <div class="panel">
          <h2>Overview</h2>
          <div class="meta"><strong>Description:</strong> ${escapeHtml(client.description || "-")}</div>
          <div class="meta"><strong>Start Date:</strong> ${escapeHtml(client.start_date || "-")}</div>
          <div class="meta"><strong>Slug:</strong> ${escapeHtml(client.slug || "-")}</div>
          <div class="meta"><strong>Account Manager:</strong> ${escapeHtml(client.account_manager_name || "-")}</div>
          <div class="meta"><strong>Project Manager:</strong> ${escapeHtml(client.project_manager_name || "-")}</div>
          <div class="meta"><strong>GTM Associates:</strong> ${escapeHtml(
            (Array.isArray(client.gtm_associate_user_ids)
              ? client.gtm_associate_user_ids
              : []
            )
              .map(
                (id) =>
                  (users.find((u) => String(u.id) === String(id)) || {}).name,
              )
              .filter(Boolean)
              .join(", ") || "-",
          )}</div>
          <div class="meta">
  <strong>Last Activity:</strong>
  ${
    timelineEvents.length
      ? `${escapeHtml(formatDateTime(timelineEvents[0].at))} · ${escapeHtml(timelineEvents[0].text)}`
      : "-"
  }
</div>
          <div class="meta">
            <strong>Google Drive Folder:</strong>
            ${
              client.google_drive_folder_url
                ? `<a href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer">📁 Open Client Folder</a>`
                : `<span style="color: var(--danger);">Not set</span>`
            }
          </div>
        </div>

        <div class="panel">
          <h2>Services</h2>
          ${
            services.length
              ? services
                  .map(
                    (s) =>
                      `<div class="item"><div class="item-title">${escapeHtml(s.name)}</div></div>`,
                  )
                  .join("")
              : `<div class="meta">No services selected.</div>`
          }
        </div>
      </div>

      <div class="grid-2">
        <div class="panel">
          <h2>Client Contacts</h2>
          ${
            contacts.length
              ? contacts
                  .map(
                    (c) => `
                <div class="item">
                  <div class="item-title">${escapeHtml(c.name || "-")} ${c.is_primary ? "· Primary" : ""}</div>
                  <div class="meta">${escapeHtml(c.role || "-")}</div>
                  <div class="meta">${escapeHtml(c.email || "-")} · ${escapeHtml(c.phone || "-")}</div>
                </div>
              `,
                  )
                  .join("")
              : `<div class="meta">No contacts added.</div>`
          }
        </div>

        <div class="panel">
          <h2>Recent Updates</h2>
          ${
            updates.length
              ? updates
                  .map(
                    (u) => `
                <div class="item">
                  <div class="item-title">${escapeHtml(u.title || "Update")}</div>
                  <div class="meta">${escapeHtml(u.update_text || "")}</div>
                </div>
              `,
                  )
                  .join("")
              : `<div class="meta">No updates yet.</div>`
          }
        </div>
      </div>

      <div style="margin-top:16px;">${renderSummaryWithGoals({ period: "weekly", summaryRow: reportSummaries.weekly, goalsRow: clientGoals, editable: true, clientId: client.id, users })}</div>
    `
    : ""
}

${leadsTabHtml}

${campaignsTabHtml}

${meetingsTabHtml}

${blockersTabHtml}

${performanceTabHtml}

${incentivesTabHtml}

${reportTabHtml}

${
  activeTab === "task"
    ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Task</h2>
            <div class="work-summary-chips">
              <span class="summary-chip">All ${workItems.length}</span>
              <span class="summary-chip">Todo ${workItems.filter((w) => w.status === "todo").length}</span>
              <span class="summary-chip">In Progress ${workItems.filter((w) => w.status === "in_progress").length}</span>
              <span class="summary-chip">Done ${workItems.filter((w) => w.status === "done").length}</span>
              <span class="summary-chip">Overdue ${overdueCount}</span>
              <span class="summary-chip">High priority ${highPriorityCount}</span>
            </div>
          </div>

          <button class="btn btn-primary" type="button" onclick="openWorkItemModal()">+ Add Work Item</button>
        </div>

        ${workAlertStrip}

        <div class="work-card-list">
          ${
            workItems.length
              ? workItems
                  .map((w) => {
                    const ownerName =
                      users.find(
                        (u) => String(u.id) === String(w.owner_user_id),
                      )?.name || "-";

                    const dep = w.dependency_work_item_id
                      ? workItems.find(
                          (x) =>
                            String(x.id) === String(w.dependency_work_item_id),
                        )
                      : null;

                    const isBlockedByDependency = dep && dep.status !== "done";

                    const statusClass =
                      w.status === "done"
                        ? "badge badge-ok"
                        : w.status === "in_progress"
                          ? "badge badge-info"
                          : isBlockedByDependency
                            ? "badge badge-warn"
                            : "badge badge-muted";

                    const dependencyText = dep
                      ? isBlockedByDependency
                        ? `Blocked by #${escapeHtml(dep.id)} · ${escapeHtml(dep.title)}`
                        : `Dependency complete: #${escapeHtml(dep.id)} · ${escapeHtml(dep.title)}`
                      : "No dependency";

                    return `
                    <div class="work-card">
                      <div class="work-card-top">
                        <div>
                          <div class="work-card-title">${escapeHtml(w.title || "Untitled")}</div>
                          <div class="meta">${escapeHtml(w.description || "No description")}</div>
                        </div>

                        <div style="display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end;">
                          ${isOverdue(w) ? `<span class="overdue-pill">Overdue</span>` : ""}
                          <span class="${statusClass}">
                            ${isBlockedByDependency && w.status !== "done" ? "blocked" : escapeHtml(w.status || "todo")}
                          </span>
                          <span class="${priorityBadgeClass(w.priority)}">${escapeHtml(w.priority || "medium")}</span>
                        </div>
                      </div>

                      <div class="work-card-meta">
                        <div><strong>Owner:</strong> ${escapeHtml(ownerName)}</div>
                        <div><strong>Due:</strong> ${escapeHtml(w.due_date || "-")}</div>
                        <div><strong>Depends:</strong> ${dependencyText}</div>
                        <div><strong>Milestone:</strong> ${escapeHtml(w.milestone_id ? getMilestoneTitle(w.milestone_id) : "-")}</div>
                        <div><strong>Last updated:</strong> ${escapeHtml(w.updated_at ? formatDateTime(w.updated_at) : "-")}</div>
                      </div>

                      <div class="work-card-actions">
                        <button class="btn" type="button" onclick="openWorkItemDetail(${Number(w.id)})">Open / Edit</button>
                        <button class="btn" type="button" onclick="quickUpdateWorkItem(${Number(w.id)}, 'in_progress')">Start</button>
                        <button class="btn" type="button" onclick="quickUpdateWorkItem(${Number(w.id)}, 'done')">Done</button>
                        <button class="btn" type="button" onclick="archiveWorkItem(${Number(w.id)})">Archive</button>
                      </div>
                    </div>
                  `;
                  })
                  .join("")
              : `<div class="meta">No work items yet. Add the first work item for this client.</div>`
          }
        </div>
      </div>
    `
    : ""
}

${linkedTasksTabHtml}

${
  activeTab === "updates"
    ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Updates / Progress Timeline</h2>
            <div class="meta">Manual client updates + automatic work-item activity.</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openClientUpdateModal()">+ Add Update</button>
        </div>

        <div class="work-summary-chips">
          <span class="summary-chip">Manual Updates ${updates.length}</span>
          <span class="summary-chip">Activity Logs ${activityLogs.length}</span>
          <span class="summary-chip">Timeline ${timelineEvents.length}</span>
        </div>

        <div style="margin-top:16px;">
          ${
            timelineEvents.length
              ? timelineEvents
                  .map(
                    (event) => `
                <div class="item">
                  <div class="item-title">
                    ${escapeHtml(event.title)}
                    ${event.relatedWorkItemTitle ? ` · ${escapeHtml(event.relatedWorkItemTitle)}` : ""}
                  </div>
                  <div class="meta">${escapeHtml(event.text)}</div>
                  <div class="meta">
                    ${escapeHtml(event.at ? formatDateTime(event.at) : "-")}
                    · by ${escapeHtml(event.by || "-")}
                    · ${escapeHtml(event.type === "manual_update" ? "Manual update" : "System activity")}
                  </div>
                </div>
              `,
                  )
                  .join("")
              : `<div class="meta">No updates or activity yet.</div>`
          }
        </div>
      </div>
    `
    : ""
}

${
  activeTab === "actions"
    ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Actions Needed</h2>
            <div class="meta">Track simple client or WeSolve follow-ups.</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openActionModal()">+ Add Action</button>
        </div>

        ${
          actions.length
            ? actions
                .map(
                  (a) => `
              <div class="work-card">
                <div class="work-card-top">
                  <div>
                    <div class="work-card-title">${escapeHtml(a.title)}</div>
                    <div class="meta">${escapeHtml(a.notes || "")}</div>
                  </div>
                  <div style="display:flex; gap:8px; flex-wrap:wrap;">
                    <span class="badge badge-info">${escapeHtml(a.status || "Open")}</span>
                    <span class="badge badge-muted">${escapeHtml(a.priority || "Medium")}</span>
                  </div>
                </div>

                <div class="work-card-meta">
                  <div><strong>Owner:</strong> ${escapeHtml(a.owner_type || "-")} ${a.owner_name ? "· " + escapeHtml(a.owner_name) : ""}</div>
                  <div><strong>Due:</strong> ${escapeHtml(a.due_date || "-")}</div>
                  <div><strong>Updated:</strong> ${escapeHtml(a.updated_at ? formatDateTime(a.updated_at) : "-")}</div>
                </div>

                <div class="work-card-actions">
                  <button class="btn" type="button" onclick="openActionEditModal(${Number(a.id)})">Edit</button>
                  <button class="btn" type="button" onclick="archiveAction(${Number(a.id)})">Archive</button>
                </div>
              </div>
            `,
                )
                .join("")
            : `<div class="meta">No actions yet.</div>`
        }
      </div>
    `
    : ""
}

${
  activeTab === "team"
    ? `
      <div class="panel">
        <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
          <div>
            <h2 style="margin:0;">Team ${VIS_CHIP.internal}</h2>
            <div class="meta">Assigned employees, roles, and open task counts. Internal only.</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openContributorModal()">+ Add Contributor</button>
        </div>

        <div class="emp-section-head">
          <h3 style="margin:0;">Employees on ${escapeHtml(client.name)} ${VIS_CHIP.internal}</h3>
          <div class="meta">Every WeSolve employee assigned to this project — assigned tasks, progress, deadlines, and current work status. Visible to the WeSolve team only.</div>
        </div>
        <div class="emp-card-list">
          ${employeeCardsHtml}
        </div>

        <h3 style="margin:24px 0 10px;">Open task load</h3>
        <div style="overflow-x:auto; margin-bottom:18px;">
          <table class="work-table" style="width:100%; border-collapse:collapse;">
            <thead><tr><th style="text-align:left;">Team member</th><th style="text-align:left;">Role</th><th style="text-align:left;">Open tasks</th></tr></thead>
            <tbody>${teamTaskRowsHtml}</tbody>
          </table>
        </div>

        <h3 style="margin:6px 0 10px;">Contributors</h3>
        ${
          contributors.length
            ? contributors
                .map(
                  (p) => `
              <div class="work-card">
                <div class="work-card-top">
                  <div>
                    <div class="work-card-title">${escapeHtml(p.name)}</div>
                    <div class="meta">${escapeHtml(p.role || "-")}</div>
                  </div>
                  <div style="display:flex; gap:8px; flex-wrap:wrap;">
                    <span class="badge badge-info">${escapeHtml(p.person_type || "-")}</span>
                    <span class="badge badge-muted">${escapeHtml(p.status || "Active")}</span>
                  </div>
                </div>

                <div class="work-card-meta">
                  <div><strong>Email:</strong> ${escapeHtml(p.email || "-")}</div>
                  <div><strong>Phone:</strong> ${escapeHtml(p.phone || "-")}</div>
                  <div><strong>Can update work:</strong> ${p.can_update_work ? "Yes" : "No"}</div>
                  <div><strong>Can view client dashboard:</strong> ${p.can_view_client_dashboard ? "Yes" : "No"}</div>
                </div>

                ${p.notes ? `<div class="meta" style="margin-top:10px;">${escapeHtml(p.notes)}</div>` : ""}

                <div class="work-card-actions">
                  <button class="btn" type="button" onclick="openContributorEditModal(${Number(p.id)})">Edit</button>
                  <button class="btn" type="button" onclick="archiveContributor(${Number(p.id)})">Archive</button>
                </div>
              </div>
            `,
                )
                .join("")
            : `<div class="meta">No contributors yet.</div>`
        }
      </div>
    `
    : ""
}

${
  activeTab === "milestones"
    ? `
      <div class="panel">
        <div class="section-head">
          <div>
            <h2 class="section-title">Milestones</h2>
            <div class="section-subtitle">Project checkpoints connected to work items.</div>
          </div>
          <button class="btn btn-primary" type="button" onclick="openMilestoneModal()">+ Add Milestone</button>
        </div>

        <div class="standard-list">
          ${
            milestones.length
              ? milestones
                  .map((m) => {
                    const linkedCount = workItems.filter(
                      (w) => String(w.milestone_id || "") === String(m.id),
                    ).length;

                    return `
                    <div class="standard-card">
                      <div class="standard-card-top">
                        <div>
                          <div class="standard-card-title">${escapeHtml(m.title || "Milestone")}</div>
                          <div class="meta">${escapeHtml(m.notes || "")}</div>
                        </div>
                        <div style="display:flex; gap:8px; flex-wrap:wrap;">
                          <span class="badge badge-info">${escapeHtml(m.status || "planned")}</span>
                          <span class="badge badge-muted">${linkedCount} work items</span>
                        </div>
                      </div>

                      <div class="standard-card-meta">
                        <div><strong>Due:</strong> ${escapeHtml(m.due_date || "-")}</div>
                        <div><strong>Updated:</strong> ${escapeHtml(m.updated_at ? formatDateTime(m.updated_at) : "-")}</div>
                      </div>

                      <div class="standard-card-actions">
                        <button class="btn" type="button" onclick="openMilestoneEditModal(${Number(m.id)})">Edit</button>
                        <button class="btn" type="button" onclick="closeMilestone(${Number(m.id)})">Close</button>
                        <button class="btn" type="button" onclick="archiveMilestone(${Number(m.id)})">Archive</button>
                      </div>
                    </div>
                  `;
                  })
                  .join("")
              : `<div class="meta">No milestones yet.</div>`
          }
        </div>
      </div>
    `
    : ""
}

${
  activeTab === "documents"
    ? `
      <div class="panel">
        <h2>Documents</h2>
        <div class="meta" style="margin-bottom:12px;">
          Main document system is Google Drive.
          ${
            client.google_drive_folder_url
              ? `<a href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer">Open Client Folder</a>`
              : `<span style="color: var(--danger);">Google Drive folder not set</span>`
          }
        </div>

        ${
          documents.length
            ? documents
                .map(
                  (d) => `
              <div class="item">
                <div class="item-title">${escapeHtml(d.title || d.name || "Document")}</div>
                <div class="meta">${escapeHtml(d.url || "-")}</div>
              </div>
            `,
                )
                .join("")
            : `<div class="meta">No separate documents tracked. Use the Google Drive folder.</div>`
        }
      </div>
    `
    : ""
}
</div>
<div id="milestoneModal" class="work-modal" onclick="closeMilestoneModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="milestoneModalTitle" style="font-size:22px; font-weight:800;">Add Milestone</div>
      <button class="btn" type="button" onclick="closeMilestoneModal()">Close</button>
    </div>

    <input id="milestoneId" type="hidden" />

    <div class="form-grid">
      <div class="form-field">
        <label>Title</label>
        <input id="milestoneTitle" placeholder="Example: MVP Launch" />
      </div>

      <div class="form-field">
        <label>Due Date</label>
        <input id="milestoneDueDate" type="date" />
      </div>

      <div class="form-field">
        <label>Status</label>
        <select id="milestoneStatus">
          <option value="planned">Planned</option>
          <option value="in_progress">In Progress</option>
          <option value="done">Done</option>
          <option value="closed">Closed</option>
        </select>
      </div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Notes</label>
        <textarea id="milestoneNotes"></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeMilestoneModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveMilestone()">Save Milestone</button>
    </div>
  </div>
</div>
<div id="workItemModal" class="work-modal" onclick="closeWorkItemModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">Add Work Item</div>
      <button class="btn" type="button" onclick="closeWorkItemModal()">Close</button>
    </div>

    <div class="form-grid">
      <div class="form-field">
        <label>Title</label>
        <input id="workTitle" placeholder="Example: Build landing page" />
      </div>

      <div class="form-field">
        <label>Owner</label>
        <select id="workOwner">
          <option value="">Select owner</option>
          ${users.map((u) => `<option value="${u.id}">${escapeHtml(u.name)}</option>`).join("")}
        </select>
      </div>

      <div class="form-field">
        <label>Priority</label>
        <select id="workPriority">
          <option value="medium">Medium</option>
          <option value="high">High</option>
          <option value="low">Low</option>
        </select>
      </div>

      <div class="form-field">
        <label>Due Date</label>
        <input id="workDueDate" type="date" />
      </div>

      <div class="form-field">
        <label>Depends On</label>
        <select id="workDependency">
          <option value="">No dependency</option>
          ${workItems.map((w) => `<option value="${w.id}">#${w.id} · ${escapeHtml(w.title)}</option>`).join("")}
        </select>
      </div>
      
      <div class="form-field">
  <label>Milestone</label>
  <select id="workMilestone">
    <option value="">No milestone</option>
    ${milestones.map((m) => `<option value="${m.id}">${escapeHtml(m.title)}</option>`).join("")}
  </select>
</div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Description</label>
        <textarea id="workDescription" placeholder="Add details, expected outcome, blockers, etc."></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeWorkItemModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="createWorkItem(${Number(client.id)})">Create Work Item</button>
    </div>
  </div>
</div>

<div id="workItemDetailModal" class="work-modal" onclick="closeWorkItemDetail(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="workItemDetailTitle" style="font-size:22px; font-weight:800;">Work Item</div>
      <button class="btn" type="button" onclick="closeWorkItemDetail()">Close</button>
    </div>
    <div id="workItemDetailBody" class="meta">Loading...</div>
  </div>
</div>

<div id="clientLeadModal" class="work-modal" onclick="closeClientLeadModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="clientLeadModalTitle" style="font-size:22px; font-weight:800;">Add Lead</div>
      <button class="btn" type="button" onclick="closeClientLeadModal()">Close</button>
    </div>

    <input type="hidden" id="leadId" value="" />

    <div class="form-grid">
      <div class="form-field">
        <label>Company / Business Name</label>
        <input id="leadCompany" placeholder="Acme Manufacturing" />
      </div>
      <div class="form-field">
        <label>Contact Name</label>
        <input id="leadContactName" placeholder="Jane Doe" />
      </div>
      <div class="form-field">
        <label>Phone</label>
        <input id="leadPhone" placeholder="+91..." />
      </div>
      <div class="form-field">
        <label>Email</label>
        <input id="leadEmail" placeholder="name@company.com" />
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
        <label>Country</label>
        <input id="leadCountryField" placeholder="US" />
      </div>
      <div class="form-field">
        <label>Website</label>
        <input id="leadWebsite" placeholder="https://" />
      </div>
      <div class="form-field">
        <label>Personal LinkedIn</label>
        <input id="leadPersonLinkedin" placeholder="https://www.linkedin.com/in/…" />
      </div>
      <div class="form-field">
        <label>Company LinkedIn</label>
        <input id="leadCompanyLinkedin" placeholder="https://www.linkedin.com/company/…" />
      </div>
      <div class="form-field">
        <label>Funding Amount</label>
        <input id="leadFundingAmount" placeholder="$500 million" />
      </div>
      <div class="form-field">
        <label>Funding Date / Month</label>
        <input id="leadFundingDate" placeholder="October 2024" />
      </div>
      <div class="form-field">
        <label>Funding Round</label>
        <input id="leadFundingRound" placeholder="Series B" />
      </div>
      <div class="form-field">
        <label>Lead Category</label>
        <select id="leadCategory">
          <option value="b2b">B2B</option>
          <option value="b2c">B2C</option>
        </select>
      </div>
      <div class="form-field">
        <label>Category Type</label>
        <select id="leadCategoryType">
          <option value="">Select…</option>
          ${CLIENT_LEAD_CATEGORY_TYPES.map((c) => `<option value="${escapeHtml(c.key)}">${escapeHtml(c.label)}</option>`).join("")}
        </select>
      </div>
      <div class="form-field">
        <label>Lead Source</label>
        <select id="leadSource">
          <option value="manual">Manual</option>
          <option value="apollo">Apollo</option>
          <option value="apify">Apify</option>
          <option value="linkedin">LinkedIn</option>
          <option value="website">Website</option>
          <option value="google_map">Google Maps</option>
        </select>
      </div>
      <div class="form-field">
        <label>Pipeline Stage</label>
        <select id="leadPipelineStage">
          ${CLIENT_LEAD_PIPELINE_STAGES.map((s) => `<option value="${s.key}">${escapeHtml(s.label)}</option>`).join("")}
        </select>
      </div>
      <div class="form-field">
        <label>Outreach Status</label>
        <select id="leadOutreachStatus">
          ${CLIENT_LEAD_OUTREACH_STATUSES.map((s) => `<option value="${s.key}">${escapeHtml(s.label)}</option>`).join("")}
        </select>
      </div>
      <div class="form-field">
        <label>Demo Status</label>
        <select id="leadDemoStatus">
          ${CLIENT_LEAD_DEMO_STATUSES.map((s) => `<option value="${s.key}">${escapeHtml(s.label)}</option>`).join("")}
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
        <label>Assigned To</label>
        <select id="leadAssignedTo">
          <option value="">Unassigned</option>
          ${Array.from(new Set((users || []).map((u) => u && u.name).filter(Boolean)))
            .map((n) => `<option value="${escapeHtml(n)}">${escapeHtml(n)}</option>`)
            .join("")}
        </select>
      </div>
      <div class="form-field">
        <label>Assign for Phone</label>
        <select id="leadPhoneAssignedTo">
          <option value="">Unassigned</option>
          ${Array.from(new Set((users || []).map((u) => u && u.name).filter(Boolean)))
            .map((n) => `<option value="${escapeHtml(n)}">${escapeHtml(n)}</option>`)
            .join("")}
        </select>
      </div>
      <div class="form-field">
        <label>Assign for Email</label>
        <select id="leadEmailAssignedTo">
          <option value="">Unassigned</option>
          ${Array.from(new Set((users || []).map((u) => u && u.name).filter(Boolean)))
            .map((n) => `<option value="${escapeHtml(n)}">${escapeHtml(n)}</option>`)
            .join("")}
        </select>
      </div>
      <div class="form-field">
        <label>Verified By</label>
        <select id="leadVerifiedBy">
          <option value="">Not verified</option>
          ${Array.from(new Set((users || []).map((u) => u && u.name).filter(Boolean)))
            .map((n) => `<option value="${escapeHtml(n)}">${escapeHtml(n)}</option>`)
            .join("")}
        </select>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Notes history</label>
        <div id="leadNotesHistory" class="meta" style="display:flex; flex-direction:column; gap:8px; max-height:220px; overflow-y:auto;"></div>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Add a note</label>
        <textarea id="leadNewNote" placeholder="Saved with your name and the current date/time."></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeClientLeadModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveClientLead(${Number(client.id)})">Save Lead</button>
    </div>
  </div>
</div>

<div id="leadImportModal" class="work-modal" onclick="closeLeadImportModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()" style="max-width:520px;">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">Import Leads from Excel</div>
      <button class="btn" type="button" onclick="closeLeadImportModal()">Close</button>
    </div>

    <div class="form-grid" style="grid-template-columns:1fr;">
      <div class="form-field">
        <label>Category Type</label>
        <select id="leadImportCategoryType">
          <option value="">Select…</option>
          ${CLIENT_LEAD_CATEGORY_TYPES.map((c) => `<option value="${escapeHtml(c.key)}">${escapeHtml(c.label)}</option>`).join("")}
        </select>
        <div class="meta" style="margin-top:6px; font-size:12px;">Applied to every lead in the sheet.</div>
      </div>
      <div class="form-field">
        <label>Excel / CSV File</label>
        <input type="file" id="clientLeadsExcelFile" accept=".xlsx,.xls,.csv" />
        <div class="meta" style="margin-top:6px; font-size:12px;">Rows whose email already exists are updated with the sheet's data; new emails are added. Every "Assigned to" and "Verified by" name must match an active user, or the whole sheet is rejected.</div>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeLeadImportModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="uploadClientLeadsExcel(${Number(client.id)})">Import</button>
    </div>
  </div>
</div>

<div id="blockerModal" class="work-modal" onclick="closeBlockerModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="blockerModalTitle" style="font-size:22px; font-weight:800;">Add Blocker</div>
      <button class="btn" type="button" onclick="closeBlockerModal()">Close</button>
    </div>

    <input type="hidden" id="blockerId" value="" />

    <div class="form-grid">
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Title</label>
        <input id="blockerTitle" placeholder="Example: Waiting on client API credentials" />
      </div>
      <div class="form-field">
        <label>Side</label>
        <select id="blockerSide">
          <option value="internal">Internal</option>
          <option value="client_side">Client-side</option>
        </select>
      </div>
      <div class="form-field">
        <label>Priority</label>
        <select id="blockerPriority">
          <option value="medium">Medium</option>
          <option value="high">High</option>
          <option value="low">Low</option>
        </select>
      </div>
      <div class="form-field">
        <label>Owner</label>
        <select id="blockerOwner">
          <option value="">Select owner</option>
          ${users.map((u) => `<option value="${u.id}">${escapeHtml(u.name)}</option>`).join("")}
        </select>
      </div>
      <div class="form-field">
        <label>Related Work Item</label>
        <select id="blockerWorkItem">
          <option value="">No related work item</option>
          ${workItems.map((w) => `<option value="${w.id}">#${w.id} · ${escapeHtml(w.title)}</option>`).join("")}
        </select>
      </div>
      <div class="form-field" id="blockerStatusField" style="display:none;">
        <label>Resolution Status</label>
        <select id="blockerStatus">
          <option value="open">Open</option>
          <option value="in_progress">In Progress</option>
          <option value="resolved">Resolved</option>
        </select>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Description</label>
        <textarea id="blockerDescription" placeholder="What is blocked and why"></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeBlockerModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveBlocker(${Number(client.id)})">Save Blocker</button>
    </div>
  </div>
</div>

<div id="meetingModal" class="work-modal" onclick="closeMeetingModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="meetingModalTitle" style="font-size:22px; font-weight:800;">Log Meeting</div>
      <button class="btn" type="button" onclick="closeMeetingModal()">Close</button>
    </div>

    <input type="hidden" id="meetingId" value="" />

    <div class="form-field" style="margin-bottom:14px;">
      <label>Meeting Details (AI Quick Fill)</label>
      <textarea id="meetingAiNotes" rows="6" style="min-height:120px;" placeholder="Write or paste the full meeting details here — what was discussed, who joined, decisions, action items — then click Auto-fill."></textarea>
      <div style="display:flex; justify-content:flex-end; margin-top:8px;">
        <button class="btn btn-primary" type="button" id="meetingAiFillBtn" onclick="aiFillMeetingFromNotes()">✨ Auto-fill with AI</button>
      </div>
    </div>

    <div class="form-grid">
      <div class="form-field">
        <label>Title</label>
        <input id="meetingTitle" placeholder="Example: Weekly sync call" />
      </div>
      <div class="form-field">
        <label>Date</label>
        <input id="meetingDate" type="date" />
      </div>
      <div class="form-field">
        <label>Type</label>
        <select id="meetingType">
          <option value="sync_call">Sync Call</option>
          <option value="review">Review</option>
          <option value="internal">Internal</option>
          <option value="adhoc">Ad-hoc</option>
        </select>
      </div>
      <div class="form-field">
        <label>Duration (min)</label>
        <input id="meetingDuration" type="number" min="0" step="5" placeholder="e.g. 30" />
      </div>
      <div class="form-field">
        <label>Participants</label>
        <input id="meetingParticipants" placeholder="Names, comma-separated" />
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Summary</label>
        <textarea id="meetingSummary" placeholder="Brief call summary"></textarea>
      </div>
      <div class="form-field meeting-edit-only" style="grid-column:1 / -1;">
        <label>Discussion Points</label>
        <textarea id="meetingDiscussionPoints" placeholder="Key points discussed"></textarea>
      </div>
      <div class="form-field meeting-edit-only" style="grid-column:1 / -1;">
        <label>Decisions Taken</label>
        <textarea id="meetingDecisions" placeholder="Decisions made"></textarea>
      </div>
      <div class="form-field meeting-edit-only" style="grid-column:1 / -1;">
        <label>Deliverables</label>
        <textarea id="meetingDeliverables" placeholder="Agreed deliverables"></textarea>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Action Items</label>
        <textarea id="meetingActionItems" placeholder="Who does what"></textarea>
      </div>
      <div class="form-field meeting-edit-only" style="grid-column:1 / -1;">
        <label>Follow-ups</label>
        <textarea id="meetingFollowUps" placeholder="Follow-up items"></textarea>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Next Steps</label>
        <textarea id="meetingNextSteps" placeholder="Next steps"></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeMeetingModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveMeeting(${Number(client.id)})">Save Meeting</button>
    </div>
  </div>
</div>

<div id="campaignModal" class="work-modal" onclick="closeCampaignModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="campaignModalTitle" style="font-size:22px; font-weight:800;">Add Campaign</div>
      <button class="btn" type="button" onclick="closeCampaignModal()">Close</button>
    </div>
    <input type="hidden" id="campaignId" value="" />
    <div class="form-grid">
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Name</label>
        <input id="campaignName" placeholder="Example: Q3 cold email blast" />
      </div>
      <div class="form-field">
        <label>Type</label>
        <select id="campaignType">
          <option value="email">Email</option>
          <option value="calling">Calling</option>
          <option value="linkedin">LinkedIn</option>
          <option value="whatsapp">WhatsApp</option>
          <option value="sms">SMS</option>
          <option value="events">Events / Webinar</option>
          <option value="ads">Paid Ads</option>
          <option value="content">Content / SEO</option>
          <option value="referral">Referral</option>
          <option value="other">Other</option>
        </select>
      </div>
      <div class="form-field">
        <label>Channel / Tool</label>
        <input id="campaignChannel" placeholder="e.g. Apollo, Instantly" />
      </div>
      <div class="form-field">
        <label>Status</label>
        <select id="campaignStatus">
          <option value="planned">Planned</option>
          <option value="active">Active</option>
          <option value="paused">Paused</option>
          <option value="completed">Completed</option>
        </select>
      </div>
      <div class="form-field">
        <label>Sent</label>
        <input id="campaignSent" type="number" min="0" value="0" />
      </div>
      <div class="form-field">
        <label>Responses</label>
        <input id="campaignResponses" type="number" min="0" value="0" />
      </div>
      <div class="form-field">
        <label>Positive replies</label>
        <input id="campaignPositiveReplies" type="number" min="0" value="0" />
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Notes</label>
        <textarea id="campaignNotes" placeholder="Performance, segments, etc."></textarea>
      </div>
    </div>
    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeCampaignModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveCampaign(${Number(client.id)})">Save Campaign</button>
    </div>
  </div>
</div>

<div id="incentiveModal" class="work-modal" onclick="closeIncentiveModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="incentiveModalTitle" style="font-size:22px; font-weight:800;">Add Incentive</div>
      <button class="btn" type="button" onclick="closeIncentiveModal()">Close</button>
    </div>
    <input type="hidden" id="incentiveId" value="" />
    <div class="form-grid">
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Title</label>
        <input id="incentiveTitle" placeholder="Example: Converted Acme deal commission" />
      </div>
      <div class="form-field">
        <label>GTM (attribution)</label>
        <select id="incentiveGtm">
          <option value="">Select team member</option>
          ${users.map((u) => `<option value="${u.id}">${escapeHtml(u.name)}</option>`).join("")}
        </select>
      </div>
      <div class="form-field">
        <label>Related Lead</label>
        <select id="incentiveLead">
          <option value="">No lead</option>
          ${incentiveLeadOptions}
        </select>
      </div>
      <div class="form-field">
        <label>Amount</label>
        <input id="incentiveAmount" type="number" min="0" step="0.01" value="0" />
      </div>
      <div class="form-field">
        <label>Status</label>
        <select id="incentiveStatus">
          <option value="pending">Pending</option>
          <option value="approved">Approved</option>
          <option value="paid">Paid</option>
        </select>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Credit log / Notes</label>
        <textarea id="incentiveNotes" placeholder="Attribution details, calculation, etc."></textarea>
      </div>
    </div>
    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeIncentiveModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveIncentive(${Number(client.id)})">Save Incentive</button>
    </div>
  </div>
</div>

<div id="reportModal" class="work-modal" onclick="closeReportModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="reportModalTitle" style="font-size:22px; font-weight:800;">New Weekly Report</div>
      <button class="btn" type="button" onclick="closeReportModal()">Close</button>
    </div>
    <input type="hidden" id="reportId" value="" />
    <div class="form-grid">
      <div class="form-field">
        <label>Period Label</label>
        <input id="reportPeriod" placeholder="e.g. Week 23 · Jun 3–9" />
      </div>
      <div class="form-field">
        <label>Week Start</label>
        <input id="reportWeekStart" type="date" />
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Summary</label>
        <textarea id="reportSummary" placeholder="Overall progress this week"></textarea>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Highlights</label>
        <textarea id="reportHighlights" placeholder="Wins, milestones hit"></textarea>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Lowlights / Risks</label>
        <textarea id="reportLowlights" placeholder="Risks, blockers, misses"></textarea>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Next Week Plan</label>
        <textarea id="reportNextWeek" placeholder="Plan for next week"></textarea>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label><input type="checkbox" id="reportClientVisible" checked /> Visible to client (when published)</label>
      </div>
    </div>
    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeReportModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveReport(${Number(client.id)})">Save Report</button>
    </div>
  </div>
</div>

<div id="goalsModal" class="work-modal" onclick="closeGoalsModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">🎯 Edit Goals</div>
      <button class="btn" type="button" onclick="closeGoalsModal()">Close</button>
    </div>
    <div class="form-grid">
      ${renderGoalsModalInner(clientGoals)}
    </div>
    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeGoalsModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveGoals(${Number(client.id)})">Save Goals</button>
    </div>
  </div>
</div>

<div id="clientUpdateModal" class="work-modal" onclick="closeClientUpdateModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">Add Client Update</div>
      <button class="btn" type="button" onclick="closeClientUpdateModal()">Close</button>
    </div>

    <div class="form-grid">
      <div class="form-field">
        <label>Title</label>
        <input id="clientUpdateTitle" placeholder="Example: Weekly progress update" />
      </div>

      <div class="form-field">
        <label>Related Work Item</label>
        <select id="clientUpdateWorkItem">
          <option value="">No related work item</option>
          ${workItems.map((w) => `<option value="${w.id}">#${w.id} · ${escapeHtml(w.title)}</option>`).join("")}
        </select>
      </div>

      <div class="form-field">
        <label>Update Type</label>
        <select id="clientUpdateType">
          <option value="general">General</option>
          <option value="progress">Progress</option>
          <option value="blocker">Blocker</option>
          <option value="client_call">Client Call</option>
          <option value="delivery">Delivery</option>
        </select>
      </div>

      <div class="form-field">
        <label>Visibility</label>
        <select id="clientUpdateVisibility">
          <option value="internal">Internal only</option>
          <option value="client">Client visible later</option>
        </select>
      </div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Update</label>
        <textarea id="clientUpdateText" placeholder="Write what happened, what changed, next step, blocker, etc."></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeClientUpdateModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="createClientUpdate(${Number(client.id)})">Save Update</button>
    </div>
  </div>
</div>

<div id="leadNotesHistoryModal" class="work-modal" onclick="closeLeadNotesHistoryModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">Notes History</div>
      <button class="btn" type="button" onclick="closeLeadNotesHistoryModal()">Close</button>
    </div>
    <div id="leadNotesHistoryModalBody" style="display:flex; flex-direction:column; gap:8px; max-height:60vh; overflow-y:auto;"></div>
  </div>
</div>

<div id="leadNoteModal" class="work-modal" onclick="closeLeadNoteModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">Add Note</div>
      <button class="btn" type="button" onclick="closeLeadNoteModal()">Close</button>
    </div>

    <div class="form-grid">
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Note</label>
        <textarea id="leadNoteText" placeholder="Write a note. Saved with your name and the current date/time."></textarea>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Voice note (optional)</label>
        <input type="file" id="leadNoteAudio" accept="audio/*" />
        <div class="meta" style="font-size:11px; margin-top:4px;">Attach an audio recording — it will be saved for playback, transcribed automatically, and the transcription added to the note.</div>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeLeadNoteModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="confirmLeadNote()">Save Note</button>
    </div>
  </div>
</div>

<div id="leadDemoNotesModal" class="work-modal" onclick="closeLeadDemoNotesModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">Demo Status Note</div>
      <button class="btn" type="button" onclick="closeLeadDemoNotesModal()">Close</button>
    </div>

    <div class="form-grid">
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Add a note for this demo status change (required)</label>
        <textarea id="leadDemoNotesText" placeholder="What changed? Outcome, next step, etc."></textarea>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Voice note (optional)</label>
        <input type="file" id="leadDemoNotesAudio" accept="audio/*" />
        <div class="meta" style="font-size:11px; margin-top:4px;">Attach an audio recording — it will be saved for playback, transcribed automatically, and the transcription added to the note.</div>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeLeadDemoNotesModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="confirmLeadDemoNotes()">Save</button>
    </div>
  </div>
</div>

<div id="leadStageNotesModal" class="work-modal" onclick="closeLeadStageNotesModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div style="font-size:22px; font-weight:800;">Status Change Note</div>
      <button class="btn" type="button" onclick="closeLeadStageNotesModal()">Close</button>
    </div>

    <div class="form-grid">
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Add a note for this status change (required)</label>
        <textarea id="leadStageNotesText" placeholder="What changed? Outcome, next step, etc."></textarea>
      </div>
      <div class="form-field" id="leadStageCallbackField" style="grid-column:1 / -1; display:none;">
        <label>Callback date (required)</label>
        <input type="date" id="leadStageCallbackDate" />
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Voice note (optional)</label>
        <input type="file" id="leadStageNotesAudio" accept="audio/*" />
        <div class="meta" style="font-size:11px; margin-top:4px;">Attach an audio recording — it will be saved for playback, transcribed automatically, and the transcription added to the note.</div>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeLeadStageNotesModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="confirmLeadStageNotes()">Save</button>
    </div>
  </div>
</div>

<div id="leadQuickUpdateModal" class="work-modal" onclick="closeLeadQuickUpdate(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="leadQuickTitle" style="font-size:22px; font-weight:800;">Update Lead</div>
      <button class="btn" type="button" onclick="closeLeadQuickUpdate()">Close</button>
    </div>

    <div class="form-grid">
      <div class="form-field">
        <label>Status</label>
        <select id="leadQuickStatus" onchange="onLeadQuickStatusChange()">
          ${CLIENT_LEAD_PIPELINE_STAGES.map(
            (s) =>
              `<option value="${escapeHtml(s.key)}">${escapeHtml(s.label)}</option>`,
          ).join("")}
        </select>
      </div>
      <div class="form-field" id="leadQuickCallbackField" style="display:none;">
        <label>Callback date (required)</label>
        <input type="date" id="leadQuickCallbackDate" />
      </div>
      <div class="form-field">
        <label>Demo</label>
        <select id="leadQuickDemo">
          ${CLIENT_LEAD_DEMO_STATUSES.map(
            (s) =>
              `<option value="${escapeHtml(s.key)}">${escapeHtml(s.label)}</option>`,
          ).join("")}
        </select>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Reached Via</label>
        <div style="display:flex; flex-wrap:wrap; gap:6px 16px;">
          ${REACH_VIA_CHANNELS.map(
            (c) =>
              `<label style="display:inline-flex; align-items:center; gap:6px; font-size:13px; font-weight:400;"><input type="checkbox" class="lqu-reach" value="${escapeHtml(c.key)}" /> ${escapeHtml(c.label)}</label>`,
          ).join("")}
        </div>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Add a note for this change (required)</label>
        <textarea id="leadQuickNote" placeholder="What changed? Outcome, next step, etc."></textarea>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Voice note (optional)</label>
        <input type="file" id="leadQuickAudio" accept="audio/*" />
        <div class="meta" style="font-size:11px; margin-top:4px;">Attach an audio recording — it will be saved for playback, transcribed automatically, and the transcription added to the note.</div>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Or record a voice note</label>
        <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
          <button id="leadQuickRecBtn" class="btn" type="button" onclick="toggleLeadQuickRecording()">● Record</button>
          <span id="leadQuickRecStatus" class="meta" style="font-size:12px;"></span>
          <button id="leadQuickRecDiscard" class="btn" type="button" style="display:none;" onclick="discardLeadQuickRecording()">Discard</button>
        </div>
        <audio id="leadQuickRecPreview" controls style="display:none; margin-top:8px; width:100%; max-width:320px; height:34px;"></audio>
        <div class="meta" style="font-size:11px; margin-top:4px;">Record directly from your microphone — it will be saved, transcribed automatically, and the transcription added to the note.</div>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeLeadQuickUpdate()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="confirmLeadQuickUpdate()">Save</button>
    </div>
  </div>
</div>

<div id="actionModal" class="work-modal" onclick="closeActionModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="actionModalTitle" style="font-size:22px; font-weight:800;">Add Action</div>
      <button class="btn" type="button" onclick="closeActionModal()">Close</button>
    </div>

    <input id="actionId" type="hidden" />

    <div class="form-grid">
      <div class="form-field">
        <label>Title</label>
        <input id="actionTitle" placeholder="Need logo from client" />
      </div>

      <div class="form-field">
        <label>Owner Type</label>
        <select id="actionOwnerType">
          <option value="WeSolve">WeSolve</option>
          <option value="Client">Client</option>
        </select>
      </div>

      <div class="form-field">
        <label>Owner Name</label>
        <input id="actionOwnerName" placeholder="Aj / Malikah / Client" />
      </div>

      <div class="form-field">
        <label>Due Date</label>
        <input id="actionDueDate" type="date" />
      </div>

      <div class="form-field">
        <label>Status</label>
        <select id="actionStatus">
          <option>Open</option>
          <option>In Progress</option>
          <option>Waiting</option>
          <option>Done</option>
        </select>
      </div>

      <div class="form-field">
        <label>Priority</label>
        <select id="actionPriority">
          <option>Low</option>
          <option selected>Medium</option>
          <option>High</option>
          <option>Urgent</option>
        </select>
      </div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Notes</label>
        <textarea id="actionNotes"></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeActionModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveAction()">Save Action</button>
    </div>
  </div>
</div>

<div id="contributorModal" class="work-modal" onclick="closeContributorModal(event)">
  <div class="work-modal-card" onclick="event.stopPropagation()">
    <div style="display:flex; justify-content:space-between; gap:12px; align-items:center; margin-bottom:14px;">
      <div id="contributorModalTitle" style="font-size:22px; font-weight:800;">Add Contributor</div>
      <button class="btn" type="button" onclick="closeContributorModal()">Close</button>
    </div>

    <input id="contributorId" type="hidden" />

    <div class="form-grid">
      <div class="form-field">
        <label>Person Type</label>
        <select id="contributorPersonType">
          <option>Internal</option>
          <option selected>Contractor</option>
          <option>Client</option>
        </select>
      </div>

      <div class="form-field">
        <label>Name</label>
        <input id="contributorName" placeholder="Name" />
      </div>

      <div class="form-field">
        <label>Email</label>
        <input id="contributorEmail" placeholder="email@example.com" />
      </div>

      <div class="form-field">
        <label>Phone</label>
        <input id="contributorPhone" placeholder="+91..." />
      </div>

      <div class="form-field">
        <label>Role</label>
        <input id="contributorRole" placeholder="Developer / Designer / Client Contact" />
      </div>

      <div class="form-field">
        <label>Status</label>
        <select id="contributorStatus">
          <option>Active</option>
          <option>Inactive</option>
        </select>
      </div>

      <div class="form-field">
        <label>
          <input id="contributorCanUpdateWork" type="checkbox" style="width:auto;" />
          Can update work
        </label>
      </div>

      <div class="form-field">
        <label>
          <input id="contributorCanViewClientDashboard" type="checkbox" style="width:auto;" />
          Can view client dashboard
        </label>
      </div>

      <div class="form-field" style="grid-column:1 / -1;">
        <label>Notes</label>
        <textarea id="contributorNotes"></textarea>
      </div>
    </div>

    <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px;">
      <button class="btn" type="button" onclick="closeContributorModal()">Cancel</button>
      <button class="btn btn-primary" type="button" onclick="saveContributor()">Save Contributor</button>
    </div>
  </div>
</div>

        
<script>
  const WORK_ITEM_USERS = ${JSON.stringify(users.map((u) => ({ id: u.id, name: u.name })))};
const WORK_ITEMS = ${JSON.stringify(
    workItems.map((w) => ({
      id: w.id,
      title: w.title,
      status: w.status,
      owner_user_id: w.owner_user_id,
      dependency_work_item_id: w.dependency_work_item_id,
      milestone_id: w.milestone_id,
      priority: w.priority,
      due_date: w.due_date,
      description: w.description,
      created_at: w.created_at,
      updated_at: w.updated_at,
    })),
  )};
  
  const CLIENT_ACTIONS = ${JSON.stringify(actions)};
const CLIENT_CONTRIBUTORS = ${JSON.stringify(contributors)};
const CLIENT_ID = ${Number(client.id)};
const CLIENT_MILESTONES = ${JSON.stringify(milestones)};

async function generateClientViewLink() {
  // Open the tab synchronously on the click gesture so popup blockers
  // don't reject it after the async fetch resolves. Note: passing
  // "noopener" makes window.open return null, so we keep the handle and
  // sever the opener manually instead.
  const newTab = window.open("", "_blank");
  if (newTab) newTab.opener = null;

  const res = await fetch("/api/clients/" + CLIENT_ID + "/client-view-link", {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.ok) {
    if (newTab) newTab.close();
    alert(json.error || "Failed to create client link");
    return;
  }

  const url = json.data.url;

  if (newTab) {
    newTab.location.href = url;
  } else {
    // Popup was blocked — fall back to navigating the current tab.
    window.open(url, "_blank", "noopener,noreferrer");
  }
}


function openMilestoneModal() {
  document.getElementById("milestoneModalTitle").textContent = "Add Milestone";
  document.getElementById("milestoneId").value = "";
  document.getElementById("milestoneTitle").value = "";
  document.getElementById("milestoneDueDate").value = "";
  document.getElementById("milestoneStatus").value = "planned";
  document.getElementById("milestoneNotes").value = "";
  document.getElementById("milestoneModal").classList.add("open");
}

function openMilestoneEditModal(id) {
  const milestone = CLIENT_MILESTONES.find(function(m) {
    return Number(m.id) === Number(id);
  });

  if (!milestone) {
    alert("Milestone not found");
    return;
  }

  document.getElementById("milestoneModalTitle").textContent = "Edit Milestone";
  document.getElementById("milestoneId").value = milestone.id;
  document.getElementById("milestoneTitle").value = milestone.title || "";
  document.getElementById("milestoneDueDate").value = milestone.due_date || "";
  document.getElementById("milestoneStatus").value = milestone.status || "planned";
  document.getElementById("milestoneNotes").value = milestone.notes || "";
  document.getElementById("milestoneModal").classList.add("open");
}

function closeMilestoneModal(event) {
  if (event && event.target && event.target.id !== "milestoneModal") return;
  document.getElementById("milestoneModal").classList.remove("open");
}

async function saveMilestone() {
  const id = document.getElementById("milestoneId").value;

  const payload = {
    title: document.getElementById("milestoneTitle").value.trim(),
    due_date: document.getElementById("milestoneDueDate").value || null,
    status: document.getElementById("milestoneStatus").value,
    notes: document.getElementById("milestoneNotes").value.trim()
  };

  if (!payload.title) {
    alert("Milestone title is required");
    return;
  }

  const url = id
    ? "/api/clients/" + CLIENT_ID + "/milestones/" + id
    : "/api/clients/" + CLIENT_ID + "/milestones";

  const method = id ? "PUT" : "POST";

  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to save milestone");
    return;
  }

  window.location.reload();
}

async function closeMilestone(id) {
  if (!confirm("Close this milestone?")) return;

  const res = await fetch("/api/clients/" + CLIENT_ID + "/milestones/" + id, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status: "closed" })
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to close milestone");
    return;
  }

  window.location.reload();
}

async function archiveMilestone(id) {
  if (!confirm("Archive this milestone? Work items will remain, but the milestone will be hidden.")) return;

  const res = await fetch("/api/clients/" + CLIENT_ID + "/milestones/" + id + "/archive", {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to archive milestone");
    return;
  }

  window.location.reload();
}

function openActionModal() {
  document.getElementById("actionModalTitle").textContent = "Add Action";
  document.getElementById("actionId").value = "";
  document.getElementById("actionTitle").value = "";
  document.getElementById("actionOwnerType").value = "WeSolve";
  document.getElementById("actionOwnerName").value = "";
  document.getElementById("actionDueDate").value = "";
  document.getElementById("actionStatus").value = "Open";
  document.getElementById("actionPriority").value = "Medium";
  document.getElementById("actionNotes").value = "";
  document.getElementById("actionModal").classList.add("open");
}

function openActionEditModal(id) {
  const action = CLIENT_ACTIONS.find(function(a) {
    return Number(a.id) === Number(id);
  });

  if (!action) {
    alert("Action not found");
    return;
  }

  document.getElementById("actionModalTitle").textContent = "Edit Action";
  document.getElementById("actionId").value = action.id;
  document.getElementById("actionTitle").value = action.title || "";
  document.getElementById("actionOwnerType").value = action.owner_type || "WeSolve";
  document.getElementById("actionOwnerName").value = action.owner_name || "";
  document.getElementById("actionDueDate").value = action.due_date || "";
  document.getElementById("actionStatus").value = action.status || "Open";
  document.getElementById("actionPriority").value = action.priority || "Medium";
  document.getElementById("actionNotes").value = action.notes || "";
  document.getElementById("actionModal").classList.add("open");
}

function closeActionModal(event) {
  if (event && event.target && event.target.id !== "actionModal") return;
  document.getElementById("actionModal").classList.remove("open");
}

async function saveAction() {
  const id = document.getElementById("actionId").value;

  const payload = {
    title: document.getElementById("actionTitle").value.trim(),
    owner_type: document.getElementById("actionOwnerType").value,
    owner_name: document.getElementById("actionOwnerName").value.trim(),
    due_date: document.getElementById("actionDueDate").value || null,
    status: document.getElementById("actionStatus").value,
    priority: document.getElementById("actionPriority").value,
    notes: document.getElementById("actionNotes").value.trim()
  };

  if (!payload.title) {
    alert("Action title is required");
    return;
  }

  const url = id
    ? "/api/clients/" + CLIENT_ID + "/actions/" + id
    : "/api/clients/" + CLIENT_ID + "/actions";

  const method = id ? "PUT" : "POST";

  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  const json = await res.json();

  if (!json.success && !json.ok) {
    alert(json.error || "Failed to save action");
    return;
  }

  window.location.reload();
}

async function archiveAction(id) {
  if (!confirm("Archive this action?")) return;

  const res = await fetch("/api/clients/" + CLIENT_ID + "/actions/" + id + "/archive", {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.success && !json.ok) {
    alert(json.error || "Failed to archive action");
    return;
  }

  window.location.reload();
}

function openContributorModal() {
  document.getElementById("contributorModalTitle").textContent = "Add Contributor";
  document.getElementById("contributorId").value = "";
  document.getElementById("contributorPersonType").value = "Contractor";
  document.getElementById("contributorName").value = "";
  document.getElementById("contributorEmail").value = "";
  document.getElementById("contributorPhone").value = "";
  document.getElementById("contributorRole").value = "";
  document.getElementById("contributorStatus").value = "Active";
  document.getElementById("contributorCanUpdateWork").checked = true;
  document.getElementById("contributorCanViewClientDashboard").checked = false;
  document.getElementById("contributorNotes").value = "";
  document.getElementById("contributorModal").classList.add("open");
}

function openContributorEditModal(id) {
  const person = CLIENT_CONTRIBUTORS.find(function(p) {
    return Number(p.id) === Number(id);
  });

  if (!person) {
    alert("Contributor not found");
    return;
  }

  document.getElementById("contributorModalTitle").textContent = "Edit Contributor";
  document.getElementById("contributorId").value = person.id;
  document.getElementById("contributorPersonType").value = person.person_type || "Contractor";
  document.getElementById("contributorName").value = person.name || "";
  document.getElementById("contributorEmail").value = person.email || "";
  document.getElementById("contributorPhone").value = person.phone || "";
  document.getElementById("contributorRole").value = person.role || "";
  document.getElementById("contributorStatus").value = person.status || "Active";
  document.getElementById("contributorCanUpdateWork").checked = !!person.can_update_work;
  document.getElementById("contributorCanViewClientDashboard").checked = !!person.can_view_client_dashboard;
  document.getElementById("contributorNotes").value = person.notes || "";
  document.getElementById("contributorModal").classList.add("open");
}

function closeContributorModal(event) {
  if (event && event.target && event.target.id !== "contributorModal") return;
  document.getElementById("contributorModal").classList.remove("open");
}

async function saveContributor() {
  const id = document.getElementById("contributorId").value;

  const payload = {
    person_type: document.getElementById("contributorPersonType").value,
    name: document.getElementById("contributorName").value.trim(),
    email: document.getElementById("contributorEmail").value.trim(),
    phone: document.getElementById("contributorPhone").value.trim(),
    role: document.getElementById("contributorRole").value.trim(),
    status: document.getElementById("contributorStatus").value,
    can_update_work: document.getElementById("contributorCanUpdateWork").checked,
    can_view_client_dashboard: document.getElementById("contributorCanViewClientDashboard").checked,
    notes: document.getElementById("contributorNotes").value.trim()
  };

  if (!payload.name || !payload.role) {
    alert("Name and role are required");
    return;
  }

  const url = id
    ? "/api/clients/" + CLIENT_ID + "/contributors/" + id
    : "/api/clients/" + CLIENT_ID + "/contributors";

  const method = id ? "PUT" : "POST";

  const res = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  const json = await res.json();

  if (!json.success && !json.ok) {
    alert(json.error || "Failed to save contributor");
    return;
  }

  window.location.reload();
}

async function archiveContributor(id) {
  if (!confirm("Archive this contributor?")) return;

  const res = await fetch("/api/clients/" + CLIENT_ID + "/contributors/" + id + "/archive", {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });

  const json = await res.json();

  if (!json.success && !json.ok) {
    alert(json.error || "Failed to archive contributor");
    return;
  }

  window.location.reload();
}

  function showLoadingModal(message) {
    const modal = document.getElementById("workItemDetailModal");
    const title = document.getElementById("workItemDetailTitle");
    const body = document.getElementById("workItemDetailBody");

    title.textContent = "Opening this page";
    body.innerHTML =
      '<div style="padding:20px; text-align:center;">' +
        '<div style="font-size:18px; font-weight:800; margin-bottom:8px;">Please wait...</div>' +
        '<div class="meta">' + escapeHtmlClient(message || "Loading details...") + '</div>' +
      '</div>';

    modal.classList.add("open");
  }
  
  function openClientUpdateModal() {
  document.getElementById("clientUpdateTitle").value = "";
  document.getElementById("clientUpdateWorkItem").value = "";
  document.getElementById("clientUpdateType").value = "general";
  document.getElementById("clientUpdateVisibility").value = "internal";
  document.getElementById("clientUpdateText").value = "";

  document.getElementById("clientUpdateModal").classList.add("open");
}

function closeClientUpdateModal(event) {
  if (event && event.target && event.target.id !== "clientUpdateModal") return;
  document.getElementById("clientUpdateModal").classList.remove("open");
}

async function createClientUpdate(clientId) {
  const updateText = document.getElementById("clientUpdateText").value.trim();

  if (!updateText) {
    alert("Update text is required");
    return;
  }

  const payload = {
    title: document.getElementById("clientUpdateTitle").value.trim(),
    update_text: updateText,
    update_type: document.getElementById("clientUpdateType").value,
    related_work_item_id: document.getElementById("clientUpdateWorkItem").value || null,
    is_client_visible: document.getElementById("clientUpdateVisibility").value === "client"
  };

  showLoadingModal("Saving client update...");

  const res = await fetch("/api/clients/" + clientId + "/updates", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  const json = await res.json();

  if (!json.ok) {
    alert(json.error || "Failed to save update");
    closeWorkItemDetail();
    return;
  }

  window.location.reload();
}

function openWorkItemModal() {
  document.getElementById("workTitle").value = "";
  document.getElementById("workOwner").value = "";
  document.getElementById("workPriority").value = "medium";
  document.getElementById("workDueDate").value = "";
  document.getElementById("workDependency").value = "";
  document.getElementById("workDescription").value = "";

  document.getElementById("workItemModal").classList.add("open");
}

  function closeWorkItemModal(event) {
    if (event && event.target && event.target.id !== "workItemModal") return;
    document.getElementById("workItemModal").classList.remove("open");
  }

  async function createWorkItem(clientId) {
    const title = document.getElementById("workTitle").value.trim();

    if (!title) {
      alert("Title is required");
      return;
    }

    showLoadingModal("Creating work item...");

const payload = {
  client_id: clientId,
  title,
  description: document.getElementById("workDescription").value.trim(),
  owner_user_id: document.getElementById("workOwner").value || null,
  priority: document.getElementById("workPriority").value,
  due_date: document.getElementById("workDueDate").value || null,
  dependency_work_item_id: document.getElementById("workDependency").value || null,
  milestone_id: document.getElementById("workMilestone").value || null
};

    const res = await fetch("/api/client-work-items", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const json = await res.json();

    if (!json.ok) {
      alert("Create failed: " + (json.error || "Unknown error"));
      console.error("Create work item failed:", json);
      closeWorkItemDetail();
      return;
    }

    window.location.reload();
  }

  async function quickUpdateWorkItem(id, status) {
    showLoadingModal("Updating work item status...");

    const res = await fetch("/api/client-work-items/" + id, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status })
    });

    const json = await res.json();

    if (!json.ok) {
      alert(json.error || "Failed to update work item");
      closeWorkItemDetail();
      return;
    }

    window.location.reload();
  }

  async function archiveWorkItem(id) {
    if (!confirm("Archive this work item? It will be hidden but not permanently deleted.")) {
      return;
    }

    showLoadingModal("Archiving work item...");

    const res = await fetch("/api/client-work-items/" + id, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ archive: true })
    });

    const json = await res.json();

    if (!json.ok) {
      alert(json.error || "Failed to archive work item");
      closeWorkItemDetail();
      return;
    }

    window.location.reload();
  }

  async function openWorkItemDetail(id) {
    showLoadingModal("Opening work item details...");

    const res = await fetch("/api/client-work-items/" + id);
    const json = await res.json();

    if (!json.ok) {
      document.getElementById("workItemDetailBody").innerHTML =
        escapeHtmlClient(json.error || "Failed to load work item");
      return;
    }

    const w = json.data;

    const ownerOptions = WORK_ITEM_USERS.map(function(u) {
      return '<option value="' + u.id + '" ' +
        (String(w.owner_user_id || "") === String(u.id) ? "selected" : "") +
        '>' + escapeHtmlClient(u.name) + '</option>';
    }).join("");

    const dependencyOptions = WORK_ITEMS
      .filter(function(item) {
        return Number(item.id) !== Number(w.id);
      })
      .map(function(item) {
        return '<option value="' + item.id + '" ' +
          (String(w.dependency_work_item_id || "") === String(item.id) ? "selected" : "") +
          '>#' + item.id + ' · ' + escapeHtmlClient(item.title) + ' (' + escapeHtmlClient(item.status || "todo") + ')</option>';
      })
      .join("");
      
      const milestoneOptions = ${JSON.stringify(
        milestones.map((m) => ({ id: m.id, title: m.title })),
      )}.map(function(m) {
  return '<option value="' + m.id + '" ' +
    (String(w.milestone_id || "") === String(m.id) ? "selected" : "") +
    '>' + escapeHtmlClient(m.title) + '</option>';
}).join("");

    document.getElementById("workItemDetailTitle").textContent =
      "#" + w.id + " — Edit Work Item";

    document.getElementById("workItemDetailBody").innerHTML =
      '<div class="form-grid">' +

        '<div class="form-field">' +
          '<label>Title</label>' +
          '<input id="editWorkTitle" value="' + escapeHtmlClient(w.title || "") + '" />' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Status</label>' +
          '<select id="editWorkStatus">' +
            '<option value="todo" ' + ((w.status || "todo") === "todo" ? "selected" : "") + '>Todo</option>' +
            '<option value="in_progress" ' + (w.status === "in_progress" ? "selected" : "") + '>In Progress</option>' +
            '<option value="done" ' + (w.status === "done" ? "selected" : "") + '>Done</option>' +
          '</select>' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Priority</label>' +
          '<select id="editWorkPriority">' +
            '<option value="low" ' + (w.priority === "low" ? "selected" : "") + '>Low</option>' +
            '<option value="medium" ' + ((w.priority || "medium") === "medium" ? "selected" : "") + '>Medium</option>' +
            '<option value="high" ' + (w.priority === "high" ? "selected" : "") + '>High</option>' +
          '</select>' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Due Date</label>' +
          '<input id="editWorkDueDate" type="date" value="' + escapeHtmlClient(w.due_date || "") + '" />' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Owner</label>' +
          '<select id="editWorkOwner">' +
            '<option value="">No owner</option>' +
            ownerOptions +
          '</select>' +
        '</div>' +

        '<div class="form-field">' +
          '<label>Depends On</label>' +
          '<select id="editWorkDependency">' +
            '<option value="">No dependency</option>' +
            dependencyOptions +
          '</select>' +
        '</div>' +
        
        '<div class="form-field">' +
  '<label>Milestone</label>' +
  '<select id="editWorkMilestone">' +
    '<option value="">No milestone</option>' +
    milestoneOptions +
  '</select>' +
'</div>' +

        '<div class="form-field" style="grid-column:1 / -1;">' +
          '<label>Description</label>' +
          '<textarea id="editWorkDescription">' + escapeHtmlClient(w.description || "") + '</textarea>' +
        '</div>' +

      '</div>' +

      '<div style="margin-top:14px; padding-top:14px; border-top:1px solid rgba(255,255,255,0.10);">' +
        '<div><strong>Created:</strong> ' + escapeHtmlClient(w.created_at || "-") + '</div>' +
        '<div><strong>Last Updated:</strong> ' + escapeHtmlClient(w.updated_at || "-") + '</div>' +
      '</div>' +

      '<div style="display:flex; justify-content:flex-end; gap:10px; margin-top:16px; flex-wrap:wrap;">' +
        '<button class="btn" type="button" onclick="closeWorkItemDetail()">Cancel</button>' +
        '<button class="btn" type="button" onclick="archiveWorkItem(' + Number(w.id) + ')">Archive</button>' +
        '<button class="btn btn-primary" type="button" onclick="saveWorkItemChanges(' + Number(w.id) + ')">Save Changes</button>' +
      '</div>';
  }

  async function saveWorkItemChanges(id) {
    const title = document.getElementById("editWorkTitle").value.trim();

    if (!title) {
      alert("Title is required");
      return;
    }

    showLoadingModal("Saving work item changes...");


const payload = {
  title,
  status: document.getElementById("editWorkStatus").value,
  priority: document.getElementById("editWorkPriority").value,
  owner_user_id: document.getElementById("editWorkOwner").value || null,
  due_date: document.getElementById("editWorkDueDate").value || null,
  dependency_work_item_id: document.getElementById("editWorkDependency").value || null,
  milestone_id: document.getElementById("editWorkMilestone").value || null,
  description: document.getElementById("editWorkDescription").value.trim()
};

    const res = await fetch("/api/client-work-items/" + id, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const json = await res.json();

    if (!json.ok) {
      alert(json.error || "Failed to update work item");
      closeWorkItemDetail();
      return;
    }

    window.location.reload();
  }

function closeWorkItemDetail(e) {
  if (!e || e.target.id === "workItemDetailModal") {
    document.getElementById("workItemDetailModal").classList.remove("open");
  }
}

document.addEventListener("keydown", function(event) {
  if (event.key === "Escape") {
    document.querySelectorAll(".work-modal.open").forEach(function(modal) {
      // The lead quick-update modal needs its close handler to run so the call
      // icon is reverted and is_call_made is persisted back to false. Just
      // stripping .open would leave the icon locked (red) and the call marked.
      if (modal.id === "leadQuickUpdateModal") {
        closeLeadQuickUpdate();
        return;
      }
      modal.classList.remove("open");
    });
  }
});

// ---------------------------------------------------------------------------
// Client Leads
// ---------------------------------------------------------------------------
function setLeadFormValue(id, value) {
  var el = document.getElementById(id);
  if (el) el.value = value == null ? "" : value;
}

// The Assigned To dropdown only lists current team members; older leads may
// carry a name that's no longer on the team (renamed/removed). Rather than
// silently dropping it, add a one-off option so the existing value stays visible.
function setLeadAssignedToValue(value) {
  var el = document.getElementById("leadAssignedTo");
  if (!el) return;
  var v = value || "";
  if (v && !Array.prototype.some.call(el.options, function (o) { return o.value === v; })) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    el.appendChild(opt);
  }
  el.value = v;
}

// Verified By mirrors Assigned To: the dropdown lists current team members, but
// a lead may carry a name no longer on the team. Add a one-off option so the
// stored value stays visible instead of silently resetting to "Not verified".
function setLeadVerifiedByValue(value) {
  var el = document.getElementById("leadVerifiedBy");
  if (!el) return;
  var v = value || "";
  if (v && !Array.prototype.some.call(el.options, function (o) { return o.value === v; })) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    el.appendChild(opt);
  }
  el.value = v;
}

// Assign for Phone / Assign for Email mirror Assigned To (same team-member
// dropdown, same "keep an off-team name visible" rule), so they share one
// setter keyed by the select's id.
function setLeadUserSelectValue(elId, value) {
  var el = document.getElementById(elId);
  if (!el) return;
  var v = value || "";
  if (v && !Array.prototype.some.call(el.options, function (o) { return o.value === v; })) {
    var opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    el.appendChild(opt);
  }
  el.value = v;
}

// Client-lead notes are an append-only history stored as a JSON array of
// { text, at, by }. Legacy rows hold a plain string -> single entry.
function parseLeadNotesClient(raw) {
  if (Array.isArray(raw)) return raw.filter(function(n){ return n && typeof n === "object" && n.text != null; });
  if (typeof raw !== "string") return [];
  var t = raw.trim();
  if (!t) return [];
  if (t.charAt(0) === "[") {
    try {
      var arr = JSON.parse(t);
      if (Array.isArray(arr)) return arr.filter(function(n){ return n && typeof n === "object" && n.text != null; });
    } catch (e) {}
  }
  return [{ text: t, at: null, by: null }];
}

function renderNotesInto(box, raw) {
  if (!box) return;
  var history = parseLeadNotesClient(raw);
  if (!history.length) {
    box.innerHTML = '<div class="meta">No notes yet.</div>';
    return;
  }
  // Newest first for readability.
  box.innerHTML = history.slice().reverse().map(function(n){
    var when = n.at ? new Date(n.at).toLocaleString("en-IN", { timeZone: "Asia/Kolkata", year: "numeric", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", hour12: true }) : "";
    var byline = [n.by ? escapeHtmlClient(n.by) : "", when].filter(Boolean).join(" · ");
    return '<div style="padding:8px 10px; border:1px solid var(--line); border-radius:8px;">' +
             '<div style="white-space:pre-wrap; color:var(--text, inherit);">' + escapeHtmlClient(n.text) + '</div>' +
             (n.audio_url ? '<audio controls preload="none" style="margin-top:6px; width:100%; max-width:340px; height:34px;" src="' + escapeHtmlClient(n.audio_url) + '"></audio>' : '') +
             (byline ? '<div class="meta" style="font-size:11px; margin-top:4px;">' + byline + '</div>' : '') +
           '</div>';
  }).join("");
}

function renderLeadNotesHistory(raw) {
  renderNotesInto(document.getElementById("leadNotesHistory"), raw);
}

function closeLeadNotesHistoryModal(event) {
  if (event && event.target && event.target.id !== "leadNotesHistoryModal") return;
  document.getElementById("leadNotesHistoryModal").classList.remove("open");
}

async function openLeadNotesHistory(clientId, leadId) {
  showLoadingModal("Loading notes...");
  try {
    var res = await fetch("/api/clients/" + clientId + "/leads/" + leadId);
    var json = await res.json();
    document.getElementById("workItemDetailModal").classList.remove("open");
    if (!json.ok) { alert(json.error || "Failed to load notes"); return; }
    renderNotesInto(document.getElementById("leadNotesHistoryModalBody"), (json.data || {}).notes);
    document.getElementById("leadNotesHistoryModal").classList.add("open");
  } catch (e) {
    document.getElementById("workItemDetailModal").classList.remove("open");
    alert("Failed to load notes");
  }
}

function openClientLeadModal() {
  document.getElementById("clientLeadModalTitle").textContent = "Add Lead";
  setLeadFormValue("leadId", "");
  ["leadCompany","leadContactName","leadPhone","leadEmail","leadCity","leadState","leadCountryField","leadWebsite","leadPersonLinkedin","leadCompanyLinkedin","leadFundingAmount","leadFundingDate","leadFundingRound","leadCategoryType","leadAssignedTo","leadPhoneAssignedTo","leadEmailAssignedTo","leadVerifiedBy","leadNewNote"].forEach(function(id){ setLeadFormValue(id, ""); });
  renderLeadNotesHistory("");
  setLeadFormValue("leadCategory", "b2b");
  setLeadFormValue("leadSource", "manual");
  setLeadFormValue("leadPipelineStage", "prospect_identified");
  setLeadFormValue("leadOutreachStatus", "not_started");
  setLeadFormValue("leadDemoStatus", "not_scheduled");
  setLeadFormValue("leadStatus", "new");
  document.getElementById("clientLeadModal").classList.add("open");
}

function closeClientLeadModal(event) {
  if (event && event.target && event.target.id !== "clientLeadModal") return;
  document.getElementById("clientLeadModal").classList.remove("open");
}

// Leads "Filter" popup: toggle open/closed and close on an outside click.
function toggleClientLeadFilterPopup(event) {
  if (event) event.stopPropagation();
  var pop = document.getElementById("clientLeadFilterPopup");
  var btn = event && event.currentTarget;
  if (!pop) return;
  var open = pop.style.display !== "flex";
  pop.style.display = open ? "flex" : "none";
  if (btn && btn.setAttribute) btn.setAttribute("aria-expanded", String(open));
}
document.addEventListener("click", function (e) {
  var pop = document.getElementById("clientLeadFilterPopup");
  if (!pop || pop.style.display !== "flex") return;
  var wrap = document.getElementById("clientLeadFilterWrap");
  if (wrap && !wrap.contains(e.target)) pop.style.display = "none";
});

// Bulk email search: a multi-line paste (e.g. an email list, one per line)
// would otherwise have its newlines stripped by the single-line input, fusing
// the emails together. Normalize separators to single spaces on paste so the
// server can split the list back into tokens.
function normalizeLeadSearchPaste(e) {
  var text = (e.clipboardData || window.clipboardData).getData("text") || "";
  // NOTE: this function is emitted inside a server-side template literal, so
  // every regex backslash must be doubled to survive into the browser (a
  // single-escaped newline class would be turned into a real line break here,
  // producing a regex literal that spans lines and breaks the whole script).
  if (!/[\\r\\n]/.test(text)) return; // single-line paste — browser handles it
  e.preventDefault();
  var norm = text.replace(/[\\s,;]+/g, " ").trim();
  var input = e.target;
  var start = input.selectionStart == null ? input.value.length : input.selectionStart;
  var end = input.selectionEnd == null ? input.value.length : input.selectionEnd;
  input.value = input.value.slice(0, start) + norm + input.value.slice(end);
  var pos = start + norm.length;
  if (input.setSelectionRange) input.setSelectionRange(pos, pos);
}

// Collapsed multi-selects inside the filter popup (Reached via / Category
// type): the button expands the inline checkbox list; the summary shows
// "All", the single picked label, or "N selected".
function toggleLeadFilterMs(btn) {
  var panel = btn.parentElement.querySelector(".lead-filter-ms-panel");
  if (!panel) return;
  panel.style.display = panel.style.display === "none" ? "flex" : "none";
}

function updateLeadFilterMsSummary(cb) {
  var panel = cb.closest(".lead-filter-ms-panel");
  var summary = panel && panel.parentElement.querySelector(".lead-filter-ms-btn span");
  if (!summary) return;
  var checked = panel.querySelectorAll("input:checked");
  summary.textContent =
    checked.length === 0
      ? "All"
      : checked.length === 1
        ? checked[0].parentElement.textContent.trim()
        : checked.length + " selected";
}

// Auto-open the Add Lead modal when navigated here via the nav "Add Lead" link
// (/clients/:id?tab=leads&addLead=1).
(function () {
  try {
    var params = new URLSearchParams(window.location.search);
    if (params.get("addLead") === "1" && document.getElementById("clientLeadModal")) {
      openClientLeadModal();
    }
  } catch (e) {}
})();

async function openClientLeadDetail(clientId, leadId) {
  showLoadingModal("Loading lead...");
  try {
    const res = await fetch("/api/clients/" + clientId + "/leads/" + leadId);
    const json = await res.json();
    if (!json.ok) { alert(json.error || "Failed to load lead"); document.getElementById("workItemDetailModal").classList.remove("open"); return; }
    const l = json.data || {};
    document.getElementById("clientLeadModalTitle").textContent = "Edit Lead";
    setLeadFormValue("leadId", l.id);
    setLeadFormValue("leadCompany", l.company || l.business_name || "");
    setLeadFormValue("leadContactName", l.contact_name || "");
    setLeadFormValue("leadPhone", l.phone || "");
    setLeadFormValue("leadEmail", l.email || "");
    setLeadFormValue("leadCity", l.city || "");
    setLeadFormValue("leadState", l.state || "");
    setLeadFormValue("leadCountryField", l.country || "");
    setLeadFormValue("leadWebsite", l.website || "");
    setLeadFormValue("leadPersonLinkedin", l.person_linkedin_url || "");
    setLeadFormValue("leadCompanyLinkedin", l.company_linkedin_url || "");
    setLeadFormValue("leadFundingAmount", l.company_last_round_amount || "");
    setLeadFormValue("leadFundingDate", l.company_last_funding_date || "");
    setLeadFormValue("leadFundingRound", l.company_funding_round || "");
    setLeadFormValue("leadCategory", l.lead_category || "b2b");
    setLeadFormValue("leadCategoryType", l.category_type || "");
    setLeadFormValue("leadSource", l.lead_source || "manual");
    setLeadFormValue("leadPipelineStage", l.pipeline_stage || "prospect_identified");
    setLeadFormValue("leadOutreachStatus", l.outreach_status || "not_started");
    setLeadFormValue("leadDemoStatus", l.demo_status || "not_scheduled");
    setLeadFormValue("leadStatus", l.status || "new");
    setLeadAssignedToValue(l.assigned_to || "");
    setLeadUserSelectValue("leadPhoneAssignedTo", l.phone_assigned_to || "");
    setLeadUserSelectValue("leadEmailAssignedTo", l.email_assigned_to || "");
    setLeadVerifiedByValue(l.verified_by || "");
    setLeadFormValue("leadNewNote", "");
    renderLeadNotesHistory(l.notes);
    document.querySelectorAll(".work-modal.open").forEach(function(m){ m.classList.remove("open"); });
    document.getElementById("clientLeadModal").classList.add("open");
  } catch (e) {
    alert("Failed to load lead");
  }
}

async function saveClientLead(clientId) {
  const company = document.getElementById("leadCompany").value.trim();
  const phone = document.getElementById("leadPhone").value.trim();
  if (!company && !phone) {
    alert("Enter at least a company name or phone number.");
    return;
  }
  const leadId = document.getElementById("leadId").value;
  const newNote = document.getElementById("leadNewNote").value.trim();
  const payload = {
    business_name: company,
    company: company,
    contact_name: document.getElementById("leadContactName").value.trim(),
    phone: phone,
    email: document.getElementById("leadEmail").value.trim(),
    city: document.getElementById("leadCity").value.trim(),
    state: document.getElementById("leadState").value.trim(),
    country: document.getElementById("leadCountryField").value.trim(),
    website: document.getElementById("leadWebsite").value.trim(),
    person_linkedin_url: document.getElementById("leadPersonLinkedin").value.trim(),
    company_linkedin_url: document.getElementById("leadCompanyLinkedin").value.trim(),
    company_last_round_amount: document.getElementById("leadFundingAmount").value.trim(),
    company_last_funding_date: document.getElementById("leadFundingDate").value.trim(),
    company_funding_round: document.getElementById("leadFundingRound").value.trim(),
    lead_category: document.getElementById("leadCategory").value,
    category_type: document.getElementById("leadCategoryType").value.trim(),
    lead_source: document.getElementById("leadSource").value,
    pipeline_stage: document.getElementById("leadPipelineStage").value,
    outreach_status: document.getElementById("leadOutreachStatus").value,
    demo_status: document.getElementById("leadDemoStatus").value,
    status: document.getElementById("leadStatus").value,
    assigned_to: document.getElementById("leadAssignedTo").value.trim(),
    phone_assigned_to: document.getElementById("leadPhoneAssignedTo").value.trim(),
    email_assigned_to: document.getElementById("leadEmailAssignedTo").value.trim(),
    verified_by: document.getElementById("leadVerifiedBy").value.trim()
  };
  // The edit payload omits notes so the existing append-only history is never
  // overwritten. Any new note is appended afterwards via add_note (which records
  // author + timestamp), for both create and edit.
  showLoadingModal(leadId ? "Updating lead..." : "Creating lead...");
  const url = leadId
    ? "/api/clients/" + clientId + "/leads/" + leadId
    : "/api/clients/" + clientId + "/leads";
  const res = await fetch(url, {
    method: leadId ? "PATCH" : "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const json = await res.json();
  if (!json.ok) { alert((leadId ? "Update" : "Create") + " failed: " + (json.error || "Unknown error")); return; }

  const savedLeadId = leadId || (json.data && json.data.id);
  if (savedLeadId && newNote) {
    const noteRes = await fetch("/api/clients/" + clientId + "/leads/" + savedLeadId, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ add_note: newNote })
    });
    const noteJson = await noteRes.json();
    if (!noteJson.ok) { alert("Lead saved, but note failed: " + (noteJson.error || "Unknown error")); return; }
  }
  window.location.reload();
}

async function deleteClientLead(clientId, leadId) {
  const hasSwal = typeof Swal !== "undefined";
  if (hasSwal) {
    const result = await Swal.fire({
      title: "Delete this lead?",
      text: "Are you sure you want to delete that lead?",
      icon: "warning",
      showCancelButton: true,
      confirmButtonText: "Yes, delete it",
      cancelButtonText: "No",
      reverseButtons: true
    });
    if (!result.isConfirmed) return;
  } else if (!confirm("Are you sure you want to delete that lead?")) {
    return;
  }
  showLoadingModal("Deleting lead...");
  // Always dismiss the loading modal on any failure, otherwise the
  // "Please wait..." overlay stays open and the page looks stuck.
  const hideLoading = () => {
    const modal = document.getElementById("workItemDetailModal");
    if (modal) modal.classList.remove("open");
  };
  try {
    const res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
      method: "DELETE",
      headers: { "Content-Type": "application/json" }
    });
    const json = await res.json();
    if (!json.ok) {
      hideLoading();
      if (hasSwal) { Swal.fire("Error", json.error || "Failed to delete lead", "error"); }
      else { alert(json.error || "Failed to delete lead"); }
      return;
    }
    window.location.reload();
  } catch (err) {
    hideLoading();
    if (hasSwal) { Swal.fire("Error", "Failed to delete lead", "error"); }
    else { alert("Failed to delete lead"); }
  }
}

var __pendingLeadStage = null;

function updateLeadStage(clientId, leadId, stage, selectEl) {
  // Don't save the status change yet — require a note via the themed modal first.
  __pendingLeadStage = { clientId: clientId, leadId: leadId, stage: stage, selectEl: selectEl || null };
  const textarea = document.getElementById("leadStageNotesText");
  if (textarea) textarea.value = "";
  const audioInput = document.getElementById("leadStageNotesAudio");
  if (audioInput) audioInput.value = "";
  const callbackField = document.getElementById("leadStageCallbackField");
  const callbackDate = document.getElementById("leadStageCallbackDate");
  if (callbackDate) callbackDate.value = "";
  if (callbackField) callbackField.style.display = stage === "follow_up_required" ? "" : "none";
  document.getElementById("leadStageNotesModal").classList.add("open");
  if (textarea) textarea.focus();
}

function closeLeadStageNotesModal(event) {
  if (event && event.target && event.target.id !== "leadStageNotesModal") return;
  // Cancelled — revert the dropdown to its previous value (nothing was saved).
  if (__pendingLeadStage && __pendingLeadStage.selectEl &&
      typeof __pendingLeadStage.selectEl.dataset.prev !== "undefined") {
    __pendingLeadStage.selectEl.value = __pendingLeadStage.selectEl.dataset.prev;
  }
  __pendingLeadStage = null;
  document.getElementById("leadStageNotesModal").classList.remove("open");
}

async function confirmLeadStageNotes() {
  if (!__pendingLeadStage) return;
  const notes = document.getElementById("leadStageNotesText").value.trim();
  const audioInput = document.getElementById("leadStageNotesAudio");
  const audioFile = audioInput && audioInput.files && audioInput.files[0] ? audioInput.files[0] : null;
  if (!notes && !audioFile) { alert("Add a note or attach a voice note before saving the status change."); return; }
  const { clientId, leadId, stage } = __pendingLeadStage;
  const callbackDateEl = document.getElementById("leadStageCallbackDate");
  const callbackDate = stage === "follow_up_required" && callbackDateEl ? callbackDateEl.value : "";
  if (stage === "follow_up_required" && !callbackDate) {
    alert("Select a callback date for this follow-up.");
    return;
  }
  __pendingLeadStage = null;
  document.getElementById("leadStageNotesModal").classList.remove("open");
  try {
    if (audioFile) {
      showLoadingModal("Uploading & transcribing voice note...");
      const fd = new FormData();
      fd.append("audio", audioFile);
      fd.append("pipeline_stage", stage);
      if (stage === "follow_up_required") fd.append("callback_date", callbackDate);
      if (notes) fd.append("text", notes);
      const res = await fetch("/api/clients/" + clientId + "/leads/" + leadId + "/note-audio", { method: "POST", body: fd });
      const json = await res.json();
      if (!json.ok) { alert(json.error || "Failed to update stage"); return; }
    } else {
      showLoadingModal("Updating pipeline stage...");
      const body = { pipeline_stage: stage, add_note: notes };
      if (stage === "follow_up_required") body.callback_date = callbackDate;
      const res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      const json = await res.json();
      if (!json.ok) { alert(json.error || "Failed to update stage"); return; }
    }
    window.location.reload();
  } catch (e) {
    alert("Failed to update stage");
  }
}

// Quick-update modal launched from the phone icon next to a lead's company name.
// Lets you change status, demo and reached-via channels together with a required
// note (typed or voice) in a single save.
var __pendingLeadQuick = null;
var __leadQuickRecorder = null;
var __leadQuickChunks = [];
var __leadQuickBlob = null;
var __leadQuickRecInterval = null;
var __leadQuickRecSeconds = 0;

function fmtLeadQuickRecTime(s) {
  var m = Math.floor(s / 60);
  var sec = s % 60;
  return m + ":" + (sec < 10 ? "0" : "") + sec;
}

function resetLeadQuickRecordingUI() {
  var btn = document.getElementById("leadQuickRecBtn");
  if (btn) { btn.textContent = "● Record"; btn.classList.remove("btn-primary"); }
  var status = document.getElementById("leadQuickRecStatus");
  if (status) status.textContent = "";
  var discard = document.getElementById("leadQuickRecDiscard");
  if (discard) discard.style.display = "none";
  var preview = document.getElementById("leadQuickRecPreview");
  if (preview) {
    if (preview.src) { try { URL.revokeObjectURL(preview.src); } catch (e) {} }
    preview.removeAttribute("src");
    preview.style.display = "none";
  }
}

// Fully tears down any in-progress or finished recording (stops the mic,
// clears the timer and the buffered blob) and resets the UI.
function clearLeadQuickRecording() {
  if (__leadQuickRecInterval) { clearInterval(__leadQuickRecInterval); __leadQuickRecInterval = null; }
  try {
    if (__leadQuickRecorder && __leadQuickRecorder.state !== "inactive") {
      __leadQuickRecorder.stop();
    }
  } catch (e) {}
  try {
    if (__leadQuickRecorder && __leadQuickRecorder.stream) {
      __leadQuickRecorder.stream.getTracks().forEach(function (t) { t.stop(); });
    }
  } catch (e) {}
  __leadQuickRecorder = null;
  __leadQuickChunks = [];
  __leadQuickBlob = null;
  __leadQuickRecSeconds = 0;
  resetLeadQuickRecordingUI();
}

async function toggleLeadQuickRecording() {
  var btn = document.getElementById("leadQuickRecBtn");
  var status = document.getElementById("leadQuickRecStatus");
  // Currently recording -> stop and keep the result.
  if (__leadQuickRecorder && __leadQuickRecorder.state === "recording") {
    __leadQuickRecorder.stop();
    return;
  }
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia ||
      typeof MediaRecorder === "undefined") {
    alert("Recording is not supported in this browser. Use the file upload instead.");
    return;
  }
  try {
    var stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    __leadQuickChunks = [];
    __leadQuickBlob = null;
    var rec = new MediaRecorder(stream);
    __leadQuickRecorder = rec;
    rec.ondataavailable = function (e) {
      if (e.data && e.data.size > 0) __leadQuickChunks.push(e.data);
    };
    rec.onstop = function () {
      if (__leadQuickRecInterval) { clearInterval(__leadQuickRecInterval); __leadQuickRecInterval = null; }
      stream.getTracks().forEach(function (t) { t.stop(); });
      var type = (__leadQuickChunks[0] && __leadQuickChunks[0].type) || rec.mimeType || "audio/webm";
      __leadQuickBlob = new Blob(__leadQuickChunks, { type: type });
      if (btn) { btn.textContent = "● Record again"; btn.classList.remove("btn-primary"); }
      if (status) status.textContent = "Recorded (" + fmtLeadQuickRecTime(__leadQuickRecSeconds) + ")";
      var discard = document.getElementById("leadQuickRecDiscard");
      if (discard) discard.style.display = "";
      var preview = document.getElementById("leadQuickRecPreview");
      if (preview) { preview.src = URL.createObjectURL(__leadQuickBlob); preview.style.display = ""; }
    };
    rec.start();
    __leadQuickRecSeconds = 0;
    if (status) status.textContent = "Recording… 0:00";
    __leadQuickRecInterval = setInterval(function () {
      __leadQuickRecSeconds++;
      if (status) status.textContent = "Recording… " + fmtLeadQuickRecTime(__leadQuickRecSeconds);
    }, 1000);
    if (btn) { btn.textContent = "■ Stop"; btn.classList.add("btn-primary"); }
    var discard = document.getElementById("leadQuickRecDiscard");
    if (discard) discard.style.display = "none";
    var preview = document.getElementById("leadQuickRecPreview");
    if (preview) { preview.removeAttribute("src"); preview.style.display = "none"; }
  } catch (e) {
    alert("Could not access the microphone. Check permissions or use the file upload instead.");
  }
}

function discardLeadQuickRecording() {
  clearLeadQuickRecording();
}

function leadQuickRecordingFileName() {
  var type = (__leadQuickBlob && __leadQuickBlob.type) || "";
  var ext = "webm";
  if (type.indexOf("ogg") !== -1) ext = "ogg";
  else if (type.indexOf("mp4") !== -1 || type.indexOf("m4a") !== -1) ext = "m4a";
  else if (type.indexOf("mpeg") !== -1 || type.indexOf("mp3") !== -1) ext = "mp3";
  else if (type.indexOf("wav") !== -1) ext = "wav";
  return "voice-note." + ext;
}

function onLeadQuickStatusChange() {
  var statusSel = document.getElementById("leadQuickStatus");
  var callbackField = document.getElementById("leadQuickCallbackField");
  if (!statusSel || !callbackField) return;
  callbackField.style.display = statusSel.value === "follow_up_required" ? "" : "none";
}

function openLeadQuickUpdate(iconEl) {
  if (!iconEl || iconEl.getAttribute("aria-disabled") === "true") return;
  clearLeadQuickRecording();
  var clientId = Number(iconEl.getAttribute("data-client"));
  var leadId = Number(iconEl.getAttribute("data-lead"));
  __pendingLeadQuick = { clientId: clientId, leadId: leadId, iconEl: iconEl };
  // Mark the call as made the moment the popup opens — this sticks even if the
  // user Cancels. Optimistically lock the icon (red + disabled) so it can't be
  // clicked again, then persist server-side; revert only if that fails.
  iconEl.setAttribute("aria-disabled", "true");
  iconEl.style.cursor = "not-allowed";
  iconEl.onclick = null;
  iconEl.style.color = "#ef4444";
  // Kept so Save can await this before flipping is_call_made back to false —
  // otherwise a slow log-call could land after Save and re-lock the lead.
  __pendingLeadQuick.logCallPromise = fetch(
    "/api/clients/" + clientId + "/leads/" + leadId + "/log-call",
    { method: "POST" },
  )
    .then(function (r) { return r.json(); })
    .then(function (j) {
      if (!j || !j.ok) throw new Error((j && j.error) || "log-call failed");
      var by = j.data && j.data.call_made_by ? j.data.call_made_by : "";
      iconEl.setAttribute("title", "Call made" + (by ? " by " + by : ""));
      iconEl.setAttribute("aria-label", "Call already made");
    })
    .catch(function () {
      // Persisting the call failed — unlock the icon so it can be retried.
      iconEl.removeAttribute("aria-disabled");
      iconEl.style.cursor = "pointer";
      iconEl.style.color = "var(--muted)";
      iconEl.onclick = function () { openLeadQuickUpdate(iconEl); };
    });
  var company = iconEl.getAttribute("data-company") || "";
  var title = document.getElementById("leadQuickTitle");
  if (title) title.textContent = company ? "Update — " + company : "Update Lead";
  var statusSel = document.getElementById("leadQuickStatus");
  if (statusSel) statusSel.value = iconEl.getAttribute("data-stage") || "";
  var callbackDateEl = document.getElementById("leadQuickCallbackDate");
  if (callbackDateEl) callbackDateEl.value = iconEl.getAttribute("data-callback") || "";
  onLeadQuickStatusChange();
  var demoSel = document.getElementById("leadQuickDemo");
  if (demoSel) demoSel.value = iconEl.getAttribute("data-demo") || "";
  var reachSet = {};
  String(iconEl.getAttribute("data-reach") || "")
    .split(",")
    .filter(Boolean)
    .forEach(function (k) { reachSet[k] = true; });
  Array.prototype.forEach.call(
    document.querySelectorAll(".lqu-reach"),
    function (cb) { cb.checked = !!reachSet[cb.value]; },
  );
  var textarea = document.getElementById("leadQuickNote");
  if (textarea) textarea.value = "";
  var audioInput = document.getElementById("leadQuickAudio");
  if (audioInput) audioInput.value = "";
  document.getElementById("leadQuickUpdateModal").classList.add("open");
  if (textarea) textarea.focus();
}

function closeLeadQuickUpdate(event) {
  if (event && event.target && event.target.id !== "leadQuickUpdateModal") return;
  var pending = __pendingLeadQuick;
  __pendingLeadQuick = null;
  clearLeadQuickRecording();
  document.getElementById("leadQuickUpdateModal").classList.remove("open");
  if (!pending) return;
  // Cancelling also clears the "call made" lock (same as Save). Revert the icon
  // to clickable in place, then persist is_call_made=false after the open-time
  // write so false is the last write.
  var iconEl = pending.iconEl;
  var company = iconEl ? iconEl.getAttribute("data-company") || "" : "";
  if (iconEl) {
    iconEl.removeAttribute("aria-disabled");
    iconEl.style.cursor = "pointer";
    iconEl.style.color = "var(--muted)";
    iconEl.setAttribute(
      "title",
      "Log a call — status, demo, reached via & note" +
        (company ? " for " + company : ""),
    );
    iconEl.setAttribute("aria-label", "Log call to " + company);
    iconEl.onclick = function () { openLeadQuickUpdate(iconEl); };
  }
  (async function () {
    try {
      if (pending.logCallPromise) { try { await pending.logCallPromise; } catch (e) {} }
      await fetch("/api/clients/" + pending.clientId + "/leads/" + pending.leadId, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_call_made: false }),
      });
    } catch (e) {}
  })();
}

async function confirmLeadQuickUpdate() {
  if (!__pendingLeadQuick) return;
  var notes = document.getElementById("leadQuickNote").value.trim();
  var audioInput = document.getElementById("leadQuickAudio");
  var audioFile =
    audioInput && audioInput.files && audioInput.files[0]
      ? audioInput.files[0]
      : null;
  // A live recording (if any) takes precedence over a chosen file.
  var recordedBlob = __leadQuickBlob;
  var hasAudio = !!(recordedBlob || audioFile);
  if (!notes && !hasAudio) {
    alert("Add a note, attach a voice note, or record one before saving.");
    return;
  }
  var stage = document.getElementById("leadQuickStatus").value;
  var demo = document.getElementById("leadQuickDemo").value;
  var callbackDateEl = document.getElementById("leadQuickCallbackDate");
  var callbackDate = stage === "follow_up_required" && callbackDateEl ? callbackDateEl.value : "";
  if (stage === "follow_up_required" && !callbackDate) {
    alert("Select a callback date for this follow-up.");
    return;
  }
  var reach = {};
  Array.prototype.forEach.call(
    document.querySelectorAll(".lqu-reach"),
    function (cb) { reach["reached_via_" + cb.value] = cb.checked; },
  );
  var clientId = __pendingLeadQuick.clientId;
  var leadId = __pendingLeadQuick.leadId;
  var logCallPromise = __pendingLeadQuick.logCallPromise || null;
  var recordedName = leadQuickRecordingFileName();
  __pendingLeadQuick = null;
  // Stop the mic stream but keep the blob (captured above) for the upload.
  if (__leadQuickRecInterval) { clearInterval(__leadQuickRecInterval); __leadQuickRecInterval = null; }
  try {
    if (__leadQuickRecorder && __leadQuickRecorder.stream) {
      __leadQuickRecorder.stream.getTracks().forEach(function (t) { t.stop(); });
    }
  } catch (e) {}
  __leadQuickRecorder = null;
  document.getElementById("leadQuickUpdateModal").classList.remove("open");
  try {
    // Let the open-time "call made" write finish first, so our is_call_made
    // false below is the last write and wins.
    if (logCallPromise) { try { await logCallPromise; } catch (e3) {} }
    if (hasAudio) {
      showLoadingModal("Uploading & transcribing voice note...");
      var fd = new FormData();
      if (recordedBlob) fd.append("audio", recordedBlob, recordedName);
      else fd.append("audio", audioFile);
      fd.append("pipeline_stage", stage);
      fd.append("demo_status", demo);
      if (stage === "follow_up_required") fd.append("callback_date", callbackDate);
      fd.append("is_call_made", "false");
      Object.keys(reach).forEach(function (k) {
        fd.append(k, reach[k] ? "true" : "false");
      });
      if (notes) fd.append("text", notes);
      var resA = await fetch(
        "/api/clients/" + clientId + "/leads/" + leadId + "/note-audio",
        { method: "POST", body: fd },
      );
      var jsonA = await resA.json();
      if (!jsonA.ok) { alert(jsonA.error || "Failed to update lead"); return; }
    } else {
      showLoadingModal("Updating lead...");
      var body = {
        pipeline_stage: stage,
        demo_status: demo,
        add_note: notes,
        is_call_made: false,
      };
      if (stage === "follow_up_required") body.callback_date = callbackDate;
      Object.keys(reach).forEach(function (k) { body[k] = reach[k]; });
      var resB = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      var jsonB = await resB.json();
      if (!jsonB.ok) { alert(jsonB.error || "Failed to update lead"); return; }
    }
    // Note: the call was already marked as made when the popup opened, so Save
    // only needs to persist the status/demo/reached-via/note details.
    window.location.reload();
  } catch (e) {
    alert("Failed to update lead");
  }
}

async function updateLinkedTask(clientId, taskId, field, value) {
  showLoadingModal("Updating task...");
  const res = await fetch("/api/clients/" + clientId + "/linked-tasks/" + taskId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ [field]: value })
  });
  const json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to update task"); window.location.reload(); return; }
  window.location.reload();
}

async function updateLeadOutreach(clientId, leadId, outreachStatus) {
  showLoadingModal("Updating outreach status...");
  const res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ outreach_status: outreachStatus })
  });
  const json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to update outreach status"); return; }
  window.location.reload();
}

// Add-note modal launched from the Leads table (Notes column).
var __pendingLeadNote = null;

function openLeadNoteModal(clientId, leadId) {
  __pendingLeadNote = { clientId: clientId, leadId: leadId };
  const textarea = document.getElementById("leadNoteText");
  if (textarea) textarea.value = "";
  const audioInput = document.getElementById("leadNoteAudio");
  if (audioInput) audioInput.value = "";
  document.getElementById("leadNoteModal").classList.add("open");
  if (textarea) textarea.focus();
}

function closeLeadNoteModal(event) {
  if (event && event.target && event.target.id !== "leadNoteModal") return;
  __pendingLeadNote = null;
  document.getElementById("leadNoteModal").classList.remove("open");
}

async function confirmLeadNote() {
  if (!__pendingLeadNote) return;
  const text = document.getElementById("leadNoteText").value.trim();
  const audioInput = document.getElementById("leadNoteAudio");
  const audioFile = audioInput && audioInput.files && audioInput.files[0] ? audioInput.files[0] : null;
  if (!text && !audioFile) { alert("Write a note or attach a voice note first."); return; }
  // A typed note must be detailed (>= 75 chars). Skipped when a voice note is
  // attached, since its transcription supplies the note content.
  if (!audioFile && text.length < 75) {
    alert("Please write a more detailed note — at least 75 characters (currently " + text.length + ").");
    return;
  }
  const { clientId, leadId } = __pendingLeadNote;
  __pendingLeadNote = null;
  document.getElementById("leadNoteModal").classList.remove("open");
  try {
    if (audioFile) {
      showLoadingModal("Uploading & transcribing voice note...");
      const fd = new FormData();
      fd.append("audio", audioFile);
      if (text) fd.append("text", text);
      const res = await fetch("/api/clients/" + clientId + "/leads/" + leadId + "/note-audio", {
        method: "POST",
        body: fd
      });
      const json = await res.json();
      if (!json.ok) { alert(json.error || "Failed to save voice note"); return; }
    } else {
      showLoadingModal("Saving note...");
      const res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ add_note: text })
      });
      const json = await res.json();
      if (!json.ok) { alert(json.error || "Failed to save note"); return; }
    }
    window.location.reload();
  } catch (e) {
    alert("Failed to save note");
  }
}

var __pendingLeadDemo = null;

function updateLeadDemo(clientId, leadId, demoStatus, selectEl) {
  // Stash the pending change and ask for a note via the themed modal.
  __pendingLeadDemo = { clientId: clientId, leadId: leadId, demoStatus: demoStatus, selectEl: selectEl || null };
  const textarea = document.getElementById("leadDemoNotesText");
  if (textarea) textarea.value = "";
  const audioInput = document.getElementById("leadDemoNotesAudio");
  if (audioInput) audioInput.value = "";
  document.getElementById("leadDemoNotesModal").classList.add("open");
  if (textarea) textarea.focus();
}

function closeLeadDemoNotesModal(event) {
  if (event && event.target && event.target.id !== "leadDemoNotesModal") return;
  // Cancelled — revert the dropdown to its previous value.
  if (__pendingLeadDemo && __pendingLeadDemo.selectEl &&
      typeof __pendingLeadDemo.selectEl.dataset.prev !== "undefined") {
    __pendingLeadDemo.selectEl.value = __pendingLeadDemo.selectEl.dataset.prev;
  }
  __pendingLeadDemo = null;
  document.getElementById("leadDemoNotesModal").classList.remove("open");
}

async function confirmLeadDemoNotes() {
  if (!__pendingLeadDemo) return;
  const notes = document.getElementById("leadDemoNotesText").value.trim();
  const audioInput = document.getElementById("leadDemoNotesAudio");
  const audioFile = audioInput && audioInput.files && audioInput.files[0] ? audioInput.files[0] : null;
  if (!notes && !audioFile) { alert("Add a note or attach a voice note before saving the demo status change."); return; }
  const { clientId, leadId, demoStatus } = __pendingLeadDemo;
  __pendingLeadDemo = null;
  document.getElementById("leadDemoNotesModal").classList.remove("open");
  try {
    if (audioFile) {
      showLoadingModal("Uploading & transcribing voice note...");
      const fd = new FormData();
      fd.append("audio", audioFile);
      fd.append("demo_status", demoStatus);
      if (notes) fd.append("text", notes);
      const res = await fetch("/api/clients/" + clientId + "/leads/" + leadId + "/note-audio", { method: "POST", body: fd });
      const json = await res.json();
      if (!json.ok) { alert(json.error || "Failed to update demo status"); return; }
    } else {
      showLoadingModal("Updating demo status...");
      // Send the note as an append (add_note) so the demo change is recorded in
      // the notes history with author + timestamp.
      const res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ demo_status: demoStatus, add_note: notes })
      });
      const json = await res.json();
      if (!json.ok) { alert(json.error || "Failed to update demo status"); return; }
    }
    window.location.reload();
  } catch (e) {
    alert("Failed to update demo status");
  }
}

function openLeadImportModal() {
  const input = document.getElementById("clientLeadsExcelFile");
  if (input) input.value = "";
  const cat = document.getElementById("leadImportCategoryType");
  if (cat) cat.value = "";
  document.getElementById("leadImportModal").classList.add("open");
}

function closeLeadImportModal(event) {
  if (event && event.target && event.target.id !== "leadImportModal") return;
  document.getElementById("leadImportModal").classList.remove("open");
}

async function uploadClientLeadsExcel(clientId) {
  const input = document.getElementById("clientLeadsExcelFile");
  if (!input || !input.files || !input.files[0]) { alert("Choose an Excel file first."); return; }
  const categoryType = (document.getElementById("leadImportCategoryType") || {}).value || "";
  if (!categoryType) { alert("Select a category type first."); return; }
  const formData = new FormData();
  formData.append("file", input.files[0]);
  // Every row in the sheet is stamped with this category type.
  formData.append("category_type", categoryType);
  closeLeadImportModal();
  showLoadingModal("Importing leads from Excel...");
  const res = await fetch("/api/clients/" + clientId + "/leads/import-excel", {
    method: "POST",
    body: formData
  });
  const json = await res.json();
  // A rejected sheet (e.g. an unknown "Assigned to" name) comes back with a
  // multi-line, row-by-row message — show it as-is and drop the spinner so the
  // sheet can be fixed and re-uploaded.
  if (!json.ok) {
    hideLoadingModal();
    alert("Import failed: " + String.fromCharCode(10) + (json.error || "Unknown error"));
    input.value = "";
    return;
  }
  const d = json.data || {};
  alert([
    "Import complete",
    "Total rows: " + d.total,
    "Inserted: " + d.inserted,
    "Updated (existing email): " + (d.updated || 0),
    "Duplicates skipped: " + d.duplicates,
    "Empty skipped: " + d.skipped,
    "Errors: " + ((d.errors || []).length)
  ].join(String.fromCharCode(10)));
  window.location.reload();
}

// ---------------------------------------------------------------------------
// Client Blockers
// ---------------------------------------------------------------------------
window.__clientBlockers = ${blockersJson};

function openBlockerModal() {
  document.getElementById("blockerModalTitle").textContent = "Add Blocker";
  document.getElementById("blockerId").value = "";
  document.getElementById("blockerTitle").value = "";
  document.getElementById("blockerDescription").value = "";
  document.getElementById("blockerSide").value = "internal";
  document.getElementById("blockerPriority").value = "medium";
  document.getElementById("blockerOwner").value = "";
  document.getElementById("blockerWorkItem").value = "";
  document.getElementById("blockerStatusField").style.display = "none";
  document.getElementById("blockerModal").classList.add("open");
}

function openBlockerDetail(blockerId) {
  const b = (window.__clientBlockers || []).find(function(x){ return String(x.id) === String(blockerId); });
  if (!b) { alert("Blocker not found"); return; }
  document.getElementById("blockerModalTitle").textContent = "Edit Blocker";
  document.getElementById("blockerId").value = b.id;
  document.getElementById("blockerTitle").value = b.title || "";
  document.getElementById("blockerDescription").value = b.description || "";
  document.getElementById("blockerSide").value = b.blocker_side || "internal";
  document.getElementById("blockerPriority").value = b.priority || "medium";
  document.getElementById("blockerOwner").value = b.owner_user_id || "";
  document.getElementById("blockerWorkItem").value = b.related_work_item_id || "";
  document.getElementById("blockerStatus").value = b.resolution_status || "open";
  document.getElementById("blockerStatusField").style.display = "";
  document.getElementById("blockerModal").classList.add("open");
}

function closeBlockerModal(event) {
  if (event && event.target && event.target.id !== "blockerModal") return;
  document.getElementById("blockerModal").classList.remove("open");
}

async function saveBlocker(clientId) {
  const title = document.getElementById("blockerTitle").value.trim();
  if (!title) { alert("Title is required"); return; }
  const blockerId = document.getElementById("blockerId").value;
  const payload = {
    title: title,
    description: document.getElementById("blockerDescription").value.trim(),
    blocker_side: document.getElementById("blockerSide").value,
    priority: document.getElementById("blockerPriority").value,
    owner_user_id: document.getElementById("blockerOwner").value || null,
    related_work_item_id: document.getElementById("blockerWorkItem").value || null
  };
  if (blockerId) {
    payload.resolution_status = document.getElementById("blockerStatus").value;
  }

  showLoadingModal(blockerId ? "Updating blocker..." : "Creating blocker...");
  const url = blockerId
    ? "/api/clients/" + clientId + "/blockers/" + blockerId
    : "/api/clients/" + clientId + "/blockers";
  const res = await fetch(url, {
    method: blockerId ? "PATCH" : "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const json = await res.json();
  if (!json.ok) { alert((blockerId ? "Update" : "Create") + " failed: " + (json.error || "Unknown error")); return; }
  window.location.reload();
}

async function updateBlocker(clientId, blockerId, patch) {
  showLoadingModal("Updating blocker...");
  const res = await fetch("/api/clients/" + clientId + "/blockers/" + blockerId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch)
  });
  const json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to update blocker"); return; }
  window.location.reload();
}

async function archiveBlocker(clientId, blockerId) {
  if (!confirm("Archive this blocker? It will be hidden but not permanently deleted.")) return;
  showLoadingModal("Archiving blocker...");
  const res = await fetch("/api/clients/" + clientId + "/blockers/" + blockerId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ archive: true })
  });
  const json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to archive blocker"); return; }
  window.location.reload();
}

// ---------------------------------------------------------------------------
// Client Meetings & MOMs
// ---------------------------------------------------------------------------
window.__clientMeetings = ${meetingsJson};

var MEETING_FIELDS = [
  ["meetingTitle","title"], ["meetingDate","meeting_date"], ["meetingType","meeting_type"],
  ["meetingDuration","duration_min"],
  ["meetingParticipants","participants"], ["meetingSummary","summary"],
  ["meetingDiscussionPoints","discussion_points"], ["meetingDecisions","decisions"],
  ["meetingDeliverables","deliverables"], ["meetingActionItems","action_items"],
  ["meetingFollowUps","follow_ups"], ["meetingNextSteps","next_steps"]
];

function setMeetingEditOnlyVisible(visible) {
  var nodes = document.querySelectorAll(".meeting-edit-only");
  for (var i = 0; i < nodes.length; i++) {
    nodes[i].style.display = visible ? "" : "none";
  }
}

function openMeetingModal() {
  document.getElementById("meetingModalTitle").textContent = "Log Meeting";
  document.getElementById("meetingId").value = "";
  MEETING_FIELDS.forEach(function(f){ var el = document.getElementById(f[0]); if (el) el.value = ""; });
  var aiNotes = document.getElementById("meetingAiNotes");
  if (aiNotes) aiNotes.value = "";
  document.getElementById("meetingType").value = "sync_call";
  setMeetingEditOnlyVisible(false);
  document.getElementById("meetingModal").classList.add("open");
}

function openMeetingDetail(meetingId) {
  var m = (window.__clientMeetings || []).find(function(x){ return String(x.id) === String(meetingId); });
  if (!m) { alert("Meeting not found"); return; }
  document.getElementById("meetingModalTitle").textContent = "Edit Meeting";
  document.getElementById("meetingId").value = m.id;
  MEETING_FIELDS.forEach(function(f){ var el = document.getElementById(f[0]); if (el) el.value = m[f[1]] || ""; });
  document.getElementById("meetingType").value = m.meeting_type || "sync_call";
  setMeetingEditOnlyVisible(true);
  document.getElementById("meetingModal").classList.add("open");
}

function closeMeetingModal(event) {
  if (event && event.target && event.target.id !== "meetingModal") return;
  document.getElementById("meetingModal").classList.remove("open");
}

async function aiFillMeetingFromNotes() {
  var notesEl = document.getElementById("meetingAiNotes");
  var notes = notesEl ? notesEl.value.trim() : "";
  if (!notes) { alert("Write or paste the meeting details first."); return; }

  var btn = document.getElementById("meetingAiFillBtn");
  if (btn) { btn.disabled = true; btn.textContent = "Filling..."; }

  try {
    var res = await fetch("/api/ai/parse-meeting-notes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ notes: notes })
    });
    var json = await res.json();
    if (!json.ok) { alert(json.error || "AI auto-fill failed"); return; }

    var m = json.data || {};
    MEETING_FIELDS.forEach(function(f){
      var el = document.getElementById(f[0]);
      if (el && m[f[1]]) el.value = m[f[1]];
    });
    if (!document.getElementById("meetingDate").value) {
      document.getElementById("meetingDate").value = new Date().toISOString().slice(0, 10);
    }
  } catch (e) {
    alert("AI auto-fill failed: " + (e && e.message ? e.message : "network error"));
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "✨ Auto-fill with AI"; }
  }
}

async function saveMeeting(clientId) {
  var payload = {};
  MEETING_FIELDS.forEach(function(f){ var el = document.getElementById(f[0]); payload[f[1]] = el ? el.value.trim() : ""; });
  if (!payload.title && !payload.meeting_date) { alert("Meeting title or date is required"); return; }
  var meetingId = document.getElementById("meetingId").value;

  showLoadingModal(meetingId ? "Updating meeting..." : "Saving meeting...");
  var url = meetingId
    ? "/api/clients/" + clientId + "/meetings/" + meetingId
    : "/api/clients/" + clientId + "/meetings";
  var res = await fetch(url, {
    method: meetingId ? "PATCH" : "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  var json = await res.json();
  if (!json.ok) { alert((meetingId ? "Update" : "Save") + " failed: " + (json.error || "Unknown error")); return; }
  window.location.reload();
}

async function archiveMeeting(clientId, meetingId) {
  if (!confirm("Archive this meeting? It will be hidden but not permanently deleted.")) return;
  showLoadingModal("Archiving meeting...");
  var res = await fetch("/api/clients/" + clientId + "/meetings/" + meetingId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ archive: true })
  });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to archive meeting"); return; }
  window.location.reload();
}

// ---------------------------------------------------------------------------
// Lead client-visibility toggle
// ---------------------------------------------------------------------------
async function toggleLeadVisible(clientId, leadId, isVisible) {
  showLoadingModal("Updating visibility...");
  var res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ is_client_visible: isVisible })
  });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to update visibility"); return; }
  window.location.reload();
}

async function toggleLeadStar(clientId, leadId, starred) {
  showLoadingModal(starred ? "Starring call..." : "Removing star...");
  var res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ is_starred: starred })
  });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to update star"); return; }
  window.location.reload();
}

async function updateLeadNotes(clientId, leadId, notes) {
  showLoadingModal("Saving notes...");
  var res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ notes: notes })
  });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to save notes"); return; }
  window.location.reload();
}

function toggleReachDropdown(btn) {
  var panel = btn.parentNode.querySelector(".reach-ms-panel");
  var isOpen = panel.style.display === "block";
  // Close any other open reach dropdowns first.
  document.querySelectorAll(".reach-ms-panel").forEach(function(p){ p.style.display = "none"; });
  panel.style.display = isOpen ? "none" : "block";
}

// Close reach dropdowns when clicking outside of one.
document.addEventListener("click", function(e){
  if (!e.target.closest || !e.target.closest(".reach-ms")) {
    document.querySelectorAll(".reach-ms-panel").forEach(function(p){ p.style.display = "none"; });
  }
});

async function updateLeadReached(clientId, leadId, checkboxEl) {
  var panel = checkboxEl.closest(".reach-ms-panel");
  var body = {};
  Array.prototype.forEach.call(
    panel.querySelectorAll("input[type=checkbox]"),
    function(c){ body["reached_via_" + c.value] = c.checked; }
  );
  showLoadingModal("Updating reach channels...");
  var res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to update reach channels"); return; }
  window.location.reload();
}

async function toggleLeadReached(clientId, leadId, channel, reached) {
  var field = "reached_via_" + channel;
  var body = {};
  body[field] = reached;
  showLoadingModal("Updating reach channel...");
  var res = await fetch("/api/clients/" + clientId + "/leads/" + leadId, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to update reach channel"); return; }
  window.location.reload();
}

// ---------------------------------------------------------------------------
// Bulk lead actions (multi-select). Selection covers the visible page (<=25
// rows), or every matching lead across all pages via "Select all N leads";
// each action fans the existing single-lead endpoint out across the selection,
// then reloads once so server-side validation, notes and activity/funnel
// logging all stay identical to the single-lead path.
// ---------------------------------------------------------------------------
// Every lead id matching the current tab/search/filters (all pages), rendered
// by the server. "Select all N leads" flips leadAllPagesSelected on so bulk
// actions target this list instead of just the visible page's checkboxes; any
// manual checkbox change drops back to page-only selection.
var leadAllFilteredIds = ${JSON.stringify((leadFilteredIds || []).map(Number))};
var leadAllPagesSelected = false;

function getSelectedLeadIds() {
  if (leadAllPagesSelected && leadAllFilteredIds.length) return leadAllFilteredIds.slice();
  return Array.prototype.map.call(
    document.querySelectorAll(".lead-select:checked"),
    function(c){ return Number(c.value); }
  ).filter(Boolean);
}

function updateLeadBulkBarUI() {
  var all = document.querySelectorAll(".lead-select");
  var pageChecked = document.querySelectorAll(".lead-select:checked").length;
  var ids = getSelectedLeadIds();
  var bar = document.getElementById("leadBulkBar");
  var count = document.getElementById("leadBulkCount");
  if (count) count.textContent = leadAllPagesSelected
    ? "All " + ids.length + " selected (every page)"
    : ids.length + " selected";
  if (bar) bar.style.display = ids.length ? "flex" : "none";
  var selectAll = document.getElementById("leadSelectAll");
  if (selectAll) {
    selectAll.checked = all.length > 0 && pageChecked === all.length;
    selectAll.indeterminate = pageChecked > 0 && pageChecked < all.length;
  }
  // Offer "Select all N leads" only once the whole visible page is selected
  // and there are more matching leads beyond it.
  var allBtn = document.getElementById("leadSelectAllMatchingBtn");
  if (allBtn) {
    allBtn.style.display =
      !leadAllPagesSelected &&
      all.length > 0 &&
      pageChecked === all.length &&
      leadAllFilteredIds.length > all.length
        ? "" : "none";
  }
}

function onLeadSelectChange() {
  leadAllPagesSelected = false;
  updateLeadBulkBarUI();
}

function toggleSelectAllLeads(el) {
  var checked = !!(el && el.checked);
  leadAllPagesSelected = false;
  document.querySelectorAll(".lead-select").forEach(function(c){ c.checked = checked; });
  updateLeadBulkBarUI();
}

function selectAllMatchingLeads() {
  leadAllPagesSelected = true;
  document.querySelectorAll(".lead-select").forEach(function(c){ c.checked = true; });
  updateLeadBulkBarUI();
}

function clearLeadSelection() {
  leadAllPagesSelected = false;
  document.querySelectorAll(".lead-select").forEach(function(c){ c.checked = false; });
  ["leadBulkStage", "leadBulkDemo", "leadBulkReached", "leadBulkAssign", "leadBulkCategoryType"].forEach(function(id){
    var sel = document.getElementById(id);
    if (sel) sel.value = "";
  });
  updateLeadBulkBarUI();
}

function hideLoadingModal() {
  var modal = document.getElementById("workItemDetailModal");
  if (modal) modal.classList.remove("open");
}

// Optional note shared by bulk status/demo changes; also acts as the confirm
// step before mutating many leads at once. Returns the note (possibly ""),
// or null if the user cancelled.
async function promptBulkNote(count, label) {
  if (typeof Swal === "undefined") {
    var n = prompt("Note for this " + label + " change, applied to all " + count + " lead(s). Leave blank to skip:");
    return n === null ? null : String(n).trim();
  }
  var res = await Swal.fire({
    title: "Update " + label + " for " + count + " lead(s)",
    input: "textarea",
    inputLabel: "Note (optional) — added to every selected lead",
    inputPlaceholder: "Why is this changing?",
    showCancelButton: true,
    confirmButtonText: "Apply to " + count + " lead(s)",
    cancelButtonText: "Cancel",
    reverseButtons: true
  });
  if (!res.isConfirmed) return null;
  return String(res.value || "").trim();
}

// Fan a request out across every selected lead, then reload once. Requests go
// out in small batches so an all-pages selection (thousands of leads) doesn't
// flood the server; the loading modal shows progress between batches.
async function runBulkLeadAction(clientId, ids, makeRequest, loadingMsg) {
  var BATCH = 15;
  var failed = 0;
  var done = 0;
  showLoadingModal(loadingMsg);
  for (var i = 0; i < ids.length; i += BATCH) {
    var chunk = ids.slice(i, i + BATCH);
    var results = await Promise.all(chunk.map(function(id){
      return makeRequest(id)
        .then(function(r){ return r.json(); })
        .then(function(j){ return !!(j && j.ok); })
        .catch(function(){ return false; });
    }));
    failed += results.filter(function(ok){ return !ok; }).length;
    done += chunk.length;
    if (ids.length > BATCH) {
      showLoadingModal(loadingMsg + " (" + done + "/" + ids.length + " done)");
    }
  }
  hideLoadingModal();
  if (failed) {
    var msg = failed + " of " + ids.length + " lead(s) could not be updated.";
    if (typeof Swal !== "undefined") { await Swal.fire("Partial failure", msg, "warning"); }
    else { alert(msg); }
  }
  window.location.reload();
}

async function bulkSetStage(clientId) {
  var sel = document.getElementById("leadBulkStage");
  var stage = sel ? sel.value : "";
  if (!stage) return;
  var ids = getSelectedLeadIds();
  if (!ids.length) { if (sel) sel.value = ""; return; }
  var note = await promptBulkNote(ids.length, "status");
  if (sel) sel.value = "";
  if (note === null) return; // cancelled
  var body = { pipeline_stage: stage };
  if (note) body.add_note = note;
  await runBulkLeadAction(clientId, ids, function(id){
    return fetch("/api/clients/" + clientId + "/leads/" + id, {
      method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
    });
  }, "Updating status for " + ids.length + " lead(s)...");
}

async function bulkSetDemo(clientId) {
  var sel = document.getElementById("leadBulkDemo");
  var demo = sel ? sel.value : "";
  if (!demo) return;
  var ids = getSelectedLeadIds();
  if (!ids.length) { if (sel) sel.value = ""; return; }
  var note = await promptBulkNote(ids.length, "demo");
  if (sel) sel.value = "";
  if (note === null) return; // cancelled
  var body = { demo_status: demo };
  if (note) body.add_note = note;
  await runBulkLeadAction(clientId, ids, function(id){
    return fetch("/api/clients/" + clientId + "/leads/" + id, {
      method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
    });
  }, "Updating demo for " + ids.length + " lead(s)...");
}

async function bulkSetReached(clientId) {
  var sel = document.getElementById("leadBulkReached");
  var mode = sel ? sel.value : "";
  if (!mode) return;
  var ids = getSelectedLeadIds();
  if (sel) sel.value = "";
  if (!ids.length) return;
  var reachCols = ${JSON.stringify(REACH_VIA_CHANNELS.map((c) => c.column))};
  var body = {};
  if (mode === "none") {
    reachCols.forEach(function(col){ body[col] = false; });
  } else {
    body["reached_via_" + mode] = true;
  }
  await runBulkLeadAction(clientId, ids, function(id){
    return fetch("/api/clients/" + clientId + "/leads/" + id, {
      method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
    });
  }, "Updating reach channels for " + ids.length + " lead(s)...");
}

async function bulkSetAssigned(clientId) {
  var sel = document.getElementById("leadBulkAssign");
  var choice = sel ? sel.value : "";
  if (!choice) return;
  var ids = getSelectedLeadIds();
  if (!ids.length) { if (sel) sel.value = ""; return; }
  var assignedTo = choice === "__unassigned__" ? "" : choice;
  var label = assignedTo ? assignedTo : "Unassigned";
  var note = await promptBulkNote(ids.length, "assignee (" + label + ")");
  if (sel) sel.value = "";
  if (note === null) return; // cancelled
  var body = { assigned_to: assignedTo };
  if (note) body.add_note = note;
  await runBulkLeadAction(clientId, ids, function(id){
    return fetch("/api/clients/" + clientId + "/leads/" + id, {
      method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
    });
  }, "Assigning " + ids.length + " lead(s)...");
}

async function bulkSetCategoryType(clientId) {
  var sel = document.getElementById("leadBulkCategoryType");
  var choice = sel ? sel.value : "";
  if (!choice) return;
  var ids = getSelectedLeadIds();
  if (!ids.length) { if (sel) sel.value = ""; return; }
  var categoryType = choice === "__clear__" ? "" : choice;
  var note = await promptBulkNote(ids.length, "category type");
  if (sel) sel.value = "";
  if (note === null) return; // cancelled
  var body = { category_type: categoryType };
  if (note) body.add_note = note;
  await runBulkLeadAction(clientId, ids, function(id){
    return fetch("/api/clients/" + clientId + "/leads/" + id, {
      method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
    });
  }, "Updating category type for " + ids.length + " lead(s)...");
}

async function bulkDeleteLeads(clientId) {
  var ids = getSelectedLeadIds();
  if (!ids.length) return;
  if (typeof Swal !== "undefined") {
    var result = await Swal.fire({
      title: "Delete " + ids.length + " lead(s)?",
      text: "This cannot be undone.",
      icon: "warning",
      showCancelButton: true,
      confirmButtonText: "Yes, delete them",
      cancelButtonText: "No",
      reverseButtons: true
    });
    if (!result.isConfirmed) return;
  } else if (!confirm("Delete " + ids.length + " selected lead(s)? This cannot be undone.")) {
    return;
  }
  await runBulkLeadAction(clientId, ids, function(id){
    return fetch("/api/clients/" + clientId + "/leads/" + id, {
      method: "DELETE", headers: { "Content-Type": "application/json" }
    });
  }, "Deleting " + ids.length + " lead(s)...");
}

function uploadLeadCallRecording(clientId, leadId) {
  var input = document.createElement("input");
  input.type = "file";
  input.accept = "audio/*";
  input.onchange = async function() {
    if (!input.files || !input.files[0]) return;
    var form = new FormData();
    form.append("audio", input.files[0]);
    showLoadingModal("Uploading call recording...");
    var res = await fetch("/api/clients/" + clientId + "/leads/" + leadId + "/call-recording", {
      method: "POST",
      body: form
    });
    var json = await res.json();
    if (!json.ok) { alert(json.error || "Failed to upload recording"); return; }
    window.location.reload();
  };
  input.click();
}

// ---------------------------------------------------------------------------
// Campaigns
// ---------------------------------------------------------------------------
window.__clientCampaigns = ${campaignsJson};

function openCampaignModal() {
  document.getElementById("campaignModalTitle").textContent = "Add Campaign";
  document.getElementById("campaignId").value = "";
  document.getElementById("campaignName").value = "";
  document.getElementById("campaignType").value = "email";
  document.getElementById("campaignChannel").value = "";
  document.getElementById("campaignStatus").value = "planned";
  document.getElementById("campaignSent").value = "0";
  document.getElementById("campaignResponses").value = "0";
  document.getElementById("campaignPositiveReplies").value = "0";
  document.getElementById("campaignNotes").value = "";
  document.getElementById("campaignModal").classList.add("open");
}

function openCampaignDetail(id) {
  var c = (window.__clientCampaigns || []).find(function(x){ return String(x.id) === String(id); });
  if (!c) { alert("Campaign not found"); return; }
  document.getElementById("campaignModalTitle").textContent = "Edit Campaign";
  document.getElementById("campaignId").value = c.id;
  document.getElementById("campaignName").value = c.name || "";
  document.getElementById("campaignType").value = c.campaign_type || "email";
  document.getElementById("campaignChannel").value = c.channel || "";
  document.getElementById("campaignStatus").value = c.status || "planned";
  document.getElementById("campaignSent").value = c.sent_count || 0;
  document.getElementById("campaignResponses").value = c.response_count || 0;
  document.getElementById("campaignPositiveReplies").value = c.positive_replies || 0;
  document.getElementById("campaignNotes").value = c.notes || "";
  document.getElementById("campaignModal").classList.add("open");
}

function closeCampaignModal(event) {
  if (event && event.target && event.target.id !== "campaignModal") return;
  document.getElementById("campaignModal").classList.remove("open");
}

async function saveCampaign(clientId) {
  var name = document.getElementById("campaignName").value.trim();
  if (!name) { alert("Campaign name is required"); return; }
  var id = document.getElementById("campaignId").value;
  var payload = {
    name: name,
    campaign_type: document.getElementById("campaignType").value,
    channel: document.getElementById("campaignChannel").value.trim(),
    status: document.getElementById("campaignStatus").value,
    sent_count: Number(document.getElementById("campaignSent").value) || 0,
    response_count: Number(document.getElementById("campaignResponses").value) || 0,
    positive_replies: Number(document.getElementById("campaignPositiveReplies").value) || 0,
    notes: document.getElementById("campaignNotes").value.trim()
  };
  showLoadingModal(id ? "Updating campaign..." : "Saving campaign...");
  var url = id ? "/api/clients/" + clientId + "/campaigns/" + id : "/api/clients/" + clientId + "/campaigns";
  var res = await fetch(url, { method: id ? "PATCH" : "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
  var json = await res.json();
  if (!json.ok) { alert((id ? "Update" : "Save") + " failed: " + (json.error || "Unknown error")); return; }
  window.location.reload();
}

async function archiveCampaign(clientId, id) {
  if (!confirm("Archive this campaign?")) return;
  showLoadingModal("Archiving campaign...");
  var res = await fetch("/api/clients/" + clientId + "/campaigns/" + id, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ archive: true }) });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to archive campaign"); return; }
  window.location.reload();
}

// ---------------------------------------------------------------------------
// Incentives
// ---------------------------------------------------------------------------
window.__clientIncentives = ${incentivesJson};

function openIncentiveModal() {
  document.getElementById("incentiveModalTitle").textContent = "Add Incentive";
  document.getElementById("incentiveId").value = "";
  document.getElementById("incentiveTitle").value = "";
  document.getElementById("incentiveGtm").value = "";
  document.getElementById("incentiveLead").value = "";
  document.getElementById("incentiveAmount").value = "0";
  document.getElementById("incentiveStatus").value = "pending";
  document.getElementById("incentiveNotes").value = "";
  document.getElementById("incentiveModal").classList.add("open");
}

function openIncentiveDetail(id) {
  var i = (window.__clientIncentives || []).find(function(x){ return String(x.id) === String(id); });
  if (!i) { alert("Incentive not found"); return; }
  document.getElementById("incentiveModalTitle").textContent = "Edit Incentive";
  document.getElementById("incentiveId").value = i.id;
  document.getElementById("incentiveTitle").value = i.title || "";
  document.getElementById("incentiveGtm").value = i.gtm_user_id || "";
  document.getElementById("incentiveLead").value = i.related_lead_id || "";
  document.getElementById("incentiveAmount").value = i.amount || 0;
  document.getElementById("incentiveStatus").value = i.status || "pending";
  document.getElementById("incentiveNotes").value = i.notes || "";
  document.getElementById("incentiveModal").classList.add("open");
}

function closeIncentiveModal(event) {
  if (event && event.target && event.target.id !== "incentiveModal") return;
  document.getElementById("incentiveModal").classList.remove("open");
}

async function saveIncentive(clientId) {
  var title = document.getElementById("incentiveTitle").value.trim();
  if (!title) { alert("Incentive title is required"); return; }
  var id = document.getElementById("incentiveId").value;
  var payload = {
    title: title,
    gtm_user_id: document.getElementById("incentiveGtm").value || null,
    related_lead_id: document.getElementById("incentiveLead").value || null,
    amount: Number(document.getElementById("incentiveAmount").value) || 0,
    status: document.getElementById("incentiveStatus").value,
    notes: document.getElementById("incentiveNotes").value.trim()
  };
  showLoadingModal(id ? "Updating incentive..." : "Saving incentive...");
  var url = id ? "/api/clients/" + clientId + "/incentives/" + id : "/api/clients/" + clientId + "/incentives";
  var res = await fetch(url, { method: id ? "PATCH" : "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
  var json = await res.json();
  if (!json.ok) { alert((id ? "Update" : "Save") + " failed: " + (json.error || "Unknown error")); return; }
  window.location.reload();
}

async function archiveIncentive(clientId, id) {
  if (!confirm("Archive this incentive?")) return;
  showLoadingModal("Archiving incentive...");
  var res = await fetch("/api/clients/" + clientId + "/incentives/" + id, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ archive: true }) });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to archive incentive"); return; }
  window.location.reload();
}

// ---------------------------------------------------------------------------
// Weekly Reports
// ---------------------------------------------------------------------------
window.__clientReports = ${reportsJson};

// Report sub-view toggle (Daily / Week 1 / Week 2 / ...). The view arg is
// "daily" or "week" followed by a number. The weekly views live behind a "Week"
// dropdown; selecting one highlights the dropdown and updates its label.
// Returns false to cancel link navigation when the target view is already on the
// page (instant switch); returns true otherwise so the flyout link navigates.
function setReportView(view) {
  var views = document.querySelectorAll(".report-subview");
  if (!views.length) return true;
  var targetId = "reportView-" + view;
  if (!document.getElementById(targetId)) return true;
  for (var i = 0; i < views.length; i++) {
    views[i].style.display = views[i].id === targetId ? "" : "none";
  }
  var isWeek = /^week[0-9]+$/.test(view);
  var dailyBtn = document.querySelector('.report-subtab[data-view="daily"]');
  if (dailyBtn) dailyBtn.classList.toggle("active", view === "daily");
  var weekBtn = document.querySelector(".report-week-btn");
  if (weekBtn) weekBtn.classList.toggle("active", isWeek);
  var label = document.querySelector(".report-week-label");
  var items = document.querySelectorAll(".report-week-item");
  for (var j = 0; j < items.length; j++) {
    var match = items[j].getAttribute("data-view") === view;
    items[j].classList.toggle("active", match);
    if (match && label) label.textContent = items[j].getAttribute("data-label");
  }
  if (!isWeek && label) label.textContent = "Week";
  var menu = document.querySelector(".report-week-menu");
  if (menu) menu.classList.remove("open");
  try {
    history.replaceState(null, "", "#" + view);
  } catch (e) {}
  return false;
}

function toggleWeekMenu(e) {
  if (e) e.stopPropagation();
  var menu = document.querySelector(".report-week-menu");
  if (menu) menu.classList.toggle("open");
}

document.addEventListener("click", function () {
  var menu = document.querySelector(".report-week-menu");
  if (menu) menu.classList.remove("open");
});

// Manually (re)generate the AI report summary for a period, then reload to show it.
async function regenReportSummary(period, btn, clientId, weekStart) {
  var original = btn.textContent;
  btn.disabled = true;
  btn.textContent = "Generating…";
  try {
    var res = await fetch(
      "/api/clients/" + clientId + "/report-summary/generate",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ period: period, week_start: weekStart || null }),
      },
    );
    var json = await res.json();
    if (!json.ok) throw new Error(json.error || "Failed to generate summary");
    location.reload();
  } catch (e) {
    alert(e.message || "Failed to generate summary");
    btn.disabled = false;
    btn.textContent = original;
  }
}

function openGoalsModal(clientId) {
  document.getElementById("goalsModal").classList.add("open");
  var first = document.querySelector("#goalsRows .goal-title");
  if (first) first.focus();
}

function closeGoalsModal(event) {
  if (event && event.target && event.target.id !== "goalsModal") return;
  document.getElementById("goalsModal").classList.remove("open");
}

function goalRowMarkup() {
  return '<div class="goal-row" style="display:flex; gap:8px; align-items:center; margin-bottom:8px;">'
    + '<input type="text" class="goal-title" placeholder="Title" style="flex:1 1 auto;" />'
    + '<input type="number" class="goal-value" placeholder="Number" style="width:120px;" />'
    + '<button class="btn" type="button" onclick="removeGoalRow(this)" style="padding:6px 10px; white-space:nowrap;">✕</button>'
    + '</div>';
}

function addGoalRow() {
  var c = document.getElementById("goalsRows");
  if (!c) return;
  c.insertAdjacentHTML("beforeend", goalRowMarkup());
  var rows = c.querySelectorAll(".goal-row");
  var last = rows[rows.length - 1];
  var t = last && last.querySelector(".goal-title");
  if (t) t.focus();
}

function removeGoalRow(btn) {
  var row = btn.closest(".goal-row");
  var c = document.getElementById("goalsRows");
  if (row) row.remove();
  // Always keep at least one empty row so there's something to fill in.
  if (c && !c.querySelector(".goal-row")) c.insertAdjacentHTML("beforeend", goalRowMarkup());
}

async function saveGoals(clientId) {
  var rows = Array.prototype.slice.call(document.querySelectorAll("#goalsRows .goal-row"));
  var items = rows.map(function (r) {
    var titleEl = r.querySelector(".goal-title");
    var valueEl = r.querySelector(".goal-value");
    return {
      title: (titleEl && titleEl.value || "").trim(),
      value: (valueEl && valueEl.value || "").trim()
    };
  }).filter(function (g) { return g.title || g.value; });
  var notesEl = document.getElementById("goalsNotes");
  var notes = notesEl ? notesEl.value : "";
  var res = await fetch("/api/clients/" + clientId + "/goals", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ goals_json: items, notes: notes })
  });
  var json = await res.json();
  if (!json.ok) { alert("Save failed: " + (json.error || "Unknown error")); return; }
  window.location.reload();
}

(function initReportView() {
  var h = (location.hash || "").replace(/^#/, "");
  if (h !== "daily" && !/^week[0-9]+$/.test(h)) return;
  var go = function () {
    setReportView(h);
  };
  if (document.readyState !== "loading") go();
  else document.addEventListener("DOMContentLoaded", go);
})();

function openReportModal() {
  document.getElementById("reportModalTitle").textContent = "New Weekly Report";
  document.getElementById("reportId").value = "";
  document.getElementById("reportPeriod").value = "";
  document.getElementById("reportWeekStart").value = "";
  document.getElementById("reportSummary").value = "";
  document.getElementById("reportHighlights").value = "";
  document.getElementById("reportLowlights").value = "";
  document.getElementById("reportNextWeek").value = "";
  document.getElementById("reportClientVisible").checked = true;
  document.getElementById("reportModal").classList.add("open");
}

function openReportDetail(id) {
  var r = (window.__clientReports || []).find(function(x){ return String(x.id) === String(id); });
  if (!r) { alert("Report not found"); return; }
  document.getElementById("reportModalTitle").textContent = "Edit Weekly Report";
  document.getElementById("reportId").value = r.id;
  document.getElementById("reportPeriod").value = r.period_label || "";
  document.getElementById("reportWeekStart").value = r.week_start || "";
  document.getElementById("reportSummary").value = r.summary || "";
  document.getElementById("reportHighlights").value = r.highlights || "";
  document.getElementById("reportLowlights").value = r.lowlights || "";
  document.getElementById("reportNextWeek").value = r.next_week_plan || "";
  document.getElementById("reportClientVisible").checked = r.is_client_visible !== false;
  document.getElementById("reportModal").classList.add("open");
}

function closeReportModal(event) {
  if (event && event.target && event.target.id !== "reportModal") return;
  document.getElementById("reportModal").classList.remove("open");
}

async function saveReport(clientId) {
  var payload = {
    period_label: document.getElementById("reportPeriod").value.trim(),
    week_start: document.getElementById("reportWeekStart").value || null,
    summary: document.getElementById("reportSummary").value.trim(),
    highlights: document.getElementById("reportHighlights").value.trim(),
    lowlights: document.getElementById("reportLowlights").value.trim(),
    next_week_plan: document.getElementById("reportNextWeek").value.trim(),
    is_client_visible: document.getElementById("reportClientVisible").checked
  };
  if (!payload.period_label && !payload.week_start && !payload.summary) {
    alert("Add a period label, week start, or summary"); return;
  }
  var id = document.getElementById("reportId").value;
  showLoadingModal(id ? "Updating report..." : "Saving report...");
  var url = id ? "/api/clients/" + clientId + "/reports/" + id : "/api/clients/" + clientId + "/reports";
  var res = await fetch(url, { method: id ? "PATCH" : "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
  var json = await res.json();
  if (!json.ok) { alert((id ? "Update" : "Save") + " failed: " + (json.error || "Unknown error")); return; }
  window.location.reload();
}

async function updateReport(clientId, id, patch) {
  showLoadingModal("Updating report...");
  var res = await fetch("/api/clients/" + clientId + "/reports/" + id, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(patch) });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to update report"); return; }
  window.location.reload();
}

async function archiveReport(clientId, id) {
  if (!confirm("Archive this report?")) return;
  showLoadingModal("Archiving report...");
  var res = await fetch("/api/clients/" + clientId + "/reports/" + id, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ archive: true }) });
  var json = await res.json();
  if (!json.ok) { alert(json.error || "Failed to archive report"); return; }
  window.location.reload();
}

</script>
      
</div><!-- /.wrap: the original markup never closed this -->
  `;
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

function renderReportSummaryBody(row) {
  const json = row && row.summary_json;
  // The Reached-Via channel/status breakdown is rendered deterministically from
  // the stored stats (not the AI text) so the per-channel numbers are exact.
  const reachBreakdown =
    (row && row.stats && row.stats.outreach && row.stats.outreach.reach_breakdown) ||
    [];
  if (json && Array.isArray(json.sections) && (json.sections.length || json.headline)) {
    // Headline: bold the label up to the first colon (e.g. "Team Effort:") and
    // any **...** segments (e.g. the team roles) — matching the client format.
    const renderHeadline = (text) => {
      const str = String(text);
      const idx = str.indexOf(":");
      const hasLabel = idx > 0 && idx < 40;
      const label = hasLabel
        ? `<span style="font-weight:700;">${escapeHtml(str.slice(0, idx + 1))}</span>`
        : "";
      const rest = escapeHtml(hasLabel ? str.slice(idx + 1) : str).replace(
        /\*\*([^*]+)\*\*/g,
        '<span style="font-weight:700;">$1</span>',
      );
      return label + rest;
    };
    // Legacy bullet items (older rows without description/stats).
    const renderItem = (item) => {
      if (item && typeof item === "object" && Array.isArray(item.items)) {
        return `<li style="margin:4px 0;">${item.label ? `<span style="font-weight:700;">${escapeHtml(String(item.label))}</span>` : ""}<ul style="margin:4px 0 0; padding-left:20px; list-style:circle;">${item.items.map((s) => `<li style="margin:2px 0;">${escapeHtml(String(s))}</li>`).join("")}</ul></li>`;
      }
      return `<li style="margin:3px 0;">${escapeHtml(String(item))}</li>`;
    };
    // Stats line: "<b>380</b> leads added | <b>365</b> enriched".
    const renderStats = (stats) =>
      (stats || [])
        .filter((s) => s && (s.value != null || s.label))
        .map(
          (s) =>
            `<span style="font-weight:700;">${escapeHtml(String(s.value != null ? s.value : ""))}</span>${s.label ? ` ${escapeHtml(String(s.label))}` : ""}`,
        )
        .join(' <span style="opacity:.4;">|</span> ');
    // Reached-Via breakdown sub-list: "LinkedIn — 10 reached: 4 Connection Sent · 4 Engaged".
    const renderReachBreakdown = (breakdown) =>
      `<ul style="margin:6px 0 0; padding-left:20px; list-style:disc; font-size:13px; line-height:1.5;">${breakdown
        .filter((b) => b && b.channel)
        .map((b) => {
          const statuses = (b.statuses || [])
            .filter((s) => s && (s.count != null || s.status))
            .map(
              (s) =>
                `<span style="font-weight:700;">${escapeHtml(String(s.count != null ? s.count : ""))}</span> ${escapeHtml(String(s.status || ""))}`,
            )
            .join(" · ");
          return `<li style="margin:2px 0;"><span style="font-weight:700;">${escapeHtml(String(b.channel))}</span> — <span style="font-weight:700;">${escapeHtml(String(b.count != null ? b.count : ""))}</span> reached${statuses ? `: ${statuses}` : ""}</li>`;
        })
        .join("")}</ul>`;
    const head = json.headline
      ? `<div style="font-size:14px; line-height:1.6; margin-bottom:4px;">${renderHeadline(json.headline)}</div>`
      : "";
    const sections = (json.sections || [])
      .map((sec) => {
        const title = `<div style="font-weight:700; font-size:15px; margin-bottom:2px;">${escapeHtml(String(sec.title || ""))}</div>`;
        const hasStats = Array.isArray(sec.stats) && sec.stats.length;
        const hasDesc = sec.description != null && String(sec.description).trim();
        if (hasStats || hasDesc) {
          const desc = hasDesc
            ? `<div style="font-size:14px; line-height:1.5; margin-top:2px;">${escapeHtml(String(sec.description))}</div>`
            : "";
          const stats = hasStats
            ? `<div style="font-size:14px; line-height:1.5; margin-top:4px;">${renderStats(sec.stats)}</div>`
            : "";
          // Attach the per-channel Reached-Via / status breakdown under Outreach Execution.
          const breakdown =
            String(sec.title || "").trim() === "Outreach Execution" &&
            reachBreakdown.length
              ? renderReachBreakdown(reachBreakdown)
              : "";
          return `<div style="margin-top:16px;">${title}${desc}${stats}${breakdown}</div>`;
        }
        // Legacy shape: bullet list of items.
        return `
      <div style="margin-top:16px;">
        ${title}
        <ul style="margin:6px 0 0; padding-left:20px; list-style:disc; font-size:14px; line-height:1.5;">${(sec.items || []).map(renderItem).join("")}</ul>
      </div>`;
      })
      .join("");
    return head + sections;
  }
  if (row && row.summary_text) {
    return `<div style="font-size:14px; line-height:1.65; white-space:pre-wrap;">${escapeHtml(row.summary_text)}</div>`;
  }
  return "";
}

function renderReportSummaryPanel({
  period,
  row,
  editable,
  clientId,
  weekStart = null,
  weekLabel = "",
  rangeLabel = "",
}) {
  const isWeekly = period === "weekly";
  const title = isWeekly
    ? `${weekLabel ? escapeHtml(weekLabel) + " Summary" : "Weekly Summary"}`
    : "Daily Summary";
  const sub = isWeekly
    ? `${rangeLabel ? escapeHtml(rangeLabel) : "this week (since Monday)"} · auto-generated daily at 9 PM PST`
    : "last 24 hours · auto-generated daily at 9 PM PST";
  const contentHtml = renderReportSummaryBody(row);
  const hasContent = !!contentHtml;
  const when = row && row.created_at ? formatDateTime(row.created_at) : "";
  const body = hasContent
    ? contentHtml
    : `<div class="meta" style="font-size:13px; line-height:1.65;">🕘 Your ${isWeekly ? "weekly" : "daily"} AI summary is generated automatically every day at 9&nbsp;PM&nbsp;PST. Check back then${editable ? ", or generate it now." : "."}</div>`;
  const btn = editable
    ? `<button class="btn" type="button" style="padding:5px 12px; font-size:12px; white-space:nowrap; background:#16a34a; border:1px solid #16a34a; color:#fff;" onclick="regenReportSummary('${period}', this, ${Number(clientId)}, ${weekStart ? `'${escapeHtml(String(weekStart))}'` : "null"})">${hasContent ? "Regenerate" : "Generate now"}</button>`
    : "";
  const whenBadge = when
    ? `<span class="meta" style="font-size:12px; font-weight:400; white-space:nowrap;">Generated ${escapeHtml(when)}</span>`
    : "";
  return `
    <div class="panel" data-ai-sum="${period}" style="margin-bottom:16px;">
      <div class="panel-head" style="display:flex; align-items:flex-start; gap:12px; margin-bottom:12px;">
        <div>
          <h2 style="margin:0; display:flex; align-items:center; flex-wrap:wrap; gap:10px;">✨ ${title}${btn}${whenBadge}</h2>
          <div class="meta" style="font-size:12px;">${sub}</div>
        </div>
      </div>
      ${body}
    </div>`;
}

function normalizeClientGoalsData(row) {
  let items = [];
  if (row && Array.isArray(row.goals_json)) {
    items = row.goals_json
      .map((g) => ({
        title: String((g && g.title) || "").trim(),
        value: String(g && g.value != null ? g.value : "").trim(),
      }))
      .filter((g) => g.title || g.value);
  }
  let notes = row && row.notes != null ? String(row.notes) : "";
  if (!items.length && !notes.trim() && row && row.goals_text) {
    notes = String(row.goals_text);
  }
  return { items, notes };
}

function renderGoalRowInput(title = "", value = "") {
  return `<div class="goal-row" style="display:flex; gap:8px; align-items:center; margin-bottom:8px;">
        <input type="text" class="goal-title" placeholder="Title" value="${escapeHtml(String(title))}" style="flex:1 1 auto;" />
        <input type="number" class="goal-value" placeholder="Number" value="${escapeHtml(String(value))}" style="width:120px;" />
        <button class="btn" type="button" onclick="removeGoalRow(this)" style="padding:6px 10px; white-space:nowrap;">✕</button>
      </div>`;
}

function renderGoalsModalInner(row) {
  const { items, notes } = normalizeClientGoalsData(row);
  const rows = items.length ? items : [{ title: "", value: "" }];
  return `
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Goals (visible to the client)</label>
        <div id="goalsRows">${rows.map((g) => renderGoalRowInput(g.title, g.value)).join("")}</div>
        <button class="btn" type="button" onclick="addGoalRow()" style="margin-top:4px;">+ Add goal</button>
      </div>
      <div class="form-field" style="grid-column:1 / -1;">
        <label>Notes</label>
        <textarea id="goalsNotes" rows="6" placeholder="Additional notes…">${escapeHtml(notes)}</textarea>
      </div>`;
}

function renderClientGoalsPanel({ row, editable, clientId, users = [] }) {
  const { items, notes } = normalizeClientGoalsData(row);
  const notesText = notes.trim();
  const hasText = items.length > 0 || !!notesText;
  const when = row && row.updated_at ? formatDateTime(row.updated_at) : "";
  const byName =
    row && row.updated_by_user_id
      ? users.find((u) => String(u.id) === String(row.updated_by_user_id))?.name
      : "";
  const editBtn = editable
    ? `<button class="btn" type="button" style="padding:5px 12px; font-size:12px; white-space:nowrap;" onclick="openGoalsModal(${Number(clientId)})">${hasText ? "Edit goals" : "Add goals"}</button>`
    : "";
  const metaLine =
    hasText && when
      ? `<div class="meta" style="font-size:12px; margin-top:10px;">Last updated ${escapeHtml(when)}${byName ? ` by ${escapeHtml(byName)}` : ""}</div>`
      : "";
  const goalsListHtml = items.length
    ? `<div style="display:flex; flex-direction:column; gap:8px;">${items
        .map(
          (g) => `<div style="display:flex; align-items:baseline; justify-content:space-between; gap:12px;">
        <span style="font-weight:700; font-size:14px;">${escapeHtml(g.title)}</span>
        <span style="font-weight:700; font-size:14px; white-space:nowrap;">${escapeHtml(g.value)}</span>
      </div>`,
        )
        .join("")}</div>`
    : "";
  const notesHtml = notesText
    ? `<div style="margin-top:${items.length ? 14 : 0}px; font-size:14px; line-height:1.6; white-space:pre-wrap;">${escapeHtml(notes)}</div>`
    : "";
  const body = hasText
    ? `${goalsListHtml}${notesHtml}`
    : `<div class="meta" style="font-size:13px; line-height:1.65;">🎯 No goals set yet.${editable ? " Use “Add goals” to capture this client’s goals." : ""}</div>`;
  return `
    <div class="panel" data-client-goals="${Number(clientId)}" style="margin-bottom:16px; height:97%;">
      <div class="panel-head" style="display:flex; align-items:flex-start; gap:12px; margin-bottom:12px;">
        <div>
          <h2 style="margin:0; display:flex; align-items:center; flex-wrap:wrap; gap:10px;">🎯 Weekly Goals${editBtn}</h2>
          <div class="meta" style="font-size:12px;">Manually curated · visible to the client</div>
        </div>
      </div>
      ${body}
      ${metaLine}
    </div>`;
}

function renderSummaryWithGoals({
  period,
  summaryRow,
  goalsRow,
  editable,
  clientId,
  users = [],
  weekStart = null,
  weekLabel = "",
  rangeLabel = "",
}) {
  // Goals show alongside both the daily and weekly summaries, so the client
  // sees the curated targets next to either view.
  const goalsPanel = `<div style="flex:1 1 320px; min-width:280px;">${renderClientGoalsPanel({ row: goalsRow, editable, clientId, users })}</div>`;
  return `
    <div style="display:flex; gap:16px; align-items:stretch; flex-wrap:wrap;">
      <div style="flex:1 1 380px; min-width:300px;">${renderReportSummaryPanel({ period, row: summaryRow, editable, clientId, weekStart, weekLabel, rangeLabel })}</div>
      ${goalsPanel}
    </div>`;
}

export {
  renderClientWorkspacePage,
};
