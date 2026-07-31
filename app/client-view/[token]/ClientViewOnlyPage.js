// Markup for GET /client-view/:token.
//
// Body markup extracted verbatim from renderClientViewOnlyPage() (lib/server/app.js
// lines 11435-14074). The document shell now comes from
// app/layout.jsx, the <style> block from ./client-view.css, and the inline
// <script> from public/js/.

import { CLIENT_REPORT_MAX_WEEKS } from "@/lib/data/client-reports.js";
import { CLIENT_LEAD_CATEGORY_TYPES, CLIENT_LEAD_CATEGORY_TYPE_LABELS, CLIENT_LEAD_DEMO_STATUSES, CLIENT_LEAD_OUTREACH_STATUSES, CLIENT_LEAD_PIPELINE_STAGES, DEFAULT_CLIENT_LEAD_STAGE, REACH_VIA_CHANNELS, clientLeadStatusLabel } from "@/lib/server/constants.js";
import { APP_TIMEZONE } from "@/lib/server/runtime.js";
import { getDateStringInTimeZone, getTodayDateStringInTimeZone } from "@/lib/server/time.js";
import { escapeHtml, formatDateOnly, formatDateTime } from "@/lib/ui/html.js";

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

function renderClientViewOnlyPage({
  client,
  services = [],
  workItems = [],
  updates = [],
  actions = [],
  documents = [],
  leads = [],
  campaigns = [],
  meetings = [],
  blockers = [],
  reports = [],
  contributors = [],
  users = [],
  linkedTasks = [],
  leadAllRows = [],
  incentives = [],
  leadStageEvents = [],
  reportSummaries = { daily: null, weekly: null, weeklyByDate: {} },
  clientGoals = null,
}) {
  // Auto daily / weekly / funnel report sections — identical to the internal
  // client workspace report (shared via buildClientAutoReportSections).
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
  const openWorkItems = workItems.filter((w) => w.status !== "done");
  const doneWorkItems = workItems.filter((w) => w.status === "done");
  const clientActions = actions.filter((a) => a.owner_type === "Client");

  const serviceNames =
    services
      .map((s) => s.name)
      .filter(Boolean)
      .join(", ") || "-";

  const userNameById = {};
  users.forEach((u) => {
    userNameById[String(u.id)] = u.name || "";
  });

  // ---- Lead funnel (all leads — mirrors the internal Leads tab) --------
  const stageCount = {};
  CLIENT_LEAD_PIPELINE_STAGES.forEach((s) => {
    stageCount[s.key] = 0;
  });
  leads.forEach((l) => {
    const st = l.pipeline_stage || "prospect_identified";
    if (stageCount[st] !== undefined) stageCount[st] += 1;
  });
  const totalLeads = leads.length;
  const qualifiedLeads =
    (stageCount.qualified_opportunity || 0) +
    (stageCount.pilot_evaluation || 0) +
    (stageCount.commercial_discussion || 0) +
    (stageCount.converted || 0);
  const convertedLeads = stageCount.converted || 0;
  const meetingLeads =
    (stageCount.meeting_scheduled || 0) + (stageCount.meeting_completed || 0);

  // Note authors / assignees — the internal Leads tab builds both lists from the
  // org's users, so mirror that here and union in any name that only appears on
  // a lead (an author or assignee who is no longer an active user) so no
  // existing value becomes unfilterable.
  const extNoteAuthors = Array.from(
    new Set(
      [
        ...users.map((u) => String((u && u.name) || "").trim()),
        ...leads.flatMap((l) =>
          parseLeadNotesHistory(l.notes).map((n) =>
            String((n && n.by) || "").trim(),
          ),
        ),
      ].filter(Boolean),
    ),
  ).sort((a, b) => a.localeCompare(b));
  // Assigned-to options: same source as the internal filter (org users), plus
  // any assignee value present on a lead.
  const extAssignees = Array.from(
    new Set(
      [
        ...users.map((u) => String((u && u.name) || "").trim()),
        ...leads.map((l) => String(l.assigned_to || "").trim()),
      ].filter(Boolean),
    ),
  ).sort((a, b) => a.localeCompare(b));

  // Category Type counts across the shared leads — powers the clickable
  // pill row above the external leads table (mirrors the internal Leads tab).
  const extCategoryCounts = (() => {
    const counts = {};
    leads.forEach((l) => {
      const key = String(l.category_type || "").trim();
      if (!key) return;
      counts[key] = (counts[key] || 0) + 1;
    });
    return Object.entries(counts)
      .map(([key, count]) => ({ key, count }))
      .sort((a, b) => b.count - a.count);
  })();
  const extTodayStr = getTodayDateStringInTimeZone(APP_TIMEZONE);

  // ---- Demos (from the shared lead set) --------------------------------
  const demoLeads = leads.filter(
    (l) =>
      l.demo_status === "scheduled" ||
      l.demo_status === "completed" ||
      l.pipeline_stage === "meeting_scheduled" ||
      l.pipeline_stage === "meeting_completed",
  );

  // ---- Campaign roll-up ------------------------------------------------
  const totalSent = campaigns.reduce(
    (n, c) => n + (Number(c.sent_count) || 0),
    0,
  );
  const totalResponses = campaigns.reduce(
    (n, c) => n + (Number(c.response_count) || 0),
    0,
  );
  const totalPositiveReplies = campaigns.reduce(
    (n, c) => n + (Number(c.positive_replies) || 0),
    0,
  );

  // ---- Highlighted calls (starred leads with a recording) --------------
  const highlightedCalls = leads.filter(
    (l) => l.is_starred && l.call_recording_url,
  );

  // ---- Work progress grouped by owner (GTM-wise) -----------------------
  const workByOwner = {};
  workItems.forEach((w) => {
    const key = w.owner_user_id ? String(w.owner_user_id) : "unassigned";
    if (!workByOwner[key]) workByOwner[key] = [];
    workByOwner[key].push(w);
  });
  const workOwnerGroups = Object.keys(workByOwner).map((key) => {
    const items = workByOwner[key];
    const done = items.filter((w) => w.status === "done").length;
    return {
      name: key === "unassigned" ? "Unassigned" : userNameById[key] || "Team",
      items,
      total: items.length,
      done,
      pct: items.length ? Math.round((done / items.length) * 100) : 0,
    };
  });

  // ---- Team (PM + AM/Strategist + contributors by role text) -----------
  const teamMembers = [];
  if (client.project_manager_name)
    teamMembers.push({
      name: client.project_manager_name,
      role: "Project Manager",
    });
  // if (client.account_manager_name)
  //   teamMembers.push({
  //     name: client.account_manager_name,
  //     role: "Account Manager / Strategist",
  //   });
  (Array.isArray(client.gtm_associate_user_ids)
    ? client.gtm_associate_user_ids
    : []
  ).forEach((id) => {
    const name = userNameById[String(id)];
    if (name) teamMembers.push({ name, role: "GTM Associate" });
  });
  contributors
    .filter((c) => (c.status || "Active") === "Active")
    .forEach((c) => {
      if (c.name)
        teamMembers.push({
          name: c.name,
          role: c.role || c.person_type || "Contributor",
        });
    });

  const openBlockers = blockers.filter(
    (b) => b.resolution_status !== "resolved",
  );

  const meetingLabel = (t) =>
    ({
      sync_call: "Sync Call",
      internal: "Internal",
      review: "Review",
      adhoc: "Ad-hoc",
    })[t] || "Sync Call";
  const campaignLabel = (t) =>
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
      other: "Other",
    })[t] || "Email";

  return `
            <div class="wrap">
          <div class="topbar">
            <div class="topbar-main">
              <div class="topbar-avatar" aria-hidden="true">${escapeHtml((client.name || "?").trim().charAt(0).toUpperCase() || "?")}</div>
              <div>
                <div class="eyebrow">
                  <span>Client Project View</span>
                  ${(() => {
                    const s = String(client.status || "").toLowerCase();
                    const tone = /active|live|ongoing|won/.test(s)
                      ? "ok"
                      : /paus|hold|onboard|pending/.test(s)
                        ? "warn"
                        : /churn|lost|cancel|inactiv|closed/.test(s)
                          ? "danger"
                          : "info";
                    return `<span class="status-badge status-${tone}"><span class="status-dot"></span>${escapeHtml(client.status || "-")}</span>`;
                  })()}
                </div>
                <h1>${escapeHtml(client.name || "-")}</h1>
                <div class="subtitle">${escapeHtml(client.company_name || "")}</div>
              </div>
            </div>

            <div class="topbar-actions">
              ${
                client.google_drive_folder_url
                  ? `<a class="topbar-cta" href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer"><span aria-hidden="true">📁</span>Google Drive</a>`
                  : ""
              }
              <a class="topbar-cta" href="https://notebooklm.google.com/notebook/76c66777-16e6-447f-b6a7-d40befa08590" target="_blank" rel="noopener noreferrer"><span aria-hidden="true">📓</span>Notebook</a>
            </div>
          </div>

          <div class="stats">
            <div class="stat-card stat-info">
              <div class="stat-label">Leads</div>
              <div class="stat-value">${totalLeads}</div>
            </div>
            <div class="stat-card stat-success">
              <div class="stat-label">Qualified+</div>
              <div class="stat-value">${qualifiedLeads}</div>
            </div>
            <div class="stat-card stat-warn">
              <div class="stat-label">Open Work</div>
              <div class="stat-value">${openWorkItems.length}</div>
            </div>
            <div class="stat-card stat-danger">
              <div class="stat-label">Action Needed</div>
              <div class="stat-value">${clientActions.length}</div>
            </div>
          </div>

          <div class="client-view-tabs">
            <button class="client-view-tab active" onclick="showClientViewTab('overview', this)">Overview</button>
            <button class="client-view-tab" onclick="showClientViewTab('leads', this)">Leads</button>
            <button class="client-view-tab" onclick="showClientViewTab('work', this)">Tasks</button>
            <button class="client-view-tab" onclick="showClientViewTab('campaigns', this)">Campaigns</button>
            <button class="client-view-tab" onclick="showClientViewTab('meetings', this)">Demos &amp; Meetings</button>
            <button class="client-view-tab" onclick="showClientViewTab('blockers', this)">Blockers</button>
            <button class="client-view-tab" onclick="showClientViewTab('reports', this)">Report</button>
            <button class="client-view-tab" onclick="showClientViewTab('actions', this)">Actions Needed</button>
            <button class="client-view-tab" onclick="showClientViewTab('documents', this)">Documents</button>
          </div>

          <div id="clientViewTab-overview" class="tab-panel active">
            <div class="panel">
              <div class="panel-head">
                <h2>Overview</h2>
              </div>
              <div class="overview-grid">
                <div class="overview-card overview-card-wide">
                  <div class="overview-card-body">
                    <div class="overview-card-label">About this engagement</div>
                    <p class="overview-card-text">${escapeHtml(client.description || "Project progress and updates.")}</p>
                  </div>
                </div>

                <div class="overview-card">
                  <div class="overview-card-body">
                    <div class="overview-card-label">Engagement Start</div>
                    <div class="overview-card-value">${escapeHtml(client.start_date || "-")}</div>
                  </div>
                </div>

                <div class="overview-card">
                  <div class="overview-card-body">
                    <div class="overview-card-label">Services Engaged</div>
                    <div class="chip-row">
                      ${
                        services.length
                          ? services
                              .map((s) => s.name)
                              .filter(Boolean)
                              .map((n) => `<span class="chip">${escapeHtml(n)}</span>`)
                              .join("")
                          : `<span class="chip chip-muted">No services listed</span>`
                      }
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div class="panel">
              <div class="panel-head">
                <h2>Engagement Team</h2>
              </div>
              ${
                teamMembers.length
                  ? `<div class="team-grid">
                      ${teamMembers
                        .map(
                          (m) => `
                        <div class="team-card">
                          <div class="team-avatar" aria-hidden="true">${escapeHtml((m.name || "?").trim().charAt(0).toUpperCase() || "?")}</div>
                          <div>
                            <div class="team-name">${escapeHtml(m.name)}</div>
                            <div class="team-role">${escapeHtml(m.role)}</div>
                          </div>
                        </div>`,
                        )
                        .join("")}
                    </div>`
                  : `<div class="meta">Team details will appear here.</div>`
              }
            </div>
          </div>

          <div id="clientViewTab-leads" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Lead Funnel</h2>
              </div>

              <div class="kpi-grid">
                <div class="kpi-card"><div class="kpi-label">Total Leads</div><div class="kpi-value">${totalLeads}</div></div>
                <div class="kpi-card"><div class="kpi-label">Qualified+</div><div class="kpi-value">${qualifiedLeads}</div></div>
                <div class="kpi-card"><div class="kpi-label">Meetings</div><div class="kpi-value">${meetingLeads}</div></div>
                <div class="kpi-card"><div class="kpi-label">Converted</div><div class="kpi-value">${convertedLeads}</div></div>
              </div>

              <div class="pipeline">
                ${CLIENT_LEAD_PIPELINE_STAGES.map((s, i) => {
                  const n = CLIENT_LEAD_PIPELINE_STAGES.length;
                  const count = stageCount[s.key] || 0;
                  const share = totalLeads
                    ? Math.round((count / totalLeads) * 100)
                    : 0;
                  // Hue progresses violet (L1) → teal (Converted).
                  const hue = Math.round(255 - (i / (n - 1)) * 105);
                  return `
                  <div class="pipeline-stage" style="--stage:hsl(${hue} 70% 58%); animation-delay:${i * 55}ms;">
                    <div class="pipeline-count">${count}</div>
                    <div class="pipeline-name">${escapeHtml(s.label)}</div>
                  </div>${i < n - 1 ? `<div class="pipeline-arrow" aria-hidden="true">→</div>` : ""}`;
                }).join("")}
              </div>
            </div>

            <div class="panel">
              <div
                class="panel-head"
                style="display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;"
              >
                <h2 style="margin:0;">Leads</h2>
                <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
                  <input
                    type="search"
                    id="extLeadsSearch"
                    placeholder="Search company, phone, or emails…"
                    aria-label="Search leads by company, phone, or a pasted list of emails"
                    style="padding:8px 10px; border-radius:8px; border:1px solid rgba(255,255,255,0.14); background:rgba(255,255,255,0.04); color:inherit; font:inherit; min-width:200px;"
                  />
                  <div id="extLeadsFilterWrap" style="position:relative;">
                    <button
                      type="button"
                      id="extLeadsFilterBtn"
                      class="badge"
                      aria-haspopup="true"
                      aria-expanded="false"
                      style="background:transparent; cursor:pointer; font:inherit; padding:8px 12px;"
                    >Filter <span id="extLeadsFilterCount"></span> ▾</button>
                    <div
                      id="extLeadsFilterPopup"
                      style="display:none; position:absolute; right:0; top:calc(100% + 6px); z-index:60; width:240px; max-height:72vh; overflow:auto; flex-direction:column; gap:10px; padding:14px; background:#11162a; border:1px solid rgba(255,255,255,0.14); border-radius:12px; box-shadow:0 12px 32px rgba(0,0,0,0.45);"
                    >
                      ${(() => {
                        const lblStyle =
                          "display:flex; flex-direction:column; gap:4px; font-size:12px; font-weight:700; letter-spacing:0.02em; color:#9aa3c0;";
                        const selStyle =
                          "width:100%; padding:8px; border-radius:8px; border:1px solid rgba(255,255,255,0.14); background:rgba(255,255,255,0.04); color:inherit; font:inherit;";
                        const sel = (id, label, options) =>
                          `<label style="${lblStyle}">${label}<select id="${id}" style="${selStyle}"><option value="">All</option>${options}</select></label>`;
                        const textInput = (id, label, placeholder) =>
                          `<label style="${lblStyle}">${label}<input type="text" id="${id}" placeholder="${escapeHtml(placeholder)}" style="${selStyle}" /></label>`;
                        const dateRange = (fromId, toId, label) =>
                          `<div style="${lblStyle}">${label}<input type="date" id="${fromId}" aria-label="${label} from" style="${selStyle} color-scheme:dark;" /><input type="date" id="${toId}" aria-label="${label} to" style="${selStyle} color-scheme:dark;" /></div>`;
                        // Collapsed multi-select — mirrors the internal filter
                        // popup's control: a select-like button summarizing the
                        // selection ("All" / one label / "N selected") over an
                        // inline checkbox list. The hidden input carries the
                        // comma-separated keys so it reads like the plain
                        // selects above (see the filter script below).
                        const multiSel = (id, label, options) =>
                          `<div style="${lblStyle}">${label}
                            <div class="ext-ms" data-target="${id}" style="position:relative;">
                              <button type="button" class="ext-ms-btn" style="${selStyle} display:flex; justify-content:space-between; align-items:center; gap:6px; cursor:pointer; text-align:left;"><span class="ext-ms-summary">All</span><span style="opacity:.6;">▾</span></button>
                              <div class="ext-ms-panel" style="display:none; flex-direction:column; gap:2px; padding:8px; border:1px solid rgba(255,255,255,0.14); border-radius:8px; background:rgba(255,255,255,0.04); max-height:160px; overflow:auto; margin-top:4px;">
                                ${options
                                  .map(
                                    (o) =>
                                      `<label style="display:flex; align-items:center; gap:8px; font-size:12px; font-weight:400; letter-spacing:0; color:inherit; cursor:pointer;"><input type="checkbox" class="ext-ms-cb" value="${escapeHtml(o.key)}" data-label="${escapeHtml(o.label)}" /> ${escapeHtml(o.label)}</label>`,
                                  )
                                  .join("")}
                              </div>
                              <input type="hidden" id="${id}" value="" />
                            </div>
                          </div>`;
                        const opts = (list) =>
                          list
                            .map(
                              (o) =>
                                `<option value="${escapeHtml(o.key)}">${escapeHtml(o.label)}</option>`,
                            )
                            .join("");
                        // Option sets mirror the internal Leads tab exactly,
                        // including its "none / never set" entries.
                        const stageOpts = opts([
                          { key: "__none__", label: "None (never set)" },
                          ...CLIENT_LEAD_PIPELINE_STAGES,
                        ]);
                        const demoOpts = opts([
                          { key: "__none__", label: "None (never set)" },
                          ...CLIENT_LEAD_DEMO_STATUSES,
                        ]);
                        const reachOptions = [
                          { key: "__none__", label: "None (not reached)" },
                          ...REACH_VIA_CHANNELS.map((c) => ({
                            key: c.key,
                            label: c.label,
                          })),
                          { key: "both", label: "LinkedIn + Email" },
                        ];
                        const notesOpts = opts([
                          { key: "none", label: "No notes" },
                          { key: "added", label: "Has notes" },
                          { key: "multiple", label: "Multiple notes" },
                        ]);
                        const audioOpts = opts([
                          { key: "yes", label: "Has audio" },
                          { key: "no", label: "No audio" },
                        ]);
                        const byOpts = opts([
                          { key: "__none__", label: "No notes" },
                          ...extNoteAuthors.map((n) => ({
                            key: n.toLowerCase(),
                            label: n,
                          })),
                        ]);
                        const assigneeOpts = opts([
                          { key: "__unassigned__", label: "Unassigned" },
                          ...extAssignees.map((n) => ({
                            key: n.toLowerCase(),
                            label: n,
                          })),
                        ]);
                        const categoryOptions = [
                          { key: "__none__", label: "None (no category)" },
                          ...CLIENT_LEAD_CATEGORY_TYPES,
                        ];
                        return [
                          sel("extLeadsStageFilter", "STATUS", stageOpts),
                          sel("extLeadsDemoFilter", "DEMO", demoOpts),
                          multiSel(
                            "extLeadsCategoryFilter",
                            "CATEGORY TYPE",
                            categoryOptions,
                          ),
                          `<div style="${lblStyle}">LOCATION OF LEAD
                            <input type="text" id="extLeadsLocationFilter" placeholder="City, state, or country" style="${selStyle}" />
                            <label style="display:flex; align-items:center; gap:8px; font-size:12px; font-weight:400; letter-spacing:0; color:inherit; cursor:pointer;"><input type="checkbox" id="extLeadsLocationNone" /> None (no location data)</label>
                          </div>`,
                          extAssignees.length
                            ? sel("extLeadsAssigneeFilter", "ASSIGNED TO", assigneeOpts)
                            : "",
                          sel(
                            "extLeadsPhoneFilter",
                            "LEAD WITH NUMBER",
                            `<option value="yes">Yes</option><option value="no">No</option>`,
                          ),
                          multiSel(
                            "extLeadsReachFilter",
                            "REACHED VIA",
                            reachOptions,
                          ),
                          sel("extLeadsNotesFilter", "NOTES", notesOpts),
                          sel("extLeadsNoteAudioFilter", "NOTES AUDIO", audioOpts),
                          sel("extLeadsNoteByFilter", "NOTES BY", byOpts),
                          dateRange(
                            "extLeadsUpdatedFrom",
                            "extLeadsUpdatedTo",
                            "UPDATED AT",
                          ),
                          dateRange(
                            "extLeadsCallbackFrom",
                            "extLeadsCallbackTo",
                            "CALLBACK DATE",
                          ),
                          sel(
                            "extLeadsMissedCallbackFilter",
                            "MISSED CALLBACK",
                            `<option value="yes">Yes — overdue (past)</option><option value="no">No — upcoming (future)</option><option value="none">No callback date set</option>`,
                          ),
                          `<button type="button" id="extLeadsFilterClear" class="badge" style="background:transparent; cursor:pointer; font:inherit; margin-top:4px;">Clear filters</button>`,
                        ].join("");
                      })()}
                    </div>
                  </div>
                </div>
              </div>
              ${
                extCategoryCounts.length
                  ? `<div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; padding:0 2px 14px;">
                      <button type="button" class="badge ext-category-pill" data-category="" style="cursor:pointer; font:inherit; border:1px solid rgba(255,255,255,0.14); background:rgba(255,255,255,0.04);">All</button>
                      ${extCategoryCounts
                        .map((c) => {
                          const label = CLIENT_LEAD_CATEGORY_TYPE_LABELS[c.key] || c.key;
                          return `<button type="button" class="badge ext-category-pill" data-category="${escapeHtml(c.key)}" style="cursor:pointer; font:inherit; border:1px solid rgba(255,255,255,0.14); background:rgba(255,255,255,0.04);">${escapeHtml(label)} <span style="opacity:.7;">${c.count}</span></button>`;
                        })
                        .join("")}
                    </div>`
                  : ""
              }
              <div
                class="extLeadsPager"
                data-pos="top"
                style="display:none; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; padding:2px 2px 14px;"
              >
                <div class="meta extLeadsPageInfo"></div>
                <div style="display:flex; gap:6px; align-items:center;">
                  <button type="button" class="badge extLeadsPrev" style="background:transparent; cursor:pointer; font:inherit;">← Prev</button>
                  <span class="meta extLeadsPageLabel"></span>
                  <button type="button" class="badge extLeadsNext" style="background:transparent; cursor:pointer; font:inherit;">Next →</button>
                </div>
              </div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th class="ext-lead-sort" data-sort="name" style="cursor:pointer; user-select:none; white-space:nowrap;">Company / Contact <span class="ext-sort-arrow" style="opacity:.4;">↕</span></th>
                      <th style="text-align:left;">Phone / Email / Source</th>
                      <th class="ext-lead-sort" data-sort="stage" style="cursor:pointer; user-select:none; white-space:nowrap;">Stage <span class="ext-sort-arrow" style="opacity:.4;">↕</span></th>
                      <th class="ext-lead-sort" data-sort="demo" style="cursor:pointer; user-select:none; white-space:nowrap;">Demo <span class="ext-sort-arrow" style="opacity:.4;">↕</span></th>
                      <th class="ext-lead-sort" data-sort="notes" style="cursor:pointer; user-select:none; white-space:nowrap;">Notes <span class="ext-sort-arrow" style="opacity:.4;">↕</span></th>
                      <th style="text-align:left;">Reached Via</th>
                      <th class="ext-lead-sort" data-sort="updated" style="cursor:pointer; user-select:none; white-space:nowrap;">Updated At <span class="ext-sort-arrow" style="opacity:.4;">↕</span></th>
                    </tr>
                  </thead>
                  <tbody id="extLeadsBody">
                    ${
                      leads.length
                        ? leads
                            .map(
                              (l) => {
                                const noteHistory = parseLeadNotesHistory(l.notes);
                                const latestNote = noteHistory.length
                                  ? noteHistory[noteHistory.length - 1]
                                  : null;
                                const company = l.company || l.business_name || "(no name)";
                                // Sort metadata: stage/demo sort by their pipeline
                                // order (not alphabetically), updated_at by epoch.
                                const stageKey = l.pipeline_stage || "prospect_identified";
                                const demoKey = l.demo_status || "not_scheduled";
                                const stageIdx = CLIENT_LEAD_PIPELINE_STAGES.findIndex(
                                  (s) => s.key === stageKey,
                                );
                                const demoIdx = CLIENT_LEAD_DEMO_STATUSES.findIndex(
                                  (s) => s.key === demoKey,
                                );
                                const updatedMs = l.updated_at
                                  ? new Date(l.updated_at).getTime()
                                  : 0;
                                // Calendar date (IST) of the update, for the
                                // from/to date-range filter — matches the shown date.
                                const updatedDate = l.updated_at
                                  ? getDateStringInTimeZone(
                                      new Date(l.updated_at),
                                      APP_TIMEZONE,
                                    )
                                  : "";
                                // Filter metadata (mirrors the internal leads filters).
                                const reachChannels = REACH_VIA_CHANNELS.filter(
                                  (c) => l[c.column],
                                );
                                const reachKeys = reachChannels
                                  .map((c) => c.key)
                                  .join(",");
                                const reachLabel = reachChannels.length
                                  ? reachChannels.map((c) => c.label).join(", ")
                                  : "-";
                                const noteAudio = noteHistory.some(
                                  (n) => n && n.audio_url,
                                )
                                  ? "1"
                                  : "0";
                                const noteBy = Array.from(
                                  new Set(
                                    noteHistory.map((n) =>
                                      String((n && n.by) || "")
                                        .trim()
                                        .toLowerCase(),
                                    ),
                                  ),
                                )
                                  .filter(Boolean)
                                  .join("|");
                                // Full history (newest first) rendered server-side so the
                                // public link can show it in a popup without an API call.
                                const notesHistoryHtml = noteHistory
                                  .slice()
                                  .reverse()
                                  .map((n) => {
                                    const when = n.at ? escapeHtml(formatDateTime(n.at)) : "";
                                    const byline = [n.by ? escapeHtml(n.by) : "", when]
                                      .filter(Boolean)
                                      .join(" · ");
                                    const audioHtml = n.audio_url
                                      ? `<audio controls preload="none" style="margin-top:4px; width:100%; max-width:240px; height:30px;"><source src="${escapeHtml(n.audio_url)}" /></audio>`
                                      : "";
                                    return `<div style="padding:8px 10px; border:1px solid var(--line); border-radius:8px;"><div style="white-space:pre-wrap;">${escapeHtml(n.text)}</div>${audioHtml}${byline ? `<div class="meta" style="font-size:11px; margin-top:4px;">${byline}</div>` : ""}</div>`;
                                  })
                                  .join("");
                                const loc = [l.city, l.state].filter(Boolean).join(", ");
                                const locationSearch = [l.city, l.state, l.country]
                                  .filter(Boolean)
                                  .join(" ")
                                  .toLowerCase();
                                const categoryKey = String(l.category_type || "").trim();
                                const callbackDate = String(l.callback_date || "").trim();
                                const callbackOverdue =
                                  callbackDate && callbackDate < extTodayStr;
                                const assignee = String(l.assigned_to || "")
                                  .trim()
                                  .toLowerCase();
                                const email = String(l.email || "")
                                  .trim()
                                  .toLowerCase();
                                // Free-text search haystack — same columns the
                                // internal Leads tab searches (company, contact,
                                // phone, email, source, assignee, location, notes).
                                const searchHaystack = [
                                  company,
                                  l.contact_name,
                                  l.phone,
                                  l.email,
                                  l.lead_source,
                                  l.assigned_to,
                                  l.city,
                                  l.state,
                                  l.country,
                                  noteHistory.map((n) => n && n.text).join(" "),
                                ]
                                  .filter(Boolean)
                                  .join(" ")
                                  .toLowerCase();
                                // Raw stage/demo keys (empty when never set) so the
                                // "None (never set)" filter can tell a null column
                                // from one explicitly set to the default.
                                const stageRaw = String(l.pipeline_stage || "");
                                const demoRaw = String(l.demo_status || "");
                                return `
                        <tr class="ext-lead-row" data-stage="${escapeHtml(stageRaw)}" data-demo="${escapeHtml(demoRaw)}" data-reach="${escapeHtml(reachKeys)}" data-notescount="${noteHistory.length}" data-noteaudio="${noteAudio}" data-noteby="${escapeHtml(noteBy)}" data-category="${escapeHtml(categoryKey)}" data-location="${escapeHtml(locationSearch)}" data-callback="${escapeHtml(callbackDate)}" data-phone="${l.phone ? "1" : "0"}" data-assignee="${escapeHtml(assignee)}" data-email="${escapeHtml(email)}" data-search="${escapeHtml(searchHaystack)}" data-name="${escapeHtml(company.toLowerCase())}" data-stageidx="${stageIdx}" data-demoidx="${demoIdx}" data-updated="${updatedMs}" data-updateddate="${escapeHtml(updatedDate)}">
                          <td>
                            <strong>${escapeHtml(company)}</strong>
                            ${l.contact_name ? `<div class="meta">${escapeHtml(l.contact_name)}</div>` : ""}
                            ${loc ? `<div class="meta">${escapeHtml(loc)}</div>` : ""}
                            ${callbackDate ? `<div style="font-size:12px; font-weight:700; margin-top:2px; color:${callbackOverdue ? "#ef4444" : "#22c55e"};">Callback: ${escapeHtml(formatDateOnly(callbackDate))}</div>` : ""}
                          </td>
                          <td style="width:180px; font-size:12px; word-break:break-word;">
                            <div>${escapeHtml(l.phone || "-")}</div>
                            <div class="meta">${escapeHtml(l.email || "-")}</div>
                            <div class="meta">${escapeHtml(l.lead_source || "-")}</div>
                          </td>
                          <td><span class="badge">${escapeHtml(clientLeadStatusLabel(CLIENT_LEAD_PIPELINE_STAGES, l.pipeline_stage || "prospect_identified", "Prospect Identified"))}</span></td>
                          <td>${escapeHtml(clientLeadStatusLabel(CLIENT_LEAD_DEMO_STATUSES, l.demo_status || "not_scheduled", "Not Scheduled"))}</td>
                          <td style="max-width:360px;">
                            ${
                              latestNote
                                ? `<div style="font-size:12px; white-space:pre-wrap; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; text-overflow:ellipsis; cursor:pointer;" title="Click to view full note${noteHistory.length > 1 ? " history" : ""}" data-company="${escapeHtml(company)}" onclick="openExtLeadNotes(this)">${escapeHtml(latestNote.text)}</div>
                                   ${latestNote.audio_url ? `<audio controls preload="none" style="margin-top:4px; width:100%; max-width:240px; height:30px;"><source src="${escapeHtml(latestNote.audio_url)}" /></audio>` : ""}
                                   ${noteHistory.length > 1 ? `<div class="meta" style="font-size:11px; cursor:pointer; text-decoration:underline;" data-company="${escapeHtml(company)}" onclick="openExtLeadNotes(this)">+${noteHistory.length - 1} earlier note${noteHistory.length - 1 === 1 ? "" : "s"}</div>` : ""}
                                   <div class="ext-lead-notes-data" style="display:none;">${notesHistoryHtml}</div>`
                                : `<span class="meta">-</span>`
                            }
                          </td>
                          <td class="meta" style="font-size:12px;">${escapeHtml(reachLabel)}</td>
                          <td class="meta" style="white-space:nowrap;">${l.updated_at ? escapeHtml(formatDateTime(l.updated_at)) : "-"}</td>
                        </tr>`;
                              },
                            )
                            .join("")
                        : `<tr><td colspan="7" class="meta">No leads shared yet.</td></tr>`
                    }
                    <tr id="extLeadsNoMatch" style="display:none;"><td colspan="7" class="meta">No leads match your filters.</td></tr>
                  </tbody>
                </table>
              </div>
              <div
                class="extLeadsPager"
                data-pos="bottom"
                style="display:none; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; padding:14px 2px 2px;"
              >
                <div class="meta extLeadsPageInfo"></div>
                <div style="display:flex; gap:6px; align-items:center;">
                  <button type="button" class="badge extLeadsPrev" style="background:transparent; cursor:pointer; font:inherit;">← Prev</button>
                  <span class="meta extLeadsPageLabel"></span>
                  <button type="button" class="badge extLeadsNext" style="background:transparent; cursor:pointer; font:inherit;">Next →</button>
                </div>
              </div>
              <script>
                (function () {
                  var PAGE_SIZE = 25;
                  var tbody = document.getElementById("extLeadsBody");
                  var toArr = function (nl) {
                    return Array.prototype.slice.call(nl);
                  };
                  var pagers = toArr(document.querySelectorAll(".extLeadsPager"));
                  if (!tbody || !pagers.length) return;
                  var rows = toArr(tbody.querySelectorAll("tr.ext-lead-row"));
                  if (!rows.length) return;
                  var infos = toArr(document.querySelectorAll(".extLeadsPageInfo"));
                  var labels = toArr(document.querySelectorAll(".extLeadsPageLabel"));
                  var prevs = toArr(document.querySelectorAll(".extLeadsPrev"));
                  var nexts = toArr(document.querySelectorAll(".extLeadsNext"));
                  var noMatch = document.getElementById("extLeadsNoMatch");
                  var searchInput = document.getElementById("extLeadsSearch");
                  var stageInput = document.getElementById("extLeadsStageFilter");
                  var demoInput = document.getElementById("extLeadsDemoFilter");
                  var reachInput = document.getElementById("extLeadsReachFilter");
                  var notesInput = document.getElementById("extLeadsNotesFilter");
                  var audioInput = document.getElementById("extLeadsNoteAudioFilter");
                  var byInput = document.getElementById("extLeadsNoteByFilter");
                  var categoryInput = document.getElementById("extLeadsCategoryFilter");
                  var assigneeInput = document.getElementById("extLeadsAssigneeFilter");
                  var locationInput = document.getElementById("extLeadsLocationFilter");
                  var locationNoneInput = document.getElementById("extLeadsLocationNone");
                  var updatedFromInput = document.getElementById("extLeadsUpdatedFrom");
                  var updatedToInput = document.getElementById("extLeadsUpdatedTo");
                  var callbackFromInput = document.getElementById("extLeadsCallbackFrom");
                  var callbackToInput = document.getElementById("extLeadsCallbackTo");
                  var phoneInput = document.getElementById("extLeadsPhoneFilter");
                  var missedCallbackInput = document.getElementById("extLeadsMissedCallbackFilter");
                  // Today (IST calendar date) for the missed-callback comparison —
                  // mirrors the red/green callback badge (overdue = date < today).
                  var extTodayStr = "${extTodayStr}";
                  var filterBtn = document.getElementById("extLeadsFilterBtn");
                  var filterPopup = document.getElementById("extLeadsFilterPopup");
                  var filterCount = document.getElementById("extLeadsFilterCount");
                  var filterClear = document.getElementById("extLeadsFilterClear");
                  var categoryPills = toArr(document.querySelectorAll(".ext-category-pill"));
                  // Collapsed multi-selects (CATEGORY TYPE, REACHED VIA): each
                  // wires its checkbox list to a hidden input holding the
                  // comma-separated selected keys, and keeps its button summary
                  // in sync ("All" / one label / "N selected"). Mirrors the
                  // internal Leads tab's multi-select controls.
                  var msWraps = toArr(document.querySelectorAll(".ext-ms"));
                  msWraps.forEach(function (wrap) {
                    var btn = wrap.querySelector(".ext-ms-btn");
                    var panel = wrap.querySelector(".ext-ms-panel");
                    var summary = wrap.querySelector(".ext-ms-summary");
                    var hidden = wrap.querySelector("input[type=hidden]");
                    var cbs = toArr(wrap.querySelectorAll(".ext-ms-cb"));
                    function syncSummary() {
                      var picked = cbs.filter(function (c) { return c.checked; });
                      summary.textContent =
                        picked.length === 0
                          ? "All"
                          : picked.length === 1
                            ? picked[0].getAttribute("data-label")
                            : picked.length + " selected";
                      hidden.value = picked
                        .map(function (c) { return c.value; })
                        .join(",");
                    }
                    if (btn)
                      btn.addEventListener("click", function (e) {
                        e.stopPropagation();
                        panel.style.display =
                          panel.style.display === "flex" ? "none" : "flex";
                      });
                    cbs.forEach(function (c) {
                      c.addEventListener("change", function () {
                        syncSummary();
                        applyFilters();
                      });
                    });
                    // Expose a reset used by "Clear filters".
                    wrap._reset = function () {
                      cbs.forEach(function (c) { c.checked = false; });
                      syncSummary();
                    };
                    syncSummary();
                  });
                  // Plain <select> filters (single-value). Multi-selects carry
                  // their value on a hidden input included here so the badge
                  // count and clear logic treat them uniformly.
                  var selects = [
                    stageInput,
                    demoInput,
                    categoryInput,
                    assigneeInput,
                    phoneInput,
                    reachInput,
                    notesInput,
                    audioInput,
                    byInput,
                    missedCallbackInput,
                  ];
                  var cur = 1;
                  var filtered = rows;
                  var sortHeaders = toArr(document.querySelectorAll(".ext-lead-sort"));
                  // Numeric columns sort high→low on first click; text sorts A→Z.
                  var NUMERIC_SORTS = { stage: 1, demo: 1, notes: 1, updated: 1 };
                  var sortKey = "";
                  var sortDir = 1; // 1 = ascending, -1 = descending
                  function sortValue(row, key) {
                    if (key === "name")
                      return row.getAttribute("data-name") || "";
                    if (key === "stage")
                      return Number(row.getAttribute("data-stageidx") || -1);
                    if (key === "demo")
                      return Number(row.getAttribute("data-demoidx") || -1);
                    if (key === "notes")
                      return Number(row.getAttribute("data-notescount") || 0);
                    if (key === "updated")
                      return Number(row.getAttribute("data-updated") || 0);
                    return "";
                  }
                  function sortFiltered() {
                    if (!sortKey) return;
                    filtered.sort(function (a, b) {
                      var av = sortValue(a, sortKey);
                      var bv = sortValue(b, sortKey);
                      if (av < bv) return -1 * sortDir;
                      if (av > bv) return 1 * sortDir;
                      return 0;
                    });
                    // The page slice is revealed via display toggling, so the DOM
                    // must physically match the sorted order to render correctly.
                    for (var i = 0; i < filtered.length; i++)
                      tbody.appendChild(filtered[i]);
                  }
                  function updateSortArrows() {
                    for (var i = 0; i < sortHeaders.length; i++) {
                      var th = sortHeaders[i];
                      var arrow = th.querySelector(".ext-sort-arrow");
                      if (!arrow) continue;
                      if (th.getAttribute("data-sort") === sortKey) {
                        arrow.textContent = sortDir === 1 ? "↑" : "↓";
                        arrow.style.opacity = "1";
                      } else {
                        arrow.textContent = "↕";
                        arrow.style.opacity = ".4";
                      }
                    }
                  }
                  function setDisabled(btns, dis) {
                    for (var i = 0; i < btns.length; i++) {
                      var btn = btns[i];
                      if (!btn) continue;
                      btn.disabled = dis;
                      btn.style.opacity = dis ? "0.4" : "1";
                      btn.style.pointerEvents = dis ? "none" : "auto";
                      btn.style.cursor = dis ? "default" : "pointer";
                    }
                  }
                  function setText(els, txt) {
                    for (var i = 0; i < els.length; i++)
                      if (els[i]) els[i].textContent = txt;
                  }
                  function val(el) {
                    return (el && el.value) || "";
                  }
                  function applyFilters() {
                    var q = (searchInput && searchInput.value || "")
                      .trim()
                      .toLowerCase();
                    // Default stage/demo keys — a lead with a null column falls
                    // back to these in the UI, so filtering on the default value
                    // also matches never-set rows (mirrors the internal filter).
                    var DEFAULT_STAGE = "${DEFAULT_CLIENT_LEAD_STAGE}";
                    var DEFAULT_DEMO = "${CLIENT_LEAD_DEMO_STATUSES[0].key}";
                    var stage = val(stageInput);
                    var demo = val(demoInput);
                    // Multi-selects: comma-separated key lists.
                    var reachKeys = val(reachInput)
                      .split(",")
                      .filter(Boolean);
                    var categoryKeys = val(categoryInput)
                      .split(",")
                      .filter(Boolean);
                    var notes = val(notesInput);
                    var audio = val(audioInput);
                    var by = val(byInput);
                    var assignee = val(assigneeInput);
                    var location = (locationInput && locationInput.value || "")
                      .trim()
                      .toLowerCase();
                    var locationNone = !!(locationNoneInput && locationNoneInput.checked);
                    var updatedFrom = val(updatedFromInput);
                    var updatedTo = val(updatedToInput);
                    var callbackFrom = val(callbackFromInput);
                    var callbackTo = val(callbackToInput);
                    var hasPhone = val(phoneInput);
                    var missedCallback = val(missedCallbackInput);
                    filtered = rows.filter(function (r) {
                      var rStage = r.getAttribute("data-stage") || "";
                      if (stage) {
                        if (stage === "__none__") {
                          if (rStage !== "") return false;
                        } else if (stage === DEFAULT_STAGE) {
                          if (rStage !== DEFAULT_STAGE && rStage !== "") return false;
                        } else if (rStage !== stage) {
                          return false;
                        }
                      }
                      var rDemo = r.getAttribute("data-demo") || "";
                      if (demo) {
                        if (demo === "__none__") {
                          if (rDemo !== "") return false;
                        } else if (demo === DEFAULT_DEMO) {
                          if (rDemo !== DEFAULT_DEMO && rDemo !== "") return false;
                        } else if (rDemo !== demo) {
                          return false;
                        }
                      }
                      if (categoryKeys.length) {
                        var rc = r.getAttribute("data-category") || "";
                        var catOk = categoryKeys.some(function (k) {
                          return k === "__none__" ? rc === "" : rc === k;
                        });
                        if (!catOk) return false;
                      }
                      if (assignee) {
                        var ra = r.getAttribute("data-assignee") || "";
                        if (assignee === "__unassigned__") {
                          if (ra !== "") return false;
                        } else if (ra !== assignee) {
                          return false;
                        }
                      }
                      if (locationNone) {
                        if ((r.getAttribute("data-location") || "") !== "")
                          return false;
                      }
                      if (
                        location &&
                        (r.getAttribute("data-location") || "").indexOf(location) === -1
                      )
                        return false;
                      if (reachKeys.length) {
                        var ch = (r.getAttribute("data-reach") || "")
                          .split(",")
                          .filter(Boolean);
                        // Match a lead reached via ANY selected channel. "both"
                        // needs LinkedIn + Email; "__none__" needs no channel.
                        var reachOk = reachKeys.some(function (k) {
                          if (k === "both")
                            return ch.indexOf("linkedin") !== -1 && ch.indexOf("email") !== -1;
                          if (k === "__none__") return ch.length === 0;
                          return ch.indexOf(k) !== -1;
                        });
                        if (!reachOk) return false;
                      }
                      if (notes) {
                        var nc = Number(r.getAttribute("data-notescount") || 0);
                        if (notes === "none" && nc > 0) return false;
                        if (notes === "added" && nc < 1) return false;
                        if (notes === "multiple" && nc < 2) return false;
                      }
                      if (audio) {
                        var ha = r.getAttribute("data-noteaudio") === "1";
                        if (audio === "yes" && !ha) return false;
                        if (audio === "no" && ha) return false;
                      }
                      if (by) {
                        var authors = (r.getAttribute("data-noteby") || "")
                          .split("|")
                          .filter(Boolean);
                        if (by === "__none__") {
                          if (authors.length > 0) return false;
                        } else if (authors.indexOf(by) === -1) {
                          return false;
                        }
                      }
                      if (updatedFrom || updatedTo) {
                        // Compare YYYY-MM-DD strings (IST calendar date). Rows with
                        // no update date are excluded once a bound is set.
                        var ud = r.getAttribute("data-updateddate") || "";
                        if (!ud) return false;
                        if (updatedFrom && ud < updatedFrom) return false;
                        if (updatedTo && ud > updatedTo) return false;
                      }
                      if (callbackFrom || callbackTo) {
                        var cb = r.getAttribute("data-callback") || "";
                        if (!cb) return false;
                        if (callbackFrom && cb < callbackFrom) return false;
                        if (callbackTo && cb > callbackTo) return false;
                      }
                      if (hasPhone) {
                        var hp = r.getAttribute("data-phone") === "1";
                        if (hasPhone === "yes" && !hp) return false;
                        if (hasPhone === "no" && hp) return false;
                      }
                      if (missedCallback) {
                        var mcb = r.getAttribute("data-callback") || "";
                        // "none" = no callback date set at all.
                        if (missedCallback === "none") {
                          if (mcb) return false;
                        } else {
                          // Only leads with a callback date qualify (no badge =
                          // no match). "yes" = overdue (past); "no" = upcoming.
                          if (!mcb) return false;
                          var overdue = mcb < extTodayStr;
                          if (missedCallback === "yes" && !overdue) return false;
                          if (missedCallback === "no" && overdue) return false;
                        }
                      }
                      if (
                        q &&
                        (r.getAttribute("data-search") || "").indexOf(q) === -1
                      )
                        return false;
                      return true;
                    });
                    cur = 1;
                    updateFilterCount();
                    updateCategoryPillActive();
                    render();
                  }
                  function updateFilterCount() {
                    if (!filterCount) return;
                    var n = 0;
                    for (var i = 0; i < selects.length; i++)
                      if (val(selects[i])) n++;
                    if (
                      (locationInput && locationInput.value.trim()) ||
                      (locationNoneInput && locationNoneInput.checked)
                    )
                      n++;
                    if (val(updatedFromInput) || val(updatedToInput)) n++;
                    if (val(callbackFromInput) || val(callbackToInput)) n++;
                    filterCount.textContent = n ? "(" + n + ")" : "";
                  }
                  function render() {
                    sortFiltered();
                    var total = filtered.length;
                    var totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
                    if (cur > totalPages) cur = totalPages;
                    var start = (cur - 1) * PAGE_SIZE;
                    var end = start + PAGE_SIZE;
                    // Hide all rows, then reveal only the current page's slice.
                    for (var i = 0; i < rows.length; i++)
                      rows[i].style.display = "none";
                    for (var j = start; j < end && j < total; j++)
                      filtered[j].style.display = "";
                    if (noMatch)
                      noMatch.style.display = total ? "none" : "";
                    setText(
                      infos,
                      total
                        ? "Showing " +
                            (start + 1) +
                            "–" +
                            Math.min(end, total) +
                            " of " +
                            total
                        : "0 results",
                    );
                    setText(labels, "Page " + cur + " of " + totalPages);
                    setDisabled(prevs, cur <= 1);
                    setDisabled(nexts, cur >= totalPages);
                    // Only show the pagers when paging actually applies.
                    for (var p = 0; p < pagers.length; p++)
                      pagers[p].style.display = total > PAGE_SIZE ? "flex" : "none";
                  }
                  for (var pi = 0; pi < prevs.length; pi++)
                    prevs[pi].addEventListener("click", function () {
                      if (cur > 1) {
                        cur--;
                        render();
                      }
                    });
                  for (var ni = 0; ni < nexts.length; ni++)
                    nexts[ni].addEventListener("click", function () {
                      var totalPages = Math.max(
                        1,
                        Math.ceil(filtered.length / PAGE_SIZE),
                      );
                      if (cur < totalPages) {
                        cur++;
                        render();
                      }
                    });
                  if (searchInput)
                    searchInput.addEventListener("input", applyFilters);
                  for (var s = 0; s < selects.length; s++)
                    if (selects[s])
                      selects[s].addEventListener("change", applyFilters);
                  if (updatedFromInput)
                    updatedFromInput.addEventListener("change", applyFilters);
                  if (updatedToInput)
                    updatedToInput.addEventListener("change", applyFilters);
                  if (locationInput)
                    locationInput.addEventListener("input", applyFilters);
                  if (locationNoneInput)
                    locationNoneInput.addEventListener("change", applyFilters);
                  if (callbackFromInput)
                    callbackFromInput.addEventListener("change", applyFilters);
                  if (callbackToInput)
                    callbackToInput.addEventListener("change", applyFilters);
                  // The CATEGORY TYPE control is a multi-select; find its wrap so
                  // the pill row can drive its checkboxes directly.
                  var categoryWrap = null;
                  for (var mw = 0; mw < msWraps.length; mw++)
                    if (msWraps[mw].getAttribute("data-target") === "extLeadsCategoryFilter")
                      categoryWrap = msWraps[mw];
                  // Category Type pills — clicking one selects that single
                  // category in the popup's multi-select (or "All" clears it)
                  // and re-filters; the active pill is highlighted to match.
                  function updateCategoryPillActive() {
                    var keys = val(categoryInput).split(",").filter(Boolean);
                    // A pill is "active" only when it's the sole selection.
                    var active = keys.length === 1 ? keys[0] : "";
                    for (var i = 0; i < categoryPills.length; i++) {
                      var pill = categoryPills[i];
                      var isActive = pill.getAttribute("data-category") === active;
                      pill.style.background = isActive
                        ? "#8b7cf6"
                        : "rgba(255,255,255,0.04)";
                      pill.style.borderColor = isActive
                        ? "#8b7cf6"
                        : "rgba(255,255,255,0.14)";
                      pill.style.color = isActive ? "#fff" : "inherit";
                    }
                  }
                  function setCategorySelection(key) {
                    if (!categoryWrap) return;
                    var cbs = toArr(categoryWrap.querySelectorAll(".ext-ms-cb"));
                    cbs.forEach(function (c) {
                      c.checked = !!key && c.value === key;
                    });
                    // Re-derive the hidden input + button summary.
                    var picked = cbs.filter(function (c) { return c.checked; });
                    var summary = categoryWrap.querySelector(".ext-ms-summary");
                    if (summary)
                      summary.textContent =
                        picked.length === 0
                          ? "All"
                          : picked.length === 1
                            ? picked[0].getAttribute("data-label")
                            : picked.length + " selected";
                    categoryInput.value = picked
                      .map(function (c) { return c.value; })
                      .join(",");
                  }
                  for (var cp = 0; cp < categoryPills.length; cp++) {
                    categoryPills[cp].addEventListener("click", function () {
                      setCategorySelection(this.getAttribute("data-category") || "");
                      updateCategoryPillActive();
                      applyFilters();
                    });
                  }
                  // Sortable column headers: click to sort, click again to flip.
                  for (var h = 0; h < sortHeaders.length; h++) {
                    sortHeaders[h].addEventListener("click", function () {
                      var key = this.getAttribute("data-sort");
                      if (sortKey === key) {
                        sortDir = -sortDir;
                      } else {
                        sortKey = key;
                        sortDir = NUMERIC_SORTS[key] ? -1 : 1;
                      }
                      cur = 1;
                      updateSortArrows();
                      render();
                    });
                  }
                  // Filter popup open/close.
                  if (filterBtn && filterPopup) {
                    filterBtn.addEventListener("click", function (e) {
                      e.stopPropagation();
                      var open = filterPopup.style.display === "flex";
                      filterPopup.style.display = open ? "none" : "flex";
                      filterBtn.setAttribute("aria-expanded", open ? "false" : "true");
                    });
                    filterPopup.addEventListener("click", function (e) {
                      e.stopPropagation();
                    });
                    document.addEventListener("click", function () {
                      filterPopup.style.display = "none";
                      filterBtn.setAttribute("aria-expanded", "false");
                    });
                  }
                  if (filterClear)
                    filterClear.addEventListener("click", function () {
                      for (var i = 0; i < selects.length; i++)
                        if (selects[i]) selects[i].value = "";
                      // Reset the multi-select controls (checkboxes + summary +
                      // hidden value) so they don't drift from the cleared state.
                      msWraps.forEach(function (w) {
                        if (w._reset) w._reset();
                      });
                      if (locationInput) locationInput.value = "";
                      if (locationNoneInput) locationNoneInput.checked = false;
                      if (updatedFromInput) updatedFromInput.value = "";
                      if (updatedToInput) updatedToInput.value = "";
                      if (callbackFromInput) callbackFromInput.value = "";
                      if (callbackToInput) callbackToInput.value = "";
                      updateCategoryPillActive();
                      applyFilters();
                    });
                  updateCategoryPillActive();
                  updateFilterCount();
                  render();
                })();
              </script>
            </div>

            <div class="panel">
              <div class="panel-head">
                <h2>Highlighted Calls</h2>
              </div>
              <div class="meta" style="margin-bottom:12px;">High-quality conversations flagged by the team for your review.</div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Company / Contact</th>
                      <th>Stage</th>
                      <th>Recording</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      highlightedCalls.length
                        ? highlightedCalls
                            .map(
                              (l) => `
                      <tr>
                        <td><strong>${escapeHtml(l.company || l.business_name || "(no name)")}</strong>${l.contact_name ? `<div class="meta">${escapeHtml(l.contact_name)}</div>` : ""}</td>
                        <td><span class="badge">${escapeHtml(clientLeadStatusLabel(CLIENT_LEAD_PIPELINE_STAGES, l.pipeline_stage || "prospect_identified", "Prospect Identified"))}</span></td>
                        <td><audio controls preload="none" style="max-width:240px; vertical-align:middle;"><source src="${escapeHtml(l.call_recording_url)}" /></audio> <a href="${escapeHtml(l.call_recording_url)}" target="_blank" rel="noopener noreferrer">Open</a></td>
                      </tr>`,
                            )
                            .join("")
                        : `<tr><td colspan="3" class="meta">No highlighted calls shared yet.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-campaigns" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Campaign Tracking</h2>
              </div>

              <div class="kpi-grid">
                <div class="kpi-card"><div class="kpi-label">Campaigns</div><div class="kpi-value">${campaigns.length}</div></div>
                <div class="kpi-card"><div class="kpi-label">Active</div><div class="kpi-value">${campaigns.filter((c) => c.status === "active").length}</div></div>
                <div class="kpi-card"><div class="kpi-label">Total Outreach</div><div class="kpi-value">${totalSent}</div></div>
                <div class="kpi-card"><div class="kpi-label">Responses</div><div class="kpi-value">${totalResponses}</div></div>
                <div class="kpi-card"><div class="kpi-label">Positive Replies</div><div class="kpi-value">${totalPositiveReplies}</div></div>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Campaign</th>
                      <th>Type</th>
                      <th>Channel</th>
                      <th>Status</th>
                      <th>Sent</th>
                      <th>Responses</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      campaigns.length
                        ? campaigns
                            .map((c) => {
                              const sent = Number(c.sent_count) || 0;
                              const responses = Number(c.response_count) || 0;
                              const rate = sent
                                ? Math.round((responses / sent) * 100)
                                : 0;
                              return `
                        <tr>
                          <td><strong>${escapeHtml(c.name || "Untitled")}</strong></td>
                          <td><span class="badge">${escapeHtml(campaignLabel(c.campaign_type))}</span></td>
                          <td>${escapeHtml(c.channel || "-")}</td>
                          <td><span class="badge">${escapeHtml(c.status || "planned")}</span></td>
                          <td>${sent}</td>
                          <td>${responses}${sent ? ` (${rate}%)` : ""}</td>
                        </tr>`;
                            })
                            .join("")
                        : `<tr><td colspan="6" class="meta">No campaigns shared yet.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-meetings" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Demos Scheduled</h2>
              </div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Company / Contact</th>
                      <th>Demo Status</th>
                      <th>Stage</th>
                      <th>Owner</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      demoLeads.length
                        ? demoLeads
                            .map(
                              (l) => `
                        <tr>
                          <td><strong>${escapeHtml(l.company || l.business_name || "(no name)")}</strong>${l.contact_name ? `<div class="meta">${escapeHtml(l.contact_name)}</div>` : ""}</td>
                          <td><span class="badge">${escapeHtml(clientLeadStatusLabel(CLIENT_LEAD_DEMO_STATUSES, l.demo_status || "not_scheduled", "Not Scheduled"))}</span></td>
                          <td>${escapeHtml(clientLeadStatusLabel(CLIENT_LEAD_PIPELINE_STAGES, l.pipeline_stage || "prospect_identified", "Prospect Identified"))}</td>
                          <td>${escapeHtml(l.assigned_to || "-")}</td>
                        </tr>`,
                            )
                            .join("")
                        : `<tr><td colspan="4" class="meta">No demos scheduled yet.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>

            <div class="panel">
              <div class="panel-head">
                <h2>Sessions &amp; Strategy Calls (MOMs)</h2>
              </div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Meeting</th>
                      <th>Participants</th>
                      <th>Summary &amp; Next Steps</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      meetings.length
                        ? meetings
                            .map(
                              (m) => `
                        <tr>
                          <td>${escapeHtml(m.meeting_date || "-")}</td>
                          <td><strong>${escapeHtml(m.title || "Meeting")}</strong><div class="meta">${escapeHtml(meetingLabel(m.meeting_type))}</div></td>
                          <td>${escapeHtml(m.participants || "-")}</td>
                          <td>
                            ${m.summary ? escapeHtml(m.summary) : `<span class="meta">—</span>`}
                            ${m.action_items ? `<div class="meta" style="margin-top:6px;"><strong>Action items:</strong> ${escapeHtml(m.action_items)}</div>` : ""}
                            ${m.next_steps ? `<div class="meta" style="margin-top:4px;"><strong>Next:</strong> ${escapeHtml(m.next_steps)}</div>` : ""}
                          </td>
                        </tr>`,
                            )
                            .join("")
                        : `<tr><td colspan="4" class="meta">No sessions logged yet.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-blockers" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Pending From Your Side</h2>
              </div>
              <div class="meta" style="margin-bottom:12px;">Items awaiting approvals, responses, or actions from your team.</div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Item</th>
                      <th>Priority</th>
                      <th>Status</th>
                      <th>Logged</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      blockers.length
                        ? blockers
                            .map(
                              (b) => `
                        <tr>
                          <td><strong>${escapeHtml(b.title || "Pending item")}</strong>${b.description ? `<div class="meta">${escapeHtml(b.description)}</div>` : ""}</td>
                          <td>${escapeHtml(b.priority || "medium")}</td>
                          <td><span class="badge">${escapeHtml(String(b.resolution_status || "open").replaceAll("_", " "))}</span></td>
                          <td>${escapeHtml(b.created_at ? formatDateTime(b.created_at) : "-")}</td>
                        </tr>`,
                            )
                            .join("")
                        : `<tr><td colspan="4" class="meta">Nothing pending from your side. 🎉</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-reports" class="tab-panel">
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
            <div id="reportView-daily" class="report-subview">${renderSummaryWithGoals({ period: "daily", summaryRow: reportSummaries.daily, goalsRow: clientGoals, editable: false, clientId: client.id, users })}${dailyAutoReportHtml}${leadFunnelReportDailyHtml}</div>
            ${weeklyReports
              .map(
                (w) =>
                  `<div id="reportView-week${w.num}" class="report-subview" style="display:none;">${renderSummaryWithGoals({ period: "weekly", summaryRow: (reportSummaries.weeklyByDate || {})[w.weekStart] || null, goalsRow: clientGoals, editable: false, clientId: client.id, users, weekStart: w.weekStart, weekLabel: `Week ${w.displayNum}`, rangeLabel: w.rangeLabel })}${w.activityHtml}${w.funnelHtml}</div>`,
              )
              .join("")}

            <div class="panel">
              <div class="panel-head">
                <h2>Weekly Progress Reports</h2>
              </div>
              ${
                reports.length
                  ? reports
                      .map((r) => {
                        const period =
                          r.period_label ||
                          (r.week_start ? `Week of ${r.week_start}` : "Report");
                        const section = (label, value) =>
                          value
                            ? `<div style="margin-top:8px;"><strong>${escapeHtml(label)}:</strong><div class="meta" style="white-space:pre-wrap;">${escapeHtml(value)}</div></div>`
                            : "";
                        return `
                      <div class="work-owner-block">
                        <div style="display:flex; justify-content:space-between; gap:10px; flex-wrap:wrap;">
                          <strong>${escapeHtml(period)}</strong>
                          <span class="meta">${escapeHtml(r.created_at ? formatDateTime(r.created_at) : "")}</span>
                        </div>
                        ${section("Summary", r.summary)}
                        ${section("Highlights", r.highlights)}
                        ${section("Lowlights / Risks", r.lowlights)}
                        ${section("Next Week Plan", r.next_week_plan)}
                      </div>`;
                      })
                      .join("")
                  : `<div class="meta">No weekly reports published yet.</div>`
              }
            </div>
          </div>

          <div id="clientViewTab-work" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Tasks</h2>
              </div>
              <div class="meta" style="margin-bottom:14px;">Tasks the team is actively driving for you.</div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Task</th>
                      <th>Status</th>
                      <th>Priority</th>
                      <th>Progress</th>
                      <th>Due</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      linkedTasks.length
                        ? linkedTasks
                            .map(
                              (t) => `
                          <tr>
                            <td><strong>#${escapeHtml(t.task_no || t.id)} · ${escapeHtml(t.title || "Untitled")}</strong>${t.area ? `<div class="meta">${escapeHtml(t.area)}</div>` : ""}</td>
                            <td><span class="badge">${escapeHtml(String(t.status || "open").replace("_", " "))}</span></td>
                            <td>${escapeHtml(t.priority || "-")}</td>
                            <td>${Number(t.progress) || 0}%</td>
                            <td>${escapeHtml(t.deadline || "-")}</td>
                          </tr>`,
                            )
                            .join("")
                        : `<tr><td colspan="5" class="meta">No tasks shared yet.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>

            <div class="panel">
              <div class="panel-head">
                <h2>Work Progress</h2>
              </div>
              <div class="meta" style="margin-bottom:14px;">Ongoing activities and completion tracking by team member.</div>

              ${
                workOwnerGroups.length
                  ? workOwnerGroups
                      .map(
                        (g) => `
                    <div class="work-owner-block">
                      <div style="display:flex; justify-content:space-between; gap:10px; align-items:baseline; flex-wrap:wrap;">
                        <strong>${escapeHtml(g.name)}</strong>
                        <span class="meta">${g.done}/${g.total} complete · ${g.pct}%</span>
                      </div>
                      <div class="progress-track"><div class="progress-fill" style="width:${g.pct}%;"></div></div>
                      <div class="table-wrap" style="margin-top:12px;">
                        <table>
                          <thead>
                            <tr>
                              <th>Work Item</th>
                              <th>Status</th>
                              <th>Priority</th>
                              <th>Due Date</th>
                            </tr>
                          </thead>
                          <tbody>
                            ${g.items
                              .map(
                                (w) => `
                              <tr>
                                <td><strong>${escapeHtml(w.title || "Work item")}</strong>${w.description ? `<div class="meta">${escapeHtml(w.description)}</div>` : ""}</td>
                                <td><span class="badge">${escapeHtml(w.status || "todo")}</span></td>
                                <td>${escapeHtml(w.priority || "-")}</td>
                                <td>${escapeHtml(w.due_date || "-")}</td>
                              </tr>`,
                              )
                              .join("")}
                          </tbody>
                        </table>
                      </div>
                    </div>`,
                      )
                      .join("")
                  : `<div class="meta">No work items shared yet.</div>`
              }
            </div>
          </div>

          <div id="clientViewTab-updates" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Latest Updates</h2>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Title</th>
                      <th>Type</th>
                      <th>Update</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      updates.length
                        ? updates
                            .map(
                              (u) => `
                          <tr>
                            <td>${escapeHtml(u.created_at ? formatDateTime(u.created_at) : "-")}</td>
                            <td><strong>${escapeHtml(u.title || "Update")}</strong></td>
                            <td>${escapeHtml(u.update_type || "-")}</td>
                            <td>${escapeHtml(u.update_text || "")}</td>
                          </tr>
                        `,
                            )
                            .join("")
                        : `<tr><td colspan="4" class="meta">No client-visible updates yet.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-actions" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Actions Needed From Client</h2>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Action</th>
                      <th>Status</th>
                      <th>Priority</th>
                      <th>Due Date</th>
                      <th>Notes</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      clientActions.length
                        ? clientActions
                            .map(
                              (a) => `
                          <tr>
                            <td><strong>${escapeHtml(a.title || "Action")}</strong></td>
                            <td><span class="badge">${escapeHtml(a.status || "Open")}</span></td>
                            <td>${escapeHtml(a.priority || "Medium")}</td>
                            <td>${escapeHtml(a.due_date || "-")}</td>
                            <td>${escapeHtml(a.notes || "-")}</td>
                          </tr>
                        `,
                            )
                            .join("")
                        : `<tr><td colspan="5" class="meta">No client actions pending.</td></tr>`
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="clientViewTab-documents" class="tab-panel">
            <div class="panel">
              <div class="panel-head">
                <h2>Documents</h2>
              </div>

              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Document</th>
                      <th>Link</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${
                      client.google_drive_folder_url
                        ? `
                          <tr>
                            <td><strong>Main Google Drive Folder</strong></td>
                            <td><a href="${escapeHtml(client.google_drive_folder_url)}" target="_blank" rel="noopener noreferrer">Open Folder</a></td>
                          </tr>
                        `
                        : ""
                    }

                    ${
                      documents.length
                        ? documents
                            .map(
                              (d) => `
                          <tr>
                            <td><strong>${escapeHtml(d.title || d.name || "Document")}</strong></td>
                            <td>${d.url ? `<a href="${escapeHtml(d.url)}" target="_blank" rel="noopener noreferrer">Open</a>` : "-"}</td>
                          </tr>
                        `,
                            )
                            .join("")
                        : ""
                    }

                    ${
                      !client.google_drive_folder_url && !documents.length
                        ? `<tr><td colspan="2" class="meta">No shared documents available.</td></tr>`
                        : ""
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>

        <div id="extLeadNotesModal" class="ext-modal" onclick="closeExtLeadNotes(event)">
          <div class="ext-modal-card" onclick="event.stopPropagation()">
            <div class="ext-modal-head">
              <div>
                <div class="ext-modal-title">Notes History</div>
                <div id="extLeadNotesSubtitle" class="meta" style="font-size:12px;"></div>
              </div>
              <button class="ext-modal-close" type="button" onclick="closeExtLeadNotes()">Close</button>
            </div>
            <div id="extLeadNotesBody" style="display:flex; flex-direction:column; gap:8px; max-height:60vh; overflow-y:auto;"></div>
          </div>
        </div>

        <script src="/js/client-view.js"></script>
      
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
  renderClientViewOnlyPage,
};
