"use client";

// Team reports view. Ported from renderReportsPage() in lib/server/app.js: it
// fetches the server-rendered summary + cards HTML from /api/reports/summary and
// /api/reports/cards (dispatch shim) and injects them, filters cards by name,
// and shows the shared task modal. The fragment HTML uses the global classes in
// reports.css; the task links inside it call window.openTaskDetail (TaskModal).

import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import { formatDateOnly } from "@/lib/utils/datetime.js";
import TaskModal from "./TaskModal.jsx";

export default function ReportsConsole({ reportDate }) {
  const searchParams = useSearchParams();
  const date = searchParams.get("date") || "";
  const userId = searchParams.get("userId") || "";

  const [summaryHtml, setSummaryHtml] = useState(
    '<div class="panel" style="padding:18px; margin-bottom:16px;"><div class="muted">Loading summary...</div></div>',
  );
  const [cardsHtml, setCardsHtml] = useState(
    '<div class="panel" style="padding:18px;"><div class="muted">Loading reports...</div></div>',
  );
  const [search, setSearch] = useState("");
  const gridRef = useRef(null);

  useEffect(() => {
    const qs = new URLSearchParams();
    if (date) qs.set("date", date);
    if (userId) qs.set("userId", userId);
    const suffix = qs.toString() ? "?" + qs.toString() : "";

    let alive = true;

    fetch("/api/reports/summary" + suffix, { headers: { Accept: "application/json" } })
      .then((r) => r.json())
      .then((json) => {
        if (!alive) return;
        setSummaryHtml(
          json.ok && json.data.summaryHtml
            ? json.data.summaryHtml
            : '<div class="panel" style="padding:18px; margin-bottom:16px;"><div class="muted">No summary available.</div></div>',
        );
      })
      .catch(() => {
        if (alive)
          setSummaryHtml(
            '<div class="panel" style="padding:18px; margin-bottom:16px;"><div class="muted">Failed to load summary.</div></div>',
          );
      });

    fetch("/api/reports/cards" + suffix, { headers: { Accept: "application/json" } })
      .then((r) => r.json())
      .then((json) => {
        if (!alive) return;
        setCardsHtml(
          json.ok && json.data.cardsHtml
            ? json.data.cardsHtml
            : '<div class="panel" style="padding:18px;"><div class="muted">No users found.</div></div>',
        );
      })
      .catch(() => {
        if (alive)
          setCardsHtml(
            '<div class="panel" style="padding:18px;"><div class="muted">Failed to load reports.</div></div>',
          );
      });

    return () => {
      alive = false;
    };
  }, [date, userId]);

  // Filter the injected cards by user name (matches the original filterReports).
  useEffect(() => {
    const grid = gridRef.current;
    if (!grid) return;
    const query = search.trim().toLowerCase();
    for (const card of grid.querySelectorAll(".report-card")) {
      const name = String(card.getAttribute("data-user-name") || "");
      card.style.display = !query || name.includes(query) ? "" : "none";
    }
  }, [search, cardsHtml]);

  return (
    <div className="wrap">
      <div className="topbar">
        <div>
          <div className="eyebrow">Daily Reporting</div>
          <h1>WeSolveHR // Reports</h1>
          <div className="subtitle">
            Attendance-day so far. Task narratives + extra work + open/blocked
            snapshot.
          </div>
        </div>
      </div>

      <div className="panel" style={{ padding: "14px 16px", marginBottom: 16 }}>
        <strong>Date:</strong> {formatDateOnly(reportDate)}{" "}
        <span className="muted">(6:00 AM → next day 6:00 AM IST)</span>
      </div>

      <div dangerouslySetInnerHTML={{ __html: summaryHtml }} />

      <div className="panel" style={{ padding: "14px 16px", marginBottom: 16 }}>
        <input
          className="search-input"
          type="text"
          placeholder="Search user name"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>

      <div
        ref={gridRef}
        className="reports-grid"
        dangerouslySetInnerHTML={{ __html: cardsHtml }}
      />

      <TaskModal />
    </div>
  );
}
