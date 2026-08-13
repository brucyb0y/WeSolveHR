"use client";

// The two "Red report" cells in the Month summary, filled from
// /api/attendance/:userId/red-reports. Kept client-side because that endpoint
// is separate from getEmployeeAttendanceOverview() and the original page
// deliberately loaded it after paint.
//
// Renders BOTH cells (days + dates) so the loading/failure states stay in sync,
// matching the old loadRedReports() which always wrote to the pair together.

import { useEffect, useState } from "react";

export default function RedReports({ userId, month, labelClassName }) {
  const [state, setState] = useState({ status: "loading", data: null });

  useEffect(() => {
    if (!userId) return undefined;

    let cancelled = false;

    (async () => {
      try {
        const res = await fetch(
          `/api/attendance/${userId}/red-reports?month=${encodeURIComponent(month)}`,
          { headers: { Accept: "application/json" } },
        );
        const json = await res.json();
        if (cancelled) return;

        if (!json.ok) {
          setState({ status: "error", data: null });
          return;
        }
        setState({ status: "ready", data: json.data || {} });
      } catch (error) {
        console.error("Red reports fetch failed:", error);
        if (!cancelled) setState({ status: "error", data: null });
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [userId, month]);

  const payload = state.data || {};
  const redReportDays = Number(payload.redReportDays || 0);
  const redReportDates = Array.isArray(payload.redReportDates)
    ? payload.redReportDates
    : [];
  const redReportDatesText = payload.redReportDatesText || "None";

  let daysCell;
  let datesCell;

  if (state.status === "loading") {
    daysCell = <span className="muted">Loading...</span>;
    datesCell = <span className="muted">Loading...</span>;
  } else if (state.status === "error") {
    daysCell = <span className="muted">Failed to load</span>;
    datesCell = <span className="muted">Failed to load</span>;
  } else {
    daysCell = String(redReportDays);
    datesCell = redReportDates.length ? (
      <details>
        <summary>{redReportDays} date(s)</summary>
        <div style={{ marginTop: "8px" }}>{redReportDatesText}</div>
      </details>
    ) : (
      "None"
    );
  }

  return (
    <>
      <div className={labelClassName}>Red report days</div>
      <div>{daysCell}</div>
      <div className={labelClassName}>Red report dates</div>
      <div>{datesCell}</div>
    </>
  );
}
