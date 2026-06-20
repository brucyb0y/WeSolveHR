"use client";

// Logs console body. Ported from the inline scripts of the GET /logs handler in
// lib/server/app.js: filters (text ones debounced, selects/dates immediate),
// stats + per-person breakdowns, the log table with expandable command cells,
// and the 60s / on-focus refresh. Data comes from /api/logs (dispatch shim).

import { useCallback, useEffect, useRef, useState } from "react";
import styles from "./logs.module.css";

const EMPTY_FILTERS = {
  search: "",
  user: "",
  outcome: "",
  day: "",
  month: "",
};

function outcomeBadgeClass(status) {
  const s = String(status || "").toLowerCase();
  if (s === "completed") return styles.logBadgeSuccess;
  if (s === "failed") return styles.logBadgeDanger;
  if (s === "processing") return styles.logBadgeWarn;
  return styles.logBadgeMuted;
}

function truncateText(text, limit = 120) {
  const value = String(text || "").trim();
  if (value.length <= limit) return value;
  return value.slice(0, limit) + "…";
}

function PersonStats({ title, obj }) {
  const entries = Object.entries(obj || {}).sort((a, b) => b[1] - a[1]);
  return (
    <div className={styles.personStatBox}>
      <div className={styles.personStatTitle}>{title}</div>
      {entries.length ? (
        entries.map(([name, count]) => (
          <span className={styles.personChip} key={name}>
            {name}: {count}
          </span>
        ))
      ) : (
        <div className="muted">No commands</div>
      )}
    </div>
  );
}

export default function LogsConsole() {
  const [filters, setFilters] = useState(EMPTY_FILTERS);
  const [data, setData] = useState({ rows: [], stats: {} });
  const [expanded, setExpanded] = useState(() => new Set());

  const filtersRef = useRef(filters);
  filtersRef.current = filters;
  const debounceRef = useRef(null);

  const loadLogs = useCallback(async (f = filtersRef.current) => {
    try {
      const params = new URLSearchParams();
      if (f.search.trim()) params.set("q", f.search.trim());
      if (f.user.trim()) params.set("user", f.user.trim());
      if (f.outcome) params.set("outcome", f.outcome);
      if (f.day) params.set("day", f.day);
      if (f.month) params.set("month", f.month);

      const res = await fetch("/api/logs?" + params.toString());
      const json = await res.json();
      if (!json.ok) return;

      setData({
        rows: json.data?.rows || [],
        stats: json.data?.stats || {},
      });
    } catch (err) {
      console.error("Failed to load logs:", err);
    }
  }, []);

  useEffect(() => {
    loadLogs();
    const timer = setInterval(() => loadLogs(), 60000);
    const onVisible = () => {
      if (document.visibilityState === "visible") loadLogs();
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
      clearTimeout(debounceRef.current);
    };
  }, [loadLogs]);

  function onText(name, value) {
    const next = { ...filtersRef.current, [name]: value };
    setFilters(next);
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => loadLogs(next), 450);
  }

  function onImmediate(name, value) {
    const next = { ...filtersRef.current, [name]: value };
    setFilters(next);
    loadLogs(next);
  }

  function clearFilters() {
    setFilters(EMPTY_FILTERS);
    loadLogs(EMPTY_FILTERS);
  }

  function toggleCommand(id) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  const stats = data.stats || {};
  const rows = data.rows || [];

  return (
    <div className={styles.wrap}>
      <div className={styles.topbar}>
        <div>
          <div className={styles.eyebrow}>Message Logging</div>
          <h1>WeSolveHR // Logs Console</h1>
          <div className={styles.subtitle}>
            Inbound command visibility for tracing, debugging, and audit review
          </div>
        </div>
      </div>

      <div className={styles.statsGrid}>
        <div className={styles.logStatCard}>
          <div className={styles.logStatLabel}>Loaded Logs</div>
          <div className={styles.logStatValue}>{stats.total || 0}</div>
        </div>
        <div className={styles.logStatCard}>
          <div className={styles.logStatLabel}>Completed</div>
          <div className={styles.logStatValue}>{stats.completed || 0}</div>
        </div>
        <div className={styles.logStatCard}>
          <div className={styles.logStatLabel}>Failed</div>
          <div className={styles.logStatValue}>{stats.failed || 0}</div>
        </div>
        <div className={styles.logStatCard}>
          <div className={styles.logStatLabel}>Unknown</div>
          <div className={styles.logStatValue}>{stats.unknown || 0}</div>
        </div>
        <div className={styles.personStats} style={{ gridColumn: "1 / -1" }}>
          <PersonStats
            title="Commands Today by Person"
            obj={stats.byPersonToday}
          />
          <PersonStats
            title="Commands This Month by Person"
            obj={stats.byPersonMonth}
          />
        </div>
      </div>

      <div className={`${styles.panel} ${styles.filtersPanel}`}>
        <div className={styles.filtersGrid}>
          <input
            placeholder="Search message or SID"
            value={filters.search}
            onChange={(e) => onText("search", e.target.value)}
          />
          <input
            placeholder="Filter by username / phone"
            value={filters.user}
            onChange={(e) => onText("user", e.target.value)}
          />
          <select
            value={filters.outcome}
            onChange={(e) => onImmediate("outcome", e.target.value)}
          >
            <option value="">All outcomes</option>
            <option value="completed">Completed</option>
            <option value="failed">Failed</option>
            <option value="processing">Processing</option>
            <option value="unknown">Unknown</option>
          </select>
          <input
            type="date"
            value={filters.day}
            onChange={(e) => onImmediate("day", e.target.value)}
          />
          <input
            type="month"
            value={filters.month}
            onChange={(e) => onImmediate("month", e.target.value)}
          />
          <button onClick={() => loadLogs()}>Apply</button>
          <button onClick={clearFilters}>Clear</button>
        </div>
      </div>

      <div className={styles.panel}>
        <div className={styles.tableWrap}>
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Sender</th>
                <th>Command</th>
                <th>Outcome</th>
                <th>Type</th>
                <th>Exception</th>
                <th>SID</th>
              </tr>
            </thead>
            <tbody>
              {rows.length ? (
                rows.map((row) => {
                  const id = Number(row.id);
                  const body = String(row.body || "");
                  const error = row.outcome_error || "";
                  return (
                    <tr key={id}>
                      <td>{row.created_at_text || row.created_at || ""}</td>
                      <td>
                        <strong>{row.sender || ""}</strong>
                      </td>
                      <td className={styles.commandCell}>
                        <div className={styles.commandPreview}>
                          {truncateText(row.body || "-", 130)}
                        </div>
                        {body.length > 130 ? (
                          <>
                            <button
                              className={styles.miniLink}
                              onClick={() => toggleCommand(id)}
                            >
                              View full
                            </button>
                            <div
                              className={`${styles.commandFull} ${expanded.has(id) ? styles.open : ""}`}
                            >
                              {row.body || "-"}
                            </div>
                          </>
                        ) : null}
                      </td>
                      <td>
                        <span
                          className={`${styles.logBadge} ${outcomeBadgeClass(row.outcome_status)}`}
                        >
                          {row.outcome_status || "-"}
                        </span>
                      </td>
                      <td>{row.outcome_result_type || "-"}</td>
                      <td>
                        {error ? (
                          <span className={styles.exceptionPill} title={error}>
                            {error}
                          </span>
                        ) : (
                          <span className={styles.exceptionNone}>-</span>
                        )}
                      </td>
                      <td
                        className={styles.sidSmall}
                        title={row.message_sid || "-"}
                      >
                        {truncateText(row.message_sid || "-", 14)}
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={7} className="empty-cell">
                    No logs found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
