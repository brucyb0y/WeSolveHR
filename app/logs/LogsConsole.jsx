"use client";

// The logs console: filters, stats and the row table.
//
// This stays client-driven on purpose — it is a live view. Behaviour carried
// over from the old inline script: search/user inputs are debounced 450ms,
// outcome/day/month refetch immediately, the table reloads every 60s and again
// whenever the tab becomes visible. /api/logs already returns JSON, so it is a
// real API and remains the data source.
//
// The one structural change: rows are React elements instead of a concatenated
// innerHTML string, so the escapeHtmlClient() helper is gone.

import { useCallback, useEffect, useRef, useState } from "react";
import styles from "./logs.module.css";

const DEBOUNCE_MS = 450;
const REFRESH_MS = 60000;
const COMMAND_LIMIT = 130;
const SID_LIMIT = 14;

const EMPTY_FILTERS = {
  q: "",
  user: "",
  outcome: "",
  day: "",
  month: "",
};

const COLUMNS = [
  "Time",
  "Sender",
  "Command",
  "Outcome",
  "Type",
  "Exception",
  "SID",
];

const OUTCOME_OPTIONS = [
  { value: "", label: "All outcomes" },
  { value: "completed", label: "Completed" },
  { value: "failed", label: "Failed" },
  { value: "processing", label: "Processing" },
  { value: "unknown", label: "Unknown" },
];

function outcomeBadgeClass(status) {
  const s = String(status || "").toLowerCase();
  if (s === "completed") return `${styles.logBadge} ${styles.logBadgeSuccess}`;
  if (s === "failed") return `${styles.logBadge} ${styles.logBadgeDanger}`;
  if (s === "processing") return `${styles.logBadge} ${styles.logBadgeWarn}`;
  return `${styles.logBadge} ${styles.logBadgeMuted}`;
}

function truncateText(text, limit = 120) {
  const value = String(text || "").trim();
  if (value.length <= limit) return value;
  return `${value.slice(0, limit)}…`;
}

function PersonStats({ title, data }) {
  const entries = Object.entries(data || {}).sort((a, b) => b[1] - a[1]);

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

function CommandCell({ body }) {
  const [open, setOpen] = useState(false);
  const text = String(body || "-");
  const isLong = text.length > COMMAND_LIMIT;

  return (
    <td className={styles.commandCell}>
      <div className={styles.commandPreview}>
        {truncateText(text, COMMAND_LIMIT)}
      </div>
      {isLong ? (
        <>
          <button
            className={styles.miniLink}
            onClick={() => setOpen((v) => !v)}
            type="button"
          >
            View full
          </button>
          <div
            className={`${styles.commandFull} ${open ? styles.open : ""}`}
          >
            {text}
          </div>
        </>
      ) : null}
    </td>
  );
}

export default function LogsConsole() {
  const [filters, setFilters] = useState(EMPTY_FILTERS);
  const [rows, setRows] = useState([]);
  const [stats, setStats] = useState({});

  // Filters are read through a ref inside loadLogs so the polling interval and
  // the visibility listener never capture a stale snapshot.
  const filtersRef = useRef(filters);
  filtersRef.current = filters;

  const isFirstRun = useRef(true);
  const previousFilters = useRef(filters);

  const loadLogs = useCallback(async () => {
    try {
      const current = filtersRef.current;
      const params = new URLSearchParams();

      if (current.q.trim()) params.set("q", current.q.trim());
      if (current.user.trim()) params.set("user", current.user.trim());
      if (current.outcome) params.set("outcome", current.outcome);
      if (current.day) params.set("day", current.day);
      if (current.month) params.set("month", current.month);

      const res = await fetch(`/api/logs?${params.toString()}`);
      const json = await res.json();

      if (!json.ok) return;

      setRows(json.data?.rows || []);
      setStats(json.data?.stats || {});
    } catch (err) {
      console.error("Failed to load logs:", err);
    }
  }, []);

  // Initial load, 60s poll, and refresh when the tab regains focus.
  useEffect(() => {
    loadLogs();

    const timer = setInterval(loadLogs, REFRESH_MS);
    const onVisible = () => {
      if (document.visibilityState === "visible") loadLogs();
    };

    document.addEventListener("visibilitychange", onVisible);

    return () => {
      clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [loadLogs]);

  // One effect for every filter change, so a change never triggers two fetches.
  // Typing in the search/user boxes is debounced; the outcome/day/month pickers
  // fire straight away, matching the old input-vs-change listener split. The
  // first run is skipped because the mount effect above already loaded once.
  useEffect(() => {
    if (isFirstRun.current) {
      isFirstRun.current = false;
      previousFilters.current = filters;
      return undefined;
    }

    const textChanged =
      filters.q !== previousFilters.current.q ||
      filters.user !== previousFilters.current.user;

    previousFilters.current = filters;

    const timer = setTimeout(loadLogs, textChanged ? DEBOUNCE_MS : 0);
    return () => clearTimeout(timer);
  }, [filters, loadLogs]);

  const set = (key) => (e) =>
    setFilters((f) => ({ ...f, [key]: e.target.value }));

  return (
    <>
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

        <div className={styles.personStats}>
          <PersonStats
            title="Commands Today by Person"
            data={stats.byPersonToday}
          />
          <PersonStats
            title="Commands This Month by Person"
            data={stats.byPersonMonth}
          />
        </div>
      </div>

      <div className={`${styles.panel} ${styles.filtersPanel}`}>
        <div className={styles.filtersGrid}>
          <input
            placeholder="Search message or SID"
            value={filters.q}
            onChange={set("q")}
          />
          <input
            placeholder="Filter by username / phone"
            value={filters.user}
            onChange={set("user")}
          />
          <select value={filters.outcome} onChange={set("outcome")}>
            {OUTCOME_OPTIONS.map((option) => (
              <option value={option.value} key={option.value}>
                {option.label}
              </option>
            ))}
          </select>
          <input type="date" value={filters.day} onChange={set("day")} />
          <input type="month" value={filters.month} onChange={set("month")} />
          <button onClick={loadLogs} type="button">
            Apply
          </button>
          <button onClick={() => setFilters(EMPTY_FILTERS)} type="button">
            Clear
          </button>
        </div>
      </div>

      <div className={styles.panel}>
        <div className={styles.tableWrap}>
          <table>
            <thead>
              <tr>
                {COLUMNS.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.length ? (
                rows.map((row) => {
                  const error = row.outcome_error || "";

                  return (
                    <tr key={row.id}>
                      <td>{row.created_at_text || row.created_at || ""}</td>
                      <td>
                        <strong>{row.sender || ""}</strong>
                      </td>
                      <CommandCell body={row.body} />
                      <td>
                        <span className={outcomeBadgeClass(row.outcome_status)}>
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
                        {truncateText(row.message_sid || "-", SID_LIMIT)}
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
    </>
  );
}
