// Leads pagination bar. Rendered above and below the table.
//
// The bar disappears entirely when there is only one page — `hasPrev` and
// `hasNext` both false — rather than showing a dead "Page 1 of 1".
//
// The top copy is drawn compact so it does not dominate the space above the
// table; the bottom keeps full size. That is the only difference between them.
//
// prevHref/nextHref arrive as strings rather than a href-building function:
// this renders inside a client subtree, and functions cannot be passed from a
// server component across that boundary.

import styles from "./workspace.module.css";

export default function LeadsPagination({
  pagination,
  prevHref,
  nextHref,
  compact,
}) {
  if (!pagination || (!pagination.hasPrev && !pagination.hasNext)) return null;

  const pageSize = pagination.pageSize || 25;
  const total = pagination.total || 0;
  const current = pagination.page || 1;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const start = total ? (current - 1) * pageSize + 1 : 0;
  const end = Math.min(current * pageSize, total);

  const btnStyle = compact ? { padding: "4px 10px", fontSize: 12 } : undefined;
  const disabledStyle = {
    ...btnStyle,
    opacity: 0.4,
    pointerEvents: "none",
    cursor: "default",
  };

  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        gap: compact ? 8 : 12,
        flexWrap: "wrap",
        padding: compact ? "4px 2px" : "10px 2px",
      }}
    >
      <div
        className={styles.meta}
        style={compact ? { fontSize: 12 } : undefined}
      >
        Showing {start}–{end} of {total}
      </div>
      <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
        {pagination.hasPrev ? (
          <a className={styles.btn} style={btnStyle} href={prevHref}>
            ← Prev
          </a>
        ) : (
          <span className={styles.btn} style={disabledStyle}>
            ← Prev
          </span>
        )}
        <span className={styles.btn} style={disabledStyle}>
          Page {current} of {totalPages}
        </span>
        {pagination.hasNext ? (
          <a className={styles.btn} style={btnStyle} href={nextHref}>
            Next →
          </a>
        ) : (
          <span className={styles.btn} style={disabledStyle}>
            Next →
          </span>
        )}
      </div>
    </div>
  );
}
