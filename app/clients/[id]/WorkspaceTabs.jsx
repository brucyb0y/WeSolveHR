// Workspace tab bar. Tabs are plain links — each one is a separate page load
// (?tab=key), and only the active tab's panel renders. No client state needed.
//
// FIXED HERE: every tabLink() call in the original passed its arguments into
// the wrong parameters. The signature was
//
//     tabLink(key, label, icon = "", count = null, tone = "")
//
// but the call sites read
//
//     tabLink("task", "Task", openWorkCount, overdueCount > 0 ? "attention" : "")
//
// so the number landed in `icon`, the "attention" string landed in `count`, and
// `tone` was never passed. Three consequences, all live:
//
//   1. `showCount` requires typeof count === "number" — it was a string, so the
//      count badge never rendered at all;
//   2. the number rendered inside the .tab-ico span, which carries
//      aria-hidden="true" — so screen readers never announced it;
//   3. tone === "attention" was never true, making .tabCountAttention dead CSS.
//
// The counts were still *visible* (in the icon slot), which is why it went
// unnoticed. Passing them correctly is the only sane thing to do in JSX — you
// cannot reproduce an argument-order mistake without writing it deliberately.
//
// Visible effect: Task / Leads / Campaigns / Blockers / Actions Needed now show
// a real badge instead of a bare number, and Task / Blockers / Actions Needed
// highlight when they need attention.

import styles from "./workspace.module.css";

export default function WorkspaceTabs({
  clientId,
  activeTab,
  counts,
  firstWeekLabel,
}) {
  const href = (key) => `/clients/${Number(clientId)}?tab=${key}`;
  const reportHref = href("report");
  const reportActive = activeTab === "report";

  const tabs = [
    { key: "overview", label: "Overview" },
    {
      key: "task",
      label: "Task",
      count: counts.openWork,
      tone: counts.overdue > 0 ? "attention" : "",
    },
    { key: "leads", label: "Leads", count: counts.leads },
    { key: "campaigns", label: "Campaigns", count: counts.campaigns },
    { key: "meetings", label: "Meetings & MOMs" },
    {
      key: "blockers",
      label: "Blockers",
      count: counts.openBlockers,
      tone: counts.openBlockers > 0 ? "attention" : "",
    },
    { key: "team", label: "Team" },
    { key: "performance", label: "Performance" },
    { key: "incentives", label: "Incentives" },
  ];

  const trailing = {
    key: "actions",
    label: "Actions Needed",
    count: counts.openActions,
    tone: counts.openActions > 0 ? "attention" : "",
  };

  const TabLink = ({ tab }) => {
    const isActive = activeTab === tab.key;
    const showCount = typeof tab.count === "number" && tab.count > 0;

    return (
      <a
        className={`${styles.tab} ${isActive ? styles.active : ""}`}
        href={href(tab.key)}
        aria-current={isActive ? "page" : "false"}
      >
        <span className="tab-label">{tab.label}</span>
        {showCount ? (
          <span
            className={`${styles.tabCount} ${
              tab.tone === "attention" ? styles.tabCountAttention : ""
            }`}
          >
            {tab.count > 99 ? "99+" : tab.count}
          </span>
        ) : null}
      </a>
    );
  };

  return (
    <div className={styles.tabs} role="tablist">
      {tabs.map((tab) => (
        <TabLink tab={tab} key={tab.key} />
      ))}

      <div className={styles.tabFlyoutWrap}>
        <a
          className={`${styles.tab} ${reportActive ? styles.active : ""}`}
          href={reportHref}
          aria-current={reportActive ? "page" : "false"}
        >
          <span className="tab-label">Report</span>
        </a>
        <div className={styles.tabFlyout} role="menu">
          <a
            className={styles.tabFlyoutItem}
            role="menuitem"
            href={`${reportHref}#daily`}
          >
            Daily Report
          </a>
          <a
            className={styles.tabFlyoutItem}
            role="menuitem"
            href={`${reportHref}#week1`}
          >
            Week {firstWeekLabel} Report
          </a>
        </div>
      </div>

      <TabLink tab={trailing} />
    </div>
  );
}
