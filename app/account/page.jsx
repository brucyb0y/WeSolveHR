// /account — replaces the inline HTML in app.get("/account").
//
// Session-only auth (requireUserLogin), as before. See account.module.css for
// why this page carries its own background instead of the shared theme.

import TopNav from "@/components/TopNav";
import { requireUser } from "@/lib/auth";
import {
  isManagerOrAdmin,
  formatDateTime,
  formatDateOnly,
  ACCOUNT_FIELD_OPTIONS,
} from "@/lib/server/app.js";
import { getAccountData } from "@/lib/data/account";
import ProfileFieldSelect from "./ProfileFieldSelect";
import styles from "./account.module.css";

export const metadata = { title: "My Account" };
export const dynamic = "force-dynamic";

const FEEDBACK_LABELS = {
  feedback: "Feedback",
  appreciation: "Appreciation",
  coaching: "Coaching",
  one_on_one: "1:1 Note",
};

function AdminTable({ title, columns, rows, emptyText, renderRow }) {
  return (
    <div className={styles.card}>
      <div className={styles.sectionEyebrow}>Admin only</div>
      <h2>{title}</h2>
      <div className={styles.tableWrap}>
        <table>
          <thead>
            <tr>
              {columns.map((c) => (
                <th key={c}>{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map(renderRow)
            ) : (
              <tr>
                <td colSpan={columns.length} className={styles.emptyCell}>
                  {emptyText}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default async function AccountPage() {
  const user = await requireUser();
  const isAdminView = isManagerOrAdmin(user);
  const data = await getAccountData(user, isAdminView);

  const {
    appraisal,
    feedbackItems,
    ptoRemaining,
    sickRemaining,
    timeText,
    notesText,
    futureLeaveRows,
    teamFeedbackRows,
    teamAppraisalRows,
    leaveSummaryRows,
  } = data;

  const appraisalRows = appraisal
    ? [
        { label: "Rating", value: appraisal.rating || "-" },
        { label: "Review Date", value: formatDateTime(appraisal.created_at) },
        { label: "Strengths", value: appraisal.strengths || "-" },
        {
          label: "Improvement Areas",
          value: appraisal.improvement_areas || "-",
        },
        { label: "Manager Comment", value: appraisal.manager_comment || "-" },
      ]
    : [];

  return (
    <div className={styles.page}>
      <TopNav active="account" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div className={styles.titleBlock}>
            <h1>{user.name || "My Account"}</h1>
            <p>{user.role || ""}</p>
          </div>
        </div>

        <div className={styles.grid}>
          <div className={styles.column}>
            <div className={styles.card}>
              <h2>Profile</h2>
              <div className={styles.profileMeta}>
                <div className={styles.metaBox}>
                  <div className={styles.metaLabel}>Name</div>
                  <div className={styles.metaValue}>{user.name || "-"}</div>
                </div>
                <div className={styles.metaBox}>
                  <div className={styles.metaLabel}>Role</div>
                  <div className={styles.metaValue}>{user.role || "-"}</div>
                </div>
                <div className={styles.metaBox}>
                  <div className={styles.metaLabel}>Department</div>
                  <div className={styles.metaValue}>
                    <ProfileFieldSelect
                      field="department"
                      options={ACCOUNT_FIELD_OPTIONS.department}
                      currentValue={user.department}
                    />
                  </div>
                </div>
                <div className={styles.metaBox}>
                  <div className={styles.metaLabel}>Designation</div>
                  <div className={styles.metaValue}>
                    <ProfileFieldSelect
                      field="designation"
                      options={ACCOUNT_FIELD_OPTIONS.designation}
                      currentValue={user.designation}
                    />
                  </div>
                </div>
                <div className={styles.metaBox}>
                  <div className={styles.metaLabel}>Time</div>
                  <div className={styles.metaValue}>{timeText}</div>
                </div>
                <div className={styles.metaBox}>
                  <div className={styles.metaLabel}>Notes</div>
                  <div className={styles.metaValue}>{notesText}</div>
                </div>
              </div>
            </div>

            <div className={styles.card}>
              <h2>Leave Balance</h2>
              <div className={styles.statsRow}>
                <div className={styles.statCard}>
                  <div className={styles.statLabel}>PTO Remaining</div>
                  <div className={styles.statValue}>{ptoRemaining}</div>
                </div>
                <div className={styles.statCard}>
                  <div className={styles.statLabel}>Sick Remaining</div>
                  <div className={styles.statValue}>{sickRemaining}</div>
                </div>
              </div>
            </div>

            <div className={styles.card}>
              <h2>Last Appraisal</h2>
              {appraisal ? (
                <div className={styles.appraisalBlock}>
                  {appraisalRows.map((row) => (
                    <div className={styles.appraisalRow} key={row.label}>
                      <div className={styles.appraisalLabel}>{row.label}</div>
                      <div className={styles.appraisalValue}>{row.value}</div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className={styles.emptyState}>No appraisal yet</div>
              )}
            </div>
          </div>

          <div className={styles.card}>
            <h2>Feedback Timeline</h2>
            <div className={styles.timeline}>
              {feedbackItems.length ? (
                feedbackItems.map((item) => (
                  <div className={styles.timelineItem} key={item.id}>
                    <div className={styles.timelineBadge}>
                      {FEEDBACK_LABELS[item.type] || item.type}
                    </div>
                    <div className={styles.timelineDate}>
                      {formatDateTime(item.created_at)}
                    </div>
                    <div className={styles.timelineNote}>
                      {item.note || item.manager_comment || ""}
                    </div>
                  </div>
                ))
              ) : (
                <div className={styles.emptyState}>No feedback yet</div>
              )}
            </div>
          </div>
        </div>

        {isAdminView ? (
          <div className={styles.adminSection}>
            <AdminTable
              title="Future Leave"
              columns={["Employee", "Leave Date", "Created By", "Note"]}
              rows={futureLeaveRows}
              emptyText="No upcoming leave found"
              renderRow={(row) => (
                <tr key={row.id}>
                  <td>{row.users?.name || "-"}</td>
                  <td>{formatDateOnly(row.off_date)}</td>
                  <td>{row.created_by?.name || "-"}</td>
                  <td>{row.note || "-"}</td>
                </tr>
              )}
            />

            <AdminTable
              title="Team Feedback"
              columns={["Employee", "Type", "Note", "Created By", "Created At"]}
              rows={teamFeedbackRows}
              emptyText="No team feedback found"
              renderRow={(row) => (
                <tr key={row.id}>
                  <td>{row.users?.name || "-"}</td>
                  <td>{FEEDBACK_LABELS[row.type] || row.type || "-"}</td>
                  <td>{row.note || row.manager_comment || "-"}</td>
                  <td>{row.created_by?.name || "-"}</td>
                  <td>{formatDateTime(row.created_at)}</td>
                </tr>
              )}
            />

            <AdminTable
              title="Team Appraisals"
              columns={[
                "Employee",
                "Rating",
                "Strengths",
                "Improvement Areas",
                "Manager Comment",
                "Created At",
              ]}
              rows={teamAppraisalRows}
              emptyText="No team appraisals found"
              renderRow={(row) => (
                <tr key={row.id}>
                  <td>{row.users?.name || "-"}</td>
                  <td>{row.rating || "-"}</td>
                  <td>{row.strengths || "-"}</td>
                  <td>{row.improvement_areas || "-"}</td>
                  <td>{row.manager_comment || "-"}</td>
                  <td>{formatDateTime(row.created_at)}</td>
                </tr>
              )}
            />

            <AdminTable
              title="Leave Summary"
              columns={[
                "Employee",
                "Total Leave Entries",
                "Upcoming Leaves",
                "Next Leave",
              ]}
              rows={leaveSummaryRows}
              emptyText="No leave summary found"
              renderRow={(row) => (
                <tr key={row.name}>
                  <td>{row.name || "-"}</td>
                  <td>{String(row.totalLeaveCount ?? 0)}</td>
                  <td>{String(row.upcomingLeaveCount ?? 0)}</td>
                  <td>
                    {row.nextLeaveDate
                      ? formatDateOnly(row.nextLeaveDate)
                      : "-"}
                  </td>
                </tr>
              )}
            />
          </div>
        ) : null}
      </div>
    </div>
  );
}
