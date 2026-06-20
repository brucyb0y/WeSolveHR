// My Account (Server Component). Replaces the GET /account handler: any logged-in
// user sees their profile, leave balances, last appraisal and feedback timeline;
// managers/admins additionally get the team tables. Static page — no client JS.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { getAccountData } from "@/lib/services/account.js";
import { formatDateTime, formatDateOnly } from "@/lib/utils/datetime.js";
import styles from "./account.module.css";

export const metadata = { title: "My Account | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const FEEDBACK_LABELS = {
  feedback: "Feedback",
  appreciation: "Appreciation",
  coaching: "Coaching",
  one_on_one: "1:1 Note",
};

export default async function AccountPage() {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const {
    isAdminView,
    appraisal,
    feedbackItems,
    ptoRemaining,
    sickRemaining,
    futureLeaveRows,
    teamFeedbackRows,
    teamAppraisalRows,
    leaveSummaryRows,
  } = await getAccountData(user);

  return (
    <>
      <TopNav active="account" />
      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div className={styles.titleBlock}>
            <h1>{user.name || "My Account"}</h1>
            <p>{user.role || ""}</p>
          </div>
        </div>

        <div className={styles.grid}>
          <div className={styles.sideColumn}>
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
                  <div className={styles.appraisalRow}>
                    <div className={styles.appraisalLabel}>Rating</div>
                    <div className={styles.appraisalValue}>
                      {appraisal.rating || "-"}
                    </div>
                  </div>
                  <div className={styles.appraisalRow}>
                    <div className={styles.appraisalLabel}>Review Date</div>
                    <div className={styles.appraisalValue}>
                      {formatDateTime(appraisal.created_at)}
                    </div>
                  </div>
                  <div className={styles.appraisalRow}>
                    <div className={styles.appraisalLabel}>Strengths</div>
                    <div className={styles.appraisalValue}>
                      {appraisal.strengths || "-"}
                    </div>
                  </div>
                  <div className={styles.appraisalRow}>
                    <div className={styles.appraisalLabel}>Improvement Areas</div>
                    <div className={styles.appraisalValue}>
                      {appraisal.improvement_areas || "-"}
                    </div>
                  </div>
                  <div className={styles.appraisalRow}>
                    <div className={styles.appraisalLabel}>Manager Comment</div>
                    <div className={styles.appraisalValue}>
                      {appraisal.manager_comment || "-"}
                    </div>
                  </div>
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
            <div className={styles.card}>
              <div className={styles.sectionEyebrow}>Admin only</div>
              <h2>Future Leave</h2>
              <div className={styles.tableWrap}>
                <table>
                  <thead>
                    <tr>
                      <th>Employee</th>
                      <th>Leave Date</th>
                      <th>Created By</th>
                      <th>Note</th>
                    </tr>
                  </thead>
                  <tbody>
                    {futureLeaveRows.length ? (
                      futureLeaveRows.map((row) => (
                        <tr key={row.id}>
                          <td>{row.users?.name || "-"}</td>
                          <td>{formatDateOnly(row.off_date)}</td>
                          <td>{row.created_by?.name || "-"}</td>
                          <td>{row.note || "-"}</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan={4} className="empty-cell">
                          No upcoming leave found
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className={styles.card}>
              <div className={styles.sectionEyebrow}>Admin only</div>
              <h2>Team Feedback</h2>
              <div className={styles.tableWrap}>
                <table>
                  <thead>
                    <tr>
                      <th>Employee</th>
                      <th>Type</th>
                      <th>Note</th>
                      <th>Created By</th>
                      <th>Created At</th>
                    </tr>
                  </thead>
                  <tbody>
                    {teamFeedbackRows.length ? (
                      teamFeedbackRows.map((row) => (
                        <tr key={row.id}>
                          <td>{row.users?.name || "-"}</td>
                          <td>{FEEDBACK_LABELS[row.type] || row.type || "-"}</td>
                          <td>{row.note || row.manager_comment || "-"}</td>
                          <td>{row.created_by?.name || "-"}</td>
                          <td>{formatDateTime(row.created_at)}</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan={5} className="empty-cell">
                          No team feedback found
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className={styles.card}>
              <div className={styles.sectionEyebrow}>Admin only</div>
              <h2>Team Appraisals</h2>
              <div className={styles.tableWrap}>
                <table>
                  <thead>
                    <tr>
                      <th>Employee</th>
                      <th>Rating</th>
                      <th>Strengths</th>
                      <th>Improvement Areas</th>
                      <th>Manager Comment</th>
                      <th>Created At</th>
                    </tr>
                  </thead>
                  <tbody>
                    {teamAppraisalRows.length ? (
                      teamAppraisalRows.map((row) => (
                        <tr key={row.id}>
                          <td>{row.users?.name || "-"}</td>
                          <td>{row.rating || "-"}</td>
                          <td>{row.strengths || "-"}</td>
                          <td>{row.improvement_areas || "-"}</td>
                          <td>{row.manager_comment || "-"}</td>
                          <td>{formatDateTime(row.created_at)}</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan={6} className="empty-cell">
                          No team appraisals found
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className={styles.card}>
              <div className={styles.sectionEyebrow}>Admin only</div>
              <h2>Leave Summary</h2>
              <div className={styles.tableWrap}>
                <table>
                  <thead>
                    <tr>
                      <th>Employee</th>
                      <th>Total Leave Entries</th>
                      <th>Upcoming Leaves</th>
                      <th>Next Leave</th>
                    </tr>
                  </thead>
                  <tbody>
                    {leaveSummaryRows.length ? (
                      leaveSummaryRows.map((row, i) => (
                        <tr key={i}>
                          <td>{row.name || "-"}</td>
                          <td>{String(row.totalLeaveCount ?? 0)}</td>
                          <td>{String(row.upcomingLeaveCount ?? 0)}</td>
                          <td>
                            {row.nextLeaveDate
                              ? formatDateOnly(row.nextLeaveDate)
                              : "-"}
                          </td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan={4} className="empty-cell">
                          No leave summary found
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </>
  );
}
