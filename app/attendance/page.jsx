// /attendance — replaces the inline HTML in app.get("/attendance").
//
// BEHAVIOUR CHANGE (deliberate): the tab bar now switches tabs.
//
// The original markup had four tab buttons but only three tab panels: there was
// no #tab-overview element, so getElementById("tab-overview") returned null and
// the overview content — which sat unwrapped, directly inside .wrap — was never
// hidden. Selecting "Late & Exceptions", "Leave & No Update" or "Team Summary"
// revealed that panel BELOW the still-visible overview. Worse, .wrap's closing
// </div> landed before those three panels, so they rendered outside the
// max-width container, full-bleed and unpadded.
//
// The intent is unambiguous (the click handler looks up an element the author
// meant to exist), and reproducing malformed nesting in JSX would require doing
// it on purpose, so it is fixed: all four panels are siblings inside .wrap and
// only the selected one is shown.
//
// To revert, drop the #tab-overview wrapper in AttendanceBoard and move the
// other three panels outside the container.

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import AttendanceBoard from "./AttendanceBoard";
import styles from "./attendance.module.css";

export const metadata = { title: "Attendance" };
export const dynamic = "force-dynamic";

export default async function AttendancePage() {
  const user = await requireDashboardUser();

  return (
    <>
      <TopNav active="attendance" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>WeSolveHR</div>
            <h1>Attendance</h1>
            <div className={styles.subtitle}>
              Team attendance overview and exceptions
            </div>
          </div>
        </div>

        <AttendanceBoard />
      </div>
    </>
  );
}
