// Attendance overview (Server Component). Replaces the GET /attendance handler:
// authenticate, then mount the client console. All attendance data is fetched
// client-side from /api/attendance + /api/attendance/insights (served by the
// dispatch shim), so this RSC only gates access.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import AttendanceConsole from "./AttendanceConsole.jsx";

export const metadata = { title: "Attendance | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function AttendancePage() {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  return (
    <>
      <TopNav active="attendance" />
      <AttendanceConsole />
    </>
  );
}
