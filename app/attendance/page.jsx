// GET /attendance — ported from lib/server/app.js.
//
// The original handler emitted a static document and let the client fetch its
// data, so this page only guards access and renders that markup.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderAttendancePage } from "./AttendancePage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./attendance.css";

export const metadata = { title: "Attendance" };
export const dynamic = "force-dynamic";

export default async function AttendancePage() {
  await requireDashboardAuthPage();
  return <RawHtml html={renderAttendancePage()} />;
}
