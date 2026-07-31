// GET /tasks — ported from lib/server/app.js.
//
// The original handler emitted a static document and let the client fetch its
// data, so this page only guards access and renders that markup.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderTasksPage } from "./TasksPage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./tasks.css";

export const metadata = { title: "Tasks" };
export const dynamic = "force-dynamic";

export default async function TasksPage() {
  await requireDashboardAuthPage();
  return <RawHtml html={renderTasksPage()} />;
}
