// GET /  — ported from lib/server/app.js lines 39087-39130.

import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderTopNav } from "@/lib/ui/nav.js";
import "./home.css";

export const metadata = { title: "WeSolveHR" };

export default function HomePage() {
  return (
    <>
      <RawHtml html={renderTopNav("dashboard")} />
      <div className="box">
        <h1>WeSolveHR Server</h1>
        <p>Webhook + Dashboard is running.</p>
        <a href="/dashboard">Open Dashboard</a>
      </div>
    </>
  );
}
