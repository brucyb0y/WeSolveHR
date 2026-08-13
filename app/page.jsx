// "/" — replaces the inline HTML in app.get("/").
//
// This route has no auth middleware, so TopNav is rendered with
// authenticated={false}: its Clients dropdown must stay empty here, matching the
// old behaviour where the client-side fetch to /api/clients/nav-list redirected
// to /login for an anonymous visitor.
//
// The original centred everything by putting `display:grid; place-items:center;
// height:100vh` on <body>, which made the nav a centred grid item rather than a
// full-width bar. That layout is reproduced on a wrapper div (body is now styled
// globally). min-height replaces height so the nav cannot be clipped; with this
// page's content the rendered result is the same.

import TopNav from "@/components/TopNav";
import { getSessionUser } from "@/lib/auth";
import styles from "./home.module.css";

export const metadata = { title: "WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function HomePage() {
  const user = await getSessionUser();

  return (
    <div className={styles.shell}>
      <TopNav active="dashboard" user={user} authenticated={false} />
      <div className={styles.box}>
        <h1>WeSolveHR Server</h1>
        <p>Webhook + Dashboard is running.</p>
        <a href="/dashboard">Open Dashboard</a>
      </div>
    </div>
  );
}
