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
