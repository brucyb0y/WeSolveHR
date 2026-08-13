// /tasks — replaces the inline HTML in app.get("/tasks").
//
// Initial filter state is resolved here from the query string, reproducing
// applyFiltersFromUrl(): explicit progressBucket params win; otherwise
// blocked=true seeds its own bucket set; otherwise the markup defaults apply.
// Passing it down as props means the first render is already filtered, instead
// of the old sequence that painted defaults and then rewrote every control.

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import TasksConsole from "./TasksConsole";
import styles from "./tasks.module.css";

export const metadata = { title: "Tasks" };
export const dynamic = "force-dynamic";

// The options carrying `selected` in the original markup.
const DEFAULT_BUCKETS = [
  "not_begun",
  "zero_to_fifty",
  "fifty_to_hundred",
  "hide_cancelled",
];

// What applyFiltersFromUrl() selected when ?blocked=true arrived with no
// explicit progressBucket params.
const BLOCKED_BUCKETS = [
  "not_begun",
  "zero_to_fifty",
  "fifty_to_hundred",
  "complete",
  "hide_cancelled",
];

const asArray = (v) => (v === undefined ? [] : Array.isArray(v) ? v : [v]);

export default async function TasksPage({ searchParams }) {
  const user = await requireDashboardUser();
  const sp = await searchParams;

  const blocked = sp?.blocked === "true";
  const buckets = asArray(sp?.progressBucket);

  const progressBucket = buckets.length
    ? buckets
    : blocked
      ? BLOCKED_BUCKETS
      : DEFAULT_BUCKETS;

  const initialFilters = {
    search: sp?.search || "",
    assignee: sp?.assignee || "",
    business: sp?.business || "",
    area: sp?.area || "",
    status: sp?.status || "",
    priority: sp?.priority || "",
    blocked,
    overdue: sp?.overdue === "true",
    progressBucket,
  };

  return (
    <>
      <TopNav active="tasks" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Task Operations</div>
            <h1>WeSolveHR // Tasks Console</h1>
            <div className={styles.subtitle}>
              Filter and inspect work across the team without changing backend
              behavior
            </div>
          </div>
        </div>

        <TasksConsole
          initialFilters={initialFilters}
          initialWaitingOn={sp?.waitingOn || ""}
        />
      </div>
    </>
  );
}
