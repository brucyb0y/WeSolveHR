// Help & Commands (Server Component). Replaces GET /help: any logged-in user;
// admin-only sections show for managers/admins. The guide itself is a client
// island (search filter + click-to-copy).

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser, isManagerOrAdmin } from "@/lib/services/auth.js";
import HelpGuide from "./HelpGuide.jsx";

export const metadata = { title: "Help & Commands | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function HelpPage() {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  return (
    <>
      <TopNav active="help" />
      <HelpGuide isAdmin={isManagerOrAdmin(user)} />
    </>
  );
}
