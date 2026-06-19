// Root layout for converted (JSX) pages. Route handlers (app/**/route.js served
// through the dispatch shim) bypass layouts entirely, so this only wraps real
// page.jsx routes — currently /login, with more added as the migration proceeds.

import "./globals.css";

export const metadata = {
  title: "WeSolveHR",
  description: "Team operations workspace",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
