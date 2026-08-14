// Root layout for the React tree.
//
// The HTML-string pages in lib/server/app.js each emitted a whole document with
// the theme inlined in a <style> tag. Converted pages instead inherit this
// document shell and the same rules from globals.css, which is generated from
// those very builders (scripts/gen-css.mjs), so the rendered result is
// unchanged.
//
// Route handlers under app/**/route.js are unaffected by this file — they still
// return complete documents of their own through the Express adapter.

import "./globals.css";

export const metadata = {
  title: "WeSolveHR",
  description: "Internal operations dashboard",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      {/*
        suppressHydrationWarning is here for BROWSER EXTENSIONS, not for our own
        markup. Extensions inject attributes into <body> before React hydrates —
        ColorZilla adds cz-shortcut-listen="true", Grammarly adds data-gr-*, and
        others do the same — which React reports as a hydration mismatch the app
        cannot fix or control.

        It is deliberately narrow: the flag suppresses warnings for THIS
        element's own attributes and text only, never its descendants. Real
        hydration bugs inside the page still surface as errors.
      */}
      <body suppressHydrationWarning>{children}</body>
    </html>
  );
}
