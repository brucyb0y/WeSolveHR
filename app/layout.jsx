// Root layout. The ported pages each build their own <body> content, so this
// only supplies the document shell that every page previously emitted itself
// (<html> and <body>, both attribute-free in the original markup).

export const metadata = {
  title: "WeSolveHR",
};

// No viewport is declared here on purpose: the original markup only emitted a
// viewport meta on /help and /team-work, and those two pages declare their own.

// Browser extensions commonly stamp attributes onto <html>/<body> before React
// hydrates — ColorZilla's cz-shortcut-listen, Grammarly's data-gr-* and so on —
// which React reports as a hydration mismatch even though the server output is
// clean. suppressHydrationWarning silences that for these two elements only: it
// applies one level deep, so a genuine mismatch inside a page still surfaces.
export default function RootLayout({ children }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body suppressHydrationWarning>{children}</body>
    </html>
  );
}
