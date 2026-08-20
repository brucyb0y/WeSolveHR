import "./globals.css";

export const metadata = {
  title: "WeSolveHR",
  description: "Internal operations dashboard",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body suppressHydrationWarning>{children}</body>
    </html>
  );
}
