"use client";

// A whole-row link. The old markup used
// onclick="window.location.href='/leads/<business>'" on the <tr>.
//
// window.location is kept rather than router.push because /leads/:business is
// still an Express-backed route handler returning a full HTML document, which
// the client-side router cannot consume as an RSC payload. This becomes a
// router.push once that page is converted.

export default function ClickableRow({ href, className, children }) {
  return (
    <tr
      className={className}
      onClick={() => {
        window.location.href = href;
      }}
    >
      {children}
    </tr>
  );
}
