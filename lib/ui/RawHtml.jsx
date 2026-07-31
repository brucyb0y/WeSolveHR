// Bridge for page markup that is still produced as an HTML string by the
// ported builders.
//
// `display: contents` keeps this wrapper out of the layout tree, so the
// injected nodes remain direct layout children of <body> exactly as they were
// when each page emitted its own document. That matters for the pages whose
// CSS lays out the body itself (e.g. `body { display: grid }` on /).

// suppressHydrationWarning is required, not cosmetic. On hydration React
// compares this element's `__html` against the wrapper's live innerHTML, and
// the browser's parser re-serialises markup as it sees fit — it normalises
// attributes and whitespace, inserts implied elements such as <tbody>, and
// repairs the odd unbalanced tag the ported markup still carries. So the two
// strings rarely match byte-for-byte even though the DOM is exactly right.
// React says as much in the warning: "This won't be patched up."
//
// Suppressing is safe here because this subtree is server-rendered and never
// re-rendered on the client: the pages hold no React state and every link is a
// plain <a>, so navigation is a full document load. It is also scoped to this
// one element — real mismatches elsewhere in the tree are still reported.
export default function RawHtml({ html }) {
  return (
    <div
      data-raw-html=""
      style={{ display: "contents" }}
      suppressHydrationWarning
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
