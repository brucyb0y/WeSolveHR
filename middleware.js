import { NextResponse } from "next/server";

// The App Router cannot host a page.jsx and a route.js at the same path, but
// three of the ported screens are URLs that answer both GET (render the form)
// and POST (submit it): /login, /clients/:id/edit and /clients/:id/reset.
//
// The GET stays a real page; the POST is rewritten to a handler under
// /form-post/* so the browser keeps posting to the original URL and no form
// markup had to change. The rewrite tags the request with a header the handler
// checks, so the /form-post/* URLs are not usable directly.

export const FORM_POST_HEADER = "x-wesolve-form-post";

const FORM_POST_PATHS = [
  /^\/login$/,
  /^\/clients\/[^/]+\/edit$/,
  /^\/clients\/[^/]+\/reset$/,
];

export function middleware(request) {
  if (request.method !== "POST") return NextResponse.next();

  const { pathname } = request.nextUrl;
  if (!FORM_POST_PATHS.some((re) => re.test(pathname))) return NextResponse.next();

  const url = request.nextUrl.clone();
  url.pathname = "/form-post" + pathname;

  const headers = new Headers(request.headers);
  headers.set(FORM_POST_HEADER, "1");
  return NextResponse.rewrite(url, { request: { headers } });
}

export const config = {
  matcher: ["/login", "/clients/:id/edit", "/clients/:id/reset"],
};
