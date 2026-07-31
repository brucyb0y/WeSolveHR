// Helpers for the POST-only handlers under app/form-post/*, which middleware.js
// rewrites to from the matching page URL.

import { FORM_POST_HEADER } from "../../middleware.js";

// Reject direct hits on /form-post/*; only the middleware rewrite sets this.
export function assertRewritten(request) {
  if (request.headers.get(FORM_POST_HEADER) !== "1") {
    return new Response("Not Found", { status: 404 });
  }
  return null;
}

// Express's urlencoded/multipart body parsing, as req.body was produced before.
export async function readFormBody(request) {
  const type = request.headers.get("content-type") || "";
  const body = {};
  if (type.includes("application/x-www-form-urlencoded")) {
    const params = new URLSearchParams(await request.text());
    for (const key of params.keys()) {
      const all = params.getAll(key);
      body[key] = all.length > 1 ? all : all[0];
    }
  } else if (type.includes("multipart/form-data")) {
    const form = await request.formData();
    for (const [key, value] of form.entries()) {
      if (value && typeof value.arrayBuffer === "function") continue;
      body[key] = value;
    }
  } else if (type.includes("application/json")) {
    const text = await request.text();
    Object.assign(body, text ? JSON.parse(text) : {});
  }
  return body;
}

export function redirectTo(request, path, setCookie) {
  const headers = new Headers({ location: new URL(path, request.url).toString() });
  if (setCookie) headers.append("set-cookie", setCookie);
  return new Response("", { status: 302, headers });
}
