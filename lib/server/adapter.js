// Express -> Web (Next.js App Router) adapter.
//
// The original server.js registered routes as `app.get/post/...(path, ...handlers)`
// and relied on express middleware (urlencoded/json/session/multer) plus the
// req/res API. This module reproduces that surface so the original route bodies
// run completely unchanged inside Next.js route handlers.

import { loadSession, commitSession } from "./session.js";

// ---------------------------------------------------------------------------
// Route collector: a drop-in replacement for the express `app` object.
// app.get/post/put/delete/patch record routes; app.use records mounted
// middleware (e.g. app.use("/api", requireDashboardAuth)). app.listen is a no-op.
// ---------------------------------------------------------------------------
export function createCollector() {
  const routes = [];
  const mounts = [];
  const add = (method, path, handlers) =>
    routes.push({ method, path, handlers });
  return {
    routes,
    mounts,
    get: (p, ...h) => add("GET", p, h),
    post: (p, ...h) => add("POST", p, h),
    put: (p, ...h) => add("PUT", p, h),
    delete: (p, ...h) => add("DELETE", p, h),
    patch: (p, ...h) => add("PATCH", p, h),
    use: (a, ...rest) => {
      if (typeof a === "string") mounts.push({ prefix: a, handlers: rest });
      else mounts.push({ prefix: "/", handlers: [a, ...rest] });
    },
    listen: () => {},
  };
}

// ---------------------------------------------------------------------------
// multer-compatible in-memory upload. The adapter pre-parses multipart bodies
// into req._files; upload.single(field) then exposes req.file with the same
// shape multer's memoryStorage produced ({ buffer, originalname, mimetype, ...}).
// ---------------------------------------------------------------------------
export function createUpload() {
  const single = (field) => async (req, _res, next) => {
    const file = req._files && req._files[field];
    if (file) {
      const buf = Buffer.from(await file.arrayBuffer());
      req.file = {
        fieldname: field,
        originalname: file.name,
        mimetype: file.type || "application/octet-stream",
        size: buf.length,
        buffer: buf,
      };
    }
    next();
  };
  const passthrough = () => async (_req, _res, next) => next();
  return {
    single,
    array: single,
    fields: passthrough,
    none: passthrough,
    any: passthrough,
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function matchPath(pattern, pathname) {
  const pSegs = pattern.split("/");
  const uSegs = pathname.split("/");
  if (pSegs.length !== uSegs.length) return null;
  const params = {};
  for (let i = 0; i < pSegs.length; i++) {
    const ps = pSegs[i];
    const us = uSegs[i];
    if (ps.startsWith(":")) {
      params[ps.slice(1)] = decodeURIComponent(us);
    } else if (ps !== us) {
      return null;
    }
  }
  return params;
}

function toObject(params) {
  const out = {};
  for (const key of params.keys()) {
    const all = params.getAll(key);
    out[key] = all.length > 1 ? all : all[0];
  }
  return out;
}

function normalizeType(t) {
  const s = String(t);
  if (s.includes("/")) {
    if (s.startsWith("text/") && !s.includes("charset")) {
      return s + "; charset=utf-8";
    }
    return s;
  }
  switch (s) {
    case "html":
      return "text/html; charset=utf-8";
    case "json":
      return "application/json; charset=utf-8";
    case "text":
    case "txt":
      return "text/plain; charset=utf-8";
    case "xml":
      return "application/xml";
    default:
      return s;
  }
}

// ---------------------------------------------------------------------------
// Build an express-like req from a Web Request.
// ---------------------------------------------------------------------------
async function buildReq(request, pattern, sessionBox) {
  const url = new URL(request.url);
  const pathname = url.pathname;

  const headers = {};
  request.headers.forEach((v, k) => {
    headers[k.toLowerCase()] = v;
  });

  const req = {
    method: request.method,
    url: pathname + url.search,
    originalUrl: pathname + url.search,
    path: pathname,
    baseUrl: "",
    query: toObject(url.searchParams),
    params: matchPath(pattern, pathname) || {},
    headers,
    body: {},
    rawBody: undefined,
    file: undefined,
    _files: undefined,
    loggedInUser: undefined,
    session: sessionBox.session,
    protocol: headers["x-forwarded-proto"] || "http",
    ip:
      (headers["x-forwarded-for"] || "").split(",")[0].trim() ||
      headers["x-real-ip"] ||
      "",
    get(name) {
      return this.headers[String(name).toLowerCase()];
    },
  };

  const method = request.method.toUpperCase();
  if (method !== "GET" && method !== "HEAD") {
    const ct = headers["content-type"] || "";
    try {
      if (ct.includes("application/json")) {
        const text = await request.text();
        req.rawBody = text;
        req.body = text ? JSON.parse(text) : {};
      } else if (ct.includes("application/x-www-form-urlencoded")) {
        const text = await request.text();
        req.rawBody = text;
        req.body = toObject(new URLSearchParams(text));
      } else if (ct.includes("multipart/form-data")) {
        const form = await request.formData();
        const body = {};
        const files = {};
        for (const [name, value] of form.entries()) {
          if (value && typeof value.arrayBuffer === "function") {
            files[name] = value;
          } else {
            body[name] = value;
          }
        }
        req.body = body;
        req._files = files;
      } else {
        const text = await request.text();
        req.rawBody = text;
        req.body = {};
      }
    } catch (err) {
      console.error("adapter buildReq body parse error:", err);
      req.body = {};
    }
  }

  return req;
}

// ---------------------------------------------------------------------------
// Build an express-like res that accumulates status/headers/body.
// ---------------------------------------------------------------------------
function buildRes(req) {
  return {
    req,
    statusCode: 200,
    _headers: {},
    _body: undefined,
    _ended: false,
    status(code) {
      this.statusCode = code;
      return this;
    },
    setHeader(name, value) {
      this._headers[String(name).toLowerCase()] = String(value);
      return this;
    },
    getHeader(name) {
      return this._headers[String(name).toLowerCase()];
    },
    get(name) {
      return this.getHeader(name);
    },
    type(t) {
      this.setHeader("content-type", normalizeType(t));
      return this;
    },
    json(obj) {
      if (!this._headers["content-type"]) {
        this.setHeader("content-type", "application/json; charset=utf-8");
      }
      this._body = JSON.stringify(obj);
      this._ended = true;
      return this;
    },
    send(body) {
      if (this._ended) return this;
      if (body === undefined || body === null) {
        this._body = "";
      } else if (Buffer.isBuffer(body)) {
        if (!this._headers["content-type"]) {
          this.setHeader("content-type", "application/octet-stream");
        }
        this._body = body;
      } else if (typeof body === "object") {
        return this.json(body);
      } else {
        if (!this._headers["content-type"]) {
          this.setHeader("content-type", "text/html; charset=utf-8");
        }
        this._body = String(body);
      }
      this._ended = true;
      return this;
    },
    redirect(arg1, arg2) {
      const location = typeof arg1 === "number" ? arg2 : arg1;
      const status = typeof arg1 === "number" ? arg1 : 302;
      this.statusCode = status;
      this.setHeader("location", location);
      // Reproduce express's small negotiated redirect body (invisible to
      // browsers, which follow Location, but kept for byte-fidelity).
      const accept = (this.req && this.req.headers["accept"]) || "";
      const message = REDIRECT_MESSAGES[status] || "Redirecting";
      const mode = chooseRedirectFormat(accept);
      if (mode === "html") {
        const u = escapeRedirectUrl(location);
        if (!this._headers["content-type"])
          this.setHeader("content-type", "text/html; charset=utf-8");
        this._body = `<p>${message}. Redirecting to <a href="${u}">${u}</a></p>`;
      } else if (mode === "text") {
        if (!this._headers["content-type"])
          this.setHeader("content-type", "text/plain; charset=utf-8");
        this._body = `${message}. Redirecting to ${location}`;
      } else {
        this._body = "";
      }
      this._ended = true;
      return this;
    },
    writeHead(code, headersObj) {
      this.statusCode = code;
      if (headersObj) {
        for (const [k, v] of Object.entries(headersObj)) this.setHeader(k, v);
      }
      return this;
    },
    end(body) {
      if (body !== undefined && body !== null) this._body = body;
      this._ended = true;
      return this;
    },
  };
}

function prefixMatches(prefix, path) {
  if (prefix === "/") return true;
  return path === prefix || path.startsWith(prefix + "/");
}

const REDIRECT_MESSAGES = {
  301: "Moved Permanently",
  302: "Found",
  303: "See Other",
  307: "Temporary Redirect",
  308: "Permanent Redirect",
};

function escapeRedirectUrl(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// Mirror express's res.redirect content negotiation (req.accepts(['text','html'])).
function chooseRedirectFormat(accept) {
  if (!accept) return "text"; // no Accept -> */* -> first provided type (text)
  const a = accept.toLowerCase();
  const qFor = (type) => {
    const group = type.split("/")[0] + "/*";
    let best = -1;
    for (const partRaw of a.split(",")) {
      const part = partRaw.trim();
      const segs = part.split(";").map((s) => s.trim());
      const t = segs[0];
      let q = 1;
      for (let i = 1; i < segs.length; i++) {
        const m = segs[i].match(/^q=([0-9.]+)/);
        if (m) q = parseFloat(m[1]);
      }
      if (t === type || t === group || t === "*/*") {
        if (q > best) best = q;
      }
    }
    return best;
  };
  const qHtml = qFor("text/html");
  const qText = qFor("text/plain");
  if (qHtml < 0 && qText < 0) return "default";
  return qHtml > qText ? "html" : "text"; // tie -> text (express returns first)
}

// express strips the mount path from req.url/req.path inside app.use(path, fn).
// The only consumer in this app is requireDashboardAuth's isApiRoute check, so
// reproducing this is what makes unauthenticated /api/* redirect (302) instead
// of returning the 401 JSON branch.
function applyMountPath(req, prefix) {
  if (prefix === "/") return;
  req._savedPath = req.path;
  req._savedUrl = req.url;
  req._savedBaseUrl = req.baseUrl;
  const stripped = req.path.slice(prefix.length) || "/";
  const qIdx = req.originalUrl.indexOf("?");
  const search = qIdx >= 0 ? req.originalUrl.slice(qIdx) : "";
  req.path = stripped;
  req.url = stripped + search;
  req.baseUrl = prefix;
}

function restoreMountPath(req, prefix) {
  if (prefix === "/") return;
  req.path = req._savedPath;
  req.url = req._savedUrl;
  req.baseUrl = req._savedBaseUrl;
}

// Run an express-style middleware/handler chain. Stops when a handler responds
// (res._ended) or fails to call next().
async function runChain(req, res, handlers) {
  for (const handler of handlers) {
    let nextCalled = false;
    const next = (err) => {
      if (err) throw err;
      nextCalled = true;
    };
    await handler(req, res, next);
    if (res._ended) return;
    if (!nextCalled) return;
  }
}

// ---------------------------------------------------------------------------
// Orchestrate a single request for the given (method, pattern).
// ---------------------------------------------------------------------------
export async function runRoute(app, method, pattern, request) {
  // First-match semantics, like express.
  const route = app.routes.find(
    (r) => r.method === method && r.path === pattern,
  );
  if (!route) return new Response("Not Found", { status: 404 });

  const sessionBox = loadSession(request.headers.get("cookie"));
  const req = await buildReq(request, pattern, sessionBox);
  const res = buildRes(req);

  try {
    // Phase 1: mounted middleware (e.g. app.use("/api", requireDashboardAuth)),
    // run first and with the mount path stripped from req.path/req.url, exactly
    // as express applied it ahead of the matched route's own handlers.
    for (const m of app.mounts) {
      if (!prefixMatches(m.prefix, pattern)) continue;
      applyMountPath(req, m.prefix);
      try {
        await runChain(req, res, m.handlers);
      } finally {
        restoreMountPath(req, m.prefix);
      }
      if (res._ended) break;
    }
    // Phase 2: the matched route's own handler chain, with the full path.
    if (!res._ended) {
      await runChain(req, res, route.handlers);
    }
  } catch (err) {
    console.error("Route handler error:", method, pattern, err);
    if (!res._ended) {
      res.statusCode = 500;
      res.setHeader("content-type", "text/html; charset=utf-8");
      res._body = "Internal Server Error";
    }
  }

  const setCookie = commitSession(sessionBox.session, sessionBox.meta);

  const headers = new Headers();
  for (const [k, v] of Object.entries(res._headers)) headers.set(k, v);
  if (setCookie) headers.append("set-cookie", setCookie);

  let body;
  if (res._body === undefined || res._body === null) {
    body = "";
  } else if (Buffer.isBuffer(res._body)) {
    body = res._body;
  } else if (typeof res._body === "string") {
    body = res._body;
  } else {
    body = String(res._body);
  }

  return new Response(body, { status: res.statusCode, headers });
}
