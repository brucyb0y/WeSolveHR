// Bridge between a generated App Router route file and the original Express
// handler chain held in the route registry (lib/server/app.js).

import { app } from "./app.js";
import { runRoute } from "./adapter.js";

export function dispatch(method, pattern, request) {
  return runRoute(app, method, pattern, request);
}
