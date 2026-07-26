/**
 * #163 wiring guard: the /health route must go through the cached, coalesced
 * checker — not probe the backends directly.
 *
 * Why this exists as a separate, source-level test:
 *
 * `healthCache.test.ts` proves the checker bounds amplification, but it proves
 * nothing about whether `/health` still USES it. `src/index.ts` starts a server
 * on import, so it cannot be imported into a unit test to assert the wiring
 * behaviourally — which is exactly why the probe was extracted in the first
 * place.
 *
 * That leaves a gap with real teeth: if the handler were ever changed back to
 * calling `getConnection().getSlot()` / `getSupabase()` inline, every test in
 * healthCache.test.ts would stay green while #163 silently returned. Source
 * introspection is a weak instrument, but a weak guard on the actual regression
 * beats a strong guard on a layer the regression can bypass.
 */

import { describe, it, expect, beforeAll } from "vitest";
import { readFile } from "node:fs/promises";
import { join } from "node:path";

let source: string;
/** The body of the app.get("/health", ...) handler. */
let handler: string;

beforeAll(async () => {
  source = await readFile(join(process.cwd(), "src/index.ts"), "utf8");

  const start = source.indexOf('app.get("/health"');
  expect(start, "/health route not found in src/index.ts").toBeGreaterThan(-1);
  // Up to the next top-level route/mount registration.
  const rest = source.slice(start + 1);
  const end = rest.search(/\napp\.(get|post|use|route|all)\(/);
  handler = end === -1 ? rest : rest.slice(0, end);
});

describe("#163: /health wiring", () => {
  it("routes the probe through the cached checker", () => {
    expect(source).toContain("createHealthChecker");
    expect(handler).toContain("healthCheck()");
  });

  it("does not probe the RPC directly inside the handler", () => {
    // The direct call belongs in the probeRpc callback passed to
    // createHealthChecker, where the cache and single-flight wrap it — not in
    // the request path, where every request would hit it.
    expect(handler).not.toMatch(/getConnection\s*\(/);
    expect(handler).not.toMatch(/getSlot\s*\(/);
  });

  it("does not query the database directly inside the handler", () => {
    expect(handler).not.toMatch(/getSupabase\s*\(/);
  });

  it("still surfaces a non-ok status as 503", () => {
    // Guards the pre-existing behaviour the cache change had to preserve:
    // degraded and down must not be reported as healthy to load balancers.
    expect(handler).toMatch(/status === "ok" \? 200 : 503/);
  });

  it("keeps the checker construction outside the request path", () => {
    // Constructing it per-request would give every request its own empty cache,
    // reintroducing the amplification with the cache technically present.
    const ctorIndex = source.indexOf("createHealthChecker(");
    const routeIndex = source.indexOf('app.get("/health"');
    expect(ctorIndex).toBeGreaterThan(-1);
    expect(ctorIndex).toBeLessThan(routeIndex);
  });
});
