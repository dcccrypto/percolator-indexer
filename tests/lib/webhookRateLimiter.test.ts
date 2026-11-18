/**
 * GH#234 (PR #235 by @0x-SquidSol, ported to v2.2): the /webhook/trades limiter, driven through a
 * real Hono app with the S3-C11 client-IP key the service uses.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { Hono } from "hono";
import { describe, expect, it, vi } from "vitest";
import { clientIp } from "../../src/lib/clientIp.js";
import {
  createWebhookRateLimiter,
  DEFAULT_WEBHOOK_RATE_LIMIT_PER_MIN,
  webhookRateLimitPerMin,
  type WebhookRateLimiterOptions,
} from "../../src/lib/webhookRateLimiter.js";

const HELIUS_IP = "203.0.113.7";

function appWith(opts: Partial<WebhookRateLimiterOptions> = {}) {
  const rl = createWebhookRateLimiter({
    maxRequests: 3,
    windowMs: 60_000,
    keyOf: (c) => clientIp({ header: (n) => c.req.header(n), depth: 1 }),
    ...opts,
  });
  const app = new Hono();
  app.use("/webhook/trades", rl.middleware);
  // Stands in for routes/webhook.ts: a request without the secret is rejected as 401.
  app.post("/webhook/trades", (c) =>
    c.req.header("authorization") === "secret" ? c.json({ ok: true }, 200) : c.json({ error: "Unauthorized" }, 401),
  );
  const post = (xff: string, auth = true) =>
    app.request("/webhook/trades", {
      method: "POST",
      headers: { "x-forwarded-for": xff, ...(auth ? { authorization: "secret" } : {}) },
      body: "[]",
    });
  return { rl, post };
}

describe("GH#234: webhook rate-limit budget", () => {
  it("defaults to 900/min, above the measured live delivery rate (438 POSTs/min worst case)", () => {
    expect(DEFAULT_WEBHOOK_RATE_LIMIT_PER_MIN).toBe(900);
    expect(webhookRateLimitPerMin(undefined)).toBe(900);
  });

  it("WEBHOOK_RATE_LIMIT_PER_MIN: a positive integer is used; anything else falls back, never NaN", () => {
    expect(webhookRateLimitPerMin("250")).toBe(250);
    expect(webhookRateLimitPerMin(" 1200 ")).toBe(1200);
    for (const bad of ["", "   ", "abc", "0", "-5", "1.5", "NaN", "Infinity"]) {
      const v = webhookRateLimitPerMin(bad);
      expect(v, JSON.stringify(bad)).toBe(900);
    }
  });

  it("a minute of live Helius traffic from one egress IP is served at the default budget", async () => {
    const { post } = appWith({ maxRequests: webhookRateLimitPerMin(undefined) });
    for (let i = 0; i < 438; i++) {
      const res = await post(`10.0.0.1, ${HELIUS_IP}`);
      expect(res.status, `delivery ${i}`).toBe(200);
    }
  });

  it("index.ts wires the parsed budget and the S3-C11 key into the extracted limiter", () => {
    const src = readFileSync(join(__dirname, "..", "..", "src", "index.ts"), "utf8");
    expect(src).toMatch(/createWebhookRateLimiter\(\{/);
    expect(src).toMatch(/maxRequests: webhookRateLimitPerMin\(/);
    expect(src).not.toMatch(/rateLimitMaxRequests = 100/);
    expect(src).toMatch(/app\.use\("\/webhook\/trades", rateLimiter\)/);
  });
});

describe("GH#234: webhook limiter behaviour on the S3-C11 key", () => {
  it("rejects with 429 + Retry-After once the budget is spent, and calls onLimit", async () => {
    const onLimit = vi.fn();
    const { post } = appWith({ onLimit });
    for (let i = 0; i < 3; i++) expect((await post(HELIUS_IP)).status).toBe(200);
    const res = await post(HELIUS_IP);
    expect(res.status).toBe(429);
    expect(Number(res.headers.get("retry-after"))).toBeGreaterThan(0);
    expect(onLimit).toHaveBeenCalledWith({ ip: HELIUS_IP, count: 4 });
  });

  it("an unauthenticated flood naming the Helius IP in a spoofed left entry spends only the flooder's bucket", async () => {
    const { rl, post } = appWith();
    const attacker = "198.51.100.66";
    for (let i = 0; i < 50; i++) await post(`${HELIUS_IP}, ${attacker}`, false);
    expect((await post(`${HELIUS_IP}, ${attacker}`, false)).status).toBe(429);
    expect(rl.buckets.get(HELIUS_IP)).toBeUndefined();
    for (let i = 0; i < 3; i++) {
      expect((await post(`10.0.0.1, ${HELIUS_IP}`)).status, `genuine delivery ${i}`).toBe(200);
    }
  });

  it("requests the route rejects still spend the caller's own budget (the route reads the body before auth)", async () => {
    const { rl, post } = appWith();
    for (let i = 0; i < 3; i++) expect((await post(HELIUS_IP, false)).status).toBe(401);
    expect(rl.buckets.get(HELIUS_IP)?.count).toBe(3);
    expect((await post(HELIUS_IP, false)).status).toBe(429);
  });

  it("the window resets, and pruning drops expired buckets", async () => {
    let t = 1_000_000;
    const { rl, post } = appWith({ now: () => t });
    for (let i = 0; i < 3; i++) await post(HELIUS_IP);
    expect((await post(HELIUS_IP)).status).toBe(429);
    t += 60_001;
    expect((await post(HELIUS_IP)).status).toBe(200);
    t += 60_001;
    rl.pruneExpired();
    expect(rl.buckets.size).toBe(0);
  });
});

describe("window boundary and prune", () => {
  it("a request at exactly resetTime still belongs to the window; one ms later opens a new one", async () => {
    let t = 5_000;
    const { rl, post } = appWith({ now: () => t });
    for (let i = 0; i < 3; i++) await post(HELIUS_IP);
    t = 5_000 + 60_000; // exactly resetTime: t > resetTime is false
    expect((await post(HELIUS_IP)).status).toBe(429);
    t += 1;
    expect((await post(HELIUS_IP)).status).toBe(200);
    expect(rl.buckets.get(HELIUS_IP)?.count).toBe(1);
  });

  it("pruneExpired keeps live buckets (incl. exactly at resetTime) and drops only expired ones", async () => {
    let t = 0;
    const { rl, post } = appWith({ now: () => t });
    await post("10.0.0.1");
    t = 30_000;
    await post("10.0.0.2");
    t = 60_000;
    rl.pruneExpired();
    expect([...rl.buckets.keys()].sort()).toEqual(["10.0.0.1", "10.0.0.2"]);
    t = 60_001;
    rl.pruneExpired();
    expect([...rl.buckets.keys()]).toEqual(["10.0.0.2"]);
  });

  it("defaults to the wall clock when no now() is injected", async () => {
    const { rl, post } = appWith();
    await post(HELIUS_IP);
    expect(rl.buckets.get(HELIUS_IP)!.resetTime).toBeGreaterThan(Date.now());
  });
});

describe("backwards wall-clock step (NTP correction, VM resume)", () => {
  const HOUR = 3_600_000;

  it("a bucket spent before a 1 h backwards step resets within one window of real elapsed time", async () => {
    let t = 10 * HOUR;
    const { post } = appWith({ now: () => t });
    for (let i = 0; i < 3; i++) await post(HELIUS_IP);
    expect((await post(HELIUS_IP)).status).toBe(429);
    t -= HOUR; // clock steps back
    // The step itself may still be inside the old window; after one real window it must be served.
    t += 60_001;
    expect((await post(HELIUS_IP)).status).toBe(200);
  });

  it("Retry-After never exceeds the window after a backwards step", async () => {
    let t = 10 * HOUR;
    const { post } = appWith({ now: () => t });
    for (let i = 0; i < 3; i++) await post(HELIUS_IP);
    t -= HOUR;
    const res = await post(HELIUS_IP);
    expect(Number(res.headers.get("retry-after") ?? "0")).toBeLessThanOrEqual(60);
  });

  it("pruneExpired drops a bucket whose resetTime is more than one window ahead of now", async () => {
    let t = 10 * HOUR;
    const { rl, post } = appWith({ now: () => t });
    await post(HELIUS_IP);
    t -= HOUR;
    rl.pruneExpired();
    expect(rl.buckets.size).toBe(0);
  });
});
