// Regression test for J-20261120-IDX-REPORT-SECRETURLINLOGS: Sentry events carried the Helius RPC
// api-key, the Helius webhook-management api-key and the Discord webhook token in their http
// breadcrumbs (`data["http.query"]`, `data.url`). FAKE values only; everything is local (a stub
// Sentry ingest and stub HTTP servers on 127.0.0.1).
import { describe, it, expect, afterAll } from "vitest";
import http from "node:http";
import { gunzipSync } from "node:zlib";
import type { AddressInfo } from "node:net";

// Variable path: on the old code the module does not exist, so the end-to-end test runs without
// the scrubber and fails on the leak instead of on a missing import.
const SCRUB_PATH = "../../src/lib/sentryScrub.js";
type ScrubModule = typeof import("../../src/lib/sentryScrub.js");
const loadScrub = (): Promise<ScrubModule | null> => import(/* @vite-ignore */ SCRUB_PATH).catch(() => null);

function listen(handler: http.RequestListener): Promise<http.Server> {
  return new Promise((resolve) => {
    const s = http.createServer(handler);
    s.listen(0, "127.0.0.1", () => resolve(s));
  });
}
const port = (s: http.Server) => (s.address() as AddressInfo).port;

// Built at runtime: Sentry ships source context of the throwing frame, so a literal secret in this
// file would show up in the envelope even when every breadcrumb is clean.
const fake = (name: string) => ["FAKE", name].join("");
const RPC_KEY = fake("HELIUS1");
const WH_KEY = fake("HELIUSWH2");
const DISCORD_TOKEN = fake("DISCORD8");

const servers: http.Server[] = [];
afterAll(() => servers.forEach((s) => s.close()));

// Shared by the two end-to-end tests below (one Sentry client per process).
const envelopes: string[] = [];
let base = "";

describe("Sentry events carry no api-key or Discord webhook token", () => {
  it("an exception captured after RPC, Helius and Discord calls ships scrubbed breadcrumbs", async () => {
    const sentry = await listen((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        // The node transport gzips bodies over 32 KB; unzip so the string checks see the payload.
        const body = Buffer.concat(chunks);
        envelopes.push((req.headers["content-encoding"] === "gzip" ? gunzipSync(body) : body).toString("utf8"));
        res.end("{}");
      });
    });
    const ok = await listen((_req, res) => {
      res.setHeader("content-type", "application/json");
      res.end('{"jsonrpc":"2.0","id":1,"result":1}');
    });
    servers.push(sentry, ok);

    process.env.SENTRY_DSN = `http://FAKESENTRYPUB7@127.0.0.1:${port(sentry)}/1`;
    const { initSentry, captureException } = await import("@percolatorct/shared");
    initSentry("indexer");
    const scrub = await loadScrub();
    // Empty env: the patterns alone must catch these, without knowing the configured values.
    scrub?.installSentryScrubber({});

    base = `http://127.0.0.1:${port(ok)}`;
    // web3.js Connection RPC call: RPC_URL carries the key in its query string.
    await fetch(`${base}/?api-key=${RPC_KEY}`, { method: "POST", body: "{}" });
    // HeliusWebhookManager call (src/services/HeliusWebhookManager.ts builds ?api-key=).
    await fetch(`${base}/v0/webhooks?api-key=${WH_KEY}`);
    // Discord alert POST: the token is in the URL path.
    await fetch(`${base}/api/webhooks/1/${DISCORD_TOKEN}`, { method: "POST", body: "{}" });
    captureException(new Error("any later failure"), { tags: { context: "regression" } });

    // Wait for the error event itself: sampled http.server transactions from the stubs can arrive first.
    const errorEvent = () => envelopes.find((e) => e.includes('"type":"event"'));
    for (let i = 0; i < 100 && !errorEvent(); i++) await new Promise((r) => setTimeout(r, 100));
    expect(errorEvent()).toBeDefined();
    const all = envelopes.join("\n");
    expect.soft(all).not.toContain(RPC_KEY);
    expect.soft(all).not.toContain(WH_KEY);
    expect.soft(all).not.toContain(DISCORD_TOKEN);
    expect.soft(all).not.toContain('"http.query"');
    // The breadcrumbs themselves are kept (host and path survive), only the secrets go.
    // Checked on the error event itself: the stubs' http.server transactions carry the same paths.
    expect.soft(errorEvent()!).toContain(`/api/webhooks/1/[Filtered]`);
    expect.soft(errorEvent()!).toContain(`127.0.0.1:${port(ok)}/v0/webhooks`);
  }, 60_000);

  it("a sampled transaction is still sent, and carries no api-key", async () => {
    const Sentry = await import("@sentry/node");
    // shared samples 10% of transactions; force this one. The event holds live scopes in
    // sdkProcessingMetadata, which the scrubber must leave alone (walking them threw and dropped it).
    Sentry.getClient()!.getOptions().tracesSampleRate = 1;
    envelopes.length = 0;
    await Sentry.startSpan({ name: "scrub-tx", forceTransaction: true }, async () => {
      await fetch(`${base}/?api-key=${RPC_KEY}`, { method: "POST", body: "{}" });
    });
    const tx = () => envelopes.find((e) => e.includes('"type":"transaction"') && e.includes('"transaction":"scrub-tx"'));
    for (let i = 0; i < 100 && !tx(); i++) await new Promise((r) => setTimeout(r, 100));
    expect(tx()).toBeDefined();
    expect(envelopes.join("\n")).not.toContain(RPC_KEY);
  }, 60_000);
});

describe("scrubString / scrubDeep", () => {
  it("redacts api-key params and Discord webhook tokens", async () => {
    const m = await loadScrub();
    expect(m).not.toBeNull();
    const { scrubString } = m!;
    expect(scrubString("https://rpc.example/?api-key=AAAA&x=1")).toBe("https://rpc.example/?api-key=[Filtered]&x=1");
    expect(scrubString("?API_KEY=abc")).toBe("?API_KEY=[Filtered]");
    expect(scrubString("https://discord.com/api/webhooks/123/tok-en_X?wait=true")).toBe(
      "https://discord.com/api/webhooks/123/[Filtered]?wait=true",
    );
    expect(scrubString("https://discord.com/api/v10/webhooks/123/tok")).toBe("https://discord.com/api/v10/webhooks/123/[Filtered]");
    expect(scrubString("fetch failed")).toBe("fetch failed");
  });

  it("redacts configured secret values wherever they appear", async () => {
    const m = await loadScrub();
    expect(m).not.toBeNull();
    const { scrubDeep } = m!;
    const secrets = ["FAKEPATHKEY12345"];
    const ev = { message: "GET https://rpc.example/FAKEPATHKEY12345/", list: ["x FAKEPATHKEY12345 y"] };
    expect(scrubDeep(ev, secrets)).toEqual({ message: "GET https://rpc.example/[Filtered]/", list: ["x [Filtered] y"] });
  });

  it("drops http.query/url.query keys and tolerates cycles", async () => {
    const m = await loadScrub();
    expect(m).not.toBeNull();
    const { scrubDeep } = m!;
    const crumb: Record<string, unknown> = { data: { "http.query": "?api-key=K", "url.query": "api-key=K", url: "http://h/", n: 1 } };
    crumb.self = crumb;
    scrubDeep(crumb);
    expect(crumb.data).toEqual({ url: "http://h/", n: 1 });
    expect(crumb.self).toBe(crumb);
  });

  it("collects configured secret values from the env (FAKE values)", async () => {
    const m = await loadScrub();
    expect(m).not.toBeNull();
    const got = m!.configuredSecrets({
      HELIUS_API_KEY: "FAKEUNITWHKEY1",
      SUPABASE_KEY: "FAKEUNITSUPA1",
      RPC_URL: "https://rpc.example/?api-key=FAKEUNITRPC1",
      FALLBACK_RPC_URL: "not a url",
      DISCORD_ALERT_WEBHOOK: "https://discord.com/api/webhooks/1/FAKEUNITDISC1",
      HELIUS_WEBHOOK_SECRET: "short",
    });
    expect(got.sort()).toEqual(["FAKEUNITDISC1", "FAKEUNITRPC1", "FAKEUNITSUPA1", "FAKEUNITWHKEY1"]);
  });

  it("installSentryScrubber is idempotent once a client exists", async () => {
    const m = await loadScrub();
    expect(m).not.toBeNull();
    expect(m!.installSentryScrubber({})).toBe(true);
    expect(m!.installSentryScrubber({})).toBe(true);
  });
});
