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
const FILTERED_ = "[Filtered]";

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

  // J-20261122-IDX-BUG-SENTRYSCRUBNONPLAINLEAK: scrubDeep returned class instances untouched, but
  // Sentry serialises them (toJSON(), else own enumerable props), so a key inside one was sent as is.
  it("class instances in extra and console breadcrumbs ship scrubbed (web3 Connection, toJSON, bytes)", async () => {
    const { captureException } = await import("@percolatorct/shared");
    const { Connection } = await import("@solana/web3.js");
    const k = (n: string) => fake(`NP${n}KEY`);
    const keys = ["CLASS", "TOJSON", "CONN", "CONSOLE", "BYTES", "MAP", "HDRS", "CAUSE", "ERRPROP"].map(k);
    const u = (n: string) => `${base}/?api-key=${k(n)}`;
    class Holder {
      constructor(public endpoint: string) {}
    }
    class Endpoint {
      constructor(private readonly href: string) {}
      toJSON() {
        return { href: this.href };
      }
    }
    envelopes.length = 0;
    console.error("rpc client", new Holder(u("CONSOLE")));
    captureException(new Error("non-plain extra"), {
      extra: {
        cls: new Holder(u("CLASS")),
        tojson: new Endpoint(u("TOJSON")),
        conn: new Connection(u("CONN")),
        bytes: new TextEncoder().encode(u("BYTES")),
        // Sentry already sends these without their contents ({} / name+message+stack): no leak on the old code.
        map: new Map([["rpc", u("MAP")]]),
        hdrs: new Headers({ "x-rpc": u("HDRS") }),
        cause: new Error("outer", { cause: new Error(u("CAUSE")) }),
        errProp: Object.assign(new Error("plain"), { config: { url: u("ERRPROP") } }),
      },
    });
    const ev = () => envelopes.find((e) => e.includes('"type":"event"') && e.includes("non-plain extra"));
    for (let i = 0; i < 100 && !ev(); i++) await new Promise((r) => setTimeout(r, 100));
    expect(ev()).toBeDefined();
    const all = envelopes.join("\n");
    for (const key of keys) expect.soft(all, key).not.toContain(key);
    // The objects are still reported, only the secret goes; the live Connection is not modified.
    expect.soft(ev()!).toContain(`"endpoint":"${base}/?api-key=[Filtered]"`);
    expect.soft(ev()!).toContain(`"href":"${base}/?api-key=[Filtered]"`);
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

  it("scrubs URL and Error objects held in event data", async () => {
    const m = await loadScrub();
    expect(m).not.toBeNull();
    const ev = m!.scrubDeep({
      extra: {
        u: new URL("https://rpc.example/?api-key=FAKEURLKEY1") as unknown,
        e: new Error("POST https://discord.com/api/webhooks/1/FAKEERRTOK1 failed") as unknown,
      },
    });
    expect(ev.extra.u).toBe("https://rpc.example/?api-key=[Filtered]");
    expect(ev.extra.e).toMatchObject({ name: "Error", message: "POST https://discord.com/api/webhooks/1/[Filtered] failed" });
    expect(JSON.stringify(ev)).not.toMatch(/FAKEURLKEY1|FAKEERRTOK1/);
  });

  it("leaves no configured secret reachable inside non-plain objects", async () => {
    const m = await loadScrub();
    expect(m).not.toBeNull();
    const KEY = fake("SUPASERVICEKEY9");
    // Every string reachable through own props (enumerable or not), iteration and byte views.
    const reachable = (v: unknown, seen = new Set<unknown>()): string => {
      if (typeof v === "string") return v;
      if (v === null || typeof v !== "object" || seen.has(v)) return "";
      seen.add(v);
      if (ArrayBuffer.isView(v)) return Buffer.from(v.buffer, v.byteOffset, v.byteLength).toString("utf8");
      const out = Object.getOwnPropertyNames(v).map((p) => reachable((v as Record<string, unknown>)[p], seen));
      if (Symbol.iterator in v) for (const x of v as Iterable<unknown>) out.push(reachable(x, seen));
      return out.join(" ");
    };
    class Custom {
      constructor(public auth: string) {}
    }
    const proto = Object.create({ kind: "custom" }) as Record<string, unknown>;
    proto.headers = { apikey: KEY };
    const cyclic = new Custom(`Bearer ${KEY}`) as Custom & { self?: unknown };
    cyclic.self = cyclic;
    const live = new Custom(KEY);
    const ev = m!.scrubDeep(
      {
        extra: {
          map: new Map<string, unknown>([["k", KEY], ["nested", { key: KEY }]]),
          set: new Set([KEY]),
          headers: new Headers({ apikey: KEY }),
          proto,
          live,
          cyclic,
          json: { toJSON: () => ({ key: KEY }) } as unknown,
          bytes: Buffer.from(`key=${KEY}`),
          cleanBytes: new Uint8Array([1, 2, 3]),
          cause: new Error("outer", { cause: new Error(KEY) }),
          errProp: Object.assign(new Error("plain"), { body: KEY }),
          throwing: Object.defineProperty(new Custom(KEY), "boom", {
            enumerable: true,
            get() {
              throw new Error("getter");
            },
          }),
        },
      },
      [KEY],
    );
    expect(reachable(ev)).not.toContain(KEY);
    // Copies are scrubbed; the live object (e.g. the indexer's own Connection) is left as it was.
    expect(live.auth).toBe(KEY);
    expect(ev.extra.live).toEqual({ auth: FILTERED_ });
    expect(ev.extra.proto).toEqual({ headers: { apikey: FILTERED_ } });
    expect(ev.extra.json).toEqual({ key: FILTERED_ });
    expect(ev.extra.bytes).toBe(FILTERED_);
    expect(ev.extra.cleanBytes).toEqual(new Uint8Array([1, 2, 3]));
    expect(ev.extra.throwing).toBe(FILTERED_);
    expect(ev.extra.cyclic).toEqual({ auth: `Bearer ${FILTERED_}`, self: FILTERED_ });
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
      HELIUS_MAINNET_API_KEY: "FAKEUNITMAIN1",
      HELIUS_DEVNET_API_KEY: "FAKEUNITDEV1",
    });
    expect(got.sort()).toEqual(["FAKEUNITDEV1", "FAKEUNITDISC1", "FAKEUNITMAIN1", "FAKEUNITRPC1", "FAKEUNITSUPA1", "FAKEUNITWHKEY1"]);
  });

  it("installSentryScrubber is idempotent once a client exists", async () => {
    const m = await loadScrub();
    expect(m).not.toBeNull();
    expect(m!.installSentryScrubber({})).toBe(true);
    expect(m!.installSentryScrubber({})).toBe(true);
  });
});
