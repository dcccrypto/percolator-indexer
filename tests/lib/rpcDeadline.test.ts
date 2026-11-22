/**
 * Hung-RPC liveness (J-20261121-IDX-FIX-HUNGRPCLIVENESS): the per-call RPC deadline.
 *
 * The last block runs a REAL web3.js Connection against a local HTTP server that sends headers and then one byte
 * every 100 ms forever (the report's "trickle" mode, which undici's 300 s inactivity timeouts never cut off). Without
 * the deadline fetch the call never settles; with it the request is aborted and the socket closed.
 */
import { describe, it, expect, vi, afterEach } from "vitest";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { Connection } from "@solana/web3.js";
import {
  withDeadline, withCallDeadlines, deadlineFetch, deadlineConnection, resolveRpcCallTimeoutMs, RpcDeadlineError,
  DEFAULT_RPC_CALL_TIMEOUT_MS,
} from "../../src/lib/rpcDeadline.js";

afterEach(() => {
  vi.useRealTimers();
});

describe("resolveRpcCallTimeoutMs", () => {
  it("defaults to 60 s, honours 0 (off) and a decimal in [1 s, 300 s], rejects the rest with a warning", () => {
    const warn = vi.fn();
    expect(resolveRpcCallTimeoutMs(undefined, warn)).toBe(DEFAULT_RPC_CALL_TIMEOUT_MS);
    expect(resolveRpcCallTimeoutMs("  ", warn)).toBe(DEFAULT_RPC_CALL_TIMEOUT_MS);
    expect(resolveRpcCallTimeoutMs("0", warn)).toBe(0);
    expect(resolveRpcCallTimeoutMs("1000", warn)).toBe(1000);
    expect(resolveRpcCallTimeoutMs("300000", warn)).toBe(300_000);
    expect(warn).not.toHaveBeenCalled();
    for (const bad of ["999", "300001", "30s", "1e4", "-5", "0x10", "abc"]) {
      expect(resolveRpcCallTimeoutMs(bad, warn), bad).toBe(DEFAULT_RPC_CALL_TIMEOUT_MS);
    }
    expect(warn).toHaveBeenCalledTimes(7);
  });
});

describe("withDeadline", () => {
  it("rejects a never-settling promise with RpcDeadlineError after ms", async () => {
    vi.useFakeTimers();
    const p = withDeadline(new Promise(() => {}), 1_000, "rpc.getSlot");
    const out = p.catch((e) => e);
    await vi.advanceTimersByTimeAsync(999);
    let settled = false;
    void p.then(() => (settled = true), () => (settled = true));
    await Promise.resolve();
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    const err = await out;
    expect(err).toBeInstanceOf(RpcDeadlineError);
    expect(String(err.message)).toBe("rpc.getSlot timed out after 1000 ms");
  });

  it("passes a value or a rejection through unchanged and leaves no timer behind", async () => {
    vi.useFakeTimers();
    await expect(withDeadline(Promise.resolve(7), 1_000, "x")).resolves.toBe(7);
    const boom = new Error("FAKE boom");
    await expect(withDeadline(Promise.reject(boom), 1_000, "x")).rejects.toBe(boom);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("is a pass-through when ms is 0", async () => {
    vi.useFakeTimers();
    const p = withDeadline(new Promise(() => {}), 0, "x");
    expect(vi.getTimerCount()).toBe(0);
    let settled = false;
    void p.then(() => (settled = true), () => (settled = true));
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(settled).toBe(false);
  });
});

describe("withCallDeadlines", () => {
  class Chain {
    calls = 0;
    sync() { return this.calls; }
    async ok(n: number) { this.calls++; return n * 2; }
    hang() { this.calls++; return new Promise<number>(() => {}); }
  }

  it("races promise-returning methods (this preserved) and leaves sync members alone", async () => {
    vi.useFakeTimers();
    const c = withCallDeadlines(new Chain(), 500, "chain");
    await expect(c.ok(21)).resolves.toBe(42);
    expect(c.sync()).toBe(1);
    expect(c.calls).toBe(1);
    const hung = c.hang().catch((e) => e);
    await vi.advanceTimersByTimeAsync(500);
    const err = await hung;
    expect(err).toBeInstanceOf(RpcDeadlineError);
    expect(err.message).toBe("chain.hang timed out after 500 ms");
  });

  it("returns the target itself when ms is 0, and one stable proxy per (target, ms)", () => {
    const t = new Chain();
    expect(withCallDeadlines(t, 0)).toBe(t);
    expect(withCallDeadlines(t, 500)).toBe(withCallDeadlines(t, 500));
    expect(withCallDeadlines(t, 500)).not.toBe(withCallDeadlines(t, 600));
  });

  it("deadlineConnection wraps a non-Connection double with the race only", async () => {
    vi.useFakeTimers();
    const dbl = { getSlot: vi.fn(() => new Promise<number>(() => {})) };
    const c = deadlineConnection(dbl, 2_000);
    const out = c.getSlot().catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(2_000);
    expect(await out).toBeInstanceOf(RpcDeadlineError);
    expect(deadlineConnection(dbl, 0)).toBe(dbl);
  });
});

describe("deadlineFetch", () => {
  it("adds an abort signal and keeps the caller's own signal", async () => {
    const base = vi.fn(async (_i: unknown, init?: RequestInit) => {
      expect(init?.signal).toBeInstanceOf(AbortSignal);
      expect(init?.method).toBe("POST");
      return new Response("ok");
    });
    await deadlineFetch(1_000, base as never)("http://127.0.0.1:1/", { method: "POST" });
    const caller = new AbortController();
    let seen: AbortSignal | undefined;
    const base2 = vi.fn(async (_i: unknown, init?: RequestInit) => { seen = init?.signal ?? undefined; return new Response("ok"); });
    await deadlineFetch(60_000, base2 as never)("http://127.0.0.1:1/", { signal: caller.signal });
    caller.abort();
    expect(seen?.aborted).toBe(true);
  });
});

describe("real web3 Connection against a trickling RPC (headers, then 1 byte / 100 ms, forever)", () => {
  async function trickleServer() {
    const sockets = new Set<import("node:net").Socket>();
    let held = 0; // responses being trickled right now
    const server = http.createServer((req, res) => {
      req.resume();
      req.on("end", () => {
        res.writeHead(200, { "content-type": "application/json" });
        res.flushHeaders();
        held++;
        const t = setInterval(() => res.write(" "), 100);
        res.on("close", () => { clearInterval(t); held--; });
      });
    });
    server.on("connection", (s) => { sockets.add(s); s.on("close", () => sockets.delete(s)); });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
    const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    return {
      url,
      heldResponses: () => held,
      close: async () => { for (const s of sockets) s.destroy(); await new Promise((r) => server.close(() => r(null))); },
    };
  }

  it("without the deadline getSlot is still pending after 1.5 s; with a 500 ms deadline it rejects and the socket closes", async () => {
    const srv = await trickleServer();
    try {
      const plain = new Connection(srv.url, "confirmed");
      let plainSettled = false;
      const plainCall = plain.getSlot().then(() => (plainSettled = true), () => (plainSettled = true));
      void plainCall;

      const guarded = deadlineConnection(new Connection(srv.url, "confirmed"), 500);
      const t0 = Date.now();
      const err = await guarded.getSlot().then(() => null, (e: unknown) => e);
      const took = Date.now() - t0;
      // The fetch abort (TimeoutError) or the promise race (RpcDeadlineError), whichever fires first.
      expect(["TimeoutError", "RpcDeadlineError", "AbortError"]).toContain((err as Error)?.name);
      expect(took).toBeLessThan(5_000);

      await new Promise((r) => setTimeout(r, 1_500));
      // The guarded request was aborted (its response closed); only the unguarded one is still being trickled.
      expect(plainSettled).toBe(false);
      expect(srv.heldResponses()).toBe(1);
    } finally {
      await srv.close();
    }
  }, 20_000);

  it("keeps the endpoint and commitment of the shared connection", () => {
    const base = new Connection("http://127.0.0.1:1", "finalized");
    const c = deadlineConnection(base, 1_000);
    expect(c).not.toBe(base);
    expect(c).toBeInstanceOf(Connection);
    expect(c.rpcEndpoint).toBe(base.rpcEndpoint);
    expect(c.commitment).toBe("finalized");
    expect(deadlineConnection(base, 1_000)).toBe(c);
  });
});
