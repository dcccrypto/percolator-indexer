// DRAFT killers for the HeliusWebhookManager mutation run (IDX-REPORT-HELIUSWEBHOOKMGRLIVENESSMUT).
// Green on feat/v22-indexer 9db52dd. Copy to tests/services/ to run.
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const { mockFetch, sharedMock, captureException, sendCriticalAlert } = vi.hoisted(() => ({
  mockFetch: vi.fn(),
  captureException: vi.fn(),
  sendCriticalAlert: vi.fn(),
  sharedMock: {
    config: {
      heliusApiKey: "test-api-key",
      webhookUrl: "https://my-server.example.com",
      webhookSecret: "webhook-secret-123",
      rpcUrl: "https://api-devnet.helius-rpc.com",
      allProgramIds: [] as string[],
    },
    logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
  },
}));

vi.stubGlobal("fetch", mockFetch);
vi.mock("@percolatorct/shared", () => ({
  get config() { return sharedMock.config; },
  createLogger: vi.fn(() => sharedMock.logger),
  captureException,
  sendCriticalAlert,
}));

import { HeliusWebhookManager } from "../../src/services/HeliusWebhookManager.js";

const P1 = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
const P2 = "8HHS857Nx5THjyM8F2JanwBKuMt9PCfsnWiNjE7BMjbM";
const P3 = "4QomuXW5Tw73r5xzByNi2dsgQdGaZEkATC8GpYPZQET2";
const TARGET = "https://my-server.example.com/webhook/trades";

function res(body: unknown, ok = true, status = 200) {
  return {
    ok, status,
    json: vi.fn().mockResolvedValue(body),
    text: vi.fn().mockResolvedValue(typeof body === "string" ? body : JSON.stringify(body)),
  } as unknown as Response;
}
const bodyOf = (i: number) => JSON.parse(String(mockFetch.mock.calls[i][1].body));

describe("HeliusWebhookManager liveness killers (draft)", () => {
  let m: HeliusWebhookManager;
  beforeEach(() => {
    vi.clearAllMocks();
    sendCriticalAlert.mockResolvedValue(undefined);
    Object.assign(sharedMock.config, {
      heliusApiKey: "test-api-key",
      webhookUrl: "https://my-server.example.com",
      webhookSecret: "webhook-secret-123",
      rpcUrl: "https://api-devnet.helius-rpc.com",
      // padded + duplicated on purpose: the payload must use the normalised list (getProgramIds)
      allProgramIds: [P1, ` ${P2} `, P3, P1],
    });
    m = new HeliusWebhookManager();
  });
  afterEach(() => vi.restoreAllMocks());

  // M26 M27 M28 M29
  it("POST watches every configured program id, trimmed and deduped, in order", async () => {
    mockFetch.mockResolvedValueOnce(res([])).mockResolvedValueOnce(res({ webhookID: "w" }));
    await m.start();
    expect(bodyOf(1).accountAddresses).toEqual([P1, P2, P3]);
  });

  // M30 (and M26-M29 on the repair path)
  it("PUT repair sends the full payload, so a changed program-id list is pushed to the existing webhook", async () => {
    mockFetch.mockResolvedValueOnce(res([{ webhookID: "ex", webhookURL: TARGET, accountAddresses: [P1] }]))
      .mockResolvedValueOnce(res({}));
    await m.start();
    expect(mockFetch.mock.calls[1][1].method).toBe("PUT");
    const b = bodyOf(1);
    expect(b.accountAddresses).toEqual([P1, P2, P3]);
    expect(b.webhookURL).toBe(TARGET);
    expect(b.webhookType).toBe("enhancedDevnet");
    expect(b.transactionTypes).toEqual(["ANY"]);
    expect(b.authHeader).toBe("webhook-secret-123");
  });

  // M21
  it("a webhook on the same host but another path is not ours: create, don't overwrite it", async () => {
    mockFetch.mockResolvedValueOnce(res([{ webhookID: "other-path", webhookURL: "https://my-server.example.com/webhook/other" }]))
      .mockResolvedValueOnce(res({ webhookID: "ours" }));
    await m.start();
    expect(mockFetch.mock.calls[1][1].method).toBe("POST");
    expect(m.getStatus().webhookId).toBe("ours");
  });

  // M06 M09 M05: a skipped start must stay visibly not-active
  it.each([["heliusApiKey"], ["webhookUrl"]] as const)("missing %s leaves status idle and reRegister ok:false", async (k) => {
    (sharedMock.config as any)[k] = "";
    await m.start();
    expect(m.getStatus()).toEqual({ status: "idle", webhookId: null, error: null });
    expect(await m.reRegister()).toEqual({ ok: false, webhookId: undefined });
    expect(mockFetch).not.toHaveBeenCalled();
  });

  // M14 M16 M17: every failure kind reaches Sentry and the critical alert, with the error text
  it.each([
    ["list 503", () => mockFetch.mockResolvedValueOnce(res("down", false, 503)), "Helius webhook list failed: 503"],
    ["list net", () => mockFetch.mockRejectedValueOnce(new Error("ECONNRESET")), "Helius webhook list failed: ECONNRESET"],
    ["create 429", () => mockFetch.mockResolvedValueOnce(res([])).mockResolvedValueOnce(res("slow", false, 429)), "Helius webhook creation failed: 429 slow"],
    ["update 500", () => mockFetch.mockResolvedValueOnce(res([{ webhookID: "e", webhookURL: TARGET }])).mockResolvedValueOnce(res("boom", false, 500)), "Helius webhook update failed: 500 boom"],
  ])("%s -> failed + captureException + critical alert carrying the error", async (_n, arrange, msg) => {
    arrange();
    await m.start();
    expect(m.getStatus().status).toBe("failed");
    expect(captureException).toHaveBeenCalledTimes(1);
    expect(captureException.mock.calls[0][1]).toEqual({ tags: { context: "helius-webhook-registration" } });
    expect(sendCriticalAlert).toHaveBeenCalledTimes(1);
    const [title, fields] = sendCriticalAlert.mock.calls[0];
    expect(title).toMatch(/polling-only/);
    expect(fields[0].value).toBe(msg);
  });

  // M34 M35: no Helius call may hang boot (index.ts awaits start() before the stream phase)
  it("every Helius call carries a 15 s abort signal", async () => {
    const spy = vi.spyOn(AbortSignal, "timeout");
    mockFetch.mockResolvedValueOnce(res([])).mockResolvedValueOnce(res({ webhookID: "w" }));
    await m.start();
    mockFetch.mockResolvedValueOnce(res([{ webhookID: "w", webhookURL: TARGET }])).mockResolvedValueOnce(res({}));
    await m.start();
    mockFetch.mockResolvedValueOnce(res([]));
    await m.listWebhooks();
    expect(mockFetch).toHaveBeenCalledTimes(5);
    for (const [, opts] of mockFetch.mock.calls) expect(opts.signal).toBeInstanceOf(AbortSignal);
    expect(spy).toHaveBeenCalledTimes(5);
    for (const c of spy.mock.calls) expect(c[0]).toBe(15_000);
  });
});
