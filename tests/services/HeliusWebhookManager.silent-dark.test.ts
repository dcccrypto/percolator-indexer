// HWM-1 / HWM-2: the manager must never report "active" while the webhook stream watches nothing.
// Both tests failed on feat/v22-indexer 9db52dd (status "active", no alert) and pass with the fix.
// No real Helius calls: fetch is a vi.fn stub.
import { describe, it, expect, vi, beforeEach } from "vitest";

const { mockFetch, sharedMock, captureException, sendCriticalAlert } = vi.hoisted(() => ({
  mockFetch: vi.fn(),
  captureException: vi.fn(),
  sendCriticalAlert: vi.fn(),
  sharedMock: {
    config: {
      heliusApiKey: "test-api-key",
      webhookUrl: "https://my-server.example.com",
      webhookSecret: "s",
      rpcUrl: "https://api-devnet.helius-rpc.com",
      allProgramIds: ["FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"] as string[],
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

const TARGET = "https://my-server.example.com/webhook/trades";
const res = (body: unknown, ok = true, status = 200) =>
  ({ ok, status, json: vi.fn().mockResolvedValue(body), text: vi.fn().mockResolvedValue(JSON.stringify(body)) }) as unknown as Response;
const writes = () => mockFetch.mock.calls.filter((c) => c[1]?.method === "POST" || c[1]?.method === "PUT");

function expectFailedAndAlerted(m: HeliusWebhookManager, msg: string) {
  expect(m.getStatus()).toEqual({ status: "failed", webhookId: null, error: msg });
  expect(captureException).toHaveBeenCalledTimes(1);
  expect(sendCriticalAlert).toHaveBeenCalledTimes(1);
  expect(sendCriticalAlert.mock.calls[0][1][0].value).toBe(msg);
}

describe("HeliusWebhookManager never reports active while watching nothing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sendCriticalAlert.mockResolvedValue(undefined);
    sharedMock.config.allProgramIds = ["FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"];
  });

  // HWM-1: a 2xx create body without webhookID (error envelope / API shape change).
  it.each([
    ["error envelope", { error: "quota exceeded" }],
    ["null body", null],
    ["empty-string id", { webhookID: "" }],
    ["numeric id", { webhookID: 42 }],
  ])("HWM-1: a 2xx create response with %s is a failure, not active", async (_n, body) => {
    const m = new HeliusWebhookManager();
    mockFetch.mockResolvedValueOnce(res([])).mockResolvedValueOnce(res(body));
    await m.start();
    expectFailedAndAlerted(m, "Helius webhook creation failed: 200 response has no webhookID");
    expect(await m.reRegister()).toMatchObject({ ok: false });
  });

  // HWM-2: ALL_PROGRAM_IDS that normalises to [] (e.g. " " or ","): no POST watching nothing, no PUT
  // wiping the existing webhook's addresses, status failed and alerted.
  it.each([
    ["no existing webhook", () => mockFetch.mockResolvedValueOnce(res([])).mockResolvedValueOnce(res({ webhookID: "watches-nothing" }))],
    ["existing webhook", () => mockFetch.mockResolvedValueOnce(res([{ webhookID: "ex", webhookURL: TARGET }])).mockResolvedValueOnce(res({}))],
  ])("HWM-2: an empty program-id list never registers or repairs a webhook (%s)", async (_n, arrange) => {
    for (const ids of [[" "], ["", "  "], []]) {
      vi.clearAllMocks();
      mockFetch.mockReset();
      sendCriticalAlert.mockResolvedValue(undefined);
      sharedMock.config.allProgramIds = ids;
      arrange();
      const m = new HeliusWebhookManager();
      await m.start();
      expect(writes()).toHaveLength(0);
      expectFailedAndAlerted(m, "ALL_PROGRAM_IDS has no program ids: refusing to register a webhook that watches nothing");
    }
  });
});
