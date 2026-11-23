// DRAFT failing-first tests (src bugs, NOT mutants). Expected to FAIL on feat/v22-indexer 9db52dd.
// Each shows the manager reporting status "active" while the stream path watches nothing.
import { describe, it, expect, vi, beforeEach } from "vitest";

const { mockFetch, sharedMock, sendCriticalAlert } = vi.hoisted(() => ({
  mockFetch: vi.fn(),
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
  captureException: vi.fn(),
  sendCriticalAlert,
}));
import { HeliusWebhookManager } from "../../src/services/HeliusWebhookManager.js";

const res = (body: unknown, ok = true, status = 200) =>
  ({ ok, status, json: vi.fn().mockResolvedValue(body), text: vi.fn().mockResolvedValue(JSON.stringify(body)) }) as unknown as Response;

describe("HeliusWebhookManager silent-dark (failing-first draft)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sendCriticalAlert.mockResolvedValue(undefined);
    sharedMock.config.allProgramIds = ["FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"];
  });

  // HWM-1: a 2xx create body without webhookID (error envelope / API shape change) is reported as
  // { status: "active", webhookId: undefined } with no alert. Base: status "active".
  it("HWM-1: a 2xx create response without webhookID is a failure, not active", async () => {
    const m = new HeliusWebhookManager();
    mockFetch.mockResolvedValueOnce(res([])).mockResolvedValueOnce(res({ error: "quota exceeded" }));
    await m.start();
    expect(m.getStatus().status).toBe("failed");
    expect(sendCriticalAlert).toHaveBeenCalledTimes(1);
  });

  // HWM-2: ALL_PROGRAM_IDS that normalises to [] (e.g. "," or " ") registers a webhook watching
  // no accounts and reports active. Base: POST is sent with accountAddresses [] and status "active".
  it("HWM-2: an empty program-id list never registers a webhook that watches nothing", async () => {
    sharedMock.config.allProgramIds = [" "];
    const m = new HeliusWebhookManager();
    mockFetch.mockResolvedValueOnce(res([])).mockResolvedValueOnce(res({ webhookID: "watches-nothing" }));
    await m.start();
    const writes = mockFetch.mock.calls.filter((c) => c[1]?.method === "POST" || c[1]?.method === "PUT");
    expect(writes).toHaveLength(0);
    expect(m.getStatus().status).toBe("failed");
  });
});
