import { describe, it, expect, vi, beforeEach } from "vitest";

// vi.hoisted runs before vi.mock hoisting, so these are available in the factory
const { mockFetch, sharedMock } = vi.hoisted(() => ({
  mockFetch: vi.fn(),
  sharedMock: {
    config: {
      heliusApiKey: "test-api-key",
      webhookUrl: "https://my-server.example.com",
      webhookSecret: "webhook-secret-123",
      rpcUrl: "https://api-devnet.helius-rpc.com",
      allProgramIds: ["ProgramId111111111111111111111111111111"],
    },
    logger: {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    },
  },
}));

vi.stubGlobal("fetch", mockFetch);

vi.mock("@percolator/shared", () => ({
  get config() { return sharedMock.config; },
  createLogger: vi.fn(() => sharedMock.logger),
}));

import { HeliusWebhookManager } from "../../src/services/HeliusWebhookManager.js";

function mockResponse(body: any, opts: { ok?: boolean; status?: number } = {}) {
  const { ok = true, status = 200 } = opts;
  return {
    ok,
    status,
    json: vi.fn().mockResolvedValue(body),
    text: vi.fn().mockResolvedValue(typeof body === "string" ? body : JSON.stringify(body)),
  } as unknown as Response;
}

describe("HeliusWebhookManager", () => {
  let manager: HeliusWebhookManager;

  beforeEach(() => {
    vi.clearAllMocks();
    sharedMock.config.heliusApiKey = "test-api-key";
    sharedMock.config.webhookUrl = "https://my-server.example.com";
    sharedMock.config.webhookSecret = "webhook-secret-123";
    sharedMock.config.rpcUrl = "https://api-devnet.helius-rpc.com";
    sharedMock.config.allProgramIds = ["ProgramId111111111111111111111111111111"];
    manager = new HeliusWebhookManager();
  });

  describe("getStatus", () => {
    it("should return idle status initially", () => {
      expect(manager.getStatus()).toEqual({ status: "idle", webhookId: null, error: null });
    });
  });

  describe("start", () => {
    it("should skip when heliusApiKey is missing", async () => {
      sharedMock.config.heliusApiKey = "";
      await manager.start();
      expect(mockFetch).not.toHaveBeenCalled();
    });

    it("should skip when webhookUrl is missing", async () => {
      sharedMock.config.webhookUrl = "";
      await manager.start();
      expect(mockFetch).not.toHaveBeenCalled();
    });

    it("should create a new webhook when no existing found", async () => {
      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockResolvedValueOnce(mockResponse({ webhookID: "new-wh-123" }));

      await manager.start();

      expect(mockFetch).toHaveBeenCalledTimes(2);
      const [, postOpts] = mockFetch.mock.calls[1];
      expect(postOpts.method).toBe("POST");
      const body = JSON.parse(postOpts.body);
      expect(body.webhookURL).toBe("https://my-server.example.com/webhook/trades");
      expect(body.transactionTypes).toEqual(["ANY"]);
      expect(body.webhookType).toBe("enhancedDevnet");
      expect(body.authHeader).toBe("webhook-secret-123");

      expect(manager.getStatus()).toEqual({ status: "active", webhookId: "new-wh-123", error: null });
    });

    it("should update an existing webhook when URL matches", async () => {
      mockFetch.mockResolvedValueOnce(
        mockResponse([{ webhookID: "existing-456", webhookURL: "https://my-server.example.com/webhook/trades" }]),
      );
      mockFetch.mockResolvedValueOnce(mockResponse({}));

      await manager.start();

      const [putUrl, putOpts] = mockFetch.mock.calls[1];
      expect(putOpts.method).toBe("PUT");
      expect(putUrl).toContain("/existing-456");
      expect(manager.getStatus().webhookId).toBe("existing-456");
    });

    it("should create new if existing webhook has different URL", async () => {
      mockFetch.mockResolvedValueOnce(
        mockResponse([{ webhookID: "other-wh", webhookURL: "https://other.com/hook" }]),
      );
      mockFetch.mockResolvedValueOnce(mockResponse({ webhookID: "brand-new" }));

      await manager.start();

      expect(mockFetch.mock.calls[1][1].method).toBe("POST");
      expect(manager.getStatus().webhookId).toBe("brand-new");
    });

    it("should use mainnet webhook type when rpcUrl is not devnet", async () => {
      sharedMock.config.rpcUrl = "https://api-mainnet.helius-rpc.com";
      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockResolvedValueOnce(mockResponse({ webhookID: "mainnet-wh" }));

      await manager.start();

      const body = JSON.parse(mockFetch.mock.calls[1][1].body);
      expect(body.webhookType).toBe("enhanced");
    });

    it("should set failed status on create failure", async () => {
      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockResolvedValueOnce(mockResponse("Rate limited", { ok: false, status: 429 }));

      await manager.start();

      expect(manager.getStatus().status).toBe("failed");
      expect(manager.getStatus().error).toContain("429");
    });

    it("should set failed status on network error", async () => {
      mockFetch.mockRejectedValueOnce(new Error("Network refused"));

      await manager.start();

      expect(manager.getStatus().status).toBe("failed");
    });

    it("should proceed to create if findExistingWebhook GET fails", async () => {
      mockFetch.mockResolvedValueOnce(mockResponse("Unauthorized", { ok: false, status: 401 }));
      mockFetch.mockResolvedValueOnce(mockResponse({ webhookID: "fallback-create" }));

      await manager.start();

      expect(manager.getStatus().status).toBe("active");
      expect(manager.getStatus().webhookId).toBe("fallback-create");
    });
  });

  describe("stop", () => {
    it("should clear webhookId without API calls", async () => {
      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockResolvedValueOnce(mockResponse({ webhookID: "to-clear" }));
      await manager.start();

      mockFetch.mockClear();
      await manager.stop();

      expect(manager.getStatus().webhookId).toBeNull();
      expect(mockFetch).not.toHaveBeenCalled();
    });
  });

  describe("listWebhooks", () => {
    it("should return webhooks array on success", async () => {
      mockFetch.mockResolvedValueOnce(mockResponse([{ webhookID: "wh-1" }]));
      const result = await manager.listWebhooks();
      expect(result).toEqual([{ webhookID: "wh-1" }]);
    });

    it("should return null when heliusApiKey is missing", async () => {
      sharedMock.config.heliusApiKey = "";
      const result = await manager.listWebhooks();
      expect(result).toBeNull();
    });

    it("should return null on fetch error", async () => {
      mockFetch.mockRejectedValueOnce(new Error("Timeout"));
      const result = await manager.listWebhooks();
      expect(result).toBeNull();
    });
  });

  describe("reRegister", () => {
    it("should return ok:true on success", async () => {
      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockResolvedValueOnce(mockResponse({ webhookID: "re-reg-wh" }));

      const result = await manager.reRegister();
      expect(result).toEqual({ ok: true, webhookId: "re-reg-wh" });
    });

    it("should clear previous error on retry", async () => {
      mockFetch.mockRejectedValueOnce(new Error("Network error"));
      await manager.start();
      expect(manager.getStatus().error).toBeTruthy();

      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockResolvedValueOnce(mockResponse({ webhookID: "recovered" }));
      await manager.reRegister();
      expect(manager.getStatus().error).toBeNull();
    });
  });
});
