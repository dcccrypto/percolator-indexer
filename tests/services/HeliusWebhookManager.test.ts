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

vi.mock("@percolatorct/shared", () => ({
  get config() { return sharedMock.config; },
  createLogger: vi.fn(() => sharedMock.logger),
  captureException: vi.fn(),
  sendCriticalAlert: vi.fn().mockResolvedValue(undefined),
}));

import { HeliusWebhookManager, resolveWebhookApiBase, DEFAULT_HELIUS_WEBHOOK_API_BASE } from "../../src/services/HeliusWebhookManager.js";

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

    const posts = () => mockFetch.mock.calls.filter((c) => c[1]?.method === "POST").length;

    it("does NOT create a webhook when the list call returns non-2xx (no duplicate)", async () => {
      mockFetch.mockResolvedValue(mockResponse("Unauthorized", { ok: false, status: 401 }));
      await manager.start();
      expect(posts()).toBe(0);
      expect(manager.getStatus().status).toBe("failed");
      expect(manager.getStatus().error).toMatch(/list/i);
    });

    it("does NOT create a webhook when the list call fails with a network error", async () => {
      mockFetch.mockRejectedValue(new Error("ECONNRESET"));
      await manager.start();
      expect(posts()).toBe(0);
      expect(manager.getStatus().status).toBe("failed");
    });

    it("does NOT create a webhook when the list body is not an array (error object)", async () => {
      mockFetch.mockResolvedValue(mockResponse({ error: "rate limited" }));
      await manager.start();
      expect(posts()).toBe(0);
      expect(manager.getStatus().status).toBe("failed");
    });

    it("repeated reRegister during a list outage never POSTs", async () => {
      mockFetch.mockResolvedValue(mockResponse("down", { ok: false, status: 503 }));
      await manager.reRegister();
      await manager.reRegister();
      expect(posts()).toBe(0);
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

  describe("webhook API host", () => {
    it("defaults to the account-level mainnet host even on devnet (api-devnet 404s for rotated keys)", async () => {
      delete process.env.HELIUS_WEBHOOK_API_BASE;
      sharedMock.config.rpcUrl = "https://devnet.helius-rpc.com/?api-key=k";
      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockResolvedValueOnce(mockResponse({ webhookID: "wh-1" }));
      await manager.start();
      const urls = mockFetch.mock.calls.map((c) => String(c[0]));
      expect(urls.every((u) => u.startsWith(DEFAULT_HELIUS_WEBHOOK_API_BASE + "/v0/webhooks?"))).toBe(true);
      expect(urls.some((u) => u.includes("api-devnet"))).toBe(false);
      const body = JSON.parse(String((mockFetch.mock.calls[1][1] as RequestInit).body));
      expect(body.webhookType).toBe("enhancedDevnet");
    });

    it("honours HELIUS_WEBHOOK_API_BASE and keeps the id in the path", async () => {
      process.env.HELIUS_WEBHOOK_API_BASE = "https://api.helius.xyz/";
      try {
        mockFetch.mockResolvedValueOnce(mockResponse([{ webhookID: "abc", webhookURL: "https://my-server.example.com/webhook/trades" }]));
        mockFetch.mockResolvedValueOnce(mockResponse({}));
        await manager.start();
        expect(String(mockFetch.mock.calls[0][0])).toMatch(/^https:\/\/api\.helius\.xyz\/v0\/webhooks\?api-key=/);
        expect(String(mockFetch.mock.calls[1][0])).toMatch(/^https:\/\/api\.helius\.xyz\/v0\/webhooks\/abc\?api-key=/);
      } finally {
        delete process.env.HELIUS_WEBHOOK_API_BASE;
      }
    });

    it("rejects a non-https or path-bearing override", () => {
      expect(() => resolveWebhookApiBase("http://evil.example.com")).toThrow();
      expect(() => resolveWebhookApiBase("https://api.helius.xyz/v0")).toThrow();
      expect(resolveWebhookApiBase("")).toBe(DEFAULT_HELIUS_WEBHOOK_API_BASE);
    });
  });
  describe("additional branch coverage", () => {
    it("records a failed status when the PUT update fails", async () => {
      mockFetch.mockResolvedValueOnce(
        mockResponse([{ webhookID: "wh-1", webhookURL: "https://my-server.example.com/webhook/trades" }]),
      );
      mockFetch.mockResolvedValueOnce(mockResponse("boom", { ok: false, status: 500 }));
      await manager.start();
      expect(manager.getStatus().status).toBe("failed");
      expect(manager.getStatus().error).toContain("Helius webhook update failed: 500 boom");
      expect(manager.getStatus().webhookId).toBeNull();
    });

    it("stringifies non-Error throws and survives a failing critical alert", async () => {
      const shared = await import("@percolatorct/shared");
      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockRejectedValueOnce("plain string failure");
      vi.mocked(shared.sendCriticalAlert).mockRejectedValueOnce(new Error("discord down"));
      await manager.start();
      expect(manager.getStatus()).toEqual({ status: "failed", webhookId: null, error: "plain string failure" });
      expect(sharedMock.logger.error).toHaveBeenCalledWith(
        "Failed to send webhook registration alert",
        expect.anything(),
      );
    });

    it("redacts the api key from fetch errors, logs the cause, and handles non-Error rejections", async () => {
      const err = new Error("fetch failed for https://x/v0/webhooks?api-key=SECRET123 now", {
        cause: new Error("ECONNRESET"),
      });
      mockFetch.mockRejectedValueOnce(err);
      await manager.start();
      const call = sharedMock.logger.error.mock.calls.find((c) => c[0] === "Fetch to Helius API failed")!;
      expect(call[1].message).toContain("api-key=REDACTED");
      expect(call[1].message).not.toContain("SECRET123");
      expect(call[1].cause).toBe("ECONNRESET");

      mockFetch.mockRejectedValueOnce("string rejection");
      await manager.start();
      const second = sharedMock.logger.error.mock.calls.filter((c) => c[0] === "Fetch to Helius API failed")[1];
      expect(second[1]).toEqual({ message: "string rejection", cause: undefined });
    });

    it("treats a non-Error cause as absent and tolerates an unreadable error body on list failure", async () => {
      const err = new Error("net down", { cause: "not-an-error" });
      mockFetch.mockRejectedValueOnce(err);
      await manager.start();
      const call = sharedMock.logger.error.mock.calls.find((c) => c[0] === "Fetch to Helius API failed")!;
      expect(call[1].cause).toBeUndefined();

      const bad = mockResponse("x", { ok: false, status: 503 });
      (bad.text as any).mockRejectedValueOnce(new Error("stream broke"));
      mockFetch.mockResolvedValueOnce(bad);
      await manager.start();
      expect(sharedMock.logger.warn).toHaveBeenCalledWith("Failed to list webhooks", { status: 503, body: "" });
    });

    it("omits authHeader when no webhook secret is configured", async () => {
      sharedMock.config.webhookSecret = "";
      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockResolvedValueOnce(mockResponse({ webhookID: "n5" }));
      await manager.start();
      const body = JSON.parse(mockFetch.mock.calls[1][1].body);
      expect(body.authHeader).toBeUndefined();
    });

    it("listWebhooks returns null on a non-ok response", async () => {
      mockFetch.mockResolvedValueOnce(mockResponse("nope", { ok: false, status: 401 }));
      expect(await manager.listWebhooks()).toBeNull();
    });

    it("reRegister reports ok:false with the message (or string) when start() throws", async () => {
      const spy = vi.spyOn(manager, "start");
      spy.mockRejectedValueOnce(new Error("kaboom"));
      expect(await manager.reRegister()).toEqual({ ok: false, error: "kaboom" });
      spy.mockRejectedValueOnce("str");
      expect(await manager.reRegister()).toEqual({ ok: false, error: "str" });
    });

    it("reRegister returns ok:false without throwing when registration fails", async () => {
      mockFetch.mockResolvedValueOnce(mockResponse([]));
      mockFetch.mockResolvedValueOnce(mockResponse("bad", { ok: false, status: 400 }));
      const r = await manager.reRegister();
      expect(r.ok).toBe(false);
      expect(r.webhookId).toBeUndefined();
    });
  });
});
