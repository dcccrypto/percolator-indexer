import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
  const queryBuilder: Record<string, ReturnType<typeof vi.fn>> = {};

  const from = vi.fn(() => queryBuilder);
  const select = vi.fn(() => queryBuilder);
  const eq = vi.fn(() => queryBuilder);
  const gte = vi.fn(() => queryBuilder);
  const order = vi.fn(() => queryBuilder);
  const limit = vi.fn();

  Object.assign(queryBuilder, {
    select,
    eq,
    gte,
    order,
    limit,
  });

  return {
    queryBuilder,
    from,
    select,
    eq,
    gte,
    order,
    limit,
    getNetwork: vi.fn(() => "mainnet"),
  };
});

vi.mock("@percolatorct/shared", () => ({
  getSupabase: vi.fn(() => ({
    from: mocks.from,
  })),
  getNetwork: mocks.getNetwork,
  config: {
    supabaseUrl: "https://example.supabase.co",
    supabaseKey: "test-key",
  },
  getConnection: vi.fn(),
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  captureException: vi.fn(),
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
}));

vi.mock("@percolatorct/sdk", () => ({
  deriveInsuranceLpMint: vi.fn(),
}));

import { InsuranceLPService } from "../../src/services/InsuranceLPService.js";

const SLAB = "SLAB111111111111111111111111111111111111111";

describe("InsuranceLPService trailing APY network isolation (#157)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-21T12:00:00.000Z"));

    mocks.from.mockReturnValue(mocks.queryBuilder);
    mocks.select.mockReturnValue(mocks.queryBuilder);
    mocks.eq.mockReturnValue(mocks.queryBuilder);
    mocks.gte.mockReturnValue(mocks.queryBuilder);
    mocks.order.mockReturnValue(mocks.queryBuilder);

    mocks.limit.mockResolvedValue({
      data: [
        {
          redemption_rate_e6: 1_000_000,
          created_at: "2026-07-19T12:00:00.000Z",
        },
      ],
      error: null,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("filters historical insurance snapshots by slab and active network", async () => {
    const service = new InsuranceLPService({
      getMarkets: () => new Map(),
    });

    await (service as any).computeTrailingAPY(SLAB, 7, 1_100_000);

    expect(mocks.from).toHaveBeenCalledOnce();
    expect(mocks.from).toHaveBeenCalledWith("insurance_snapshots");

    expect(mocks.eq).toHaveBeenCalledWith("slab", SLAB);
    expect(mocks.eq).toHaveBeenCalledWith("network", "mainnet");
    expect(mocks.getNetwork).toHaveBeenCalled();
  });
});
