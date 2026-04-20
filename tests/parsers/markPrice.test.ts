import { describe, it, expect, vi, beforeEach } from "vitest";

// We must mock BEFORE importing the module under test.
const mockGetAccountInfo = vi.fn();

vi.mock("@percolatorct/sdk", () => ({
  // detectSlabLayout returns null for small/unknown sizes, a layout object otherwise.
  // We key off data.length in the mock to avoid needing real slab binaries.
  detectSlabLayout: vi.fn((len: number) => {
    if (len < 512) return null;
    // V0 layout — no mark_price field.
    if (len === 600) return { engineOff: 0, engineMarkPriceOff: -1 };
    // V12_17 shape — has mark_price.
    return { engineOff: 0, engineMarkPriceOff: 400 };
  }),
  parseEngine: vi.fn(),
}));

vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  // Pass-through retry — just invoke the callback.
  withRetry: vi.fn(async (fn: () => unknown) => fn()),
}));

vi.mock("@solana/web3.js", () => ({
  Connection: class {},
  PublicKey: class { constructor(public key: string) {} },
}));

import { readMarkPriceE6 } from "../../src/parsers/markPrice.js";
import * as sdk from "@percolatorct/sdk";

const SLAB = "SLAB11111111111111111111111111111111111111";

function mockConn(info: { data: Uint8Array } | null) {
  mockGetAccountInfo.mockResolvedValue(info);
  return { getAccountInfo: mockGetAccountInfo } as any;
}

describe("readMarkPriceE6", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Re-install the layout default for each test (resetAllMocks would drop the impl).
    vi.mocked(sdk.detectSlabLayout).mockImplementation((len: number) => {
      if (len < 512) return null as any;
      if (len === 600) return { engineOff: 0, engineMarkPriceOff: -1 } as any;
      return { engineOff: 0, engineMarkPriceOff: 400 } as any;
    });
  });

  it("returns the e6 number when parseEngine succeeds and mark_price is in range", async () => {
    vi.mocked(sdk.parseEngine).mockReturnValue({ markPriceE6: 85_187_279n } as any);
    const conn = mockConn({ data: new Uint8Array(2048) });
    const res = await readMarkPriceE6(conn, SLAB);
    expect(res).toBe(85_187_279);
  });

  it("returns null when getAccountInfo returns no data", async () => {
    const conn = mockConn(null);
    const res = await readMarkPriceE6(conn, SLAB);
    expect(res).toBeNull();
  });

  it("returns null for a V0-style slab (layout has no mark_price field)", async () => {
    const conn = mockConn({ data: new Uint8Array(600) });
    const res = await readMarkPriceE6(conn, SLAB);
    expect(res).toBeNull();
    // parseEngine should NOT be invoked — we short-circuit on layout.
    expect(sdk.parseEngine).not.toHaveBeenCalled();
  });

  it("returns null when detectSlabLayout returns null (unknown data size)", async () => {
    const conn = mockConn({ data: new Uint8Array(100) });
    const res = await readMarkPriceE6(conn, SLAB);
    expect(res).toBeNull();
  });

  it("returns null when markPriceE6 is zero (sentinel)", async () => {
    vi.mocked(sdk.parseEngine).mockReturnValue({ markPriceE6: 0n } as any);
    const conn = mockConn({ data: new Uint8Array(2048) });
    const res = await readMarkPriceE6(conn, SLAB);
    expect(res).toBeNull();
  });

  it("returns null when markPriceE6 is out of sane range (>= 1e12)", async () => {
    vi.mocked(sdk.parseEngine).mockReturnValue({ markPriceE6: 1_000_000_000_000n } as any);
    const conn = mockConn({ data: new Uint8Array(2048) });
    const res = await readMarkPriceE6(conn, SLAB);
    expect(res).toBeNull();
  });

  it("returns null when parseEngine throws", async () => {
    vi.mocked(sdk.parseEngine).mockImplementation(() => { throw new Error("bad slab"); });
    const conn = mockConn({ data: new Uint8Array(2048) });
    const res = await readMarkPriceE6(conn, SLAB);
    expect(res).toBeNull();
  });

  it("returns null when getAccountInfo throws", async () => {
    mockGetAccountInfo.mockRejectedValue(new Error("RPC 429"));
    const conn = { getAccountInfo: mockGetAccountInfo } as any;
    const res = await readMarkPriceE6(conn, SLAB);
    expect(res).toBeNull();
  });
});
