// fetchStoredLegsMany must keep paging when PostgREST max_rows (100 here) is below STORED_LEGS_PAGE (1000):
// a short page is not the last one; only an empty page ends the read.
import { describe, it, expect, vi } from "vitest";

const SERVER_CAP = 100;
let table: Array<{ id: string; tx_signature: string }> = [];

vi.mock("@percolatorct/shared", () => ({
  getNetwork: () => "devnet",
  getSupabase: () => ({
    from: () => {
      let after: string | null = null;
      const q: Record<string, unknown> = {};
      q.select = () => q;
      q.in = () => q;
      q.eq = () => q;
      q.gt = (_c: string, v: string) => ((after = v), q);
      q.order = () => q;
      q.limit = async (n: number) => ({
        data: table
          .filter((r) => after === null || r.id > after)
          .slice(0, Math.min(n, SERVER_CAP))
          .map((r) => ({ ...r, slab_address: "s", asset_index: 0, leg_index: 0, trader: "t", side: "long", size: "1", is_liquidation: false })),
        error: null,
      });
      return q;
    },
  }),
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import { fetchStoredLegsMany } from "../../src/db/storedLegs.js";

describe("fetchStoredLegsMany under PostgREST max_rows=100", () => {
  it("returns every stored leg of one split-order tx (250 rows)", async () => {
    table = Array.from({ length: 250 }, (_, i) => ({ id: String(i).padStart(6, "0"), tx_signature: "sigA" }));
    const m = await fetchStoredLegsMany(["sigA"]);
    expect(m?.get("sigA")?.length).toBe(250); // fails: 100 (legs 101..250 look "not stored" -> double insert)
  });
});
