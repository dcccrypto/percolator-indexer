import { beforeEach, describe, expect, it, vi } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { getUnknownLayoutCount, resetUnknownLayoutState } from "../../src/layout/resolve.js";
import { LAYOUT_V22, buildMarket, pk } from "../helpers/v22Fixtures.js";

// An error that is NOT an UnknownLayoutError (here: from the layout bookkeeping hook) must be warned and
// swallowed per account, so one bad slab never aborts the whole discovery pass.
const hook = vi.hoisted(() => ({ fail: null as unknown }));
vi.mock("../../src/layout/marketVersions.js", () => ({
  noteMarketLayout: () => {
    if (hook.fail !== null) throw hook.fail;
  },
}));

import { discoverV17Markets } from "../../src/v17/discovery.js";

const PROGRAM = new PublicKey("11111111111111111111111111111111");

function scan(keys: PublicKey[]) {
  const data = buildMarket(LAYOUT_V22, { slots: 1 });
  return { getProgramAccounts: vi.fn(async () => keys.map((k) => ({ pubkey: k, account: { data: Buffer.from(data) } }))) };
}

beforeEach(() => {
  resetUnknownLayoutState();
  hook.fail = null;
});

describe("discoverV17Markets: an unexpected parse error skips only that account", () => {
  it("Error instance: account skipped, no unknown-layout count, scan still resolves", async () => {
    hook.fail = new Error("boom");
    const out = await discoverV17Markets(scan([pk(), pk()]) as never, PROGRAM);
    expect(out).toEqual([]);
    expect(getUnknownLayoutCount()).toBe(0);
  });

  it("non-Error throw value is stringified and still skipped", async () => {
    hook.fail = "plain string failure";
    const out = await discoverV17Markets(scan([pk()]) as never, PROGRAM);
    expect(out).toEqual([]);
    expect(getUnknownLayoutCount()).toBe(0);
  });

  it("control: with the hook healthy the same account is returned", async () => {
    const out = await discoverV17Markets(scan([pk()]) as never, PROGRAM);
    expect(out).toHaveLength(1);
  });
});
