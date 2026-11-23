// DISCL-1 (IDX-REPORT-DISCOVERYLIGHTDEVNETREAL): a light scan that fails used to come back as [] ("0 markets"),
// because discoverV17Markets swallowed every getProgramAccounts error. Real web3.js Connection against a local
// fault server, one test per devnet fault mode. Light path: the error is thrown. Full path: unchanged ([]),
// because the full pass falls back to the SDK's own scan.
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Connection, PublicKey } from "@solana/web3.js";
import { discoverV17Markets } from "../../src/v17/discovery.js";
import { LAYOUT_V22, buildMarket, pk } from "../helpers/v22Fixtures.js";
import { startFaultRpc, type FaultMode, type FaultRpc } from "../helpers/faultRpc.js";

const PROGRAM = new PublicKey("6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM");
let rpc: FaultRpc;
let conn: Connection;
const market = { key: pk(), data: buildMarket(LAYOUT_V22, { slots: 1 }), owner: PROGRAM };

beforeAll(async () => {
  rpc = await startFaultRpc();
  // disableRetryOnRateLimit: the final error of an exhausted 429 retry loop is the same throw; the retried
  // case (429 x6, ~7.5 s) is proven against devnet in the PR body.
  conn = new Connection(rpc.url, { commitment: "confirmed", disableRetryOnRateLimit: true });
});
afterAll(() => rpc.close());
beforeEach(() => { rpc.mode = "none"; rpc.faultsLeft = Infinity; rpc.accounts = [market]; rpc.gpaCalls = 0; });

describe("discoverV17Markets light scan surfaces RPC failures (DISCL-1)", () => {
  it("clean: light and full both return the market (one gPA each)", async () => {
    const light = await discoverV17Markets(conn, PROGRAM, undefined, { light: true });
    const full = await discoverV17Markets(conn, PROGRAM);
    expect(light.map((m) => m.slabAddress.toBase58())).toEqual([market.key.toBase58()]);
    expect(full.map((m) => m.slabAddress.toBase58())).toEqual([market.key.toBase58()]);
    expect(rpc.gpaCalls).toBe(2);
  });

  it("clean but empty program: light returns [] without throwing", async () => {
    rpc.accounts = [];
    await expect(discoverV17Markets(conn, PROGRAM, undefined, { light: true })).resolves.toEqual([]);
  });

  const faults: Array<[FaultMode, RegExp]> = [
    ["http429", /429/],
    ["rpc429", /rate limit exceeded/],
    ["truncated", /JSON|Unterminated|Unexpected end|position/i],
  ];

  for (const [mode, msg] of faults) {
    it(`${mode}: light scan rejects with the RPC error instead of returning [] (was: silent 0 markets)`, async () => {
      rpc.mode = mode;
      await expect(discoverV17Markets(conn, PROGRAM, undefined, { light: true })).rejects.toThrow(msg);
      expect(rpc.gpaCalls).toBe(1);
    });

    it(`${mode}: full scan is unchanged (still [] so the full pass falls back to the SDK scan)`, async () => {
      rpc.mode = mode;
      await expect(discoverV17Markets(conn, PROGRAM)).resolves.toEqual([]);
    });
  }
});
