/**
 * CPU profile helper: node --cpu-prof --import tsx bench/v22Events.profile.ts, then summarise the .cpuprofile.
 * Reuses the same fixtures as the bench by running 5 passes of the current module.
 */
import { BackstopMode, buildBondDepositIxV22, buildInsuranceBackstopDrawIxV22, buildSettleHoldingRentIxV22, buildInitInsuranceUnitsIxV22 } from "@percolatorct/sdk";
const { decodeV22Events, rawInstructionsFromEnhancedTx, rawInstructionsFromParsedTx } = await import(process.env.PROFILE_LEGACY ? "../tests/helpers/v22Events.legacy.js" : "../src/parsers/v22Events.js");
import { enhancedTx, parsedTx, pk } from "../tests/helpers/v22Fixtures.js";

const PROGRAM = pk(), market = pk(), lp = pk(), user = pk(), keeper = pk(), portfolio = pk(), ata = pk(), vault = pk();
const m = { programId: PROGRAM, market, registryDomain: 0, lpPortfolio: lp };
const ixs = [buildBondDepositIxV22(m, user, ata, vault, 5n, 4n), buildInsuranceBackstopDrawIxV22(m, keeper, BackstopMode.Draw, 9n), buildSettleHoldingRentIxV22(m, keeper, portfolio, 3, 12n), buildInitInsuranceUnitsIxV22(m, keeper)];
const N = 2_000;
const parsed = Array.from({ length: N }, (_, i) => parsedTx([ixs[i % 4], ixs[(i + 1) % 4]]));
const enh = Array.from({ length: N }, (_, i) => enhancedTx([ixs[i % 4], ixs[(i + 1) % 4]]));
const ids = [PROGRAM.toBase58()], set = new Set(ids), ctx = { signature: "s", slot: 1, blockTimeSec: 1_790_000_000 };
let n = 0;
for (let r = 0; r < 5; r++) for (let i = 0; i < N; i++) {
  n += decodeV22Events(rawInstructionsFromParsedTx(parsed[i] as never, ids), ctx).length;
  n += decodeV22Events(rawInstructionsFromEnhancedTx(enh[i] as never, set), ctx).length;
}
console.log("events", n);
