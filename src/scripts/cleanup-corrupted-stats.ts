/**
 * One-time cleanup script: Reset corrupted market_stats values.
 *
 * Root cause: parseEngine reads fixed offsets from slab account data.
 * When the on-chain account is not a valid Percolator slab (or uses
 * a different layout), the parser returns garbage values like:
 *   - total_open_interest: 9.865e+34
 *   - insurance_balance: 1.844e+25
 *   - c_tot: 1.401e+35
 *
 * Also fixes corrupted decimals (values > 18) to default of 6.
 *
 * Run: npx tsx packages/indexer/src/scripts/cleanup-corrupted-stats.ts
 */
import { pathToFileURL } from "node:url";
import { getSupabase } from "@percolatorct/shared";
import { CURRENT_NETWORK } from "../network.js";

// GH#1208: Use 5e17 instead of 1e18 — values like 7.997e17 are clearly corrupt
// but slip through the original 1e18 threshold.
const MAX_SANE_VALUE = 5e17;
const MAX_VALID_DECIMALS = 18;
// PostgREST caps un-ranged selects at max_rows (default 1000). Page by
// slab_address keyset so rows fixed mid-run (which leave the filter) and rows
// whose update failed (which stay in it) never shift or repeat a page.
const PAGE_SIZE = 1000;

async function fetchAll(
  supabase: any,
  columns: string,
  orFilter: string,
): Promise<any[]> {
  const out: any[] = [];
  let after: string | null = null;
  for (;;) {
    let q = supabase
      .from("markets")
      .select(columns)
      .eq("network", CURRENT_NETWORK)
      .or(orFilter)
      .order("slab_address", { ascending: true })
      .limit(PAGE_SIZE);
    if (after !== null) q = q.gt("slab_address", after);
    const { data, error } = await q;
    if (error) throw error;
    if (!data || data.length === 0) break;
    out.push(...data);
    after = data[data.length - 1].slab_address;
  }
  return out;
}

export async function main() {
  const supabase = getSupabase();
  let failures = 0;

  // Fix corrupted decimals in markets table
  const markets = await fetchAll(
    supabase,
    "slab_address, decimals, mint_address",
    `decimals.gt.${MAX_VALID_DECIMALS},decimals.lt.0`,
  );

  console.log(`Found ${markets.length} markets with invalid decimals`);
  
  for (const m of markets) {
    console.log(`  Fixing ${m.slab_address} (mint=${m.mint_address}): decimals ${m.decimals} → 6`);
    const { error } = await supabase
      .from("markets")
      .update({ decimals: 6 })
      .eq("slab_address", m.slab_address)
      .eq("network", CURRENT_NETWORK);
    if (error) {
      failures++;
      console.error(`  ERROR: ${error.message}`);
    }
  }

  // Reset corrupted stats values to null
  // Identify corrupted rows: total_open_interest > 1e18 or insurance_balance > 1e18
  const corruptedStats = await fetchAll(
    supabase,
    "slab_address, total_open_interest, insurance_balance, c_tot",
    `total_open_interest.gt.${MAX_SANE_VALUE},insurance_balance.gt.${MAX_SANE_VALUE},c_tot.gt.${MAX_SANE_VALUE}`,
  );

  console.log(`\nFound ${corruptedStats.length} markets with corrupted stats`);

  for (const s of corruptedStats) {
    console.log(`  Resetting stats for ${s.slab_address}`);
    const { error } = await supabase
      .from("markets")
      .update({
        total_open_interest: 0,
        open_interest_long: 0,
        open_interest_short: 0,
        insurance_fund: 0,
        insurance_balance: 0,
        insurance_fee_revenue: 0,
        vault_balance: 0,
        c_tot: 0,
        pnl_pos_tot: 0,
        net_lp_pos: "0",
        lp_sum_abs: 0,
        lp_max_abs: 0,
        total_accounts: 0,
        lifetime_liquidations: 0,
        lifetime_force_closes: 0,
      })
      .eq("slab_address", s.slab_address)
      .eq("network", CURRENT_NETWORK);
    if (error) {
      failures++;
      console.error(`  ERROR: ${error.message}`);
    }
  }

  if (failures > 0) {
    throw new Error(`Cleanup failed: ${failures} update(s) failed`);
  }
  console.log("\nCleanup complete!");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
