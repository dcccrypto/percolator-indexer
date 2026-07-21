/**
 * PERC-423: One-time backfill script — fix price=0 trades in DB.
 *
 * Root cause: prior to PERC-421, extractPriceFromLogs() used a rigid 5-value
 * regex that missed most trades → price stored as 0.
 *
 * Strategy:
 * 1. Fetch all trades WHERE price = 0 AND tx_signature IS NOT NULL
 * 2. Batch-request Helius Parse Transactions API (up to 100 sigs per call)
 * 3. Extract mark_price_e6 from slab account post-state data (same logic as
 *    the PERC-421 webhook fix, auto-detecting V0/V1 layout from data length;
 *    V0 slabs have no mark_price field; V1: ENGINE_OFF=640 + ENGINE_MARK_PRICE_OFF=400).
 *    There is no log-derived fallback — see extractPriceFromLogs (#162).
 *    Rows whose slab post-state is unavailable stay at 0 rather than being
 *    filled from an untrusted source.
 * 4. UPDATE trades SET price = <recovered> WHERE id = <id>
 * 5. Print a summary (fixed / skipped / failed)
 *
 * Run:
 *   cd packages/indexer
 *   HELIUS_API_KEY=xxx SUPABASE_URL=xxx SUPABASE_KEY=xxx \
 *     npx tsx src/scripts/backfill-price-zero-trades.ts [--dry-run]
 *
 * Dry-run mode prints what would be updated without writing to DB.
 */

import { pathToFileURL } from "node:url";
import { getSupabase, getNetwork } from "@percolatorct/shared";
import { config } from "@percolatorct/shared";
import { detectSlabLayout } from "@percolatorct/sdk";

// Helius Parse Transactions batch limit
const HELIUS_BATCH_SIZE = 100;

// Max concurrent Helius batches in-flight
const HELIUS_CONCURRENCY = 3;

const isDryRun = process.argv.includes("--dry-run");

// ─── Helius helpers ──────────────────────────────────────────────────────────

function getHeliusParseUrl(): string {
  const isDevnet = config.rpcUrl.includes("devnet");
  const host = isDevnet ? "api-devnet.helius-rpc.com" : "api-mainnet.helius-rpc.com";
  return `https://${host}/v0/transactions?api-key=${config.heliusApiKey}`;
}

async function fetchEnhancedTxs(signatures: string[]): Promise<any[]> {
  const url = getHeliusParseUrl();
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ transactions: signatures }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "(no body)");
    throw new Error(`Helius ${res.status}: ${text}`);
  }
  return (await res.json()) as any[];
}

// ─── Price extraction (mirrors PERC-421 webhook fix) ─────────────────────────

/**
 * Extract execution price from an enhanced Helius transaction.
 *
 * Strategy (in order):
 * 1. Read mark_price_e6 from slab account post-state.
 * 2. Return 0 — there is deliberately no log-derived fallback. See
 *    extractPriceFromLogs below.
 */
export function extractPrice(tx: any, slabAddress: string): number {
  const priceFromAccount = extractPriceFromAccountData(tx, slabAddress);
  if (priceFromAccount > 0) return priceFromAccount;
  return extractPriceFromLogs(tx);
}

function extractPriceFromAccountData(tx: any, slabAddress: string): number {
  const accountData: any[] = tx.accountData ?? [];
  for (const acc of accountData) {
    if (acc.account !== slabAddress) continue;

    let raw: Uint8Array | null = null;
    if (typeof acc.data === "string") {
      try {
        raw = Uint8Array.from(Buffer.from(acc.data, "base64"));
      } catch { /* skip */ }
    } else if (Array.isArray(acc.data) && typeof acc.data[0] === "string") {
      try {
        raw = Uint8Array.from(Buffer.from(acc.data[0], "base64"));
      } catch { /* skip */ }
    }
    if (!raw) continue;

    // Auto-detect layout version from the actual slab data length.
    // V0: no mark_price field (engineMarkPriceOff=-1) → fall through to logs.
    // V1: mark_price at ENGINE_OFF + 400.
    // v12.17: no engine.mark_price; fall back to config.mark_ewma_e6 via
    //         configMarkEwmaOff (absolute offset into the slab).
    const layout = detectSlabLayout(raw.length);
    if (!layout) continue;

    const dv = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);

    if (layout.engineMarkPriceOff >= 0) {
      const off = layout.engineOff + layout.engineMarkPriceOff;
      if (raw.length >= off + 8) {
        const markPriceE6 = dv.getBigUint64(off, true);
        if (markPriceE6 > 0n && markPriceE6 < 1_000_000_000_000n) {
          return Number(markPriceE6) / 1_000_000;
        }
      }
    }

    if (layout.configMarkEwmaOff != null && layout.configMarkEwmaOff >= 0) {
      const off = layout.configMarkEwmaOff;
      if (raw.length >= off + 8) {
        const markEwmaE6 = dv.getBigUint64(off, true);
        if (markEwmaE6 > 0n && markEwmaE6 < 1_000_000_000_000n) {
          return Number(markEwmaE6) / 1_000_000;
        }
      }
    }
  }
  return 0;
}

/**
 * #162 — NEUTERED: always returns 0.
 *
 * This function used to scrape `Program log:` lines and accept the first
 * integer in [1_000, 1e12] as price_e6. A Percolator transaction can contain
 * inner CPIs to arbitrary third-party programs, and any of them can emit a log
 * line, so an attacker could include a CPI emitting a plausible-looking value
 * and set the recorded price of their own trades. The `.eq("price", 0)` guard
 * on the UPDATE does not help — the attacker's row is genuinely still 0, so the
 * overwrite applies and the poisoned price reaches price history, candles, and
 * the public chart.
 *
 * The same scraper was neutralised in the live ingestion paths by #150
 * (webhook.ts and TradeIndexer.ts, both `return 0`). This script was missed and
 * kept the original logic — which is exactly the drift that made it exploitable,
 * since webhook.ts explicitly stores 0 and delegates recovery to THIS script.
 * The shape is kept identical to webhook.ts so the three stay comparable and a
 * future divergence is visible in a grep.
 *
 * The correct source of truth is extractPriceFromAccountData, which reads
 * mark_price_e6 / mark_ewma_e6 from the slab's post-state. If log-based
 * recovery is ever genuinely needed for V0 devnet slabs, it must be restricted
 * to log lines emitted by a verified Percolator program — not any program in
 * the transaction. Do not reintroduce the fuzzy scraper.
 */
export function extractPriceFromLogs(_tx: any): number {
  return 0;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n🔧  PERC-423 backfill — price=0 trades${isDryRun ? " [DRY RUN]" : ""}\n`);

  if (!config.heliusApiKey) {
    throw new Error("HELIUS_API_KEY is required");
  }

  const supabase = getSupabase();

  // 1. Fetch all price=0 trades with a tx signature
  console.log("Fetching price=0 trades from DB…");
  const { data: zeroPricedTrades, error: fetchErr } = await supabase
    .from("trades")
    .select("id, slab_address, tx_signature, price")
    .eq("network", getNetwork())
    .eq("price", 0)
    .not("tx_signature", "is", null)
    .order("created_at", { ascending: true });

  if (fetchErr) throw fetchErr;

  const trades = (zeroPricedTrades ?? []) as {
    id: string;
    slab_address: string;
    tx_signature: string;
    price: number;
  }[];

  console.log(`Found ${trades.length} price=0 trade(s) with tx signatures.\n`);

  if (trades.length === 0) {
    console.log("✅  Nothing to backfill.");
    return;
  }

  // Build sig→[trade] map (multiple trades can share a tx)
  const sigToTrades = new Map<string, typeof trades>();
  for (const t of trades) {
    const list = sigToTrades.get(t.tx_signature) ?? [];
    list.push(t);
    sigToTrades.set(t.tx_signature, list);
  }
  const uniqueSigs = [...sigToTrades.keys()];
  console.log(`Unique tx signatures to fetch: ${uniqueSigs.length}`);

  // 2. Batch-fetch from Helius with concurrency control
  const sigChunks: string[][] = [];
  for (let i = 0; i < uniqueSigs.length; i += HELIUS_BATCH_SIZE) {
    sigChunks.push(uniqueSigs.slice(i, i + HELIUS_BATCH_SIZE));
  }

  const txBySignature = new Map<string, any>();
  let fetchedCount = 0;
  let fetchFailures = 0;

  for (let ci = 0; ci < sigChunks.length; ci += HELIUS_CONCURRENCY) {
    const concurrentBatches = sigChunks.slice(ci, ci + HELIUS_CONCURRENCY);
    const results = await Promise.allSettled(
      concurrentBatches.map((chunk) => fetchEnhancedTxs(chunk))
    );
    for (const result of results) {
      if (result.status === "rejected") {
        console.error(`  ⚠️  Helius batch fetch failed: ${result.reason?.message ?? result.reason}`);
        fetchFailures++;
        continue;
      }
      for (const tx of result.value) {
        if (tx?.signature) {
          txBySignature.set(tx.signature, tx);
          fetchedCount++;
        }
      }
    }
    // Small delay between concurrent batches to avoid rate-limiting
    if (ci + HELIUS_CONCURRENCY < sigChunks.length) {
      await new Promise((r) => setTimeout(r, 200));
    }
  }

  console.log(`Fetched ${fetchedCount} enhanced txs (${fetchFailures} batch failure(s)).\n`);

  // 3. Extract prices and build update list
  const updates: { id: string; price: number }[] = [];
  let skipped = 0;

  for (const [sig, tradeList] of sigToTrades) {
    const tx = txBySignature.get(sig);
    if (!tx) {
      console.warn(`  ⚠️  TX not found in Helius response: ${sig.slice(0, 12)}…`);
      skipped += tradeList.length;
      continue;
    }

    for (const trade of tradeList) {
      const price = extractPrice(tx, trade.slab_address);
      if (price > 0) {
        updates.push({ id: trade.id, price });
        console.log(`  ✓  trade ${trade.id} | slab ${trade.slab_address.slice(0, 8)}… → $${price.toFixed(6)}`);
      } else {
        console.warn(`  ⚠️  Could not extract price for trade ${trade.id} (sig ${sig.slice(0, 12)}…)`);
        skipped++;
      }
    }
  }

  console.log(`\nSummary: ${updates.length} to update, ${skipped} skipped.\n`);

  if (isDryRun) {
    console.log("DRY RUN — no DB writes performed.");
    return;
  }

  if (updates.length === 0) {
    console.log("Nothing to update.");
    return;
  }

  // 4. Apply updates in batches of 50
  const UPDATE_BATCH = 50;
  let fixed = 0;
  let dbErrors = 0;

  for (let i = 0; i < updates.length; i += UPDATE_BATCH) {
    const batch = updates.slice(i, i + UPDATE_BATCH);
    const results = await Promise.allSettled(
      batch.map(({ id, price }) =>
        supabase
          .from("trades")
          .update({ price })
          .eq("id", id)
          .eq("network", getNetwork())
          .eq("price", 0) // guard: only overwrite if still 0
      )
    );
    for (const r of results) {
      if (r.status === "rejected") {
        console.error(`  DB update failed: ${r.reason?.message ?? r.reason}`);
        dbErrors++;
      } else if (r.value.error) {
        console.error(`  DB error: ${r.value.error.message}`);
        dbErrors++;
      } else {
        fixed++;
      }
    }
  }

  // 5. Verify: count remaining price=0 rows
  const { count: remaining, error: countErr } = await supabase
    .from("trades")
    .select("id", { count: "exact", head: true })
    .eq("network", getNetwork())
    .eq("price", 0);

  if (countErr) {
    console.warn(`Could not verify remaining zeros: ${countErr.message}`);
  }

  console.log(`\n✅  Done: ${fixed} updated, ${dbErrors} DB error(s), ${skipped} skipped.`);
  if (remaining !== null) {
    console.log(`Remaining price=0 rows in DB: ${remaining}`);
    if (remaining > 0) {
      console.log("  (Some rows may lack tx_signature — those require manual recovery or re-indexing.)");
    }
  }
}

// Only run when invoked directly (npx tsx src/scripts/backfill-price-zero-trades.ts).
// Without this guard, importing the module to test its price extraction would
// execute the whole backfill against Helius and Supabase.
const isDirectRun =
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href;

if (isDirectRun) {
  main().catch((err) => {
    console.error("Fatal:", err);
    process.exit(1);
  });
}
