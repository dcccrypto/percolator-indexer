import type { Connection } from "@solana/web3.js";
import type { AtlasWs, AtlasNotification } from "@percolatorct/shared";
import { createLogger, eventBus, decodeBase58 } from "@percolatorct/shared";
import { insertTradeRows, tradeKey, type IndexerTradeRow } from "../db/insertTradeRow.js";
import { isBlockedSlab } from "../blocklist.js";
import { IX_TAG } from "@percolatorct/sdk";
import { parsePercolatorFills, parsePercolatorLiquidations } from "../parsers/percolatorTxParser.js";
import { readMarkPriceE6 } from "../parsers/markPrice.js";

const log = createLogger("indexer:event-stream");

/**
 * v17 note: IX_TAG.UpdateHyperpMark no longer exists as a named constant in the v17 SDK
 * because the instruction was renamed to `ConfigureHybridOracle` (tag 34 — same numeric
 * value). The fallback ?? 34 is the authoritative value and will always be used in v17.
 *
 * DO NOT change the numeric value: the program still emits tag 34 for oracle-price updates.
 */
const TAG_UPDATE_HYPERP_MARK: number =
  (IX_TAG as Record<string, number>).UpdateHyperpMark ?? 34; // 34 = ConfigureHybridOracle in v17

export interface EventStreamDeps {
  ws: AtlasWs;
  programId: string;
  /** RPC connection used for slab-price fallback when a fill has no log-derived price. */
  connection: Connection;
  /** Optional custom callback. When set, autoIndex is ignored. */
  onTx?: (tx: unknown) => Promise<void> | void;
  /** When true, parse fills and insert into trades table automatically. */
  autoIndex?: boolean;
  /** Known slab addresses for this service — only fills touching these slabs are indexed. */
  knownSlabs?: string[];
}

/**
 * EventStreamService — low-latency (~100-500ms) stream of Percolator program transactions
 * via Helius Enhanced WebSockets (Atlas endpoint).
 *
 * Complements existing paths:
 *   - HeliusWebhookManager (primary, ~1-2s)
 *   - TradeIndexerPolling (backup, 5 min)
 *
 * Auto-indexing: when `autoIndex=true`, fills are parsed via `parsePercolatorFills` and
 * inserted into the `trades` table via `insertTrade`. Duplicate inserts are safe thanks
 * to the unique-constraint dedup inside `insertTrade` (swallows 23505).
 *
 * Price resolution: the parser no longer tries to extract price from `Program log:` lines
 * (the old `mark_price=<n>` regex never matched anything the program actually emits — see
 * percolatorTxParser.ts commit message for details). Instead, on every fill we read the
 * slab's `mark_price_e6` post-tx via `readMarkPriceE6`. Fills where both paths fail to
 * produce a price are dropped.
 *
 * Oracle updates: when a tx contains an `UpdateHyperpMark` instruction (tag 34), we read
 * the slab's mark_price post-tx and write a row to `oracle_prices`. This gives the
 * frontend price chart a high-cadence feed even when the 60s StatsCollector tick misses.
 */
export class EventStreamService {
  private started = false;
  private slabSet: Set<string>;

  constructor(private deps: EventStreamDeps) {
    this.slabSet = new Set(deps.knownSlabs ?? []);
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;

    this.deps.ws.onNotification((msg) => this.handle(msg).catch((err) => {
      log.error("handler failed", { err: String(err) });
    }));

    this.deps.ws.sub(1, "transactionSubscribe", [
      { accountInclude: [this.deps.programId], failed: false },
      {
        commitment: "confirmed",
        encoding: "jsonParsed",
        transactionDetails: "full",
        showRewards: false,
        maxSupportedTransactionVersion: 0,
      },
    ]);

    log.info("event-stream started", {
      programId: this.deps.programId,
      autoIndex: !!this.deps.autoIndex,
      knownSlabs: this.slabSet.size,
    });
  }

  /** Add a slab to the filter set at runtime (as new markets are discovered). */
  addKnownSlab(slab: string): void {
    this.slabSet.add(slab);
  }

  private async handle(msg: AtlasNotification): Promise<void> {
    if (msg.method !== "transactionNotification") return;
    const tx = (msg.params as any)?.result;
    if (!tx) return;

    if (this.deps.onTx) {
      await this.deps.onTx(tx);
      return;
    }

    if (!this.deps.autoIndex) return;

    const signature = tx.signature ?? tx.transaction?.signatures?.[0];
    if (!signature) return;

    // REDUCTION (2026-07-26): oracle_prices is no longer indexed — the frontend reads
    // price live from chain. UpdateHyperpMark txs no longer trigger a DB write here.

    const fills = parsePercolatorFills(tx, signature, [this.deps.programId]);

    // Collect this tx's rows and write them in one round-trip at the end, instead
    // of one insert per leg.
    const rows: IndexerTradeRow[] = [];
    const emits: Array<{ slab: string; side: "long" | "short"; size: string; price: number; trader: string; key: string }> = [];

    // Fills on the same slab within one tx resolve to the same post-tx mark price,
    // so the fallback slab read is memoized per slab (it was previously repeated
    // once per fill). null = the read failed or returned nothing for that slab.
    const priceBySlab = new Map<string, number | null>();

    // legIndex is the fill's position within the whole tx (fills are flattened across
    // instructions), so (tx_signature, asset_index, legIndex) is unique per tx. (H2/H3)
    for (const [legIndex, fill] of fills.entries()) {
      // #148: Use the per-fill slab derived from instruction accounts (fill.slabAddress),
      // not the tx-wide resolveSlab() result. resolveSlab returns the *first* known slab
      // in accountKeys and applies it to every fill in the tx — mis-attributing fills in
      // multi-slab transactions. The parser now populates fill.slabAddress from
      // ix.accounts[marketAccountIdx] (same logic as TradeIndexer's per-instruction guard).
      const slab = fill.slabAddress;
      if (!slab || !this.slabSet.has(slab)) {
        log.warn("skipping fill — slab not in known set", { sig: signature, slab });
        continue;
      }
      // Retired market — the markets row is gone and trades FK to it.
      if (isBlockedSlab(slab)) continue;

      let price = fill.priceE6 ?? 0;
      if (!price) {
        // Log-derived parser is neutralized (see percolatorTxParser.ts). Always
        // hit the slab for the authoritative post-tx mark price.
        // #170: isolate the fallback read — one failed slab read skips only THIS fill
        // instead of aborting the whole tx handler and losing every later fill.
        if (!priceBySlab.has(slab)) {
          try {
            priceBySlab.set(slab, await readMarkPriceE6(this.deps.connection, slab));
          } catch (err) {
            log.warn("slab price fallback failed", { sig: signature, slab, err: String(err) });
            priceBySlab.set(slab, null);
          }
        }
        const fallback = priceBySlab.get(slab) ?? null;
        if (fallback == null) {
          log.warn("skipping fill — no slab-resolved price", { sig: signature, slab });
          continue;
        }
        price = fallback;
      }

      rows.push({
        slab_address: slab,
        trader: fill.trader,
        side: fill.side,
        size: fill.sizeAbs.toString(),
        price,
        fee: 0,
        tx_signature: signature,
        asset_index: fill.assetIndex,
        leg_index: legIndex,
      });
      emits.push({
        slab,
        side: fill.side,
        size: fill.sizeAbs.toString(),
        price,
        trader: fill.trader,
        key: tradeKey({ tx_signature: signature, asset_index: fill.assetIndex, leg_index: legIndex }),
      });
    }

    // Liquidation markers (v17 crank action=1). No size/price/side — excluded from
    // volume/candles. leg_index offset (1000+) keeps them clear of fill leg indices.
    //
    // #170-style isolation: fills are now written in one batch AFTER this block, so
    // an exception here would discard the tx's fills too. Previously the fills were
    // already durable by this point. Contain the failure to the liquidation markers.
    try {
      const liqs = parsePercolatorLiquidations(tx, signature, [this.deps.programId]);
      for (const [i, liq] of liqs.entries()) {
        if (!this.slabSet.has(liq.slabAddress)) continue;
        if (isBlockedSlab(liq.slabAddress)) continue;
        rows.push({
          slab_address: liq.slabAddress,
          trader: liq.portfolio,
          side: null,
          size: null,
          price: null,
          fee: 0,
          tx_signature: signature,
          asset_index: liq.assetIndex,
          leg_index: 1000 + i,
          is_liquidation: true,
        });
      }
    } catch (err) {
      log.warn("liquidation parse failed — fills still indexed", { sig: signature, err: String(err) });
    }

    if (rows.length === 0) return;

    let written: Set<string>;
    try {
      written = new Set((await insertTradeRows(rows)).map(tradeKey));
    } catch (err) {
      // Whole batch failed — nothing was written, so emit nothing.
      log.warn("insertTradeRows failed", { sig: signature, count: rows.length, err: String(err) });
      return;
    }

    // Fan out to percolator-api WS subscribers of trades:<slab>.
    // The ws.ts handler picks this up and pushes to live chart clients.
    // Skip legs that were already indexed (re-delivered tx) — they were not written.
    for (const e of emits) {
      if (!written.has(e.key)) continue;
      try {
        eventBus.emit("trade.executed", {
          slabAddress: e.slab,
          timestamp: Date.now(),
          data: { side: e.side, size: e.size, price: e.price, trader: e.trader, signature },
        });
      } catch (err) {
        log.warn("eventBus emit failed", { err: String(err) });
      }
    }
  }

  /**
   * Returns true if the tx contains at least one top-level instruction for our program
   * with tag == UpdateHyperpMark (34).
   */
  private hasUpdateHyperpMark(tx: any): boolean {
    const ixs: any[] = tx.transaction?.message?.instructions ?? [];
    for (const ix of ixs) {
      if (ix && typeof ix === "object" && "parsed" in ix) continue;
      const programId = typeof ix.programId === "string"
        ? ix.programId
        : ix.programId?.toBase58?.();
      if (programId !== this.deps.programId) continue;
      const data = decodeBase58(ix.data);
      if (!data || data.length < 1) continue;
      if (data[0] === TAG_UPDATE_HYPERP_MARK) return true;
    }
    return false;
  }

  /** Walk tx accountKeys and return the first key that matches our known slabs. */
  private resolveSlab(tx: any): string | null {
    const keys: any[] = tx.transaction?.message?.accountKeys ?? [];
    for (const k of keys) {
      const addr = pubkeyToString(k);
      if (addr && this.slabSet.has(addr)) return addr;
    }
    return null;
  }
}

/**
 * Coerce an accountKeys entry to its base58 string form.
 *
 * Across the RPC formats we see, an accountKeys element can be any of:
 *   - a raw base58 string                                     (getTransaction with encoding="base58")
 *   - `{pubkey: "<base58>", signer, writable}`                (jsonParsed — what Atlas WS returns)
 *   - `{pubkey: PublicKey, signer, writable}`                 (some web3.js helpers)
 *   - a `PublicKey` instance                                  (direct SDK usage)
 *
 * `resolveSlab` was only handling the 1st, 3rd, and 4th shapes. Atlas WS uses
 * the 2nd — a plain object with a string-typed `pubkey`. Without this coercion
 * every streamed tx resolved as "unknown slab" and both oracle_prices inserts
 * and trade inserts were silently skipped.
 */
function pubkeyToString(k: unknown): string | null {
  if (!k) return null;
  if (typeof k === "string") return k;
  if (typeof k === "object") {
    const obj = k as { pubkey?: unknown; toBase58?: () => string };
    if (typeof obj.pubkey === "string") return obj.pubkey;
    if (obj.pubkey && typeof (obj.pubkey as { toBase58?: () => string }).toBase58 === "function") {
      return (obj.pubkey as { toBase58: () => string }).toBase58();
    }
    if (typeof obj.toBase58 === "function") return obj.toBase58();
  }
  return null;
}
