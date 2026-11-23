import { PublicKey } from "@solana/web3.js";
import type { Connection } from "@solana/web3.js";
import type { AtlasWs, AtlasNotification } from "@percolatorct/shared";
import { createLogger, eventBus, decodeBase58 } from "@percolatorct/shared";
import { insertTradeRows, tradeKey, type IndexerTradeRow, type TradeSizeProvenance } from "../db/insertTradeRow.js";
import { isBlockedSlab } from "../blocklist.js";
import { IX_TAG } from "@percolatorct/sdk";
import { parsePercolatorFills, parsePercolatorLiquidations } from "../parsers/percolatorTxParser.js";
import { readMarkPriceE6 } from "../parsers/markPrice.js";
import { resolveLotExp, perTokenPrice, cachedLotExp } from "../parsers/lotScale.js";
import { decodeV22Events, rawInstructionsFromParsedTx } from "../parsers/v22Events.js";
import { insertV22EventsWithRetry, V22EventWriteError } from "../db/insertV22Events.js";
import { recordV22LogEvents } from "./v22LogEvents.js";
import { resolveCpiLeg } from "../parsers/matcherFill.js";
import { makeMatcherContextReader } from "../lib/matcherCtx.js";
import { recordSkippedSignatures } from "../lib/skippedSignatures.js";
import { DEFAULT_WS_SILENCE_MS } from "../lib/loopLiveness.js";

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
  /**
   * Hung-socket liveness: opens a fresh socket. When set, a stream that has been silent for `silenceMs` (or whose
   * socket is closed) is torn down and replaced by `reconnect()`, then re-subscribed. Without it the service only
   * reports `isStale()`. shared's createAtlasWs pings but never checks for a pong and never reconnects, so a socket
   * that opens and goes quiet (or is closed by the server) used to stop the stream for good.
   */
  reconnect?: () => AtlasWs;
  /** No notification for this long -> stale (and reconnect). Default 5 min; 0 disables the check. */
  silenceMs?: number;
}

export { DEFAULT_WS_SILENCE_MS };
/** A closed socket gets this long (capped at silenceMs) to come back before it is replaced. */
const CLOSED_GRACE_MS = 60_000;

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
  private ws: AtlasWs;
  /** Last notification, or the last (re)subscribe. */
  private lastActivityAt = 0;
  private silenceTimer: ReturnType<typeof setInterval> | null = null;
  /** Sockets replaced after a silence / close (diagnostics). */
  reconnects = 0;

  constructor(private deps: EventStreamDeps) {
    this.slabSet = new Set(deps.knownSlabs ?? []);
    this.ws = deps.ws;
  }

  private get silenceMs(): number {
    return this.deps.silenceMs ?? DEFAULT_WS_SILENCE_MS;
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    this.attach(this.ws);

    const silenceMs = this.silenceMs;
    if (silenceMs > 0) {
      const every = Math.max(1_000, Math.min(30_000, Math.floor(silenceMs / 2)));
      this.silenceTimer = setInterval(() => this.checkSilence(), every);
      (this.silenceTimer as { unref?: () => void }).unref?.();
    }

    log.info("event-stream started", {
      programId: this.deps.programId,
      autoIndex: !!this.deps.autoIndex,
      knownSlabs: this.slabSet.size,
    });
  }

  /** Stop the silence check and close the current socket. */
  stop(): void {
    if (this.silenceTimer) clearInterval(this.silenceTimer);
    this.silenceTimer = null;
    this.started = false;
    try { this.ws.close(); } catch { /* already closed */ }
  }

  /** True when no notification has arrived for `silenceMs` (always false when the check is off). */
  isStale(now = Date.now()): boolean {
    const silenceMs = this.silenceMs;
    return this.started && silenceMs > 0 && now - this.lastActivityAt >= silenceMs;
  }

  private checkSilence(): void {
    const now = Date.now();
    const silentFor = now - this.lastActivityAt;
    const closed = !this.ws.isOpen && silentFor >= Math.min(CLOSED_GRACE_MS, this.silenceMs);
    if (!closed && !this.isStale(now)) return;
    if (!this.deps.reconnect) {
      log.warn("event-stream stale: no notification within the silence window, and no reconnect configured", {
        silentForMs: silentFor, socketOpen: this.ws.isOpen,
      });
      return;
    }
    // A closed socket is a fault; an open one that is only quiet may just be a quiet program (devnet), so info.
    const ctx = { silentForMs: silentFor, socketOpen: this.ws.isOpen, reconnects: this.reconnects + 1 };
    if (closed) log.warn("event-stream socket closed; reconnecting and re-subscribing", ctx);
    else log.info("event-stream silent; reconnecting and re-subscribing", ctx);
    let next: AtlasWs;
    try {
      next = this.deps.reconnect();
    } catch (err) {
      log.error("event-stream reconnect failed; retrying on the next check", { err: String(err) });
      return;
    }
    const old = this.ws;
    this.ws = next;
    this.reconnects++;
    try { old.close(); } catch { /* already closed */ }
    this.attach(next);
  }

  /** Listen on `ws` and subscribe. Notifications from a socket that has since been replaced are ignored. */
  private attach(ws: AtlasWs): void {
    this.lastActivityAt = Date.now();
    ws.onNotification((msg) => {
      if (ws !== this.ws) return;
      this.lastActivityAt = Date.now();
      this.handle(msg).catch((err) => {
        log.error("handler failed", { err: String(err) });
      });
    });

    ws.sub(1, "transactionSubscribe", [
      { accountInclude: [this.deps.programId], failed: false },
      {
        commitment: "confirmed",
        encoding: "jsonParsed",
        transactionDetails: "full",
        showRewards: false,
        maxSupportedTransactionVersion: 1,
      },
    ]);
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

    // v2.2 activity events (bond / rescue / units / backstop / rent / eviction / dust). Best effort and isolated:
    // a failure here must never cost the tx's fills below.
    // What failed to reach v22_events, kept so the record names the right kind, error and slab.
    const v22Failures: Array<{ kind: "instruction" | "log"; slab: string; rows: number }> = [];
    try {
      const wrapperIxs = rawInstructionsFromParsedTx(tx, [this.deps.programId]);
      const events = decodeV22Events(wrapperIxs, {
        signature,
        slot: typeof tx.slot === "number" ? tx.slot : null,
        blockTimeSec: typeof tx.blockTime === "number" ? tx.blockTime : null,
      }).filter((e) => !e.slab_address || (this.slabSet.has(e.slab_address) && !isBlockedSlab(e.slab_address)));
      if (events.length > 0) {
        try {
          await insertV22EventsWithRetry(events);
        } catch (err) {
          if (!(err instanceof V22EventWriteError)) throw err;
          // retries exhausted (counted + alerted); surfaced after the fills are indexed
          v22Failures.push({ kind: "instruction", slab: events.find((e) => e.slab_address)?.slab_address ?? "", rows: events.length });
        }
      }
      // v2.2 wrapper LOG events (fills / reductions / moves), strict frame attribution; separate write, VERSION-keyed.
      const logRes = await recordV22LogEvents({
        signature,
        err: tx.meta?.err ?? null,
        logMessages: tx.meta?.logMessages,
        wrapperInstructions: wrapperIxs,
        wrapperIds: new Set([this.deps.programId]),
        slot: typeof tx.slot === "number" ? tx.slot : null,
        blockTimeSec: typeof tx.blockTime === "number" ? tx.blockTime : null,
        isKnownMarket: (slab) => this.slabSet.has(slab),
      });
      if (logRes.status === "write_failed") v22Failures.push({ kind: "log", slab: logRes.slab ?? "", rows: logRes.rows });
    } catch (err) {
      log.warn("v22 event decode failed — fills still indexed", { sig: signature, err: String(err) });
    }

    // The fills are indexed first and unconditionally; a v22 write that failed after its retries is surfaced AFTER them
    // (the handler logs it as "handler failed"; the failure is counted and alerted in insertV22Events). Only the instruction events are retried here; the log events go through recordV22LogEvents, which reports "write_failed" once.
    await this.indexFills(tx, signature);
    if (v22Failures.length > 0) {
      // A websocket stream never redelivers and the poller does not write instruction events: make the loss durable so it can be re-indexed.
      // Its own source ("v22-events", not "trade-indexer"): the (source, signature) dedupe must not drop it when the trade path above
      // already recorded this signature, and the row must say that the v22 events (not the trades) are what is missing.
      const error = v22Failures
        .map((f) => (f.kind === "instruction" ? `v22 instruction events (${f.rows}) write failed after retries` : `v22 log events (${f.rows}) write failed (not retried)`))
        .join("; ");
      try {
        await recordSkippedSignatures([{ signature, source: "v22-events", slab: v22Failures.find((f) => f.slab)?.slab ?? "", error }]);
      } catch (recErr) {
        log.warn("recordSkippedSignatures failed", { sig: signature, err: String(recErr) });
      }
      throw new Error(`${error} for ${signature.slice(0, 16)}`);
    }
  }

  private async indexFills(tx: any, signature: string): Promise<void> {
    const fills = parsePercolatorFills(tx, signature, [this.deps.programId]);

    // Collect this tx's rows and write them in one round-trip at the end, instead
    // of one insert per leg.
    const rows: IndexerTradeRow[] = [];
    const emits: Array<{ slab: string; side: "long" | "short"; size: string; price: number; trader: string; key: string }> = [];

    // Fills on the same slab within one tx resolve to the same post-tx mark price,
    // so the fallback slab read is memoized per slab (it was previously repeated
    // once per fill). null = the read failed or returned nothing for that slab.
    const priceBySlabAsset = new Map<string, number | null>(); // #221: per (slab, asset)
    // 2 s per read, retried briefly (2 x 300 ms) on a transport error, then the post-clip request size.
    const readMatcherContext = makeMatcherContextReader(() => this.deps.connection, typeof tx.slot === "number" ? tx.slot : null, { retries: 2, retryDelayMs: 300 });

    // legIndex is the fill's position within the whole tx (fills are flattened across
    // instructions), so (tx_signature, asset_index, legIndex) is unique per tx. (H2/H3)
    for (const [legIndex, fill] of fills.entries()) {
      // RebalanceReduce (tag 44) holds a leg number but has no side/exact size here; the
      // poll and webhook paths index it (resolved against history). Never write a placeholder.
      if (fill.rebalanceReduce) continue;
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

      let priceE6Value = fill.priceE6 ?? 0;
      let sizeAbs = fill.sizeAbs;
      let sizeProvenance: TradeSizeProvenance = "legacy_unverified";
      let matcherPriced = false; // price came from the matcher CPI (no slab read), so lot_exp may not be cached yet
      if (fill.cpi) {
        // #213/#221: TradeCpi/BatchTradeCpi: executed size + booked price from the matcher call.
        const r = await resolveCpiLeg({
          evidence: fill.cpi, assetIndex: fill.assetIndex, side: fill.side, wireSizeAbs: fill.sizeAbs,
          legPos: fill.legPos ?? 0, readContext: readMatcherContext, signature,
        });
        if (r.kind === "skip") {
          if (r.reason !== "zero-fill") {
            await recordSkippedSignatures([{ signature, source: "trade-indexer", slab, error: `TradeCpi ${r.reason}: ${r.detail}`.slice(0, 480) }]);
          }
          continue; // zero fill / strict skip: no row (the leg number is already consumed)
        }
        if (r.kind === "fill") {
          matcherPriced = true;
          sizeProvenance = r.exact ? "matcher_exact" : "matcher_request_upper_bound";
          sizeAbs = r.sizeValue;
          priceE6Value = Number(r.priceE6);
          if (!r.exact) log.debug("TradeCpi executed size unverified: wrote the matcher-requested size (upper bound)", { sig: signature });
        } // "legacy": wire size + the slab-read price below, exactly as before this parser
      }
      if (!priceE6Value) {
        // Log-derived parser is neutralized (see percolatorTxParser.ts). Always
        // hit the slab for the authoritative post-tx mark price.
        // #170: isolate the fallback read — one failed slab read skips only THIS fill
        // instead of aborting the whole tx handler and losing every later fill.
        const priceKey = `${slab}:${fill.assetIndex}`;
        if (!priceBySlabAsset.has(priceKey)) {
          try {
            priceBySlabAsset.set(priceKey, await readMarkPriceE6(this.deps.connection, slab, fill.assetIndex));
          } catch (err) {
            log.warn("slab price fallback failed", { sig: signature, slab, err: String(err) });
            priceBySlabAsset.set(priceKey, null);
          }
        }
        const fallback = priceBySlabAsset.get(priceKey) ?? null;
        if (fallback == null) {
          log.warn("skipping fill — no slab-resolved price", { sig: signature, slab });
          continue;
        }
        priceE6Value = fallback;
      }

      // `trades.price` is dollars-scaled everywhere else (webhook.ts
      // extractPriceFromAccountData, TradeIndexer.ts readMarkPriceFromSlab both
      // divide by 1_000_000 before returning). `readMarkPriceE6` — like the
      // `priceE6` field name on the fill — returns the raw e6 integer, so it must
      // be divided here too. Previously this raw e6 value was stored directly
      // into `trades.price`, inflating every Atlas-indexed fill 1,000,000x.
      // per-token: the engine/matcher price is per 10^lot_exp units (lot 0 == / 1_000_000, unchanged).
      // Never guessed: cache (filled by the slab reads above), else one read of the market account. Unreadable: skip
      // the fill and keep the signature for re-index, since a lot-0 price would be stored permanently.
      // Only a matcher-priced fill can have a cold cache (every other price came from a slab read that filled it), so
      // legacy / lot 0 fills make no extra RPC and cannot be dropped by this read (cache, else 0, as before).
      let lot: number | null;
      if (matcherPriced) {
        const readSlab = async () => {
          const info = await this.deps.connection.getAccountInfo(new PublicKey(slab));
          return info?.data ? new Uint8Array(info.data) : null;
        };
        lot = await resolveLotExp(slab, fill.assetIndex, readSlab);
        if (lot === null) { // one retry after a short pause (a single failed read must not drop the fill)
          await new Promise((r) => setTimeout(r, 300));
          lot = await resolveLotExp(slab, fill.assetIndex, readSlab);
        }
      } else {
        lot = cachedLotExp(slab, fill.assetIndex);
      }
      if (lot === null) {
        // Nothing is written (a lot-0 price would be stored permanently). The poll path (TradeIndexer) holds its
        // cursor on the same unreadable market and re-indexes this tx, so the fill is retried there, not lost.
        log.warn("skipping fill for now — lot_exp unreadable; the poller retries it", { sig: signature, slab });
        await recordSkippedSignatures([{ signature, source: "trade-indexer", slab, error: "TradeCpi lot_exp unreadable: market account not read" }]);
        continue;
      }
      const price = perTokenPrice(priceE6Value, lot);

      rows.push({
        slab_address: slab,
        trader: fill.trader,
        side: fill.side,
        size: sizeAbs.toString(),
        size_provenance: sizeProvenance,
        price,
        fee: 0,
        tx_signature: signature,
        asset_index: fill.assetIndex,
        leg_index: legIndex,
      });
      emits.push({
        slab,
        side: fill.side,
        size: sizeAbs.toString(),
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
      // Record the signature so it can be re-indexed by hand (recorded, not auto-retried): without this, with the RPC poller disabled
      // (or the sig outside its look-back) the fill is lost silently.
      try {
        await recordSkippedSignatures([{
          signature,
          source: "trade-indexer",
          slab: rows[0].slab_address,
          error: `insertTradeRows failed: ${String(err)}`.slice(0, 480),
        }]);
      } catch (recErr) {
        log.warn("recordSkippedSignatures failed", { sig: signature, err: String(recErr) });
      }
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
