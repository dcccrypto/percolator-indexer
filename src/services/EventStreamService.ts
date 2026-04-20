import type { Connection } from "@solana/web3.js";
import type { AtlasWs, AtlasNotification } from "@percolatorct/shared";
import { createLogger, insertTrade, insertOraclePrice, eventBus, decodeBase58 } from "@percolatorct/shared";
import { IX_TAG } from "@percolatorct/sdk";
import { parsePercolatorFills } from "../parsers/percolatorTxParser.js";
import { readMarkPriceE6 } from "../parsers/markPrice.js";

const log = createLogger("indexer:event-stream");

/**
 * Fallback for environments where the SDK build in use predates the UpdateHyperpMark
 * instruction. The program source defines tag 34 in `percolator-prog/src/instruction.rs`.
 */
const TAG_UPDATE_HYPERP_MARK: number =
  (IX_TAG as Record<string, number>).UpdateHyperpMark ?? 34;

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

    const slab = this.resolveSlab(tx);
    if (!slab) return;

    // P2: oracle-update detection. Fire before fill processing so even a tx that
    // contains ONLY an UpdateHyperpMark (no trade) still writes an oracle row.
    if (this.hasUpdateHyperpMark(tx)) {
      try {
        const markE6 = await readMarkPriceE6(this.deps.connection, slab);
        if (markE6 != null) {
          await insertOraclePrice({
            slab_address: slab,
            price_e6: String(markE6),
            timestamp: Math.floor(Date.now() / 1000),
            tx_signature: signature,
          });
        }
      } catch (err) {
        log.warn("insertOraclePrice failed", { sig: signature, err: String(err) });
      }
    }

    const fills = parsePercolatorFills(tx, signature, [this.deps.programId]);
    for (const fill of fills) {
      let price = fill.priceE6 ?? 0;
      if (!price) {
        // Log-derived parser is neutralized (see percolatorTxParser.ts). Always
        // hit the slab for the authoritative post-tx mark price.
        const fallback = await readMarkPriceE6(this.deps.connection, slab);
        if (fallback == null) {
          log.warn("skipping fill — no slab-resolved price", { sig: signature, slab });
          continue;
        }
        price = fallback;
      }

      try {
        await insertTrade({
          slab_address: slab,
          trader: fill.trader,
          side: fill.side,
          size: fill.sizeAbs.toString(),
          price,
          fee: 0,
          tx_signature: signature,
        });
      } catch (err) {
        log.warn("insertTrade failed", { sig: signature, err: String(err) });
        continue;
      }
      // Fan out to percolator-api WS subscribers of trades:<slab>.
      // The ws.ts handler picks this up and pushes to live chart clients.
      try {
        eventBus.emit("trade.executed", {
          slabAddress: slab,
          timestamp: Date.now(),
          data: {
            side: fill.side,
            size: fill.sizeAbs.toString(),
            price,
            trader: fill.trader,
            signature,
          },
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
      const addr = typeof k === "string"
        ? k
        : k?.pubkey?.toBase58?.() ?? k?.toBase58?.();
      if (addr && this.slabSet.has(addr)) return addr;
    }
    return null;
  }
}
