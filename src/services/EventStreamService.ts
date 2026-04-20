import type { AtlasWs, AtlasNotification } from "@percolatorct/shared";
import { createLogger } from "@percolatorct/shared";

const log = createLogger("indexer:event-stream");

export interface EventStreamDeps {
  ws: AtlasWs;
  programId: string;
  /** Optional callback fired for every transactionNotification matching the filter. */
  onTx?: (tx: unknown) => Promise<void> | void;
}

/**
 * EventStreamService — low-latency (~100-500ms) stream of Percolator program transactions
 * via Helius Enhanced WebSockets (Atlas endpoint).
 *
 * Complements existing paths:
 *   - HeliusWebhookManager (primary, ~1-2s)
 *   - TradeIndexerPolling (backup, 5 min)
 *
 * Parser and Supabase insertion are wired in Task 2.3 / 2.4.
 */
export class EventStreamService {
  private started = false;
  constructor(private deps: EventStreamDeps) {}

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

    log.info("event-stream started", { programId: this.deps.programId });
  }

  private async handle(msg: AtlasNotification): Promise<void> {
    if (msg.method !== "transactionNotification") return;
    const tx = (msg.params as any)?.result;
    if (!tx) return;
    if (this.deps.onTx) {
      await this.deps.onTx(tx);
    }
  }
}
