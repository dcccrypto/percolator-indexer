/**
 * Loop liveness for /health (hung-RPC liveness, J-20261121).
 *
 * /health used to probe only getSlot and one DB count. Each polling loop has an in-flight guard, so a loop parked on a
 * held RPC call skipped every later tick while /health kept answering 200 ok. Each loop now registers here when it
 * starts and calls `beat()` whenever it makes progress (a completed pass, or a finished market inside a long pass).
 * A registered loop with no beat for longer than its budget is stale, and /health reports degraded (503).
 *
 * Budgets are deliberately loose (at least 3 intervals and at least 10 minutes) so a slow but healthy pass never trips
 * it; with the per-call RPC deadline (rpcDeadline.ts) a loop should never get near it.
 */

export const MIN_STALE_AFTER_MS = 10 * 60_000;

/** EventStreamService: a stream with no notification for this long is replaced (ATLAS_WS_SILENCE_MS overrides). */
export const DEFAULT_WS_SILENCE_MS = 5 * 60_000;

/** Budget for a loop that runs every `intervalMs`: max(3 intervals, 10 min). */
export function staleBudgetMs(intervalMs: number): number {
  return Math.max(3 * intervalMs, MIN_STALE_AFTER_MS);
}

export interface LoopState {
  ok: boolean;
  /** ms since the loop last made progress (or was registered). */
  ageMs: number;
  staleAfterMs: number;
}

export class LoopLiveness {
  private loops = new Map<string, { last: number; staleAfterMs: number }>();

  constructor(private readonly now: () => number = Date.now) {}

  /** Start watching `name`; the clock starts now. Re-registering resets it. */
  register(name: string, staleAfterMs: number): void {
    this.loops.set(name, { last: this.now(), staleAfterMs });
  }

  unregister(name: string): void {
    this.loops.delete(name);
  }

  /** `name` made progress. Ignored for a loop that is not registered (stopped, or never started). */
  beat(name: string): void {
    const l = this.loops.get(name);
    if (l) l.last = this.now();
  }

  snapshot(): Record<string, LoopState> {
    const out: Record<string, LoopState> = {};
    const now = this.now();
    for (const [name, l] of this.loops) {
      const ageMs = now - l.last;
      out[name] = { ok: ageMs <= l.staleAfterMs, ageMs, staleAfterMs: l.staleAfterMs };
    }
    return out;
  }

  /** name -> healthy, for createHealthChecker's probeLoops. */
  probe(): Record<string, boolean> {
    const out: Record<string, boolean> = {};
    for (const [name, s] of Object.entries(this.snapshot())) out[name] = s.ok;
    return out;
  }
}

/** The process-wide registry the services report to and /health reads. */
export const loopLiveness = new LoopLiveness();
