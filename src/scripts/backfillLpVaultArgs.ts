/** Pure helpers for src/scripts/backfill-lp-vault.ts (kept import-side-effect free so they can be tested). */

export interface BackfillArgs {
  write: boolean;
  registry?: string;
}

/**
 * Strict argv parsing. The old inline parsing accepted anything: `--registry` with no value (or
 * followed by another flag) silently meant "every vault" (and with --write, an unscoped write),
 * and a typo like `--wrte` silently ran a dry run. Throws on any unknown or malformed argument.
 */
export function parseBackfillArgs(args: string[]): BackfillArgs {
  let write = false;
  let registry: string | undefined;
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--write") {
      write = true;
    } else if (a === "--registry") {
      const v = args[++i];
      if (!v || v.startsWith("--")) throw new Error("--registry requires a registry public key");
      if (registry !== undefined) throw new Error("--registry given more than once");
      registry = v;
    } else {
      throw new Error(`unknown argument: ${a}`);
    }
  }
  return { write, registry };
}

export interface DrainResult {
  signatures: number;
  backlog: number;
  events: number;
  users: string[];
  reconciled: number;
  reconcileSkipped: number;
}

/** Drain a registry's backlog across several syncRegistry passes; stops when a pass folds nothing. */
export async function drainRegistry<R>(
  sync: (reg: R) => Promise<DrainResult>,
  reg: R,
): Promise<{ last: DrainResult; passes: number; events: number; users: Set<string> }> {
  const users = new Set<string>();
  let r = await sync(reg);
  let passes = 1;
  let events = r.events;
  for (const u of r.users) users.add(u);
  while (r.backlog > r.signatures) {
    r = await sync(reg);
    passes++;
    events += r.events;
    for (const u of r.users) users.add(u);
    if (r.signatures === 0) break; // no progress possible: stop rather than spin
  }
  return { last: r, passes, events, users };
}
