/**
 * GH#207 — backfill Earn LP-vault events + cost-basis positions.
 *
 * Reads the FULL history of every LP-vault registry on the configured wrapper(s)
 * (an exact average-cost basis needs every deposit, so there is no --since: a
 * partial history would be silently wrong). On devnet that history starts with
 * the v18 wrapper (GnwdeQr…, deployed 2026-09-22), so it covers everything from
 * the 2026-09-24 re-seed onward.
 *
 * DRY RUN BY DEFAULT: folds into memory and prints the positions. Pass --write
 * to upsert into Supabase (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY — the
 * indexer's own project). Idempotent: safe to re-run, and safe to run while the
 * service is live (same keys, positions are recomputed from events).
 *
 *   pnpm tsx src/scripts/backfill-lp-vault.ts                 # dry run
 *   pnpm tsx src/scripts/backfill-lp-vault.ts --write         # write
 *   pnpm tsx src/scripts/backfill-lp-vault.ts --registry <pk> # one vault
 *
 * Env: RPC_URL, NETWORK, PROGRAM_ID / ALL_PROGRAM_IDS (as the service).
 *      LP_BACKFILL_RPC_ORIGIN — optional Origin header for an Origin-restricted RPC key.
 */
import "dotenv/config";
import { Connection } from "@solana/web3.js";
import { config, getSupabase } from "@percolatorct/shared";
import { RpcLpVaultChain } from "../lpVault/chain.js";
import { MemoryLpVaultStore, SupabaseLpVaultStore, type LpVaultStore, type PositionRow } from "../lpVault/store.js";
import { LpVaultIndexer } from "../services/LpVaultIndexer.js";
import { CURRENT_NETWORK } from "../network.js";

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const write = args.includes("--write");
  const regIdx = args.indexOf("--registry");
  const only = regIdx >= 0 ? args[regIdx + 1] : undefined;

  const origin = process.env.LP_BACKFILL_RPC_ORIGIN;
  const conn = new Connection(config.rpcUrl, {
    commitment: "confirmed",
    ...(origin ? { httpHeaders: { Origin: origin } } : {}),
  });
  const memory = new MemoryLpVaultStore();
  const store: LpVaultStore = write ? new SupabaseLpVaultStore(getSupabase()) : memory;
  const indexer = new LpVaultIndexer({
    chain: new RpcLpVaultChain(conn),
    store,
    network: CURRENT_NETWORK,
    programIds: config.allProgramIds,
  });

  const registries = (await indexer.refreshRegistries()).filter((r) => !only || r.registry === only);
  console.log(`[lp-backfill] ${write ? "WRITE" : "DRY RUN"} network=${CURRENT_NETWORK} programs=${config.allProgramIds.join(",")} vaults=${registries.length}`);

  let failures = 0;
  const touched: PositionRow[] = [];
  for (const reg of registries) {
    try {
      const r = await indexer.syncRegistry(reg);
      console.log(`[lp-backfill] ${reg.registry} market=${reg.market} sigs=${r.signatures} events=${r.events} users=${r.users.length} reconciled=${r.reconciled} skipped=${r.reconcileSkipped}`);
      for (const u of r.users) {
        const p = await store.loadPosition(CURRENT_NETWORK, reg.registry, u);
        if (p) touched.push(p);
      }
    } catch (err) {
      failures++;
      console.error(`[lp-backfill] ${reg.registry} FAILED: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  for (const p of touched) {
    console.log(
      `[lp-backfill] position ${p.market_slab} ${p.user_wallet} shares=${p.lp_shares} pending=${p.pending_redeem_shares} ` +
      `basis=${p.cost_basis_atoms} realized=${p.realized_pnl_atoms} onchain=${p.onchain_lp_shares} basis_known=${p.basis_known}` +
      (p.inconsistency ? ` (${p.inconsistency})` : ""),
    );
  }
  if (!write) console.log(`[lp-backfill] dry run: ${memory.events.size} events, ${memory.positions.size} positions NOT written (pass --write)`);
  if (failures > 0) process.exit(1);
}

main().catch((err) => {
  console.error("[lp-backfill] fatal", err);
  process.exit(1);
});
