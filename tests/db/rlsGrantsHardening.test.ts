import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

// Static checks for the RLS/grants hardening migration (audit flags F1-F6). The live proof
// (before/after audit on scratch Postgres, plain and Supabase-default-privilege models) is in the PR.
const MIGRATIONS = join(__dirname, "..", "..", "supabase", "migrations");
const FILE = "20261119120000_rls_grants_hardening.sql";
const live = readFileSync(join(MIGRATIONS, FILE), "utf8")
  .split("\n")
  .filter((l) => !l.trim().startsWith("--"))
  .join("\n")
  .replace(/\s+/g, " ");

describe(`supabase/migrations/${FILE}`, () => {
  it("runs in one transaction", () => {
    expect(live.trim()).toMatch(/^BEGIN;.*COMMIT;$/);
  });

  it("F1: revokes the restored app tables from the client roles", () => {
    expect(live).toContain(
      "REVOKE ALL ON TABLE faucet_claims, devnet_mints, devnet_airdrop_claims, auto_fund_log, market_challenges FROM PUBLIC, anon, authenticated;",
    );
  });

  it("F2/F4: functions lose PUBLIC, anon and authenticated EXECUTE but keep service_role", () => {
    for (const fn of ["volume_24h_by_slab(text)", "cleanup_old_trades(integer)"]) {
      expect(live).toContain(`REVOKE ALL ON FUNCTION public.${fn} FROM PUBLIC, anon, authenticated;`);
      expect(live).toContain(`GRANT EXECUTE ON FUNCTION public.${fn} TO service_role;`);
    }
  });

  it("F3: markets_with_stats runs as the caller", () => {
    expect(live).toContain("ALTER VIEW markets_with_stats SET (security_invoker = on);");
  });

  it("F5: authenticated gets exactly the anon trades columns, re-granted after the revoke", () => {
    const revoke = live.indexOf("REVOKE ALL ON TABLE trades FROM authenticated;");
    const grant = live.indexOf("GRANT SELECT (trader, size, created_at, size_provenance) ON trades TO authenticated;");
    expect(revoke).toBeGreaterThan(-1);
    expect(grant).toBeGreaterThan(revoke);
  });

  it("F6: indexer-only tables and the skipped_signatures sequence are granted to service_role", () => {
    expect(live).toContain("GRANT ALL ON TABLE lp_vault_sync_cursor, skipped_signatures, trade_indexer_cursor TO service_role;");
    expect(live).toContain("GRANT ALL ON SEQUENCE skipped_signatures_id_seq TO service_role;");
  });

  it("never touches anon's grants on trades or markets (the launch app's anon reads)", () => {
    expect(live).not.toMatch(/\b(REVOKE|GRANT)\b[^;]*\bON (TABLE )?(public\.)?(trades|markets|market_stats|markets_with_stats)\b[^;]*\banon\b/i);
  });

  it("any later migration that recreates markets_with_stats sets security_invoker again", () => {
    const files = readdirSync(MIGRATIONS).filter((f) => f.endsWith(".sql")).sort();
    const after = files.slice(files.indexOf(FILE) + 1);
    for (const f of after) {
      const sql = readFileSync(join(MIGRATIONS, f), "utf8");
      if (/CREATE (OR REPLACE )?VIEW\s+(public\.)?markets_with_stats/i.test(sql)) {
        expect(sql, `${f} recreates markets_with_stats`).toMatch(/security_invoker\s*=\s*(on|true)/i);
      }
    }
  });
});
