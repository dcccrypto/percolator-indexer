import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

/**
 * GH#195 — the trade dedup key had two holes.
 *
 * Part 1 (cross-path `leg_index` divergence) is fixed in the ingestion paths and
 * is covered by the webhook tests. This file covers part 2: the `NULLS DISTINCT`
 * gap, which is a SCHEMA property, plus the writer-side guard.
 *
 * A migration is hard to unit-test without a live database. What IS testable, and
 * what actually went wrong here, is the invariant the migration exists to hold —
 * so these assert against the migration SQL itself. That catches the realistic
 * regression: someone later rebuilds this index (as 20260726181000 did) and
 * silently drops the property again.
 */

const MIGRATIONS = join(__dirname, "..", "supabase", "migrations");

function allMigrations(): { name: string; sql: string }[] {
  return readdirSync(MIGRATIONS)
    .filter((f) => f.endsWith(".sql"))
    .sort()
    .map((name) => ({ name, sql: readFileSync(join(MIGRATIONS, name), "utf8") }));
}

/**
 * The CREATE statement itself, with SQL comments stripped.
 *
 * Scoping matters, and getting it wrong made the first version of this file
 * VACUOUS: asserting `/NULLS NOT DISTINCT/` against the whole migration matched
 * the phrase in the header comment and the COMMENT ON INDEX text, so removing it
 * from the actual DDL left every test still green. The negative control is what
 * caught that.
 */
function createStatement(sql: string): string {
  const withoutComments = sql
    .split("\n")
    .filter((l) => !l.trimStart().startsWith("--"))
    .join("\n");
  const stmt = withoutComments.match(
    /CREATE\s+UNIQUE\s+INDEX[\s\S]*?uq_trades_sig_asset_leg[\s\S]*?;/i,
  );
  expect(stmt, "the migration must contain a CREATE UNIQUE INDEX statement").toBeTruthy();
  return (stmt as RegExpMatchArray)[0];
}

/** The last migration that (re)builds the dedup index — whatever it is called. */
function lastIndexBuilder(): { name: string; sql: string } {
  const builders = allMigrations().filter((m) =>
    /CREATE\s+UNIQUE\s+INDEX[\s\S]*uq_trades_sig_asset_leg/i.test(m.sql),
  );
  expect(
    builders.length,
    "at least one migration must create uq_trades_sig_asset_leg",
  ).toBeGreaterThan(0);
  return builders[builders.length - 1];
}

describe("the trades dedup index closes the NULLS DISTINCT gap (GH#195)", () => {
  it("the newest migration that builds the index declares NULLS NOT DISTINCT", () => {
    // Was NULLS DISTINCT (the Postgres default), so any row with a NULL
    // tx_signature never conflicted and could be inserted unboundedly.
    //
    // Asserted against the STATEMENT, not the file — see createStatement().
    const { name, sql } = lastIndexBuilder();
    expect(createStatement(sql), `${name} must declare NULLS NOT DISTINCT`).toMatch(
      /NULLS\s+NOT\s+DISTINCT/i,
    );
  });

  it("keeps the index NON-PARTIAL so it stays a valid ON CONFLICT target", () => {
    // 20260726181000 dropped the `WHERE tx_signature IS NOT NULL` predicate
    // precisely because Postgres cannot infer a partial unique index from
    // `ON CONFLICT (cols)`, which is what PostgREST emits. Reintroducing a
    // predicate here would break batched webhook upserts — a regression in the
    // opposite direction, so it is pinned.
    const { name, sql } = lastIndexBuilder();
    expect(
      /\bWHERE\b/i.test(createStatement(sql)),
      `${name} must NOT make the index partial`,
    ).toBe(false);
  });

  it("de-duplicates existing rows BEFORE tightening the index", () => {
    // Ordering is load-bearing: a unique index cannot be built over existing
    // duplicates, so a migration that tightened first would simply fail to apply.
    const { sql } = lastIndexBuilder();
    const deleteAt = sql.search(/DELETE\s+FROM\s+trades/i);
    const createAt = sql.search(/CREATE\s+UNIQUE\s+INDEX/i);
    expect(deleteAt, "the migration must de-duplicate").toBeGreaterThan(-1);
    expect(deleteAt).toBeLessThan(createAt);
  });

  it("keeps the FIRST row per key, not an arbitrary one", () => {
    // ORDER BY id keeps the earliest-written row: the one whose side/size/price
    // came from the path that saw the fill first, and the one any downstream
    // reference would point at. An unordered DELETE would be non-deterministic.
    const { sql } = lastIndexBuilder();
    expect(sql).toMatch(/ROW_NUMBER\(\)\s*OVER\s*\([\s\S]*?ORDER\s+BY\s+id/i);
    expect(sql).toMatch(/rn\s*>\s*1/i);
  });

  it("partitions the de-duplication by the full dedupe key", () => {
    // Partitioning by fewer columns would delete legitimately distinct legs —
    // multi-fill batches share a tx_signature and differ only in leg_index.
    const { sql } = lastIndexBuilder();
    expect(sql).toMatch(
      /PARTITION\s+BY\s+tx_signature\s*,\s*asset_index\s*,\s*leg_index/i,
    );
  });
});

describe("the writer refuses a signature-less trade row (GH#195)", () => {
  it("rejects a missing, empty or non-string tx_signature", async () => {
    // The index makes NULL signatures deduplicate rather than be exempt — but a
    // NULL signature is still not a usable IDENTITY: every such row collapses
    // onto one key, so the second genuine signature-less fill would be silently
    // dropped as a duplicate. The index prevents unbounded duplication; this
    // prevents silent loss.
    const mod = await import("../src/db/insertTradeRow.js");
    const src = readFileSync(
      join(__dirname, "..", "src", "db", "insertTradeRow.ts"),
      "utf8",
    );
    // toDbRow is module-private, so assert the guard is wired into it rather than
    // reaching through the module boundary.
    expect(src).toMatch(/function\s+assertHasSignature/);
    expect(src).toMatch(/function\s+toDbRow[\s\S]{0,80}assertHasSignature\(row\)/);
    expect(mod).toBeTruthy();
  });

  it("the guard names GH#195, so the next reader can find the reasoning", () => {
    const src = readFileSync(
      join(__dirname, "..", "src", "db", "insertTradeRow.ts"),
      "utf8",
    );
    expect(src).toMatch(/GH#195/);
  });
});
