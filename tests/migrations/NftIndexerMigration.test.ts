import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const migration = readFileSync(
  new URL(
    "../../migrations/20260625_position_nft_events_instruction_index.sql",
    import.meta.url,
  ),
  "utf8",
);

describe("position_nft_events instruction-index migration", () => {
  it("allows the v17 transfer event type", () => {
    expect(migration).toContain(
      "DROP CONSTRAINT IF EXISTS position_nft_events_event_type_check",
    );

    expect(migration).toContain(
      "CHECK (event_type IN ('mint', 'burn', 'transfer'))",
    );
  });

  it("removes signature-only uniqueness through the target table", () => {
    expect(migration).toMatch(
      /ALTER TABLE position_nft_events\s+DROP CONSTRAINT IF EXISTS position_nft_events_signature_key;/,
    );

    expect(migration).not.toContain("FROM pg_constraint");
  });

  it("creates the composite event identity index", () => {
    expect(migration).toMatch(
      /CREATE UNIQUE INDEX IF NOT EXISTS\s+position_nft_events_signature_instruction_index_key/,
    );

    expect(migration).toContain(
      "ON position_nft_events (signature, instruction_index)",
    );
  });

  it("documents the coordinated deployment requirement", () => {
    expect(migration).toContain(
      "Pause the NFT indexer before applying this migration",
    );

    expect(migration).not.toMatch(
      /^\s*(?:CREATE\s+(?:UNIQUE\s+)?INDEX|DROP\s+INDEX)\s+CONCURRENTLY\b/im,
    );
  });
});
