import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const migration = readFileSync(
  new URL(
    "../../migrations/20260625_position_nft_events_instruction_index.sql",
    import.meta.url,
  ),
  "utf8",
);

const validationMigration = readFileSync(
  new URL(
    "../../migrations/20260725_position_nft_events_validate_event_type_check.sql",
    import.meta.url,
  ),
  "utf8",
);

describe("position_nft_events instruction-index migration", () => {
  it("adds the v17 transfer event type without scanning existing rows", () => {
    expect(migration).toContain(
      "DROP CONSTRAINT IF EXISTS position_nft_events_event_type_check",
    );

    expect(migration).toContain(
      "CHECK (event_type IN ('mint', 'burn', 'transfer')) NOT VALID",
    );
  });

  it("validates the event-type constraint in a follow-up migration", () => {
    expect(validationMigration).toMatch(
      /ALTER TABLE position_nft_events\s+VALIDATE CONSTRAINT position_nft_events_event_type_check;/,
    );
  });

  it("removes all legacy signature-only uniqueness", () => {
    expect(migration).toMatch(
      /ALTER TABLE position_nft_events\s+DROP CONSTRAINT IF EXISTS position_nft_events_signature_key;/,
    );

    expect(migration).toMatch(
      /DROP INDEX IF EXISTS position_nft_events_signature_key;/,
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
      "Pause the NFT indexer, apply this migration",
    );

    expect(migration).toContain(
      "20260725_position_nft_events_validate_event_type_check.sql",
    );

    const executableMigrations = `${migration}\n${validationMigration}`;

    expect(executableMigrations).not.toMatch(
      /^\s*(?:CREATE\s+(?:UNIQUE\s+)?INDEX|DROP\s+INDEX)\s+CONCURRENTLY\b/im,
    );
  });
});
