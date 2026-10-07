import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { V22_EVENT_KINDS } from "../../src/parsers/v22Events.js";

const sql = readFileSync(new URL("../../supabase/migrations/20261007120000_v22_events.sql", import.meta.url), "utf8");
const live = sql.split("\n").filter((l) => !l.trim().startsWith("--")).join("\n");

describe("supabase/migrations/20261007120000_v22_events.sql (NOT applied)", () => {
  it("is marked NOT APPLIED and creates the table idempotently", () => {
    expect(sql.split("\n")[0]).toMatch(/NOT APPLIED/);
    expect(live).toMatch(/CREATE TABLE IF NOT EXISTS v22_events/);
  });

  it("RLS on, no policies, no anon/authenticated access (like skipped_signatures)", () => {
    expect(live).toMatch(/ALTER TABLE v22_events ENABLE ROW LEVEL SECURITY/);
    expect(live).toMatch(/REVOKE ALL ON TABLE v22_events FROM anon, authenticated/);
    expect(live).toMatch(/REVOKE ALL ON SEQUENCE v22_events_id_seq FROM anon, authenticated/);
    // Only service_role is granted (table + sequence); never anon/authenticated.
    expect(live).toMatch(/GRANT ALL ON TABLE v22_events TO service_role/);
    expect(live).toMatch(/GRANT ALL ON SEQUENCE v22_events_id_seq TO service_role/);
    expect([...live.matchAll(/GRANT [^;]*? TO ([a-z_, ]+);/gi)].map((m) => m[1].trim())).toEqual(["service_role", "service_role"]);
    expect(live).not.toMatch(/CREATE POLICY/i);
  });

  it("the CHECK constraint lists exactly the kinds the parser emits (drift guard)", () => {
    const m = /CHECK \(kind IN \(([\s\S]*?)\)\)/.exec(live);
    const kinds = [...(m?.[1] ?? "").matchAll(/'([a-z_]+)'/g)].map((x) => x[1]);
    expect([...kinds].sort()).toEqual([...V22_EVENT_KINDS].sort());
  });

  it("the unique key matches the writer's onConflict", () => {
    expect(live).toMatch(/UNIQUE \(signature, ix_index, inner_index, network\)/);
  });
});
