import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { V22_EVENT_KINDS, V22_LOG_EVENT_KINDS } from "../../src/parsers/v22Events.js";
import { LOG_EVENT_INNER_BASE } from "../../src/parsers/v22FillEvents.js";

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

const sql2 = readFileSync(new URL("../../supabase/migrations/20261009120000_v22_log_events.sql", import.meta.url), "utf8");
const live2 = sql2.split("\n").filter((l) => !l.trim().startsWith("--")).join("\n");

describe("supabase/migrations/20261009120000_v22_log_events.sql (NOT applied)", () => {
  it("is marked NOT APPLIED, re-runnable (DROP IF EXISTS first) and touches nothing but the kind CHECK", () => {
    expect(sql2.split("\n")[0]).toMatch(/NOT APPLIED/);
    expect(live2).toMatch(/DROP CONSTRAINT IF EXISTS v22_events_kind_check/);
    expect(live2.indexOf("DROP CONSTRAINT")).toBeLessThan(live2.indexOf("ADD CONSTRAINT"));
    expect(live2).not.toMatch(/CREATE TABLE|DROP TABLE|ADD COLUMN|GRANT|UNIQUE|DROP CONSTRAINT IF EXISTS v22_events_signature/i);
    expect(sql2).toMatch(new RegExp(`inner_index = ${LOG_EVENT_INNER_BASE.toLocaleString("en-US")}`));
  });

  it("the new CHECK lists the first migration's kinds PLUS exactly the log-event kinds (drift guard)", () => {
    const m = /CHECK \(kind IN \(([\s\S]*?)\)\)/.exec(live2);
    const kinds = [...(m?.[1] ?? "").matchAll(/'([a-z_]+)'/g)].map((x) => x[1]);
    expect([...kinds].sort()).toEqual([...V22_EVENT_KINDS, ...V22_LOG_EVENT_KINDS].sort());
  });

  it("the constraint it replaces is the one the first migration creates implicitly (column CHECK on `kind`)", () => {
    expect(live).toMatch(/kind\s+text\s+NOT NULL CHECK \(kind IN/);
  });
});
