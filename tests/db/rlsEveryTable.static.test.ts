import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

// Static invariant over ALL of supabase/migrations (no database): replay every file in order,
// keep the final state of each public table and view, then require
//   - table: ROW LEVEL SECURITY enabled, and no write privilege left for anon or authenticated;
//   - view:  security_invoker = true, and no write privilege left for anon or authenticated;
//   - materialized view: never allowed (it can have neither RLS nor security_invoker).
//
// Why (report J-20261122-IDX-REPORT-RLSANONWRITEPROBE, ~/agent-worker/reports/idx-rls-anon-write-probe.md):
//   L1: "Supabase default ACL plus a future plain view. Any new CREATE VIEW in public gets anon ALL.
//        A single-table view is auto-updatable and runs with the owner's rights, so RLS on the base
//        table is bypassed."
//   L2: "Same root cause for tables: a future table that skips REVOKE and RLS is fully anon-writable."
// So the model starts every new object with the Supabase default ACL: anon and authenticated hold ALL.
// Only an explicit REVOKE removes write privileges; a later GRANT of any write privilege adds it back.
// SELECT is not checked: reads are governed by RLS policies / column grants (other tests pin those).
// Functions are not tracked (L4, set_updated_at EXECUTE, is out of scope here).
//
// RLS_STATIC_MIGRATIONS_DIR points the check at another checkout's migrations (used to run it read-only
// against open PR heads); by default it reads this repo's supabase/migrations.

const WRITE_PRIVS = ["INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"] as const;
const CLIENT_ROLES = ["anon", "authenticated"] as const;
type ClientRole = (typeof CLIENT_ROLES)[number];
type Kind = "table" | "view" | "materialized view";

interface Rel {
  kind: Kind;
  file: string; // file of the CREATE that produced the current object
  rls: boolean;
  securityInvoker: boolean;
  writes: Record<ClientRole, Set<string>>;
}

interface Finding {
  key: string; // `${name}:${rule}`
  message: string; // names the file and the object
}

const allWrites = (): Record<ClientRole, Set<string>> => ({
  anon: new Set(WRITE_PRIVS),
  authenticated: new Set(WRITE_PRIVS),
});

/** Split SQL into statements, dropping comments, honouring '...', "...", and $tag$...$tag$ quoting. */
function splitStatements(sql: string): string[] {
  const out: string[] = [];
  let cur = "";
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
    const two = sql.slice(i, i + 2);
    if (two === "--") {
      const nl = sql.indexOf("\n", i);
      i = nl === -1 ? sql.length : nl;
      cur += " ";
      continue;
    }
    if (two === "/*") {
      const end = sql.indexOf("*/", i + 2);
      i = end === -1 ? sql.length : end + 2;
      cur += " ";
      continue;
    }
    if (c === "'" || c === '"') {
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === c && sql[j + 1] === c) j += 2;
        else if (sql[j] === c) break;
        else j++;
      }
      cur += sql.slice(i, j + 1);
      i = j + 1;
      continue;
    }
    if (c === "$") {
      const m = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i));
      if (m) {
        const end = sql.indexOf(m[0], i + m[0].length);
        const stop = end === -1 ? sql.length : end + m[0].length;
        cur += sql.slice(i, stop);
        i = stop;
        continue;
      }
    }
    if (c === ";") {
      if (cur.trim()) out.push(cur.trim());
      cur = "";
      i++;
      continue;
    }
    cur += c;
    i++;
  }
  if (cur.trim()) out.push(cur.trim());
  return out.map((s) => s.replace(/\s+/g, " "));
}

const IDENT = String.raw`(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)`;
const QNAME = String.raw`(${IDENT}(?:\s*\.\s*${IDENT})?)`;

/** Normalise a possibly schema-qualified name; returns null for objects outside schema public. */
function relName(raw: string): string | null {
  const parts = raw.split(".").map((p) => {
    const t = p.trim();
    return t.startsWith('"') ? t.slice(1, -1) : t.toLowerCase();
  });
  if (parts.length === 2 && parts[0] !== "public") return null;
  return parts[parts.length - 1];
}

const SECURITY_DDL =
  /\b(CREATE\s+(OR\s+REPLACE\s+)?((TEMP|TEMPORARY|UNLOGGED|RECURSIVE|MATERIALIZED)\s+)*(TABLE|VIEW)|ROW\s+LEVEL\s+SECURITY|REVOKE|GRANT|security_invoker|DROP\s+(TABLE|VIEW|MATERIALIZED))\b/i;

/**
 * Expand a DO block into the static statements it runs. Only the pattern the repo uses is followed:
 *   FOREACH v IN ARRAY ARRAY['a','b'] LOOP EXECUTE format('... %I ...', v); ... END LOOP;
 * Any other security-relevant DDL left in the body is returned as `unfollowed` so the check fails
 * loudly instead of silently trusting dynamic or conditional SQL.
 */
function expandDo(stmt: string): { stmts: string[]; unfollowed: string | null } {
  const m = /^DO\s+(?:LANGUAGE\s+\w+\s+)?(\$[A-Za-z0-9_]*\$)([\s\S]*)\1/i.exec(stmt);
  if (!m) return { stmts: [], unfollowed: SECURITY_DDL.test(stmt) ? stmt : null };
  let body = m[2];
  const stmts: string[] = [];
  const loop = /FOREACH\s+(\w+)\s+IN\s+ARRAY\s+ARRAY\s*\[([^\]]*)\]\s+LOOP([\s\S]*?)END\s+LOOP\s*;/gi;
  body = body.replace(loop, (whole, v: string, list: string, inner: string) => {
    const names = [...list.matchAll(/'((?:[^']|'')*)'/g)].map((x) => x[1].replace(/''/g, "'"));
    const exec = new RegExp(String.raw`EXECUTE\s+format\s*\(\s*'((?:[^']|'')*)'\s*,\s*${v}\s*\)\s*;`, "gi");
    const templates = [...inner.matchAll(exec)].map((x) => x[1].replace(/''/g, "'"));
    if (inner.replace(exec, "").trim() !== "") return whole; // something else in the loop: leave it, flagged below
    for (const n of names) for (const t of templates) stmts.push(...splitStatements(t.replace(/%I/g, `"${n}"`)));
    return " ";
  });
  return { stmts, unfollowed: SECURITY_DDL.test(body) ? body.trim() : null };
}

function parsePrivs(list: string): { all: boolean; privs: Set<string>; columnOnly: Set<string> } {
  const privs = new Set<string>();
  const columnOnly = new Set<string>();
  let all = false;
  for (const raw of list.split(/,(?![^(]*\))/)) {
    const m = /^\s*([A-Za-z ]+?)\s*(\([^)]*\))?\s*$/.exec(raw);
    if (!m) continue;
    const p = m[1].toUpperCase().replace(/\s+PRIVILEGES$/, "");
    if (p === "ALL") all = true;
    else privs.add(p);
    if (m[2]) columnOnly.add(p === "ALL" ? "ALL" : p);
  }
  return { all, privs, columnOnly };
}

/** Replay migrations (in the given order) and return the final state per public relation. */
function replay(files: { name: string; sql: string }[]): { rels: Map<string, Rel>; findings: Finding[] } {
  const rels = new Map<string, Rel>();
  const findings: Finding[] = [];

  const apply = (file: string, s: string): void => {
    let m: RegExpExecArray | null;

    if (/^DO\s/i.test(s)) {
      const { stmts, unfollowed } = expandDo(s);
      for (const x of stmts) apply(file, x);
      if (unfollowed) {
        findings.push({
          key: `${file}:unfollowed-do`,
          message: `${file}: DO block with security DDL the static check cannot follow: ${unfollowed.slice(0, 160)}`,
        });
      }
      return;
    }

    // CREATE TABLE
    m = new RegExp(String.raw`^CREATE\s+(?:(GLOBAL|LOCAL)\s+)?((?:TEMP|TEMPORARY|UNLOGGED)\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?${QNAME}`, "i").exec(s);
    if (m) {
      if (m[2] && /TEMP/i.test(m[2])) return;
      const name = relName(m[4]);
      if (name === null) return;
      if (m[3] && rels.has(name)) return;
      rels.set(name, { kind: "table", file, rls: false, securityInvoker: false, writes: allWrites() });
      return;
    }

    // CREATE MATERIALIZED VIEW
    m = new RegExp(String.raw`^CREATE\s+MATERIALIZED\s+VIEW\s+(IF\s+NOT\s+EXISTS\s+)?${QNAME}`, "i").exec(s);
    if (m) {
      const name = relName(m[2]);
      if (name === null) return;
      if (m[1] && rels.has(name)) return;
      rels.set(name, { kind: "materialized view", file, rls: false, securityInvoker: false, writes: allWrites() });
      return;
    }

    // CREATE [OR REPLACE] [TEMP] [RECURSIVE] VIEW name [(cols)] [WITH (opts)] AS
    m = new RegExp(String.raw`^CREATE\s+(OR\s+REPLACE\s+)?((?:TEMP|TEMPORARY)\s+)?(?:RECURSIVE\s+)?VIEW\s+${QNAME}\s*(\([^)]*\)\s*)?(WITH\s*\(([^)]*)\)\s*)?AS\b`, "i").exec(s);
    if (m) {
      if (m[2]) return;
      const name = relName(m[3]);
      if (name === null) return;
      const invoker = m[6] !== undefined && /\bsecurity_invoker\s*(=\s*(true|on|1|yes|'true'|'on'))?\s*(,|$)/i.test(m[6]);
      const prev = rels.get(name);
      // CREATE OR REPLACE on an existing view keeps its grants, but resets reloptions to the WITH list.
      if (m[1] && prev && prev.kind === "view") {
        prev.securityInvoker = invoker;
        return;
      }
      rels.set(name, { kind: "view", file, rls: false, securityInvoker: invoker, writes: allWrites() });
      return;
    }

    // DROP TABLE / VIEW / MATERIALIZED VIEW [IF EXISTS] a, b [CASCADE]
    m = /^DROP\s+(TABLE|VIEW|MATERIALIZED\s+VIEW)\s+(IF\s+EXISTS\s+)?(.+?)(\s+(CASCADE|RESTRICT))?$/i.exec(s);
    if (m) {
      for (const n of m[3].split(",")) {
        const name = relName(n);
        if (name !== null) rels.delete(name);
      }
      return;
    }

    // ALTER TABLE / VIEW ... (RLS, rename, view options)
    m = new RegExp(String.raw`^ALTER\s+(TABLE|VIEW|MATERIALIZED\s+VIEW)\s+(IF\s+EXISTS\s+)?(ONLY\s+)?${QNAME}\s+(.*)$`, "i").exec(s);
    if (m) {
      const name = relName(m[4]);
      if (name === null) return;
      const rest = m[5];
      const rel = rels.get(name);
      const ren = new RegExp(String.raw`^RENAME\s+TO\s+(${IDENT})$`, "i").exec(rest);
      if (ren) {
        const to = relName(ren[1]);
        if (rel && to !== null) {
          rels.delete(name);
          rels.set(to, rel);
        }
        return;
      }
      if (!rel) return;
      if (/\bDISABLE\s+ROW\s+LEVEL\s+SECURITY\b/i.test(rest)) rel.rls = false;
      else if (/\bENABLE\s+ROW\s+LEVEL\s+SECURITY\b/i.test(rest)) rel.rls = true;
      const set = /\bSET\s*\(([^)]*)\)/i.exec(rest);
      if (set && /\bsecurity_invoker\b/i.test(set[1])) {
        rel.securityInvoker = /\bsecurity_invoker\s*(=\s*(true|on|1|yes|'true'|'on'))?\s*(,|$)/i.test(set[1]);
      }
      const reset = /\bRESET\s*\(([^)]*)\)/i.exec(rest);
      if (reset && /\bsecurity_invoker\b/i.test(reset[1])) rel.securityInvoker = false;
      return;
    }

    // GRANT / REVOKE privs ON [TABLE] a, b | ALL TABLES IN SCHEMA public  TO|FROM roles
    m = /^(GRANT|REVOKE)\s+(GRANT\s+OPTION\s+FOR\s+)?(.+?)\s+ON\s+(.+?)\s+(TO|FROM)\s+(.+?)(\s+WITH\s+GRANT\s+OPTION|\s+GRANTED\s+BY\s+\S+|\s+CASCADE|\s+RESTRICT)*$/i.exec(s);
    if (m) {
      const isGrant = m[1].toUpperCase() === "GRANT";
      if (m[2]) return; // GRANT OPTION FOR only touches the grant option
      let targets: string[];
      const on = m[4].trim();
      if (/^ALL\s+(TABLES|VIEWS)\s+IN\s+SCHEMA\s+/i.test(on)) {
        if (!/\bpublic\b/i.test(on)) return;
        targets = [...rels.keys()];
      } else if (/^(SEQUENCE|FUNCTION|PROCEDURE|ROUTINE|SCHEMA|DATABASE|TYPE|DOMAIN|LANGUAGE|LARGE|FOREIGN|TABLESPACE|PARAMETER|ALL\s)/i.test(on)) {
        return;
      } else {
        targets = on
          .replace(/^TABLE\s+/i, "")
          .split(",")
          .map(relName)
          .filter((x): x is string => x !== null);
      }
      const roles = m[6].split(",").map((r) => r.trim().replace(/^GROUP\s+/i, "").replace(/"/g, "").toLowerCase());
      const { all, privs, columnOnly } = parsePrivs(m[3]);
      for (const t of targets) {
        const rel = rels.get(t);
        if (!rel) continue;
        for (const role of roles) {
          const hit: ClientRole[] = role === "public" && isGrant ? [...CLIENT_ROLES] : CLIENT_ROLES.filter((r) => r === role);
          for (const r of hit) {
            for (const p of WRITE_PRIVS) {
              if (!all && !privs.has(p)) continue;
              if (isGrant) rel.writes[r].add(p);
              // A column-level REVOKE leaves the table-level privilege in place.
              else if (!columnOnly.has(all ? "ALL" : p)) rel.writes[r].delete(p);
            }
          }
        }
      }
    }
  };

  for (const f of files) for (const s of splitStatements(f.sql)) apply(f.name, s);

  for (const [name, rel] of rels) {
    const leftover = CLIENT_ROLES.filter((r) => rel.writes[r].size > 0)
      .map((r) => `${r}: ${[...rel.writes[r]].join(",")}`)
      .join("; ");
    if (rel.kind === "materialized view") {
      findings.push({
        key: `${name}:matview`,
        message: `${rel.file}: materialized view ${name} can have neither RLS nor security_invoker`,
      });
      continue;
    }
    if (rel.kind === "table" && !rel.rls) {
      findings.push({ key: `${name}:rls`, message: `${rel.file}: table ${name} has no ENABLE ROW LEVEL SECURITY` });
    }
    if (rel.kind === "view" && !rel.securityInvoker) {
      findings.push({
        key: `${name}:security_invoker`,
        message: `${rel.file}: view ${name} is not security_invoker = true (runs with owner rights, bypasses RLS)`,
      });
    }
    if (leftover) {
      findings.push({
        key: `${name}:client-write`,
        message: `${rel.file}: ${rel.kind} ${name} keeps client write grants (${leftover}); add REVOKE ALL ... FROM anon, authenticated`,
      });
    }
  }
  return { rels, findings };
}

// Known current exceptions, keyed `${object}:${rule}`, one line of reason each. An entry that no longer
// matches a finding fails the test, so this list cannot go stale.
const ALLOWLIST: Record<string, string> = {
  "markets_with_stats:client-write":
    'L3: "markets_with_stats holds anon/authenticated INSERT/UPDATE/DELETE/TRUNCATE/REFERENCES/TRIGGER. 20260726192000 re-created the view after the baseline grant, so the default ACL applied." Inert: join view (not auto-updatable) and security_invoker is on.',
};

const MIGRATIONS = process.env.RLS_STATIC_MIGRATIONS_DIR ?? join(__dirname, "..", "..", "supabase", "migrations");

function loadMigrations(dir: string): { name: string; sql: string }[] {
  return readdirSync(dir)
    .filter((f) => f.endsWith(".sql"))
    .sort()
    .map((name) => ({ name, sql: readFileSync(join(dir, name), "utf8") }));
}

describe("supabase/migrations: every public table has RLS + client revoke, every view is security_invoker", () => {
  const files = loadMigrations(MIGRATIONS);
  const { rels, findings } = replay(files);

  it("reads the migrations and sees the known objects", () => {
    expect(files.length).toBeGreaterThan(0);
    // Guards against a parser regression that silently tracks nothing.
    for (const t of ["markets", "market_stats", "trades", "oracle_markets", "v22_events", "lp_vault_events"]) {
      expect(rels.get(t)?.kind, t).toBe("table");
    }
    expect(rels.get("markets_with_stats")?.kind).toBe("view");
  });

  it("has no violation outside the allowlist", () => {
    const unexpected = findings.filter((f) => !(f.key in ALLOWLIST)).map((f) => f.message);
    expect(unexpected, unexpected.join("\n")).toEqual([]);
  });

  it("every allowlist entry is still a real finding (no stale exceptions)", () => {
    const keys = new Set(findings.map((f) => f.key));
    const stale = Object.keys(ALLOWLIST).filter((k) => !keys.has(k));
    expect(stale).toEqual([]);
  });
});

describe("replay model (synthetic migrations)", () => {
  const run = (...sql: string[]) => replay(sql.map((s, i) => ({ name: `m${i}.sql`, sql: s }))).findings.map((f) => f.key);
  const SAFE = "CREATE TABLE t (id int); ALTER TABLE t ENABLE ROW LEVEL SECURITY; REVOKE ALL ON TABLE t FROM anon, authenticated;";

  it("a plain CREATE TABLE fails on RLS and client writes, naming the file and table", () => {
    const r = replay([{ name: "20991231000000_bad.sql", sql: "create table public.money (amount numeric);" }]);
    expect(r.findings.map((f) => f.key)).toEqual(["money:rls", "money:client-write"]);
    expect(r.findings[0].message).toContain("20991231000000_bad.sql: table money");
  });

  it("a plain CREATE VIEW fails on security_invoker and client writes", () => {
    expect(run(SAFE, "CREATE VIEW v AS SELECT * FROM t;")).toEqual(["v:security_invoker", "v:client-write"]);
  });

  it("RLS + REVOKE ALL passes; RLS without REVOKE and REVOKE without RLS fail", () => {
    expect(run(SAFE)).toEqual([]);
    expect(run("CREATE TABLE t (id int); ALTER TABLE t ENABLE ROW LEVEL SECURITY;")).toEqual(["t:client-write"]);
    expect(run("CREATE TABLE t (id int); REVOKE ALL ON t FROM anon, authenticated;")).toEqual(["t:rls"]);
  });

  it("REVOKE FROM PUBLIC alone, or from only one client role, is not enough", () => {
    expect(run("CREATE TABLE t (id int); ALTER TABLE t ENABLE ROW LEVEL SECURITY; REVOKE ALL ON t FROM PUBLIC;")).toEqual(["t:client-write"]);
    expect(run("CREATE TABLE t (id int); ALTER TABLE t ENABLE ROW LEVEL SECURITY; REVOKE ALL ON t FROM anon;")).toEqual(["t:client-write"]);
  });

  it("SELECT grants are fine; a later write grant (incl. to PUBLIC or column-level) re-opens", () => {
    expect(run(SAFE, "GRANT SELECT ON t TO anon, authenticated; GRANT SELECT (id) ON t TO anon;")).toEqual([]);
    expect(run(SAFE, "GRANT INSERT ON t TO anon;")).toEqual(["t:client-write"]);
    expect(run(SAFE, "GRANT UPDATE (id) ON t TO PUBLIC;")).toEqual(["t:client-write"]);
    expect(run(SAFE, "GRANT ALL ON TABLE t TO service_role;")).toEqual([]);
  });

  it("a column-level REVOKE does not remove the table-level privilege", () => {
    expect(run("CREATE TABLE t (id int); ALTER TABLE t ENABLE ROW LEVEL SECURITY; REVOKE UPDATE (id) ON t FROM anon, authenticated;")).toEqual(["t:client-write"]);
  });

  it("DISABLE ROW LEVEL SECURITY later fails; DROP removes the object; RENAME carries state", () => {
    expect(run(SAFE, "ALTER TABLE t DISABLE ROW LEVEL SECURITY;")).toEqual(["t:rls"]);
    expect(run("CREATE TABLE t (id int);", "DROP TABLE IF EXISTS t CASCADE;")).toEqual([]);
    expect(run(SAFE, "ALTER TABLE t RENAME TO u;", "CREATE TABLE IF NOT EXISTS u (id int);")).toEqual([]);
    expect(run(SAFE, "ALTER TABLE t RENAME COLUMN id TO id2;")).toEqual([]);
  });

  it("CREATE TABLE IF NOT EXISTS on an existing table is a no-op; a re-create after DROP starts over", () => {
    expect(run(SAFE, "CREATE TABLE IF NOT EXISTS t (id int);")).toEqual([]);
    expect(run(SAFE, "DROP TABLE t;", "CREATE TABLE t (id int);")).toEqual(["t:rls", "t:client-write"]);
  });

  it("security_invoker: WITH option, ALTER VIEW SET/RESET, and CREATE OR REPLACE without WITH drops it", () => {
    const V = "REVOKE ALL ON v FROM anon, authenticated;";
    expect(run(SAFE, `CREATE VIEW v WITH (security_invoker = true) AS SELECT * FROM t; ${V}`)).toEqual([]);
    expect(run(SAFE, `CREATE VIEW v WITH (security_invoker) AS SELECT * FROM t; ${V}`)).toEqual([]);
    expect(run(SAFE, `CREATE VIEW v WITH (security_invoker = false) AS SELECT * FROM t; ${V}`)).toEqual(["v:security_invoker"]);
    expect(run(SAFE, `CREATE VIEW v AS SELECT * FROM t; ${V}`, "ALTER VIEW v SET (security_invoker = on);")).toEqual([]);
    expect(run(SAFE, `CREATE VIEW v WITH (security_invoker = on) AS SELECT * FROM t; ${V}`, "ALTER VIEW v RESET (security_invoker);")).toEqual(["v:security_invoker"]);
    expect(run(SAFE, `CREATE VIEW v WITH (security_invoker = on) AS SELECT * FROM t; ${V}`, "CREATE OR REPLACE VIEW v AS SELECT * FROM t;")).toEqual(["v:security_invoker"]);
  });

  it("CREATE OR REPLACE VIEW keeps grants; DROP + CREATE VIEW gets the default ACL again", () => {
    const V = "CREATE VIEW v WITH (security_invoker = on) AS SELECT * FROM t; REVOKE ALL ON v FROM anon, authenticated;";
    expect(run(SAFE, V, "CREATE OR REPLACE VIEW v WITH (security_invoker = on) AS SELECT id FROM t;")).toEqual([]);
    expect(run(SAFE, V, "DROP VIEW v; CREATE VIEW v WITH (security_invoker = on) AS SELECT id FROM t; GRANT SELECT ON v TO anon;")).toEqual(["v:client-write"]);
  });

  it("materialized views are always flagged", () => {
    expect(run(SAFE, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t; REVOKE ALL ON mv FROM anon, authenticated;")).toEqual(["mv:matview"]);
  });

  it("follows the baseline DO/FOREACH/EXECUTE format loop; flags other DDL in DO blocks", () => {
    const loop = (stmt: string) => `DO $$ DECLARE x TEXT; BEGIN FOREACH x IN ARRAY ARRAY['a','b'] LOOP EXECUTE format('${stmt}', x); END LOOP; END $$;`;
    expect(
      run("CREATE TABLE a (i int); CREATE TABLE b (i int);", loop("ALTER TABLE %I ENABLE ROW LEVEL SECURITY;"), loop("REVOKE ALL ON %I FROM anon, authenticated;")),
    ).toEqual([]);
    expect(run(SAFE, "DO $$ BEGIN IF true THEN EXECUTE 'CREATE TABLE z (i int)'; END IF; END $$;")).toEqual(["m1.sql:unfollowed-do"]);
    expect(run(SAFE, "DO $$ BEGIN IF NOT EXISTS (SELECT 1) THEN ALTER PUBLICATION p ADD TABLE t; END IF; END $$;")).toEqual([]);
  });

  it("REVOKE ON ALL TABLES IN SCHEMA public covers existing objects; other schemas and comments are ignored", () => {
    expect(run("CREATE TABLE t (id int); ALTER TABLE t ENABLE ROW LEVEL SECURITY;", "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM anon, authenticated;")).toEqual([]);
    expect(run("CREATE TABLE extensions.e (id int); CREATE TABLE IF NOT EXISTS \"auth\".x (id int);")).toEqual([]);
    expect(run(SAFE, "-- CREATE TABLE c (id int);\n/* CREATE VIEW d AS SELECT 1; */ SELECT 'CREATE TABLE q (i int);';")).toEqual([]);
    expect(run("CREATE TEMP TABLE tmp (i int);")).toEqual([]);
  });
});
