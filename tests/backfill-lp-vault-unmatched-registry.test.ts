/**
 * #337 follow-up: drive the real main() of src/scripts/backfill-lp-vault.ts (it auto-runs on
 * import) with a --registry that matches no known vault. It must fail loudly (fatal log +
 * exit(1)) and must not sync, write, or even construct the Supabase store.
 * Everything external (RPC, DB, config) is mocked; no network.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const h = vi.hoisted(() => ({
  listRegistries: vi.fn(),
  syncRegistry: vi.fn(),
  supabaseStoreCtor: vi.fn(),
  getSupabase: vi.fn(),
  memoryWrites: vi.fn(),
}));

vi.mock("dotenv/config", () => ({}));
vi.mock("@solana/web3.js", async (orig) => ({ ...(await orig<typeof import("@solana/web3.js")>()), Connection: class {} }));
vi.mock("@percolatorct/shared", () => ({
  config: { rpcUrl: "http://mock.invalid", allProgramIds: ["FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"] },
  getSupabase: h.getSupabase,
}));
vi.mock("../src/network.js", () => ({ CURRENT_NETWORK: "devnet" }));
vi.mock("../src/lpVault/chain.js", () => ({
  RpcLpVaultChain: class {
    listRegistries = h.listRegistries;
  },
}));
vi.mock("../src/lpVault/store.js", () => ({
  MemoryLpVaultStore: class {
    events = new Map();
    positions = new Map();
    loadPosition = h.memoryWrites;
  },
  SupabaseLpVaultStore: class {
    constructor() {
      h.supabaseStoreCtor();
    }
  },
}));
vi.mock("../src/services/LpVaultIndexer.js", () => ({
  LpVaultIndexer: class {
    constructor(private opts: { chain: { listRegistries: (p: string[]) => Promise<unknown[]> }; programIds: string[] }) {}
    refreshRegistries() {
      return this.opts.chain.listRegistries(this.opts.programIds);
    }
    syncRegistry = h.syncRegistry;
  },
}));

async function runScript(argv: string[]) {
  const origArgv = process.argv;
  process.argv = ["node", "backfill-lp-vault.ts", ...argv];
  const exit = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
  const err = vi.spyOn(console, "error").mockImplementation(() => {});
  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  try {
    vi.resetModules();
    await import("../src/scripts/backfill-lp-vault.js");
    // main().catch(...) settles asynchronously after the import resolves
    await vi.waitFor(() => {
      if (exit.mock.calls.length === 0 && log.mock.calls.length === 0) throw new Error("not settled");
    });
    await new Promise((r) => setTimeout(r, 0));
    return { exit, err, log };
  } finally {
    process.argv = origArgv;
  }
}

describe("backfill-lp-vault main(): unmatched --registry", () => {
  beforeEach(() => {
    Object.values(h).forEach((f) => f.mockReset());
    h.listRegistries.mockResolvedValue([{ registry: "REG_A", market: "MKT_A" }]);
  });
  afterEach(() => vi.restoreAllMocks());

  it("throws (fatal + exit 1) and does no sync or DB writes, even with --write", async () => {
    const { exit, err } = await runScript(["--write", "--registry", "NOPE"]);
    expect(h.listRegistries).toHaveBeenCalledTimes(1);
    const fatal = err.mock.calls.find((c) => c[0] === "[lp-backfill] fatal");
    expect(fatal).toBeDefined();
    expect((fatal![1] as Error).message).toBe("--registry NOPE matches no known LP-vault registry");
    expect(exit).toHaveBeenCalledWith(1);
    expect(h.syncRegistry).not.toHaveBeenCalled();
  });

  it("does not trip the throw when --registry matches a known vault", async () => {
    h.syncRegistry.mockResolvedValue({ signatures: 0, backlog: 0, events: 0, users: [], reconciled: 0, reconcileSkipped: 0 });
    const { exit, err } = await runScript(["--registry", "REG_A"]);
    expect(err.mock.calls.find((c) => c[0] === "[lp-backfill] fatal")).toBeUndefined();
    expect(h.syncRegistry).toHaveBeenCalledTimes(1);
    expect(exit).not.toHaveBeenCalled();
  });
});
