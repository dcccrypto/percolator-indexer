// DISCL-1: a local JSON-RPC server that answers getProgramAccounts with the same fault shapes as the
// devnet harness proxy (IDX-REPORT-DISCOVERYLIGHTDEVNETREAL proxy.mjs), so a REAL web3.js Connection
// produces the real error for each mode. Modes:
//   none      200 + the scripted account list
//   http429   HTTP 429 + JSON-RPC body (what a rate limit that outlasts web3's retries ends in)
//   rpc429    HTTP 200 + JSON-RPC error -32429
//   truncated HTTP 200 + the clean body cut in half (JSON.parse fails inside web3.js)
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import type { PublicKey } from "@solana/web3.js";

export type FaultMode = "none" | "http429" | "rpc429" | "truncated";

export interface FaultRpc {
  url: string;
  mode: FaultMode;
  accounts: Array<{ key: PublicKey; data: Uint8Array; owner: PublicKey }>;
  gpaCalls: number;
  /** How many more getProgramAccounts calls get `mode`; after that the server answers clean. */
  faultsLeft: number;
  close(): Promise<void>;
}

export async function startFaultRpc(): Promise<FaultRpc> {
  const state: Omit<FaultRpc, "url" | "close"> = { mode: "none", accounts: [], gpaCalls: 0, faultsLeft: Infinity };
  const server: Server = createServer((req, res) => {
    let raw = "";
    req.on("data", (c) => { raw += c; });
    req.on("end", () => {
      const body = JSON.parse(raw);
      res.setHeader("content-type", "application/json");
      if (body.method !== "getProgramAccounts") {
        res.end(JSON.stringify({ jsonrpc: "2.0", id: body.id, error: { code: -32601, message: "fault rpc: method not served" } }));
        return;
      }
      state.gpaCalls++;
      const mode: FaultMode = state.faultsLeft > 0 ? state.mode : "none";
      if (mode !== "none") state.faultsLeft--;
      if (mode === "http429") {
        res.statusCode = 429;
        res.end(JSON.stringify({ jsonrpc: "2.0", id: body.id, error: { code: 429, message: "Too many requests for a specific RPC call" } }));
        return;
      }
      if (mode === "rpc429") {
        res.end(JSON.stringify({ jsonrpc: "2.0", id: body.id, error: { code: -32429, message: "rate limit exceeded" } }));
        return;
      }
      const slice = body.params[1]?.dataSlice as { offset: number; length: number } | undefined;
      const value = state.accounts.map((a) => {
        const data = slice ? a.data.slice(slice.offset, slice.offset + slice.length) : a.data;
        return {
          pubkey: a.key.toBase58(),
          account: { data: [Buffer.from(data).toString("base64"), "base64"], executable: false, lamports: 1_000_000, owner: a.owner.toBase58(), rentEpoch: 0, space: a.data.length },
        };
      });
      const plain = JSON.stringify({ jsonrpc: "2.0", id: body.id, result: value });
      res.end(mode === "truncated" ? plain.slice(0, Math.floor(plain.length / 2)) : plain);
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as AddressInfo;
  const rpc = state as FaultRpc;
  rpc.url = `http://127.0.0.1:${port}`;
  rpc.close = () => new Promise<void>((r) => server.close(() => r()));
  return rpc;
}
