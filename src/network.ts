/**
 * PERC-8192: Network isolation helper for the indexer.
 *
 * Returns the current deployment network from the NETWORK env var.
 * Evaluated once at module load time so all services share the same value.
 */
export type NetworkType = "devnet" | "testnet" | "mainnet";

function resolveNetwork(): NetworkType {
  const n = (process.env.NETWORK ?? "devnet").toLowerCase().trim();
  if (n === "mainnet") return "mainnet";
  if (n === "testnet") return "testnet";
  return "devnet";
}

/**
 * The current deployment network.
 * All Supabase queries should filter `.eq("network", CURRENT_NETWORK)` to
 * prevent devnet and mainnet rows from mixing.
 */
export const CURRENT_NETWORK: NetworkType = resolveNetwork();
