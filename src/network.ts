/**
 * PERC-8192: Network isolation helper for the indexer.
 *
 * Returns the current deployment network from the NETWORK env var.
 * Evaluated once at module load time so all services share the same value.
 */
export type NetworkType = "devnet" | "testnet" | "mainnet";

/**
 * Unset (or empty) NETWORK keeps the devnet default. A set but unknown value
 * throws at import: CURRENT_NETWORK tags every row written and filters every
 * read, so silently mapping a typo to devnet would put a non-devnet indexer's
 * rows under the devnet tag. "mainnet-beta" is accepted as "mainnet" because
 * the SDK's getCurrentNetwork() reads the same env var and treats it so.
 */
function resolveNetwork(): NetworkType {
  const raw = process.env.NETWORK;
  const n = (raw ?? "").toLowerCase().trim();
  if (n === "" || n === "devnet") return "devnet";
  if (n === "testnet") return "testnet";
  if (n === "mainnet" || n === "mainnet-beta") return "mainnet";
  throw new Error(
    `Unknown NETWORK value ${JSON.stringify(raw)}: expected one of "devnet", "testnet", "mainnet" (or unset for devnet)`,
  );
}

/**
 * The current deployment network.
 * All Supabase queries should filter `.eq("network", CURRENT_NETWORK)` to
 * prevent devnet and mainnet rows from mixing.
 */
export const CURRENT_NETWORK: NetworkType = resolveNetwork();
