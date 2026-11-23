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
 * rows under the devnet tag. "mainnet-beta" is NOT aliased: @percolatorct/shared
 * (getNetwork, validateNetworkConfig) in the same process does not accept it.
 *
 * This is the ONLY cluster resolver in indexer src. Do not call shared's
 * getNetwork(): it is exact "mainnet" else "devnet" (no case fold, trim or
 * testnet), so NETWORK=testnet would tag trades/markets/v22_events as devnet
 * while LP-vault rows get testnet (tests/network-single-resolver.test.ts).
 */
function resolveNetwork(): NetworkType {
  const raw = process.env.NETWORK;
  const n = (raw ?? "").toLowerCase().trim();
  if (n === "" || n === "devnet") return "devnet";
  if (n === "testnet") return "testnet";
  if (n === "mainnet") return "mainnet";
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
