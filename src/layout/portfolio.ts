/**
 * Portfolio and position decoding, selected by wrapper VERSION.
 *
 * - VERSION 18 (v2.1): 9,563 B account, 152 B legs.
 * - VERSION 19 (v2.2 variant B): 10,603 B account, 217 B legs (K/F remainders mid-leg, band/rent tail).
 * - Legacy (v1, `PERCOLAT` magic) accounts are not wrapper-v17 accounts at all: {@link decodePortfolio}
 *   returns `null` for them and the pre-existing v1 path is untouched.
 *
 * All offsets come from the SDK layout table through `parsePortfolioV17`, which runs the VERSION and
 * engine-discriminator guard first. A VERSION this build does not know raises `UnknownLayoutError`.
 */
import type { Connection, PublicKey } from "@solana/web3.js";
import {
  ACCOUNT_KIND,
  LAYOUT_V21,
  LAYOUT_V22,
  parsePortfolioV17,
  portfolioFilterForLayout,
  resolvePortfolioLayout,
  type LayoutTable,
  type PortfolioLegV17,
  type PortfolioV17,
} from "@percolatorct/sdk";
import { hasWrapperMagic } from "./resolve.js";

export interface DecodedPosition {
  assetIndex: number;
  marketId: bigint;
  side: "long" | "short";
  basisPosQ: bigint;
  /** v2.2 only. */
  bandLiqPending?: boolean;
  rentSnap?: bigint;
}

export interface DecodedPortfolio {
  /** Wrapper VERSION of the account (18 or 19). */
  version: number;
  layoutName: string;
  accountLen: number;
  legStride: number;
  owner: string;
  marketGroupId: string;
  capital: bigint;
  pnl: bigint;
  /** Active legs only. */
  positions: DecodedPosition[];
  raw: PortfolioV17;
}

function toPosition(l: PortfolioLegV17): DecodedPosition {
  const p: DecodedPosition = { assetIndex: l.assetIndex, marketId: l.marketId, side: l.side === 0 ? "long" : "short", basisPosQ: l.basisPosQ };
  if (l.bandLiqPending !== undefined) p.bandLiqPending = l.bandLiqPending;
  if (l.rentSnap !== undefined) p.rentSnap = l.rentSnap;
  return p;
}

/**
 * Decode a portfolio account.
 *
 * @returns `null` when the buffer is not a wrapper account of kind PORTFOLIO (market, ledger, legacy v1 ...).
 * @throws UnknownLayoutError for an unknown VERSION, a discriminator/provenance mismatch, or a length that is
 *   not the VERSION's exact PORTFOLIO_ACCOUNT_LEN (a portfolio of any other length is not a real account).
 */
export function decodePortfolio(data: Uint8Array): DecodedPortfolio | null {
  if (!hasWrapperMagic(data) || data[10] !== ACCOUNT_KIND.Portfolio) return null;
  const layout = resolvePortfolioLayout(data, { parser: "decodePortfolio", strictLength: true });
  const raw = parsePortfolioV17(data);
  return {
    version: layout.version,
    layoutName: layout.name,
    accountLen: data.length,
    legStride: layout.portfolio.legStride,
    owner: raw.owner.toBase58(),
    marketGroupId: raw.marketGroupId.toBase58(),
    capital: raw.capital,
    pnl: raw.pnl,
    positions: raw.legs.filter((l) => l.active).map(toPosition),
    raw,
  };
}

/** The layouts this build can scan for, newest first. */
export const PORTFOLIO_SCAN_LAYOUTS: readonly LayoutTable[] = [LAYOUT_V22, LAYOUT_V21];

/**
 * Fetch and decode every portfolio of one VERSION with a size filter AND a VERSION memcmp (so a length collision
 * between layouts cannot match). Accounts that fail to decode are returned in `skipped`, never thrown.
 */
export async function fetchPortfolios(
  connection: Connection,
  programId: PublicKey,
  layout: LayoutTable,
): Promise<{ portfolios: Array<{ pubkey: string; portfolio: DecodedPortfolio }>; skipped: Array<{ pubkey: string; error: string }> }> {
  const f = portfolioFilterForLayout(layout);
  const accounts = await connection.getProgramAccounts(programId, {
    filters: [
      { dataSize: f.dataSize },
      { memcmp: { offset: f.versionMemcmp.offset, bytes: f.versionMemcmp.bytes } },
      { memcmp: { offset: 10, bytes: "3" } }, // KIND_PORTFOLIO = 2 -> base58 "3"
    ],
  });
  const portfolios: Array<{ pubkey: string; portfolio: DecodedPortfolio }> = [];
  const skipped: Array<{ pubkey: string; error: string }> = [];
  for (const { pubkey, account } of accounts) {
    try {
      const p = decodePortfolio(new Uint8Array(account.data));
      if (p) portfolios.push({ pubkey: pubkey.toBase58(), portfolio: p });
    } catch (e) {
      skipped.push({ pubkey: pubkey.toBase58(), error: e instanceof Error ? e.message : String(e) });
    }
  }
  return { portfolios, skipped };
}
