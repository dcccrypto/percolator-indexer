# Indexer heap soak: per-service report (harvest of the 20k-cycle run)

Base: `feat/v22-indexer`-lineage code at `d4bf5cd` (scratch copy). Harness: `~/agent-worker/reports/idx-heap-soak/soak.mts` (unchanged, fake DB + fake RPC, one node process per service, `--expose-gc`, heap measured after 2x gc). Local only, no source changes in the indexer.

## Coverage: what actually finished

The first job hit its time limit and its soak processes did not survive it. At harvest time none were running. Last sampled cycle per service:

| service | last cycle | reached 20k? |
|---|---|---|
| control (harness only) | 20000 | yes |
| webhook | 18000 | no |
| trade | 11000 | no |
| stats | 10000 | no |
| lp | 6000 | no |

A one-at-a-time rerun was started (`out-rerun/`). Alone, lp ran about 1 min per 1k cycles for the first 2k and slows as slabs grow (the contended run took 17 min for 6k), so four sequential 20k runs do not fit this job's budget. The rerun was stopped at lp cycle ~2000; its partial output in `out-rerun/` is not used here. Rows past the last sampled cycle are `-`. Nothing here is extrapolated. Finishing trade, stats, lp and webhook needs a dedicated job.

The slab count in the harness grows 10 -> 2000 over the first 15k cycles, then stays flat. Part of every heap rise is therefore legitimate per-market state, not a leak. "heapUsed minus control" subtracts the harness's own heap (fake DB, chain history). Only webhook reached the flat-slab window (15k+), and its slope there is the only direct leak signal.

## Tables (heapUsed after 2x gc; event-loop p99 over the sample window)


### TradeIndexer (TradeIndexerPolling)

Samples: 12 (last cycle 11000); wall 25.3 min; service time 24.2 min.

| metric | start | 5k | 10k | slope 15k-20k per 1k cycles |
|---|---|---|---|---|
| slabs | 10 | 673 | 1337 |  |
| heapUsed MB (after 2x gc) | 108.7 | 126.2 | 138.0 |  |
| heapUsed minus control MB (service share) | -0.0 | 11.4 | 17.1 |  |
| rss MB | 135.3 | 221.6 | 202.7 |  |
| event-loop delay p99 ms (window) | 0.0 | 150.1 | 270.8 |  |
| event-loop delay max ms (window) | 0.0 | 363.6 | 727.2 |  |
| `TradeIndexer.lastSignature` (reflect) | 0 | 478 | 1216 |  |
| `TradeIndexer.writeHolds` (reflect) | 0 | 0 | 0 |  |
| `TradeIndexer.heldSigs` (reflect) | 0 | 8 | 20 |  |
| `TradeIndexer.holdAnchor` (reflect) | 0 | 8 | 20 |  |
| `TradeIndexer.ctxReadAttempts.m` (reflect) | 0 | 8 | 25 |  |
| `TradeIndexer.lotReadAttempts.m` (reflect) | 0 | 0 | 0 |  |
| `TradeIndexer.txCache.m` (reflect) | 0 | 1000 | 1000 |  |
| `Map src/lib/boundedTtlMap.ts:7 (BoundedTtlMap)` (live: total entries) | 0 in 6 | 7756 in 6 | 18245 in 6 |  |
| `Map src/parsers/percolatorTxParser.ts:573` (live: total entries) | 0 in 1 | 560 in 1 | 1310 in 1 |  |
| `Map src/services/TradeIndexer.ts:205` (live: total entries) | 0 in 1 | 478 in 1 | 1216 in 1 |  |
| `Map src/services/TradeIndexer.ts:224` (live: total entries) | 0 in 1 | 8 in 1 | 20 in 1 |  |
| `Map src/services/TradeIndexer.ts:230` (live: total entries) | 0 in 1 | 8 in 1 | 20 in 1 |  |
| `Set src/services/TradeIndexer.ts:561` (live: total entries) | 0 | 8 in 8 | 20 in 20 |  |

Harness (not the service): history addrs 1535, sig index 48862, fake DB rows {"markets":1395,"trade_indexer_cursor":1373,"trades":11246}.
RPC calls: {"getSignaturesForAddress":235401,"getParsedTransactions":16526,"getAccountInfo":13128}
Errors thrown out of the service: {}
Top warn/error logs: 498x log.warn: indexer:trade-indexer: matcher context read failed; holding the cursor, this signature is ret

### StatsCollector

Samples: 11 (last cycle 10000); wall 21.5 min; service time 19.7 min.

| metric | start | 5k | 10k | slope 15k-20k per 1k cycles |
|---|---|---|---|---|
| slabs | 10 | 673 | 1337 |  |
| heapUsed MB (after 2x gc) | 108.7 | 117.3 | 127.3 |  |
| heapUsed minus control MB (service share) | -0.0 | 2.6 | 6.3 |  |
| rss MB | 116.3 | 218.5 | 127.7 |  |
| event-loop delay p99 ms (window) | 0.0 | 497.0 | 768.6 |  |
| event-loop delay max ms (window) | 0.0 | 1379.9 | 2313.2 |  |
| `StatsCollector.creationCache` (reflect) | 0 | 0 | 0 |  |
| `StatsCollector.closedSlabs` (reflect) | 0 | 0 | 0 |  |
| `Map src/layout/marketVersions.ts:24` (live: total entries) | 0 in 1 | 673 in 1 | 1337 in 1 |  |
| `Map src/layout/marketVersions.ts:41` (live: total entries) | 0 | 9422 in 673 | 18718 in 1337 |  |
| `Map src/lib/boundedTtlMap.ts:7 (BoundedTtlMap)` (live: total entries) | 0 in 6 | 9422 in 6 | 18718 in 6 |  |

Harness (not the service): history addrs 1365, sig index 44108, fake DB rows {"markets":1337}.
RPC calls: {"getSignaturesForAddress":67,"getTransaction":13,"getParsedAccountInfo":67,"getMultipleAccountsInfo":13734}
Errors thrown out of the service: {}
Top warn/error logs: 67x log.warn: indexer:stats-collector: Invalid initialMarginBps — registering market with default max_leverag

### LpVaultIndexer

Samples: 7 (last cycle 6000); wall 17.5 min; service time 16.8 min.

| metric | start | 5k | slope 15k-20k per 1k cycles |
|---|---|---|---|
| slabs | 10 | 673 |  |
| heapUsed MB (after 2x gc) | 108.7 | 123.8 |  |
| heapUsed minus control MB (service share) | -0.0 | 9.1 |  |
| rss MB | 111.2 | 170.3 |  |
| event-loop delay p99 ms (window) | 0.0 | 139.1 |  |
| event-loop delay max ms (window) | 0.0 | 368.1 |  |
| `LpVaultIndexer.registries` (reflect) | 0 | 68 |  |
| `LpVaultIndexer.cursors` (reflect) | 0 | 67 |  |
| `LpVaultIndexer.cursorLoaded` (reflect) | 0 | 68 |  |
| `LpVaultIndexer.wrapperIds` (reflect) | 1 | 1 |  |
| `LpVaultIndexer.opts.programIds` (reflect) | 1 | 1 |  |
| `Map src/services/LpVaultIndexer.ts:79` (live: total entries) | 0 in 1 | 68 in 1 |  |
| `Map src/services/LpVaultIndexer.ts:80` (live: total entries) | 0 in 1 | 67 in 1 |  |
| `Set src/services/LpVaultIndexer.ts:82` (live: total entries) | 0 in 1 | 68 in 1 |  |

Harness (not the service): history addrs 724, sig index 26104, fake DB rows {"markets":766,"lp_vault_sync_cursor":81,"lp_vault_events":6000,"lp_vault_positions":81}.
RPC calls: {"lp.listRegistries":6000,"lp.getSignatures":254016,"lp.getTransaction":6000,"lp.getClaim":6469}
Errors thrown out of the service: {}
Top warn/error logs: none

### webhook handler (webhookRoutes + rate limiter)

Samples: 19 (last cycle 18000); wall 23.7 min; service time 20.0 min.

| metric | start | 5k | 10k | 15k | slope 15k-20k per 1k cycles |
|---|---|---|---|---|---|
| slabs | 10 | 673 | 1337 | 2000 |  |
| heapUsed MB (after 2x gc) | 108.7 | 124.5 | 136.3 | 147.0 | 1.33 MB |
| heapUsed minus control MB (service share) | -0.0 | 9.8 | 15.3 | 18.9 | 0.51 MB |
| rss MB | 156.8 | 226.6 | 203.9 | 152.9 |  |
| event-loop delay p99 ms (window) | 0.0 | 393.0 | 115.9 | 413.7 |  |
| event-loop delay max ms (window) | 0.0 | 673.2 | 380.4 | 1687.2 |  |
| `webhookRateLimiter.buckets` (reflect) | 0 | 1155 | 1449 | 457 | 30.9 |
| `Map src/lib/boundedTtlMap.ts:7 (BoundedTtlMap)` (live: total entries) | 0 in 6 | 9124 in 6 | 9980 in 6 | 14534 in 6 | 2680.7 |
| `Map src/lib/webhookRateLimiter.ts:70` (live: total entries) | 0 in 1 | 1155 in 1 | 1449 in 1 | 457 in 1 | 30.9 |

Harness (not the service): history addrs 4200, sig index 123956, fake DB rows {"markets":1900,"trades":10802,"v22_events":1519}.
RPC calls: {"getAccountInfo":13099}
Webhook HTTP status: {"200":17437,"500":563}
Errors thrown out of the service: {}
Top warn/error logs: 567x log.warn: indexer:webhook: matcher context read failed; answering 500 so the delivery is retried; 563x log.error: indexer:webhook: Webhook processing error — returning 500 for Helius retry

### control (harness only, no service driven)

Samples: 21 (last cycle 20000); wall 2.0 min; service time 0.0 min.

| metric | start | 5k | 10k | 15k | 20k | slope 15k-20k per 1k cycles |
|---|---|---|---|---|---|---|
| slabs | 10 | 673 | 1337 | 2000 | 2000 |  |
| heapUsed MB (after 2x gc) | 108.7 | 114.8 | 121.0 | 128.1 | 132.1 | 0.80 MB |
| rss MB | 145.6 | 160.0 | 161.8 | 163.1 | 95.5 |  |
| event-loop delay p99 ms (window) | 0.0 | 239.2 | 229.4 | 261.2 | 227.7 |  |
| event-loop delay max ms (window) | 0.0 | 258.7 | 280.2 | 574.6 | 283.9 |  |

Harness (not the service): history addrs 2200, sig index 88799, fake DB rows {"markets":1900}.
RPC calls: {}
Errors thrown out of the service: {}
Top warn/error logs: none

## Structures that grow, with file:line

Method: per-structure sizes from reflection plus every Map/Set constructed under `src/` (live count and total entries), sampled at each checkpoint. Line numbers are at `d4bf5cd`.

**Growth with no eviction, bounded only by the number of markets (slabs).** These stay flat once the market count is flat, which the harness only reached in the webhook run. In the trade, stats and lp runs they were still rising with slabs, so "flat after slabs stop growing" is not demonstrated for them:
- `src/services/TradeIndexer.ts:205` `lastSignature`: one entry per slab, no `delete`. 1216 at 10k with 1337 slabs.
- `src/services/LpVaultIndexer.ts:79` `registries`, `:80` `cursors`, `:82` `cursorLoaded`: one entry per registry, no `delete`. 68/67/68 at 5k.

**Bounded, confirmed:**
- `src/layout/marketVersions.ts:24` `notes`: LRU capped at `MAX_MARKET_NOTES` (evicts at lines 54-57). `:41` `generations` is per-note and lives and dies with it.
- `src/lib/webhookRateLimiter.ts:70` `buckets`: pruned at line 91. Entries fell from 1449 at 10k to 457 at 15k.
- `src/parsers/percolatorTxParser.ts:573` `base58Cache`: cleared at `BASE58_CACHE_MAX` (line 582).
- `src/services/TradeIndexer.ts:224` `heldSigs`, `:230` `holdAnchor`, `:561` the Set built there: deleted on resolve (lines 218, 551, 598). 20 each at 10k with 498 retried signatures, so they track the in-flight holds. `ctxReadAttempts` is deleted at line 1173.
- `src/lib/boundedTtlMap.ts:7` `BoundedTtlMap` (6 live instances, e.g. `txCache` held at exactly 1000): capped by construction. Its cumulative "total" column keeps rising, but that counts inserts, not live entries.
- `StatsCollector.creationCache` and `closedSlabs` stayed at 0 here. `closedSlabs` is an LRU with a cap (`StatsCollector.ts:389`).

**Unbounded structures found: none that grow with time at a fixed market count.** The two per-market maps above are unbounded in the number of markets, which is expected, but they have no eviction.

## Caveats
- Trade, stats and lp never reached the flat-slab window, so their 15k-20k slope is not measured. Their heap rise (trade +17.1 MB, stats +6.3 MB over control at 10k; lp +9.1 MB at 5k) is consistent with the slab growth but is not proven to flatten.
- Webhook slope 15k-18k: +0.51 MB per 1k cycles net of control (1.33 MB raw, control itself drifts 0.80 MB per 1k). That is small but non-zero over only 3 samples, and the 18k sample is the last one.
- Event-loop p99 is dominated by the harness (control alone shows 230-440 ms), so it is not a service signal.
- Harness-only artefact: the first run had a rope-string bug, fixed before this run (`out-run1-ropebug/` is kept but not used).
