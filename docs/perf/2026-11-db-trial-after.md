# DB trial: Packer list open, after the speed changes

Re-measure of the Packer's list open (steps 1-3 of `start_shopify_packing_session`) after the shared sync, on the
local Supabase stack. Same command and dataset as `2026-11-db-trial-before.md`. P HEAD `579989a`
(`chore: sync shared (perf)`, pushed), which carries F `shared/` at `d58c1fd`.

Command (service key from `supabase status`, never pasted here):

    FULFILLMENT_DB_URL=http://127.0.0.1:54321 SUPABASE_SERVICE_KEY=<service_role key> \
        .venv/bin/python scripts/bench_open_list.py --rtt-ms 0

Stack: `supabase db reset` and both loads (F `docs/trial-checklist.md` "Reset"), then F's `bench_db.py` ran once on
the same stack, then this bench. Packing list: `2026-11-20_1` / `ALL_ALMADERM` of ALMA, 5 orders, 17 items. Lock
acquired and released in each run; no other rows written.

## Median of 3 runs

| action | store | wall ms | HTTP requests | est. hosted ms |
|---|---|---:|---:|---:|
| e, Packer list open | supabase | 31.3 | 6 | 31.3 |
|   e.1 UI thread (steps 1-2) | supabase | 10.1 | 2 | 10.1 |

RTT used: **0.0 ms (--rtt-ms)**, local stack only. No hosted call was made.

### Assumed hosted RTT: 40 ms (assumption, not measured)

Hosted estimate = wall ms + HTTP requests × RTT. The 40 ms RTT is an assumed value for a hosted project; it is
not a measurement.

| action | wall ms (0 ms RTT) | HTTP requests | est. hosted ms at assumed 40 ms RTT |
|---|---:|---:|---:|
| e, Packer list open | 31.3 | 6 | 271.3 |
|   e.1 UI thread (steps 1-2) | 10.1 | 2 | 90.1 |

## Delta against before (0 ms RTT)

| action | wall ms before → after | Δ wall ms | HTTP before → after | Δ HTTP | est. at assumed 40 ms, before → after |
|---|---|---:|---|---:|---|
| e, Packer list open | 32.1 → 31.3 | −0.8 (−2%) | 6 → 6 | 0 | 272.1 → 271.3 |
|   e.1 UI thread | 9.3 → 10.1 | +0.8 (+9%) | 2 → 2 | 0 | 89.3 → 90.1 |

## Raw runs

| run | e, Packer list open wall ms | requests | e.1 UI thread wall ms | requests |
|---:|---:|---:|---:|---:|
| 1 | 35.8 | 6 | 11.4 | 2 |
| 2 | 30.1 | 6 | 8.1 | 2 |
| 3 | 31.3 | 6 | 10.1 | 2 |

## Reading

- The UI-thread sub-row (e.1, the exists check and the lock) is 10.1 ms locally and 90.1 ms at the assumed 40 ms
  RTT. The threshold for condition B is 300 ms of estimated hosted time, so this is under it.
- Step 3 (the worker's reads) still makes the other 4 requests on a thread, as before.
- The local round trip is about 1 ms, so the request count is the number to watch once a hosted RTT is applied.
