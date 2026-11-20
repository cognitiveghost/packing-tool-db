# DB trial: Packer list open, before any change

Baseline for the Packer's list open (steps 1-3 of `start_shopify_packing_session`), measured on the local
Supabase stack before any code change. Local only: `--rtt-ms 0`, so est. hosted ms equals wall ms. No hosted
call was made.

Command (service key from `supabase status`, never pasted here):

    FULFILLMENT_DB_URL=http://127.0.0.1:54321 SUPABASE_SERVICE_KEY=<service_role key> \
        .venv/bin/python scripts/bench_open_list.py --rtt-ms 0

Stack: guide server load (docs/trial-checklist.md "Reset"). Packing list: `ALL_ALMADERM` of ALMA, 5 orders,
17 items. Lock acquired and released in each run; no other rows written.

## Median of 3 runs

| action | store | wall ms | HTTP requests | est. hosted ms |
|---|---|---:|---:|---:|
| e, Packer list open | supabase | 32.1 | 6 | 32.1 |
|   e.1 UI thread (steps 1-2) | supabase | 9.3 | 2 | 9.3 |

RTT used: 0.0 ms (--rtt-ms)

## Raw runs

| run | e, Packer list open wall ms | requests | e.1 UI thread wall ms | requests |
|---:|---:|---:|---:|---:|
| 1 | 46.9 | 6 | 18.5 | 2 |
| 2 | 31.6 | 6 | 7.9 | 2 |
| 3 | 32.1 | 6 | 9.3 | 2 |

Run 1 is the slowest; it is the first run against the stack after the load. Request counts did not change between
runs.

## Reading

- Steps 1-2 (the exists check and the lock) are the UI-thread part of the open: 2 requests, about 8-19 ms locally.
- Step 3 (the worker's reads: PackerLogic and `load_packing_list_json`) runs on a thread and makes the other 4 requests.
- The row is a local number: round trips here take about 1 ms, so the request count is the number to watch
  once a hosted RTT is applied.
