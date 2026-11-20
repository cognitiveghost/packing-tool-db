"""Benchmark the Packer's list open on the local Supabase stack (guide server data).

    FULFILLMENT_DB_URL=http://127.0.0.1:54321 SUPABASE_SERVICE_KEY=... \\
        .venv/bin/python scripts/bench_open_list.py --rtt-ms 0

Prints one Markdown table: action | store | wall ms | HTTP requests | est. hosted ms.

- Target: the local stack after the guide server load (docs/trial-checklist.md "Reset"). The script never
  resets it. Refuses any URL that is not 127.0.0.1 or localhost; there is no hosted probe, so --rtt-ms is required.
- Rows: "e, Packer list open" is steps 1-3 of the open path (the list exists, the lock, the worker's reads). The
  UI-thread sub-row is steps 1-2, the part the real app runs on the UI thread.
- Steps: the exists check (gui/main_window.py start_shopify_packing_session), SessionLockManager.acquire_lock,
  SessionStartWorker.run (PackerLogic + load_packing_list_json). The lock is released at the end, untimed.
- The store is wrapped the way the app's startup wraps it: BufferedStore over SupabaseStore.
- Helpers count_requests, require_local and est_hosted_ms are copied from the shopify-fulfillment-tool
  scripts/bench_db.py, because P cannot import F.
"""

import argparse
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import httpx

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

CLIENT = "ALMA"
LOCAL_HOSTS = {"127.0.0.1", "localhost"}


@dataclass
class Row:
    action: str
    store: str
    wall_ms: float
    requests: int
    est_ms: float


class _Count:
    n = 0


@contextmanager
def count_requests():
    """Count every httpx.Client.send while active (supabase-py's sub-clients all go through it)."""
    counter = _Count()
    real_send = httpx.Client.send

    def counting_send(self, request, *args, **kwargs):
        counter.n += 1
        return real_send(self, request, *args, **kwargs)

    httpx.Client.send = counting_send
    try:
        yield counter
    finally:
        httpx.Client.send = real_send


def require_local(url: str | None) -> str:
    """Refuse to benchmark anything but the local stack: db_config falls back to the hosted URL."""
    if (urlparse(url or "").hostname or "") not in LOCAL_HOSTS:
        raise SystemExit(
            f"refusing: FULFILLMENT_DB_URL must be 127.0.0.1 or localhost, got {url!r}"
        )
    return url


def est_hosted_ms(wall_ms: float, requests: int, rtt_ms: float) -> float:
    return wall_ms + requests * rtt_ms


def timed(fn):
    """Run fn; return (result, wall ms, HTTP requests)."""
    with count_requests() as counter:
        t0 = time.perf_counter()
        out = fn()
        wall = (time.perf_counter() - t0) * 1000
    return out, wall, counter.n


def render(rows: list[Row], rtt_note: str) -> str:
    lines = [
        "| action | store | wall ms | HTTP requests | est. hosted ms |",
        "|---|---|---:|---:|---:|",
    ]
    lines += [
        f"| {r.action} | {r.store} | {r.wall_ms:.1f} | {r.requests} | {r.est_ms:.1f} |"
        for r in rows
    ]
    return "\n".join(lines) + f"\n\n{rtt_note}\n"


def make_store(cache_dir: Path, buffer_dir: Path):
    """The app's run store: BufferedStore over SupabaseStore, opened with the service key."""
    from packing_tool.offline_buffer import BufferedStore
    from shared.store_supabase import SupabaseStore
    from supabase import create_client

    url = require_local(os.environ.get("FULFILLMENT_DB_URL"))
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not key:
        raise SystemExit("SUPABASE_SERVICE_KEY is not set")
    inner = SupabaseStore(create_client(url, key), pc="bench", cache_dir=cache_dir)
    return BufferedStore(inner, buffer_dir, pc="bench")


def pick_list(store) -> dict:
    """The first unlocked packing list of the client (setup, not timed)."""
    rows = [r for r in store.packing_overview(CLIENT) if r.get("lock") is None]
    if not rows:
        raise SystemExit(f"no unlocked packing list for {CLIENT}: load the guide server first")
    return rows[0]


def run_benchmark(store, tmp: Path, rtt_ms: float) -> list[Row]:
    from PySide6.QtWidgets import QApplication

    from gui.workers import SessionStartWorker
    from packing_tool.profile_manager import ProfileManager
    from packing_tool.session_lock_manager import SessionLockManager

    app = QApplication.instance() or QApplication([])  # noqa: F841 - the worker moves objects to its thread
    (tmp / "config.ini").write_text("", encoding="utf-8")
    pm = ProfileManager(str(tmp / "config.ini"), store=store)
    locks = SessionLockManager(pm)

    target = pick_list(store.inner)
    session_rel = f"Sessions/CLIENT_{CLIENT}/{target['session_name']}"
    session_dir = store.local_path(session_rel)
    work_dir = store.local_path(target["work_rel"])
    packing_list_path = session_dir / "packing_lists" / f"{target['list_name']}.json"
    list_rel = f"{session_rel}/packing_lists/{target['list_name']}.json"
    print(f"Packing list: {target['session_name']} / {target['list_name']}\n")

    def ui_thread():  # steps 1-2
        if not store.exists(list_rel):
            raise FileNotFoundError(f"Packing list not found: {list_rel}")
        ok, error, _lock = locks.acquire_lock(CLIENT, work_dir)
        if not ok:
            raise RuntimeError(error)

    def worker():  # step 3
        w = SessionStartWorker(CLIENT, pm, work_dir, packing_list_path)
        w.run()
        if w.error:
            raise w.error

    try:
        _, ui_wall, ui_requests = timed(ui_thread)
        _, worker_wall, worker_requests = timed(worker)
    finally:
        locks.release_lock(work_dir)

    wall, requests = ui_wall + worker_wall, ui_requests + worker_requests
    return [
        Row("e, Packer list open", "supabase", wall, requests, est_hosted_ms(wall, requests, rtt_ms)),
        Row(
            "  e.1 UI thread (steps 1-2)", "supabase", ui_wall, ui_requests,
            est_hosted_ms(ui_wall, ui_requests, rtt_ms),
        ),
    ]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Benchmark the Packer's list open.")
    parser.add_argument(
        "--rtt-ms", type=float, required=True,
        help="RTT for est. hosted ms (0 for local-only numbers; this script makes no hosted call)",
    )
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)
        store = make_store(tmp / "cache", tmp / "buffer")
        rows = run_benchmark(store, tmp, args.rtt_ms)
    print(render(rows, f"RTT used: {args.rtt_ms:.1f} ms (--rtt-ms)"))


if __name__ == "__main__":
    main()
