"""Restart offline: the buffered list reopens with its progress, or the waiting-to-sync screen (spec §8.3)."""

import json

import pytest
from offline_fakes import FlakyStore

import gui.main_window as mw
from packing_tool.offline_buffer import OUTBOX, BufferedStore, UnreachableStore
from packing_tool.packer_logic import PackerLogic
from packing_tool.profile_manager import ProfileManager
from packing_tool.session_lock_manager import SessionLockManager
from shared.store import FileStore

LIST = "Sessions/CLIENT_M/2026-01-01_1/packing/DHL"
PL = "Sessions/CLIENT_M/2026-01-01_1/packing_lists/DHL.json"
STATE = f"{LIST}/packing_state.json"
META = {
    "client_id": "M", "session_name": "2026-01-01_1", "list_name": "DHL", "work_dir_rel": LIST,
    "worker_id": "w1", "worker_name": "Ann", "pc": "PC1", "user": "ann",
}
# One order of four units of one SKU: three packed online and offline leaves it open.
ORDERS = [("ORDER-001", "DHL", [{"sku": "SKU-AAA", "quantity": 4, "product_name": "Widget A"}])]


@pytest.fixture
def cache(tmp_path, monkeypatch):
    """The database cache the offline run's local tree is under (never the real home)."""
    path = tmp_path / "db_cache"
    monkeypatch.setattr(mw, "_db_cache_dir", lambda: path)
    return path


@pytest.fixture
def buf(tmp_path, monkeypatch):
    path = tmp_path / "buffer"
    monkeypatch.setattr(mw, "_offline_buffer_dir", lambda: path)
    return path


@pytest.fixture
def start_offline(monkeypatch, config_ini, cache, buf):
    """Open MainWindow the way a restart with the database down does: no sign-in, no lock."""
    monkeypatch.setattr(mw, "_use_file_store", lambda: False)
    monkeypatch.setattr(mw, "open_store", lambda _app, **_kw: (mw.Mode.OFFLINE_BUFFER, None))
    windows = []

    def _start():
        window = mw.MainWindow(config_path=str(config_ini))
        windows.append(window)
        return window

    yield _start
    for window in windows:
        window.sessions.shutdown()
        window.deleteLater()


def _seed_list(server_root, config_ini, session_factory, buf, *, packed_online, packed_offline,
                finish=False):
    """A list packed through a BufferedStore: some online, then the database goes away."""
    session_factory(client_id="M", session_id="2026-01-01_1", list_name="DHL", orders=ORDERS)
    server = FlakyStore(server_root)
    store = BufferedStore(server, buf, pc="PC1")
    store.begin_list(LIST, META, [PL])
    logic = PackerLogic(client_id="M", profile_manager=ProfileManager(str(config_ini), store=store),
                        work_dir=str(server_root / LIST))
    try:
        logic.load_packing_list_json(server_root / PL)
        logic.start_order_packing("ORDER-001")
        for sku in ["SKU-AAA"] * packed_online:
            logic.process_sku_scan(sku)
        server.down = True  # the same run carries on packing offline
        for sku in ["SKU-AAA"] * packed_offline:
            logic.process_sku_scan(sku)
    finally:
        logic.close()
    if finish:
        store.release_list_lock(LIST, "PC1")
    return server


def test_restart_offline_reopens_list_with_progress(
    server_root, config_ini, session_factory, buf, start_offline, monkeypatch
):
    _seed_list(server_root, config_ini, session_factory, buf, packed_online=2, packed_offline=1)
    lock_calls = []
    monkeypatch.setattr(SessionLockManager, "acquire_lock", lambda *a, **k: lock_calls.append(a))

    window = start_offline()

    assert lock_calls == []  # the list stays ours through the buffer, not a new lock
    assert isinstance(window.profile_manager.store.inner, UnreachableStore)
    assert window.logic is not None
    in_progress = window.logic.session_packing_state["in_progress"]["ORDER-001"]
    assert sum(item["packed"] for item in in_progress) == 3


def test_restart_offline_with_finished_list_shows_waiting_screen(
    server_root, config_ini, session_factory, buf, start_offline
):
    _seed_list(server_root, config_ini, session_factory, buf,
               packed_online=1, packed_offline=0, finish=True)

    window = start_offline()

    assert window.logic is None
    assert window.waiting_list == ("DHL", None)


def test_old_outbox_without_list_json_shows_waiting_screen(
    server_root, config_ini, session_factory, buf, start_offline
):
    session_factory(client_id="M", session_id="2026-01-01_1", list_name="DHL", orders=ORDERS)
    buf.mkdir(parents=True)
    line = {"op": "write_json", "rel": STATE, "list_dir": LIST, "data": {"n": 1}}
    (buf / OUTBOX).write_text(json.dumps(line) + "\n", encoding="utf-8")  # written before B2

    window = start_offline()

    assert window.logic is None
    assert window.waiting_list == ("DHL", None)


def test_offline_restart_never_opens_file_mode(
    server_root, config_ini, session_factory, buf, start_offline
):
    _seed_list(server_root, config_ini, session_factory, buf, packed_online=1, packed_offline=1)
    assert (server_root / LIST).exists()  # the share is reachable: the trap is open

    window = start_offline()

    store = window.profile_manager.store
    assert isinstance(store, BufferedStore)
    assert not isinstance(store.inner, FileStore)
