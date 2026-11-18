"""The Packer's offline UI: the Offline banner, End session offline, the waiting-to-sync screen,
and the sync driver (spec §8.4-8.5)."""

import threading

import pytest
from offline_fakes import FlakyStore
from PySide6.QtWidgets import QLabel

import gui.main_window as mw
from gui.app_pages import PAGE_BROWSER
from packing_tool.offline_buffer import BufferedStore
from packing_tool.packer_logic import PackerLogic
from packing_tool.profile_manager import ProfileManager
from shared import db_auth
from shared.db_auth import SignInCancelled
from shared.store_mode import Mode

LIST = "Sessions/CLIENT_M/2026-01-01_1/packing/DHL"
PL = "Sessions/CLIENT_M/2026-01-01_1/packing_lists/DHL.json"
STATE = f"{LIST}/packing_state.json"
META = {
    "client_id": "M", "session_name": "2026-01-01_1", "list_name": "DHL", "work_dir_rel": LIST,
    "worker_id": "w1", "worker_name": "Ann", "pc": "PC1", "user": "ann",
}
ORDERS = [("ORDER-001", "DHL", [{"sku": "SKU-AAA", "quantity": 4, "product_name": "Widget A"}])]


@pytest.fixture
def cache(tmp_path, monkeypatch):
    path = tmp_path / "db_cache"
    monkeypatch.setattr(mw, "_db_cache_dir", lambda: path)
    return path


@pytest.fixture
def buf(tmp_path, monkeypatch):
    path = tmp_path / "buffer"
    monkeypatch.setattr(mw, "_offline_buffer_dir", lambda: path)
    return path


@pytest.fixture
def start(monkeypatch, config_ini, cache, buf):
    """Open MainWindow with the database in `mode` over `store` (None: no backend, as offline)."""
    monkeypatch.setattr(mw, "_use_file_store", lambda: False)
    windows = []

    def _start(mode=Mode.OFFLINE_BUFFER, store=None):
        monkeypatch.setattr(mw, "open_store", lambda _app, **_kw: (mode, store))
        window = mw.MainWindow(config_path=str(config_ini))
        windows.append(window)
        return window

    yield _start
    for window in windows:
        window.sessions.shutdown()
        window.deleteLater()


def _seed_list(server_root, config_ini, session_factory, buf, *, packed_offline=1, finish=False):
    """A list packed online once, then offline: its progress waits in the outbox."""
    session_factory(client_id="M", session_id="2026-01-01_1", list_name="DHL", orders=ORDERS)
    server = FlakyStore(server_root)
    store = BufferedStore(server, buf, pc="PC1")
    store.begin_list(LIST, META, [PL])
    logic = PackerLogic(client_id="M", profile_manager=ProfileManager(str(config_ini), store=store),
                        work_dir=str(server_root / LIST))
    try:
        logic.load_packing_list_json(server_root / PL)
        logic.start_order_packing("ORDER-001")
        logic.process_sku_scan("SKU-AAA")
        server.down = True
        for _ in range(packed_offline):
            logic.process_sku_scan("SKU-AAA")
    finally:
        logic.close()
    if finish:
        store.release_list_lock(LIST, "PC1")
    return server


def _labels(widget):
    return [label.text() for label in widget.findChildren(QLabel)]


def _reconnect(monkeypatch, server):
    """The database is back: a sign-in reaches it, and it answers."""
    server.down = False
    monkeypatch.setattr(db_auth, "connect", lambda app_name, prompt, factory=None: object())
    monkeypatch.setattr("shared.store_supabase.SupabaseStore", lambda client, **_kw: server)


def _sync(window, qapp):
    window._sync_tick()
    window._sync_thread.join()
    qapp.processEvents()


def test_offline_banner_shows_and_clears_with_synced_toast(
    qapp, start, server_root, config_ini, session_factory, buf, monkeypatch
):
    session_factory(client_id="M", session_id="2026-01-01_1", list_name="DHL", orders=ORDERS)
    server = FlakyStore(server_root)
    window = start(Mode.SUPABASE, server)
    toasts = []
    monkeypatch.setattr(window, "_toast", lambda message, role="success": toasts.append(message))
    store = window._run_store
    store.begin_list(LIST, META, [PL])

    server.down = True
    store.write_json(STATE, {"n": 1})  # the database goes away mid-write: queued, offline

    assert not store.online
    assert not window.offline_banner.isHidden()
    assert window.offline_banner.title_label.text() == "Offline"
    assert window.offline_banner.message_label.text() == (
        "Progress is saved on this PC and syncs when the database is back."
    )

    _reconnect(monkeypatch, server)
    _sync(window, qapp)

    assert store.online
    assert window.offline_banner.isHidden()
    assert toasts == ["Synced"]


def test_end_session_enabled_while_offline(qapp, start, server_root, config_ini, session_factory,
                                           buf, monkeypatch):
    _seed_list(server_root, config_ini, session_factory, buf, packed_offline=1)
    window = start()
    assert window.logic is not None  # the list is open, offline
    monkeypatch.setattr(mw, "test_path_reachable", lambda *_args: False)  # the probe would say "down"

    window.check_connection()
    qapp.processEvents()

    assert window.command_bar.end_session_button.isEnabled()


def test_end_session_offline_shows_waiting_screen(qapp, start, server_root, config_ini,
                                                  session_factory, buf):
    _seed_list(server_root, config_ini, session_factory, buf, packed_offline=1)
    window = start()
    window._run_store.release_list_lock(LIST, "PC1")  # the end of the list, queued offline

    window._leave_packer_mode()

    assert window.stacked_widget.currentWidget() is window.waiting_page
    labels = _labels(window.waiting_page)
    assert "Waiting to sync" in labels
    assert any("List DHL is finished and waiting to sync." in text for text in labels)


def test_waiting_screen_shows_permanent_error(qapp, start, server_root, config_ini,
                                              session_factory, buf):
    _seed_list(server_root, config_ini, session_factory, buf, packed_offline=0, finish=True)
    window = start()

    window._show_waiting_to_sync("DHL", "denied")

    assert "Cannot sync offline progress: denied. Nothing is lost; tell the admin." in _labels(
        window.waiting_page
    )


def test_sync_now_flushes_and_returns_to_sessions(qapp, start, server_root, config_ini,
                                                  session_factory, buf, monkeypatch):
    server = _seed_list(server_root, config_ini, session_factory, buf, packed_offline=1, finish=True)
    window = start()
    assert window.stacked_widget.currentWidget() is window.waiting_page
    _reconnect(monkeypatch, server)

    window.waiting_page.syncRequested.emit()  # the Sync now button
    window._sync_thread.join()
    qapp.processEvents()

    assert BufferedStore.pending(buf) is None
    assert window.stacked_widget.currentWidget() is window.session_widget
    assert window.session_tabs.currentIndex() == PAGE_BROWSER


def test_sync_tick_with_refused_token_asks_on_ui_thread(
    qapp, start, server_root, config_ini, session_factory, buf, monkeypatch
):
    _seed_list(server_root, config_ini, session_factory, buf, packed_offline=1)
    window = start()
    assert isinstance(window._run_store.inner, mw.UnreachableStore)
    asked, connect_threads = [], []

    def fake_ask(parent, error=None):
        asked.append(threading.current_thread())
        # the user cancels

    def fake_connect(app_name, prompt, factory=None):
        connect_threads.append(threading.current_thread())
        prompt("Your saved sign-in has expired. Sign in again.")  # a refused saved token asks
        raise SignInCancelled()

    monkeypatch.setattr(mw.SignInDialog, "ask", staticmethod(fake_ask))
    monkeypatch.setattr(db_auth, "connect", fake_connect)

    _sync(window, qapp)  # the tick on its worker thread, then the UI thread's answer

    assert connect_threads[0] is not threading.main_thread()  # the worker never prompts
    assert asked == [threading.main_thread()]  # the dialog opens on the UI thread only


def test_sync_timer_runs_while_outbox_pending_and_stops_after(
    qapp, start, server_root, config_ini, session_factory, buf, monkeypatch
):
    server = _seed_list(server_root, config_ini, session_factory, buf, packed_offline=2)
    window = start()
    assert window._sync_timer.isActive()
    assert window._sync_timer.interval() == 60000

    _reconnect(monkeypatch, server)
    _sync(window, qapp)

    assert BufferedStore.pending(buf) is None
    assert not window._sync_timer.isActive()
