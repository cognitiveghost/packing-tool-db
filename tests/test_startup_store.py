"""Startup storage choice: MainWindow._open_profile_manager."""

from types import SimpleNamespace

import pytest
from offline_fakes import FlakyStore

import gui.main_window as mw
from packing_tool.offline_buffer import BufferedStore
from packing_tool.profile_manager import ProfileManagerError
from shared.db_auth import SignInCancelled
from shared.store import FileStore

LIST = "Sessions/CLIENT_M/2026-01-01_1/packing/DHL"
STATE = f"{LIST}/packing_state.json"


def _startup(monkeypatch, tmp_path, store, buf=None):
    """Open the run's storage the way MainWindow does; `self` is the few attributes it reads."""
    def no_share(_config):
        raise ProfileManagerError("FileServerPath not configured")

    monkeypatch.setattr(mw, "_use_file_store", lambda: False)
    monkeypatch.setattr(mw, "resolve_file_server_path", no_share)
    monkeypatch.setattr(mw, "_offline_buffer_dir", lambda: buf or tmp_path / "buffer")
    monkeypatch.setattr(mw, "open_store", lambda _app, **_kw: (mw.Mode.SUPABASE, store))
    config = tmp_path / "config.ini"
    config.write_text("[Network]\n", encoding="utf-8")
    window = SimpleNamespace()
    window._sync_offline_outbox = lambda s: mw.MainWindow._sync_offline_outbox(window, s)
    window._store_flipped = lambda online: None  # the window's on_change, when a flush flips the store
    pm = mw.MainWindow._open_profile_manager(window, str(config))
    return window, pm


def test_a_pc_with_no_file_share_still_opens_the_database(monkeypatch, tmp_path):
    store = FileStore(tmp_path / "server")  # stands in for the SupabaseStore open_store returns

    _window, pm = _startup(monkeypatch, tmp_path, store)

    assert pm.store.inner is store


def test_supabase_mode_wraps_the_store_once(monkeypatch, tmp_path):
    store = FileStore(tmp_path / "server")

    window, pm = _startup(monkeypatch, tmp_path, store)

    assert isinstance(pm.store, BufferedStore)
    assert pm.store.inner is store  # one wrapper, not one per list
    assert window._run_store is pm.store


def test_pending_outbox_flushed_before_start(monkeypatch, tmp_path):
    server = FlakyStore(tmp_path / "server")
    buf = tmp_path / "buffer"
    offline = BufferedStore(server, buf, pc="PC1")
    offline.begin_list(LIST, {}, [])
    server.down = True
    offline.write_json(STATE, {"n": 1})
    server.down = False

    _startup(monkeypatch, tmp_path, server, buf)

    assert server.read_json(STATE) == {"n": 1}
    assert BufferedStore.pending(buf) is None


def test_pending_outbox_permanent_error_opens_waiting_screen_flag(monkeypatch, tmp_path):
    server = FlakyStore(tmp_path / "server")
    buf = tmp_path / "buffer"
    offline = BufferedStore(server, buf, pc="PC1")
    offline.begin_list(LIST, {}, [])
    server.down = True
    offline.write_json(STATE, {"n": 1})
    server.down = False
    server.broken = "denied"

    window, _pm = _startup(monkeypatch, tmp_path, server, buf)

    assert window._startup_sync_error == "denied"
    assert BufferedStore.pending(buf) == LIST  # kept whole: nothing is dropped


def test_cancelled_sign_in_quits_with_message(monkeypatch, tmp_path):
    """A cancelled startup sign-in builds no window: the box says why, and the app exits 0."""

    def no_share(_config):
        raise ProfileManagerError("FileServerPath not configured")

    def cancelled(_app, **_kw):
        raise SignInCancelled()

    shown = []
    config = tmp_path / "config.ini"
    config.write_text("[Network]\n", encoding="utf-8")
    monkeypatch.setattr(mw, "_use_file_store", lambda: False)
    monkeypatch.setattr(mw, "resolve_file_server_path", no_share)
    monkeypatch.setattr(mw, "open_store", cancelled)
    monkeypatch.setattr(
        mw, "QMessageBox",
        SimpleNamespace(information=lambda parent, title, text: shown.append((title, text))),
    )

    with pytest.raises(SystemExit) as exit_info:
        mw.MainWindow(config_path=str(config))

    assert exit_info.value.code == 0
    assert shown == [("Not signed in", "The app needs a sign-in to start. Open it again to sign in.")]


def test_file_mode_shows_database_unreachable_banner(monkeypatch, config_ini):
    """Working from the file share after the database failed says so, and stays until restart."""
    monkeypatch.setattr(mw, "_use_file_store", lambda: False)
    monkeypatch.setattr(mw, "open_store", lambda _app, **_kw: (mw.Mode.FILE, None))

    window = mw.MainWindow(config_path=str(config_ini))
    try:
        banner = window.file_fallback_banner
        assert banner.isVisibleTo(window)
        assert banner.title_label.text() == "Database unreachable: working from file server"
        assert banner.message_label.text() == (
            "Changes save to the file server. Restart the app to try the database again."
        )
        assert banner.retry_button.isHidden()
    finally:
        window.sessions.shutdown()
        window.deleteLater()
