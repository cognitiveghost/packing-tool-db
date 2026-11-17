"""Startup storage choice: MainWindow._open_profile_manager."""

from types import SimpleNamespace

import pytest

import gui.main_window as mw
from packing_tool.profile_manager import ProfileManagerError
from shared.db_auth import SignInCancelled
from shared.store import FileStore


def test_a_pc_with_no_file_share_still_opens_the_database(monkeypatch, tmp_path):
    def no_share(_config):
        raise ProfileManagerError("FileServerPath not configured")

    seen = {}
    store = FileStore(tmp_path)  # stands in for the SupabaseStore open_store returns

    def fake_open_store(_app, *, fallback_root, **_kw):
        seen["fallback_root"] = fallback_root
        return mw.Mode.SUPABASE, store

    monkeypatch.setattr(mw, "_use_file_store", lambda: False)
    monkeypatch.setattr(mw, "resolve_file_server_path", no_share)
    monkeypatch.setattr(mw, "open_store", fake_open_store)
    config = tmp_path / "config.ini"
    config.write_text("[Network]\n", encoding="utf-8")

    pm = mw.MainWindow._open_profile_manager(None, str(config))

    assert seen["fallback_root"] == ""
    assert pm.store is store


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
