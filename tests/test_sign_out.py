"""Sign out is in the overflow only in database mode. Confirming forgets the sign-in and closes; cancelling does nothing."""

import pytest
from offline_fakes import FlakyStore

import gui.main_window as mw
from shared import db_auth
from shared.components.confirm_dialog import ConfirmDialog
from shared.store_mode import Mode


@pytest.fixture
def window_in_mode(monkeypatch, config_ini, tmp_path, server_root):
    """A real MainWindow whose database mode is forced, so the overflow is built for that mode."""
    monkeypatch.setattr(mw, "_use_file_store", lambda: False)
    monkeypatch.setattr(mw, "_db_cache_dir", lambda: tmp_path / "db_cache")
    monkeypatch.setattr(mw, "_offline_buffer_dir", lambda: tmp_path / "buffer")
    windows = []

    def factory(mode):
        store = FlakyStore(server_root) if mode is Mode.SUPABASE else None
        monkeypatch.setattr(mw, "open_store", lambda _app, **_kw: (mode, store))
        window = mw.MainWindow(config_path=str(config_ini))
        windows.append(window)
        return window

    yield factory
    for window in windows:
        window.sessions.shutdown()
        window.deleteLater()


def _action_texts(win):
    return [action.text() for action in win.command_bar.overflow.actions()]


def _sign_out_action(win):
    return next(a for a in win.command_bar.overflow.actions() if a.text() == "Sign out")


def test_sign_out_item_in_db_mode(window_in_mode):
    assert "Sign out" in _action_texts(window_in_mode(Mode.SUPABASE))


def test_sign_out_item_absent_outside_db_mode(window_in_mode):
    assert "Sign out" not in _action_texts(window_in_mode(Mode.FILE))


def test_sign_out_confirm_forgets_and_closes(window_in_mode, monkeypatch):
    win = window_in_mode(Mode.SUPABASE)
    forgotten, closed = [], []
    monkeypatch.setattr(ConfirmDialog, "exec", lambda self: 1)
    monkeypatch.setattr(db_auth, "forget", lambda app_name: forgotten.append(app_name))
    monkeypatch.setattr(win, "close", lambda: closed.append(True))

    _sign_out_action(win).trigger()

    assert forgotten == ["PackingTool-DB"]
    assert closed == [True]


def test_sign_out_cancel_does_nothing(window_in_mode, monkeypatch):
    win = window_in_mode(Mode.SUPABASE)
    forgotten, closed = [], []
    monkeypatch.setattr(ConfirmDialog, "exec", lambda self: 0)
    monkeypatch.setattr(db_auth, "forget", lambda app_name: forgotten.append(app_name))
    monkeypatch.setattr(win, "close", lambda: closed.append(True))

    _sign_out_action(win).trigger()

    assert forgotten == []
    assert closed == []
