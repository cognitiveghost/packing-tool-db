"""Startup storage choice: MainWindow._open_profile_manager."""

import gui.main_window as mw
from packing_tool.profile_manager import ProfileManagerError
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
