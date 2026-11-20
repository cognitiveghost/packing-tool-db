"""SessionLockManager on a real SupabaseStore: the list lock is a table row, read through store.lock_info()."""
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import make_user_client

import packing_tool.session_lock_manager as lock_module
from gui.sessions_page import SessionsPage
from packing_tool.profile_manager import ProfileManager
from packing_tool.session_lock_manager import SessionLockManager
from shared.store import StoreError
from shared.store_supabase import SupabaseStore

pytestmark = pytest.mark.supabase

SESSION = "Sessions/CLIENT_ZED/2026-11-14_1"
LIST1 = f"{SESSION}/packing/list1"


def _manager(store, hostname, user, tmp_path):
    mgr = SessionLockManager(ProfileManager(str(tmp_path / f"{hostname}.ini"), store=store))
    mgr.hostname = hostname
    mgr.username = user
    return mgr


@pytest.fixture
def pcs(supabase_client, supabase_user, tmp_path):
    a = SupabaseStore(supabase_client, pc="A", cache_dir=tmp_path / "a")
    b = SupabaseStore(make_user_client(*supabase_user), pc="B", cache_dir=tmp_path / "b")
    a.write_json(f"{SESSION}/session_info.json", {"status": "active"})
    a.write_json(f"{SESSION}/packing_lists/list1.json", {"orders": []})
    wd = Path(a.local_path(LIST1))
    return a, b, _manager(a, "A", "ann", tmp_path), _manager(b, "B", "bob", tmp_path), wd


def _wd(mgr):
    return Path(mgr.store.local_path(LIST1))


def _backdate(admin, seconds):
    old = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
    admin.table("list_locks").update({"heartbeat_at": old}).eq("list_name", "list1").execute()


def test_db_lock_free_reads_as_unlocked(pcs):
    _, _, mgr_a, _, wd = pcs
    assert mgr_a.is_locked(wd) == (False, None)


def test_db_lock_other_pc_reads_as_locked_not_mine(pcs):
    _, _, mgr_a, mgr_b, wd = pcs
    assert mgr_a.acquire_lock("ZED", wd) == (True, None, None)
    locked, lock = mgr_b.is_locked(_wd(mgr_b))
    assert locked
    assert lock["locked_by"] == "A"
    assert lock["user_name"] == "ann"
    assert not mgr_b.is_mine(lock)
    assert mgr_a.is_mine(lock)
    ok, msg, _ = mgr_b.acquire_lock("ZED", _wd(mgr_b))[:3]
    assert not ok
    assert "A" in msg


def test_db_stale_lock_force_release_takes_it(pcs, supabase_admin):
    _, store_b, mgr_a, mgr_b, wd = pcs
    mgr_a.acquire_lock("ZED", wd)
    _backdate(supabase_admin, 300)
    _, lock = mgr_b.is_locked(_wd(mgr_b))
    assert mgr_b.is_lock_stale(lock)
    assert mgr_b.force_release_lock(_wd(mgr_b), expected=lock)
    assert store_b.lock_info(LIST1).holder_pc == "B"


def test_db_force_release_without_expected_takes_over(pcs, supabase_admin):
    _, store_b, mgr_a, mgr_b, wd = pcs
    mgr_a.acquire_lock("ZED", wd)
    _backdate(supabase_admin, 300)
    assert mgr_b.force_release_lock(_wd(mgr_b))
    assert store_b.lock_info(LIST1).holder_pc == "B"


def test_db_force_release_without_expected_refuses_fresh_lock(pcs):
    _, store_b, mgr_a, mgr_b, wd = pcs
    mgr_a.acquire_lock("ZED", wd)
    assert not mgr_b.force_release_lock(_wd(mgr_b))
    assert store_b.lock_info(LIST1).holder_pc == "A"


def test_db_lock_read_error_is_unreadable_not_free(pcs, monkeypatch):
    store_a, _, mgr_a, _, wd = pcs
    monkeypatch.setattr(lock_module, "LOCK_READ_RETRY_SECONDS", 0)

    def down(_rel):
        raise StoreError("down")

    real_local_path = store_a.local_path

    def no_file(rel):
        assert ".session.lock" not in str(rel), "DB mode must not touch a lock file"
        return real_local_path(rel)

    monkeypatch.setattr(store_a, "lock_info", down)
    monkeypatch.setattr(store_a, "local_path", no_file)
    locked, lock = mgr_a.is_locked(wd)
    assert locked
    assert lock["unreadable"] is True


def test_sessions_page_treats_own_pc_db_lock_as_ours(pcs, supabase_admin):
    _, _, mgr_a, _, wd = pcs
    mgr_a.acquire_lock("ZED", wd)
    _backdate(supabase_admin, 300)
    mgr_a.process_id = -1  # a new process on the same PC
    page = SimpleNamespace(_locks=mgr_a)
    assert SessionsPage._stale_lock(page, str(wd)) is None
