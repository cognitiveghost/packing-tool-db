"""BufferedStore bound to the open list: list.json, the restart snapshot, UnreachableStore (spec §8.2)."""
import pytest

from packing_tool.offline_buffer import BufferedStore, UnreachableStore
from shared.store import FileStore, Store, StoreError, StoreUnreachable

LIST = "Sessions/CLIENT_M/2026-01-01_1/packing/DHL"
STATE = f"{LIST}/packing_state.json"
PL = "Sessions/CLIENT_M/2026-01-01_1/packing_lists/DHL.json"
CFG = "Clients/CLIENT_M/packer_config.json"
UPS = LIST.replace("DHL", "UPS")
STORE_METHODS = sorted(n for n, v in vars(Store).items() if callable(v) and not n.startswith("_"))


class FlakyStore(FileStore):
    """A FileStore whose network can be cut."""

    def __init__(self, root):
        super().__init__(root)
        self.down = False

    def _check(self):
        if self.down:
            raise StoreUnreachable("network")

    def read_json(self, rel):
        self._check()
        return super().read_json(rel)

    def write_json(self, rel, data):
        self._check()
        super().write_json(rel, data)

    def exists(self, rel):
        self._check()
        return super().exists(rel)


@pytest.fixture
def inner(tmp_path):
    return FlakyStore(tmp_path / "server")


@pytest.fixture
def buf(tmp_path):
    return tmp_path / "buf"


def test_begin_list_writes_meta_and_queues_under_it(inner, buf):
    inner.write_json(PL, {"orders": 1})
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {"worker_id": "w1"}, [PL])
    assert b.bound_list() == {"worker_id": "w1", "list_dir": LIST}
    inner.down = True
    b.write_json(STATE, {"n": 1})
    assert BufferedStore.pending_meta(buf) == {"worker_id": "w1", "list_dir": LIST}


def test_begin_list_skips_snapshot_rel_missing_on_server(inner, buf):
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {}, [CFG])  # not there: skipped, no error
    assert b.bound_list()["list_dir"] == LIST


def test_begin_list_raises_when_snapshot_unreadable(inner, buf):
    b = BufferedStore(inner, buf, pc="PC1")
    inner.down = True
    with pytest.raises(StoreUnreachable):
        b.begin_list(LIST, {}, [PL])
    assert b.bound_list() is None


def test_snapshot_served_when_unreachable(inner, buf):
    inner.write_json(PL, {"orders": 1})
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {}, [PL])
    inner.down = True
    assert b.read_json(PL) == {"orders": 1}


def test_snapshot_not_served_when_online(inner, buf):
    inner.write_json(PL, {"orders": 1})
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {}, [PL])
    inner.write_json(PL, {"orders": 2})
    assert b.read_json(PL) == {"orders": 2}


def test_unreachable_rel_outside_snapshot_still_raises(inner, buf):
    inner.write_json(CFG, {"cfg": 1})
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {}, [PL])
    inner.down = True
    with pytest.raises(StoreUnreachable):
        b.read_json(CFG)


def test_restart_rebinds_from_list_json(inner, buf):
    inner.write_json(PL, {"orders": 1})
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {}, [PL])
    inner.down = True
    b.write_json(STATE, {"n": 7})
    del b  # killed
    b2 = BufferedStore(UnreachableStore(), buf, pc="PC1")
    assert b2.bound_list()["list_dir"] == LIST
    assert b2.read_json(STATE) == {"n": 7}
    assert b2.read_json(PL) == {"orders": 1}


def test_bound_list_queues_writes_before_database_connects(buf):
    b = BufferedStore(UnreachableStore(), buf, pc="PC1")
    b.begin_list(LIST, {}, [])
    b.write_json(STATE, {"n": 1})
    assert b.read_json(STATE) == {"n": 1}
    assert b.online is False


def test_end_list_keeps_binding_while_outbox_pending(inner, buf):
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {}, [])
    inner.down = True
    b.write_json(STATE, {"n": 1})
    b.end_list()
    assert b.bound_list()["list_dir"] == LIST
    assert BufferedStore.pending_meta(buf)["list_dir"] == LIST


def test_end_list_unbinds_when_outbox_empty(inner, buf):
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {}, [PL])
    b.end_list()
    assert b.bound_list() is None
    assert BufferedStore(UnreachableStore(), buf, pc="PC1").bound_list() is None


def test_begin_other_list_refused_while_pending(inner, buf):
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {}, [])
    inner.down = True
    b.write_json(STATE, {"n": 1})
    with pytest.raises(StoreError, match="not synced"):
        b.begin_list(UPS, {}, [])


def test_offline_write_without_bound_list_refused(inner, buf):
    b = BufferedStore(inner, buf, pc="PC1")
    inner.down = True
    with pytest.raises(StoreUnreachable, match="offline"):
        b.write_json(STATE, {"n": 1})
    assert BufferedStore.pending_meta(buf) is None


def test_on_change_fires_on_flip_only(inner, buf):
    calls = []
    b = BufferedStore(inner, buf, pc="PC1", on_change=calls.append)
    b.begin_list(LIST, {}, [])
    inner.down = True
    b.write_json(STATE, {"n": 1})
    b.write_json(STATE, {"n": 2})
    assert calls == [False]
    inner.down = False
    b.flush()
    assert calls == [False, True]


@pytest.mark.parametrize("name", STORE_METHODS)
def test_unreachable_store_raises_unreachable_everywhere(name):
    store = UnreachableStore()
    assert store.derived_indexes is True
    with pytest.raises(StoreUnreachable, match="not connected"):
        getattr(store, name)()


def test_attach_swaps_inner(inner, buf):
    inner.write_json(PL, {"orders": 1})
    b = BufferedStore(UnreachableStore(), buf, pc="PC1")
    b.attach(inner)
    assert b.derived_indexes is False
    assert b.read_json(PL) == {"orders": 1}
