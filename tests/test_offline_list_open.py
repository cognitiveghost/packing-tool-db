"""Opening a list binds the run's BufferedStore to it, and the packing list is read through the store (spec §8.2)."""
from types import SimpleNamespace

from offline_fakes import FlakyStore

import gui.main_window as mw
from packing_tool.offline_buffer import BufferedStore
from packing_tool.packer_logic import PackerLogic
from packing_tool.profile_manager import ProfileManager

SESSION = "Sessions/CLIENT_M/2026-01-01_1"
LIST = f"{SESSION}/packing/DHL"
PL = f"{SESSION}/packing_lists/DHL.json"
CFG = "Clients/CLIENT_M/packer_config.json"
INFO = f"{SESSION}/session_info.json"
PACKING_LIST = {
    "list_name": "DHL",
    "orders": [{"order_number": "ORD-1", "courier": "DHL",
                "items": [{"sku": "SKU-1", "quantity": 1, "product_name": "Product"}]}],
}


def _seed(server):
    server.write_json(PL, PACKING_LIST)
    server.write_json(CFG, {"courier": "DHL"})
    server.write_json(INFO, {"pc_name": "PC1"})


def test_list_open_binds_list_with_meta_and_snapshot(tmp_path):
    root = tmp_path / "server"
    server = FlakyStore(root)
    _seed(server)
    store = BufferedStore(server, tmp_path / "buf", pc="PC1")
    window = SimpleNamespace(
        buffered_store=store,
        current_worker_id="w1",
        current_worker_name="Ann",
        lock_manager=SimpleNamespace(hostname="PC1", username="ann"),
    )

    mw.MainWindow._bind_list(window, "M", root / SESSION, root / LIST, root / PL, "DHL")

    assert store.bound_list() == {
        "client_id": "M", "session_name": "2026-01-01_1", "list_name": "DHL",
        "work_dir_rel": LIST, "worker_id": "w1", "worker_name": "Ann",
        "pc": "PC1", "user": "ann", "list_dir": LIST,
    }
    server.down = True  # the restart reads come from the snapshot
    assert store.read_json(PL) == PACKING_LIST
    assert store.read_json(CFG) == {"courier": "DHL"}
    assert store.read_json(INFO) == {"pc_name": "PC1"}


def test_packing_list_read_goes_through_store(tmp_path, config_ini):
    root = tmp_path / "server"
    server = FlakyStore(root)
    _seed(server)
    store = BufferedStore(server, tmp_path / "buf", pc="PC1")
    store.begin_list(LIST, {}, [PL])
    (root / PL).unlink()  # gone from the share: only the snapshot still has the list
    logic = PackerLogic(client_id="M", profile_manager=ProfileManager(str(config_ini), store=store),
                        work_dir=str(root / LIST))
    server.down = True  # the database goes away after the list is open
    try:
        count, _name = logic.load_packing_list_json(root / PL)
    finally:
        logic.close()

    assert count == 1
