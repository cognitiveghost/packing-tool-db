"""WorkerManager's registry on the Store: a corrupt workers.json is moved aside, as before.

A real WorkerManager over a FileStore on a temp folder; the file itself is the thing checked.
"""
from packing_tool.worker_manager import WorkerManager
from shared.store import FileStore


def _registry(tmp_path):
    return tmp_path / "Workers" / "workers.json"


def test_a_corrupt_registry_is_read_as_empty_and_moved_aside(tmp_path):
    _registry(tmp_path).parent.mkdir(parents=True)
    _registry(tmp_path).write_text("{not json", encoding="utf-8")

    assert WorkerManager(FileStore(tmp_path)).get_all_workers() == []

    assert not _registry(tmp_path).exists()
    assert (tmp_path / "Workers" / "workers.json.backup").read_text(encoding="utf-8") == "{not json"


def test_a_change_to_a_corrupt_registry_starts_from_empty_and_lands(tmp_path):
    _registry(tmp_path).parent.mkdir(parents=True)
    _registry(tmp_path).write_text("{not json", encoding="utf-8")

    manager = WorkerManager(FileStore(tmp_path))
    worker = manager.create_worker("Ana")

    assert [w.id for w in manager.get_all_workers()] == [worker.id]
    assert (tmp_path / "Workers" / "workers.json.backup").read_text(encoding="utf-8") == "{not json"


def test_stats_for_an_unknown_worker_write_nothing(tmp_path):
    manager = WorkerManager(FileStore(tmp_path))
    manager.create_worker("Ana")
    before = _registry(tmp_path).read_bytes()

    manager.update_worker_stats("worker_999", orders=3)

    assert _registry(tmp_path).read_bytes() == before
