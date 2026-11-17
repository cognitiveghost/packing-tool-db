"""
Session Registry Manager - Per-client index for fast session browser loading.

File mode (FileStore) maintains a registry_index.json per client at:
    Sessions/CLIENT_{id}/registry_index.json

This eliminates the need to scan thousands of session directories over the
network file server. Instead of a 15-20 minute scan, the browser reads a
single file per client and resolves only active session lock files to check
heartbeat freshness (typically just a few files).

Derived mode (SupabaseStore, store.derived_indexes) stores nothing: the
registry is computed by a scan of the client's folder on each read, and the
register_* / refresh calls do nothing.

Update triggers (file mode):
    - register_session_start()   → session is in_progress
    - register_session_complete() → session is completed / incomplete
    - register_session_paused()  → session is paused
    - register_available_list()  → new packing list uploaded by Shopify tool
    - ensure_registry()          → first-run migration scan (one time per client)

Design notes:
    - Every read-modify-write goes through Store.update_json (locked, atomic)
    - All registry methods are synchronous; the browser calls them on a background
      thread via RegistryRefreshWorker to keep UI responsive
    - Status values stored in registry: in_progress, paused, completed, incomplete
    - Browser adds stale / abandoned labels at display time (derived from timestamps)
"""

import logging
import os
from datetime import UTC, datetime
from pathlib import Path

from shared.metadata_utils import get_current_timestamp, parse_timestamp
from shared.store import Store, StoreError, StoreNotFound

logger = logging.getLogger(__name__)

# Seconds before an "in_progress" heartbeat is considered stale. The same as
# SessionLockManager.STALE_TIMEOUT: a session reads Stale in the list exactly
# when its lock can be taken over.
STALE_HEARTBEAT_SECONDS = 120

# Seconds before a session with no summary and no recent activity is "abandoned"
ABANDONED_SECONDS = 86400  # 24 hours


class _Unchanged(Exception):
    """Raised inside a registry update to end it without writing the file."""


def _rel(store: Store, path) -> str:
    """Store path (POSIX, relative to the server root) of a path inside the store."""
    return Path(path).relative_to(store.local_path("")).as_posix()


def _started_entry(session_id, list_name, *, summary, state, locked, has_info, info_pc, work_dir, session_dir):
    """Registry entry of a started list, or None when neither a summary nor a state describes it.

    A summary wins over a state. `locked`: the work folder holds a lock. `has_info`: the session
    has session_info.json. `info_pc`: the session's pc_name, or the local default.
    """
    if summary is not None:
        total_orders = summary.get("total_orders", 0)
        completed_orders = summary.get("completed_orders", 0)
        all_done = total_orders > 0 and completed_orders == total_orders
        return {
            "session_id": session_id,
            "packing_list_name": list_name,
            "status": "completed" if all_done else "incomplete",
            "worker_id": summary.get("worker_id"),
            "worker_name": summary.get("worker_name"),
            "pc_name": summary.get("pc_name", info_pc),
            "started_at": summary.get("started_at", ""),
            "last_updated": summary.get("completed_at", ""),
            "completed_at": summary.get("completed_at"),
            "duration_seconds": summary.get("duration_seconds"),
            "total_orders": total_orders,
            "completed_orders": completed_orders,
            "skipped_orders": summary.get("skipped_orders_count", 0),
            "total_items": summary.get("total_items", 0),
            "work_dir": str(work_dir),
            "session_path": str(session_dir),
            "metrics": summary.get("metrics"),
        }
    if state is not None:
        progress = state.get("progress", {})
        if locked:
            status = "in_progress"
        elif has_info:
            status = "paused"
        else:
            status = "incomplete"
        return {
            "session_id": session_id,
            "packing_list_name": list_name,
            "status": status,
            "worker_id": state.get("worker_id"),
            "worker_name": state.get("worker_name"),
            "pc_name": state.get("pc_name", info_pc),
            "started_at": state.get("started_at", ""),
            "last_updated": state.get("last_updated", ""),
            "completed_at": None,
            "duration_seconds": None,
            "total_orders": progress.get("total_orders", 0),
            "completed_orders": progress.get("completed_orders", 0),
            "skipped_orders": len(state.get("skipped_orders", [])),
            "total_items": progress.get("total_items", 0),
            "work_dir": str(work_dir),
            "session_path": str(session_dir),
            "metrics": None,
        }
    return None


def _available_entry(session_id, list_name, doc, pl_file, session_dir, mtime):
    """Registry entry of a packing list nobody has started. `doc` is the list document (trimmed in derived mode)."""
    # Courier may be under different keys depending on Shopify tool version
    courier = doc.get("courier") or doc.get("filters_applied", {}).get("courier", "")
    return {
        "session_id": session_id,
        "packing_list_name": list_name,
        "packing_list_path": str(pl_file),
        "session_path": str(session_dir),
        "courier": courier,
        "created_at": doc.get("created_at", ""),
        "total_orders": doc.get("total_orders", len(doc.get("orders", []))),
        "total_items": doc.get("total_items", 0),
        "mtime": mtime,
    }


class SessionRegistryManager:
    """
    Manages per-client registry_index.json files on the network file server.

    Each file is a flat JSON index of all sessions and available packing lists
    for a client. The Session Browser reads this single file instead of walking
    the directory tree, reducing load time from minutes to under a second.
    """

    REGISTRY_FILENAME = "registry_index.json"
    REGISTRY_VERSION = "1.0"

    def __init__(self, profile_manager):
        """
        Args:
            profile_manager: ProfileManager instance (uses its store).
        """
        self.profile_manager = profile_manager

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _registry_rel(self, client_id: str) -> str:
        """Store path of registry_index.json for the given client."""
        return f"Sessions/CLIENT_{client_id}/{self.REGISTRY_FILENAME}"

    def _empty_registry(self, client_id: str) -> dict:
        """Return an empty, versioned registry structure."""
        return {
            "version": self.REGISTRY_VERSION,
            "client_id": client_id,
            "last_updated": "",
            "sessions": {},
            "available_lists": {},
        }

    @staticmethod
    def _session_key(session_id: str, packing_list_name: str) -> str:
        """Composite dict key: '{session_id}::{packing_list_name}'."""
        return f"{session_id}::{packing_list_name}"

    def _usable(self, client_id: str, data) -> dict:
        """The stored registry, or an empty one when its version is not the one this code reads."""
        if isinstance(data, dict) and data.get("version") == self.REGISTRY_VERSION:
            return data
        logger.warning(f"Registry for client {client_id} has unexpected version/format, using empty.")
        return self._empty_registry(client_id)

    def _update(self, client_id: str, change) -> bool:
        """Read-modify-write the registry under the store's lock (spec D3).

        Every PC writes this one file; without the lock two writers each drop
        the other's change. `change(registry)` edits the registry in place and
        returns True to save it, or False to leave the file untouched. Derived
        mode has no file to change: nothing is written and this returns True.
        A failed read or write returns False and writes nothing.
        """
        store = self.profile_manager.store
        if store.derived_indexes:
            return True

        def apply(current):
            registry = self._empty_registry(client_id) if current is None else self._usable(client_id, current)
            if not change(registry):
                raise _Unchanged()
            registry["last_updated"] = get_current_timestamp()
            return registry

        try:
            store.update_json(self._registry_rel(client_id), apply)
            return True
        except _Unchanged:
            return False
        except StoreError:
            logger.exception(f"Failed to read or write registry for client {client_id}")
            return False

    # ------------------------------------------------------------------ #
    #  Read / Write                                                        #
    # ------------------------------------------------------------------ #

    def read_registry(self, client_id: str) -> dict:
        """
        The client's registry.

        Derived mode: computed by a scan of the client's folder. A failed read
        raises StoreError: an empty registry there reads as "no sessions" during an outage.

        File mode: registry_index.json. A missing file is an empty registry; a
        file that cannot be read raises StoreError; a readable file in an
        unexpected format is still an empty registry.
        """
        store = self.profile_manager.store
        if store.derived_indexes:
            return self.build_from_scan(client_id)
        try:
            data = store.read_json(self._registry_rel(client_id))
        except StoreNotFound:
            return self._empty_registry(client_id)
        return self._usable(client_id, data)

    def write_registry(self, client_id: str, registry: dict) -> bool:
        """
        Write the whole registry (stamps last_updated).

        Returns True on success, False on failure.
        """
        registry["last_updated"] = get_current_timestamp()
        try:
            self.profile_manager.store.write_json(self._registry_rel(client_id), registry)
            return True
        except Exception:
            logger.exception(f"Failed to write registry for client {client_id}")
            return False

    def registry_exists(self, client_id: str) -> bool:
        """Return True if registry_index.json already exists for this client (always True in derived mode)."""
        store = self.profile_manager.store
        return store.derived_indexes or store.exists(self._registry_rel(client_id))

    # ------------------------------------------------------------------ #
    #  First-run migration                                                 #
    # ------------------------------------------------------------------ #

    def ensure_registry(self, client_id: str) -> bool:
        """
        If no registry file exists for client, build it from a directory scan.

        This is the one-time migration path. After this runs, the registry file
        exists and subsequent calls are instant (file already present).

        Returns True if registry is ready (existed or successfully built).
        """
        if self.registry_exists(client_id):
            return True

        logger.info(
            f"No registry for client {client_id} — building from directory scan..."
        )
        store = self.profile_manager.store
        try:
            registry = self.build_from_scan(client_id)  # slow: kept outside the lock

            def write_if_absent(current):
                if current is not None:  # another PC finished first
                    raise _Unchanged()
                registry["last_updated"] = get_current_timestamp()
                return registry

            store.update_json(self._registry_rel(client_id), write_if_absent)
            return True
        except _Unchanged:
            return True
        except Exception:
            logger.exception(
                f"Failed to build registry for client {client_id}"
            )
            return False

    def build_from_scan(self, client_id: str) -> dict:
        """
        Build the client's registry.

        File mode walks the Sessions/CLIENT_{id}/ directory tree. This is only
        called once per client as a migration step when no registry_index.json
        exists. Derived mode reads one packing_overview on every read.
        """
        registry = self._empty_registry(client_id)
        store = self.profile_manager.store
        if store.derived_indexes:
            self._add_from_overview(registry, client_id)
            return registry
        client_rel = f"Sessions/CLIENT_{client_id}"

        if not store.exists(client_rel):
            logger.warning(f"Client directory not found: {store.local_path(client_rel)}")
            return registry

        session_count = 0
        try:
            for name in store.list_dir(client_rel):  # names only: a plain file has no packing_lists/ and is skipped
                if name.startswith(".") or name == self.REGISTRY_FILENAME:
                    continue

                session_count += 1
                self._scan_session_dir(
                    registry, client_id, name, store.local_path(f"{client_rel}/{name}")
                )

        except StoreError:
            raise  # a failed listing is not an empty folder: it must not be read as one
        except Exception:
            logger.exception(f"Error scanning {store.local_path(client_rel)}")

        logger.info(
            f"Registry scan for client {client_id} complete: "
            f"{session_count} session dirs scanned, "
            f"{len(registry['sessions'])} session entries, "
            f"{len(registry['available_lists'])} available lists"
        )
        return registry

    def _add_from_overview(self, registry: dict, client_id: str):
        """Fill registry from the Store's packing_overview (derived mode). A StoreError propagates."""
        store = self.profile_manager.store
        default_pc = os.environ.get("COMPUTERNAME", "Unknown")
        root = store.local_path("")  # joined, not local_path(rel): a dot in a list name would route it as a blob
        for row in store.packing_overview(client_id):
            session, list_name = row["session_name"], row["list_name"]
            session_dir = root / "Sessions" / f"CLIENT_{client_id}" / session
            info = row["session_info"]
            pc_name = default_pc if info is None or info.get("pc_name") is None else info["pc_name"]
            key = self._session_key(session, list_name)
            if row["started"] or row["packing_list"] is None:  # legacy barcodes/ rows have no list document
                entry = _started_entry(
                    session, list_name, summary=row["summary"], state=row["state"],
                    locked=row["lock"] is not None, has_info=info is not None, info_pc=pc_name,
                    work_dir=root / row["work_rel"], session_dir=session_dir,
                )
                if entry is not None:
                    registry["sessions"][key] = entry
            else:
                pl_file = session_dir / "packing_lists" / f"{list_name}.json"
                registry["available_lists"][key] = _available_entry(
                    session, list_name, row["packing_list"], pl_file, session_dir, None)

    def _scan_session_dir(
        self,
        registry: dict,
        client_id: str,
        session_id: str,
        session_dir: Path,
    ):
        """
        Examine one timestamped session directory and populate the registry.

        Handles both:
        - Shopify unified workflow: session_dir/packing/{list_name}/ work dirs
        - Legacy Excel workflow:    session_dir/barcodes/ work dir
        """
        # --- Shopify: discover packing lists and their work dirs ---
        store = self.profile_manager.store
        session_rel = _rel(store, session_dir)
        packing_lists_rel = f"{session_rel}/packing_lists"
        packing_lists_dir = session_dir / "packing_lists"
        packing_dir_root = session_dir / "packing"

        if store.exists(packing_lists_rel):
            for name in store.list_dir(packing_lists_rel):
                if not name.endswith(".json"):
                    continue
                pl_name = Path(name).stem
                work_dir = packing_dir_root / pl_name

                if store.exists(f"{session_rel}/packing/{pl_name}"):
                    # Session was started for this list
                    self._register_from_work_dir(
                        registry, session_id, pl_name, work_dir, session_dir
                    )
                else:
                    # Packing list uploaded but never started → available
                    self._register_available_from_file(
                        registry, session_id, pl_name, packing_lists_dir / name, session_dir
                    )

        # --- Legacy Excel: session_dir/barcodes/ ---
        barcodes_dir = session_dir / "barcodes"
        barcodes_rel = f"{session_rel}/barcodes"
        if store.exists(barcodes_rel) and not store.exists(packing_lists_rel):
            state_rel = f"{barcodes_rel}/packing_state.json"
            if store.exists(state_rel):
                try:
                    state = store.read_json(state_rel)
                    pl_name = state.get("packing_list_name", "unknown")
                    self._register_from_work_dir(
                        registry, session_id, pl_name, barcodes_dir, session_dir
                    )
                except Exception as e:
                    logger.debug(f"Could not read legacy barcodes state {state_rel}: {e}")

    def _register_from_work_dir(
        self,
        registry: dict,
        session_id: str,
        packing_list_name: str,
        work_dir: Path,
        session_dir: Path,
    ):
        """Parse a started-session work directory and add an entry to registry."""
        key = self._session_key(session_id, packing_list_name)
        store = self.profile_manager.store
        work_rel = _rel(store, work_dir)

        # Prefer summary (completed) over state file (in-progress / paused)
        summary_rel = f"{work_rel}/session_summary.json"
        state_rel = f"{work_rel}/packing_state.json"
        info_rel = f"{_rel(store, session_dir)}/session_info.json"
        pc_name = os.environ.get("COMPUTERNAME", "Unknown")
        has_info = store.exists(info_rel)

        # Read session_info for basic metadata
        if has_info:
            try:
                pc_name = store.read_json(info_rel).get("pc_name", pc_name)
            except Exception as info_exc:
                logger.debug(f"Could not read pc_name from {info_rel}: {info_exc}")

        # A summary that reads and builds an entry wins; otherwise the state decides.
        # Either failing falls through to the next source, as it always has.
        entry = None
        if store.exists(summary_rel):
            try:
                entry = _started_entry(
                    session_id, packing_list_name, summary=store.read_json(summary_rel), state=None,
                    locked=False, has_info=has_info, info_pc=pc_name, work_dir=work_dir, session_dir=session_dir,
                )
            except Exception as e:
                logger.debug(f"Could not read summary {summary_rel}: {e}")

        if entry is None and store.exists(state_rel):
            try:
                state = store.read_json(state_rel)
                # MainWindow locks the work folder (packing/<list>/), not the session folder.
                locked = store.lock_info(work_rel) is not None
                entry = _started_entry(
                    session_id, packing_list_name, summary=None, state=state,
                    locked=locked, has_info=has_info, info_pc=pc_name, work_dir=work_dir, session_dir=session_dir,
                )
            except Exception as e:
                logger.debug(f"Could not read state {state_rel}: {e}")

        if entry is not None:
            registry["sessions"][key] = entry

    def _mtime(self, pl_file: Path) -> float | None:
        """Change key refresh_available_lists compares: seconds in file mode, None when derived.

        Taken from the store's stamp, not a stat. FileStore's stamp is (mtime_ns, size).
        """
        store = self.profile_manager.store
        if store.derived_indexes:
            return None
        return store.stamp(_rel(store, pl_file))[0] / 1e9

    def _register_available_from_file(
        self,
        registry: dict,
        session_id: str,
        packing_list_name: str,
        pl_file: Path,
        session_dir: Path,
    ):
        """Parse an unstarted packing list JSON and add it to available_lists, with its mtime."""
        key = self._session_key(session_id, packing_list_name)
        store = self.profile_manager.store
        try:
            mtime = self._mtime(pl_file)  # before the read: a rewrite mid-read is re-read next time
            pl_data = store.read_json(_rel(store, pl_file))
            registry["available_lists"][key] = _available_entry(
                session_id, packing_list_name, pl_data, pl_file, session_dir, mtime
            )
        except Exception as e:
            logger.debug(f"Could not read packing list {pl_file}: {e}")

    # ------------------------------------------------------------------ #
    #  Mutation methods (called during normal app operation)              #
    # ------------------------------------------------------------------ #

    def register_session_start(
        self,
        client_id: str,
        session_id: str,
        packing_list_name: str,
        worker_id: str | None,
        worker_name: str | None,
        pc_name: str,
        total_orders: int,
        total_items: int,
        work_dir: str,
        session_path: str,
        completed_orders: int = 0,
        skipped_orders: int = 0,
    ) -> bool:
        """
        Add or overwrite a session entry as 'in_progress'.

        Resuming keeps the entry's original started_at and carries the
        orders already packed, so the Session Browser doesn't restart at 0.

        Also removes the packing list from available_lists (it has now been started).
        """
        def change(registry):
            key = self._session_key(session_id, packing_list_name)
            now = get_current_timestamp()
            existing = registry["sessions"].get(key, {})

            registry["sessions"][key] = {
                "session_id": session_id,
                "packing_list_name": packing_list_name,
                "status": "in_progress",
                "worker_id": worker_id,
                "worker_name": worker_name,
                "pc_name": pc_name,
                "started_at": existing.get("started_at") or now,
                "last_updated": now,
                "completed_at": None,
                "duration_seconds": None,
                "total_orders": total_orders,
                "completed_orders": completed_orders,
                "skipped_orders": skipped_orders,
                "total_items": total_items,
                "work_dir": work_dir,
                "session_path": session_path,
                "metrics": None,
            }

            # Remove the matching available_list entry (same session_id + list_name)
            registry["available_lists"].pop(key, None)
            return True

        return self._update(client_id, change)

    def register_session_complete(
        self,
        client_id: str,
        session_id: str,
        packing_list_name: str,
        summary: dict,
    ) -> bool:
        """
        Mark a session as 'completed' or 'incomplete' using data from
        session_summary.json.
        """
        def change(registry):
            key = self._session_key(session_id, packing_list_name)

            # Create a stub entry if it somehow isn't in the registry yet
            if key not in registry["sessions"]:
                registry["sessions"][key] = {
                    "session_id": session_id,
                    "packing_list_name": packing_list_name,
                    "session_path": "",
                    "work_dir": "",
                    "started_at": summary.get("started_at", ""),
                }

            total_orders = summary.get("total_orders", 0)
            completed_orders = summary.get("completed_orders", 0)
            all_done = total_orders > 0 and completed_orders == total_orders

            registry["sessions"][key].update(
                {
                    "status": "completed" if all_done else "incomplete",
                    "worker_id": summary.get("worker_id"),
                    "worker_name": summary.get("worker_name"),
                    "pc_name": summary.get(
                        "pc_name", registry["sessions"][key].get("pc_name", "")
                    ),
                    "completed_at": summary.get("completed_at", get_current_timestamp()),
                    "last_updated": get_current_timestamp(),
                    "duration_seconds": summary.get("duration_seconds"),
                    "total_orders": total_orders,
                    "completed_orders": completed_orders,
                    "skipped_orders": summary.get("skipped_orders_count", 0),
                    "total_items": summary.get("total_items", 0),
                    "metrics": summary.get("metrics"),
                }
            )
            return True

        return self._update(client_id, change)

    def register_session_paused(
        self,
        client_id: str,
        session_id: str,
        packing_list_name: str,
    ) -> bool:
        """
        Mark a session as 'paused' (worker stepped away without completing).
        Only updates status if the entry exists and is not already completed.
        """
        def change(registry):
            entry = registry["sessions"].get(self._session_key(session_id, packing_list_name))
            if entry is None or entry.get("status", "") in ("completed", "incomplete"):
                return False
            entry["status"] = "paused"
            entry["last_updated"] = get_current_timestamp()
            return True

        return self._update(client_id, change)

    def update_session_progress(
        self,
        client_id: str,
        session_id: str,
        packing_list_name: str,
        completed_orders: int,
        skipped_orders: int,
    ) -> bool:
        """Move a live entry's counts and last_updated as orders finish (spec Q5).

        A finished entry (completed/incomplete) is left alone: its counts
        came from the session summary and are final.
        """
        def change(registry):
            entry = registry["sessions"].get(self._session_key(session_id, packing_list_name))
            if entry is None or entry.get("status") in ("completed", "incomplete"):
                return False
            entry.update(
                {
                    "completed_orders": completed_orders,
                    "skipped_orders": skipped_orders,
                    "last_updated": get_current_timestamp(),
                }
            )
            return True

        return self._update(client_id, change)

    def register_available_list(
        self,
        client_id: str,
        session_id: str,
        packing_list_name: str,
        packing_list_path: str,
        session_path: str,
        metadata: dict,
    ) -> bool:
        """
        Add or update an available packing list in the registry.

        Called when the Session Browser detects a new packing list JSON on the
        server that is not yet represented in the registry.
        """
        def change(registry):
            key = self._session_key(session_id, packing_list_name)

            # Don't add to available_lists if a session already exists for this key
            if key in registry["sessions"]:
                return False

            registry["available_lists"][key] = {
                "session_id": session_id,
                "packing_list_name": packing_list_name,
                "packing_list_path": packing_list_path,
                "mtime": metadata.get("mtime"),
                "session_path": session_path,
                "courier": metadata.get("courier", ""),
                "created_at": metadata.get("created_at", ""),
                "total_orders": metadata.get("total_orders", 0),
                "total_items": metadata.get("total_items", 0),
            }
            return True

        return self._update(client_id, change)

    # ------------------------------------------------------------------ #
    #  Read accessors                                                      #
    # ------------------------------------------------------------------ #

    def get_sessions(self, client_id: str) -> list:
        """Return list of all session entry dicts for a client."""
        return list(self.read_registry(client_id).get("sessions", {}).values())

    def get_available_lists(self, client_id: str) -> list:
        """Return list of available packing list dicts for a client."""
        return list(self.read_registry(client_id).get("available_lists", {}).values())

    def get_all_entries(self, client_id: str) -> list:
        """
        Return a combined list of all sessions + available lists with resolved statuses.

        For entries with status 'in_progress', also reads the .session.lock file
        to determine whether the session is truly active or has gone stale.
        Entries with no recent activity are marked 'abandoned'.
        """
        registry = self.read_registry(client_id)
        entries = []

        # --- Sessions (started) ---
        for entry in registry.get("sessions", {}).values():
            resolved = dict(entry)
            resolved["status"] = self._resolve_status(resolved)
            entries.append(resolved)

        # --- Available lists (not yet started) ---
        for entry in registry.get("available_lists", {}).values():
            resolved = dict(entry)
            resolved["status"] = "not_started"
            entries.append(resolved)

        return entries

    # ------------------------------------------------------------------ #
    #  Status resolution                                                   #
    # ------------------------------------------------------------------ #

    def _resolve_status(self, entry: dict) -> str:
        """
        Compute display status from stored registry status + lock file state.

        Stored statuses: in_progress, paused, completed, incomplete
        Display statuses: in_progress, stale, paused, completed, incomplete, abandoned
        """
        stored = entry.get("status", "")

        if stored in ("completed", "incomplete"):
            return stored

        # Check for abandoned: no summary, last activity > 24 hours ago
        last_updated_str = entry.get("last_updated", "") or entry.get("started_at", "")
        if last_updated_str:
            last_updated = parse_timestamp(last_updated_str)
            if last_updated:
                age_seconds = (
                    datetime.now(UTC) - last_updated.astimezone(UTC)
                ).total_seconds()
                if age_seconds > ABANDONED_SECONDS:
                    return "abandoned"

        if stored == "paused":
            return "paused"

        if stored == "in_progress":
            # Check the lock file heartbeat to differentiate active vs stale
            # The Shopify path locks the work directory; Excel entries have no work_dir.
            lock_dir = entry.get("work_dir") or entry.get("session_path", "")
            if lock_dir:
                heartbeat_age = self._get_lock_heartbeat_age(lock_dir)
                if heartbeat_age is not None:
                    return "in_progress" if heartbeat_age < STALE_HEARTBEAT_SECONDS else "stale"
                # Lock file missing or unreadable, but registry says in_progress → treat as paused
                return "paused"

        return stored or "incomplete"

    def _get_lock_heartbeat_age(self, lock_dir: str) -> float | None:
        """
        Seconds since the work folder's list lock was last heartbeat.
        Returns None if there is no lock, it cannot be read, or it has no heartbeat.
        """
        try:
            store = self.profile_manager.store
            info = store.lock_info(_rel(store, lock_dir))
        except Exception:
            return None
        if info is None or info.heartbeat_at is None:
            return None
        return (datetime.now(UTC) - info.heartbeat_at.astimezone(UTC)).total_seconds()

    # ------------------------------------------------------------------ #
    #  Incremental available-list discovery                               #
    # ------------------------------------------------------------------ #

    def refresh_available_lists(self, client_id: str) -> int:
        """
        Bring available_lists in line with the packing_lists/ folders.

        Registers new lists, re-reads an available list whose JSON changed (or has
        no stored mtime), and drops one whose JSON is gone (withdrawn). Started
        lists (`sessions`) are never re-read here.

        Returns the number of entries added or replaced (removals are not counted).
        Derived mode computes the available lists on read, so this does nothing and returns 0.
        """
        store = self.profile_manager.store
        if store.derived_indexes:
            return 0
        registry = self.read_registry(client_id)  # snapshot, only to decide what to re-read
        found = self._empty_registry(client_id)  # what the scan turns up; merged under the lock
        client_rel = f"Sessions/CLIENT_{client_id}"
        if not store.exists(client_rel):
            return 0

        available = registry["available_lists"]
        seen: set[str] = set()  # available keys whose JSON is still on disk
        scanned_sessions: set[str] = set()  # session dirs whose packing_lists/ was listed
        try:
            for session_id in store.list_dir(client_rel):
                session_rel = f"{client_rel}/{session_id}"
                pl_rel = f"{session_rel}/packing_lists"
                if not store.exists(pl_rel):  # absent, not empty: an empty folder still counts as listed
                    continue
                session_dir = store.local_path(session_rel)
                pl_dir = store.local_path(pl_rel)

                for name in store.list_dir(pl_rel):
                    if not name.endswith(".json"):
                        continue
                    pl_name = Path(name).stem
                    key = self._session_key(session_id, pl_name)
                    if key in registry["sessions"]:
                        continue
                    seen.add(key)

                    work_dir = session_dir / "packing" / pl_name
                    pl_file = pl_dir / name
                    if store.exists(f"{session_rel}/packing/{pl_name}"):
                        # Session was started; do a full register
                        self._register_from_work_dir(
                            found, session_id, pl_name, work_dir, session_dir
                        )
                    elif key not in available or available[key].get("mtime") != self._mtime(pl_file):
                        self._register_available_from_file(
                            found, session_id, pl_name, pl_file, session_dir
                        )
                scanned_sessions.add(session_id)

        except Exception:
            logger.exception("Error scanning for new available lists")

        # Withdrawn: listed earlier, its session dir scanned now, JSON not on disk
        gone = {
            key for key, entry in available.items()
            if entry.get("session_id") in scanned_sessions and key not in seen
        }
        if not (found["sessions"] or found["available_lists"] or gone):
            return 0

        new_count = 0

        def merge(current):
            nonlocal new_count
            for key, entry in found["sessions"].items():
                if key in current["sessions"]:
                    continue  # another PC registered it while we scanned
                current["available_lists"].pop(key, None)  # started now
                current["sessions"][key] = entry
                new_count += 1
            for key, entry in found["available_lists"].items():
                if key in current["sessions"]:
                    continue
                current["available_lists"][key] = entry  # new, or replaces a stale copy
                new_count += 1
            for key in gone:
                current["available_lists"].pop(key, None)
            return bool(new_count or gone)

        if not self._update(client_id, merge):
            return 0
        return new_count
