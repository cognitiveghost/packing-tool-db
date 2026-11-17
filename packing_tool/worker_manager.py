"""
Worker Profile Management

Handles worker profiles stored on file server.
Simple trust-based system without authentication.
"""

import json
import logging
from dataclasses import asdict, dataclass

from shared.store import (
    Delta,
    Store,
    StoreError,
    StoreNotFound,
    StoreUnreachable,
    delta,
)

logger = logging.getLogger(__name__)

REGISTRY = "Workers/workers.json"
REGISTRY_BACKUP = "Workers/workers.json.backup"
REGISTRY_VERSION = "1.3.0"


@delta("workers.update_stats")
def _apply_stats(data, *, worker_id, sessions, orders, items, duration_seconds, session_id, timestamp):
    """Add one session's counts to a worker. `timestamp` is when the packer finished, so a replay keeps it."""
    data = {"workers": []} if data is None else data
    worker_dict = next((w for w in data.get('workers', []) if w['id'] == worker_id), None)
    if worker_dict is None:
        # Unchanged, not an error: a replay must not stop the sync on a deleted worker.
        logger.warning(f"Worker not found for stats update: {worker_id}")
        return data

    worker_dict['total_sessions'] = worker_dict.get('total_sessions', 0) + sessions
    worker_dict['total_orders'] = worker_dict.get('total_orders', 0) + orders
    worker_dict['total_items'] = worker_dict.get('total_items', 0) + items
    worker_dict['total_duration_seconds'] = worker_dict.get('total_duration_seconds', 0) + duration_seconds
    worker_dict['last_active'] = timestamp
    if session_id:
        worker_dict['last_session_id'] = session_id
    worker_dict['version'] = REGISTRY_VERSION

    # Recalculate averages using WorkerProfile
    worker = WorkerProfile.from_dict(worker_dict)
    worker.recalculate_averages()
    worker_dict.update(worker.to_dict())

    data["version"] = REGISTRY_VERSION
    return data


@dataclass
class WorkerProfile:
    """Worker profile data structure (v1.3.0)"""
    # Basic info
    id: str                    # e.g., "worker_001"
    name: str                  # Display name
    created_at: str           # ISO timestamp with timezone

    # Aggregate counters
    total_sessions: int = 0
    total_orders: int = 0
    total_items: int = 0
    total_duration_seconds: int = 0

    # Calculated metrics (automatically updated)
    avg_time_per_order: float = 0.0        # Average seconds per order
    avg_orders_per_session: float = 0.0    # Average orders per session

    # Activity tracking
    last_active: str | None = None      # ISO timestamp with timezone
    last_session_id: str | None = None  # Last completed session

    # Metadata
    version: str = "1.3.0"

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> WorkerProfile:
        """Create from dictionary with backward compatibility"""
        # Provide defaults for new fields
        defaults = {
            'total_duration_seconds': 0,
            'avg_time_per_order': 0.0,
            'avg_orders_per_session': 0.0,
            'last_session_id': None,
            'version': '1.3.0'
        }

        # Merge data with defaults
        merged_data = {**defaults, **data}

        return cls(**merged_data)

    def recalculate_averages(self):
        """Recalculate derived metrics from aggregate counters"""
        # Calculate average time per order
        if self.total_orders > 0 and self.total_duration_seconds > 0:
            self.avg_time_per_order = round(
                self.total_duration_seconds / self.total_orders,
                1
            )
        else:
            self.avg_time_per_order = 0.0

        # Calculate average orders per session
        if self.total_sessions > 0:
            self.avg_orders_per_session = round(
                self.total_orders / self.total_sessions,
                1
            )
        else:
            self.avg_orders_per_session = 0.0


class WorkerManager:
    r"""Manage worker profiles

    Storage structure:
        \\server\...\Workers\
        └── workers.json  (registry of all workers)
    """

    def __init__(self, store: Store):
        """Initialize WorkerManager

        Args:
            store: The server store; the registry is Workers/workers.json in it
        """
        self.store = store
        self.workers_dir = store.local_path("Workers")  # display only

        # Create directory if doesn't exist
        self._ensure_workers_directory()

        logger.info(f"WorkerManager initialized: {self.workers_dir}")

    def _ensure_workers_directory(self):
        """Create Workers directory if doesn't exist"""
        try:
            self.store.claim_dir("Workers")
            logger.debug(f"Workers directory ready: {self.workers_dir}")
        except StoreUnreachable:
            logger.warning("Workers directory not verified: database unreachable")  # offline start
        except Exception:
            logger.exception("Failed to create Workers directory")
            raise

    @staticmethod
    def _corrupt(e: StoreError) -> bool:
        """Invalid JSON in the registry, as opposed to the server being unavailable (or cut off)."""
        return isinstance(e.__cause__, json.JSONDecodeError) and not isinstance(e, StoreUnreachable)

    def _back_up_corrupt(self):
        """Move a corrupt registry aside, as reads always have."""
        if self.store.exists(REGISTRY):
            self.store.move(REGISTRY, REGISTRY_BACKUP)
        logger.warning(f"Corrupted file backed up to: {REGISTRY_BACKUP}")

    def _load_workers_registry(self) -> dict[str, list]:
        """Load workers.json registry

        Returns:
            dict: {"workers": [list of worker dicts]}
        """
        try:
            data = self.store.read_json(REGISTRY)
        except StoreNotFound:
            # Initialize empty registry
            return {"workers": []}
        except StoreError as e:
            if not self._corrupt(e):
                logger.exception("Failed to load workers registry")
                raise
            logger.exception("Corrupted workers.json")
            self._back_up_corrupt()
            return {"workers": []}

        logger.debug(f"Loaded {len(data.get('workers', []))} workers from registry")
        return data

    def _update_registry(self, change) -> dict:
        """Read-modify-write workers.json under its lock; change(data) edits it in place.

        Every PC's End session adds to this one file; unlocked, two at once
        kept only the last writer's counts (AUDIT-02-4). A change that raises
        writes nothing. A corrupt file is moved aside and the change starts from
        an empty registry, as the read always did.
        """
        def edit(current):
            data = {"workers": []} if current is None else current
            change(data)
            data["version"] = REGISTRY_VERSION
            return data

        return self._commit_registry(edit)

    def _commit_registry(self, edit) -> dict:
        """update_json on workers.json (edit is a fn(doc) -> doc, or a Delta), with corrupt-file recovery."""
        try:
            return self.store.update_json(REGISTRY, edit)
        except StoreError as e:
            if not self._corrupt(e):
                logger.exception("Failed to save workers registry")
                raise
            logger.exception("Corrupted workers.json")
            self._back_up_corrupt()
        return self.store.update_json(REGISTRY, edit)

    def get_all_workers(self) -> list[WorkerProfile]:
        """Get all worker profiles

        Returns:
            List[WorkerProfile]: List of all workers
        """
        data = self._load_workers_registry()
        workers = [WorkerProfile.from_dict(w) for w in data.get('workers', [])]

        logger.info(f"Retrieved {len(workers)} worker profiles")
        return workers

    def get_worker(self, worker_id: str) -> WorkerProfile | None:
        """Get specific worker profile

        Args:
            worker_id: Worker ID (e.g., "worker_001")

        Returns:
            WorkerProfile if found, None otherwise
        """
        workers = self.get_all_workers()

        for worker in workers:
            if worker.id == worker_id:
                return worker

        logger.warning(f"Worker not found: {worker_id}")
        return None

    def create_worker(self, name: str) -> WorkerProfile:
        """Create new worker profile

        Args:
            name: Display name for worker

        Returns:
            WorkerProfile: Newly created worker

        Raises:
            ValueError: If name already exists
        """
        # Validate name
        if not name or not name.strip():
            raise ValueError("Worker name cannot be empty")

        name = name.strip()
        from shared.metadata_utils import get_current_timestamp

        worker = None

        def add(data):
            nonlocal worker
            existing = [WorkerProfile.from_dict(w) for w in data.get('workers', [])]
            if any(w.name.lower() == name.lower() for w in existing):
                raise ValueError(f"Worker with name '{name}' already exists")
            worker = self._new_worker(self._generate_worker_id(existing), name, get_current_timestamp())
            data.setdefault('workers', []).append(worker.to_dict())

        self._update_registry(add)

        logger.info(f"Created worker: {worker.id} ({name})")
        return worker

    @staticmethod
    def _new_worker(worker_id: str, name: str, created_at: str) -> WorkerProfile:
        return WorkerProfile(
            id=worker_id,
            name=name,
            created_at=created_at,
            total_sessions=0,
            total_orders=0,
            total_items=0,
            total_duration_seconds=0,
            avg_time_per_order=0.0,
            avg_orders_per_session=0.0,
            last_active=None,
            last_session_id=None,
            version="1.3.0"
        )

    def _generate_worker_id(self, existing_workers: list[WorkerProfile]) -> str:
        """Generate unique worker ID

        Args:
            existing_workers: List of existing workers

        Returns:
            str: New worker ID (e.g., "worker_003")
        """
        # Find highest existing ID number
        max_num = 0
        for worker in existing_workers:
            if worker.id.startswith("worker_"):
                try:
                    num = int(worker.id.split("_")[1])
                    max_num = max(max_num, num)
                except (IndexError, ValueError):
                    continue

        # Generate next ID
        new_id = f"worker_{max_num + 1:03d}"
        return new_id

    def update_worker_stats(
        self,
        worker_id: str,
        sessions: int = 0,
        orders: int = 0,
        items: int = 0,
        duration_seconds: int = 0,
        session_id: str | None = None
    ):
        """Update worker statistics (incremental)

        Args:
            worker_id: Worker ID
            sessions: Number of sessions to add
            orders: Number of orders to add
            items: Number of items to add
            duration_seconds: Duration in seconds to add (new in v1.3.0)
            session_id: Last completed session ID (new in v1.3.0)
        """
        from shared.metadata_utils import get_current_timestamp

        stats = Delta("workers.update_stats", {
            "worker_id": worker_id, "sessions": sessions, "orders": orders, "items": items,
            "duration_seconds": duration_seconds, "session_id": session_id,
            "timestamp": get_current_timestamp(),
        })
        self._commit_registry(stats)

        logger.info(
            f"Updated stats for {worker_id}: "
            f"+{sessions} sessions, +{orders} orders, +{items} items, "
            f"+{duration_seconds}s duration"
        )

