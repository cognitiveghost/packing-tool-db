"""
Session lock for a packing list's work folder, on top of the shared Store.

The lock is `.session.lock` in the work folder (the JSON the other PCs read).
Taking, renewing, releasing and force-taking go through the Store's list-lock
methods; this module reads the lock back for the Packer's checks and turns
each answer into the Packer's messages.
"""

import logging
import os
import socket
import time
from datetime import datetime
from pathlib import Path

from shared.metadata_utils import parse_timestamp
from shared.store import (
    LOCK_FILENAME,
    STALE_SECONDS,
    FileStore,
    StoreError,
    StoreNotFound,
)

LOCK_READ_ATTEMPTS = 3
LOCK_READ_RETRY_SECONDS = 0.2
LOCK_UNREADABLE_MSG = "The session lock could not be read. Try again in a moment."
LOCK_RACE_MSG = "Another PC opened this session a moment ago. Try again in a moment."
REQUIRED_LOCK_FIELDS = ("locked_by", "user_name", "lock_time", "heartbeat")


def _is_valid(data) -> bool:
    return isinstance(data, dict) and all(k in data for k in REQUIRED_LOCK_FIELDS)


def _is_unreadable(data) -> bool:
    return isinstance(data, dict) and data.get("unreadable") is True


class SessionLockManager:
    """
    Manages session locks to prevent concurrent access.

    Features:
    - Lock file `.session.lock` in the work folder, via the Store's list-lock methods
    - Heartbeat mechanism to detect crashed sessions
    - Stale lock detection and recovery
    - Detailed lock information for UI display
    """

    LOCK_FILENAME = LOCK_FILENAME
    HEARTBEAT_INTERVAL = 60  # seconds - how often to update heartbeat
    STALE_TIMEOUT = STALE_SECONDS  # seconds - lock is stale after 2 minutes without heartbeat

    def __init__(self, profile_manager):
        """
        Initialize SessionLockManager.

        Args:
            profile_manager: ProfileManager instance; its `store` holds the work folders
        """
        self.profile_manager = profile_manager
        self.logger = logging.getLogger(self.__class__.__name__)
        self.hostname = socket.gethostname()
        self.username = self._get_username()
        self.process_id = os.getpid()

    @property
    def store(self):
        return self.profile_manager.store

    def _get_username(self) -> str:
        """
        Get current Windows username.

        Returns:
            Username or 'Unknown' if cannot be determined
        """
        try:
            return os.getlogin()
        except Exception:
            # Fallback to environment variable
            return os.environ.get('USERNAME', 'Unknown')

    def _dir_rel(self, session_dir) -> str:
        """Store path (POSIX, relative to the server root) of the work folder."""
        return Path(session_dir).relative_to(self.store.local_path("")).as_posix()

    def is_mine(self, data: dict) -> bool:
        """Whether this lock is ours. In the database a lock has no process id: the same PC counts as ours."""
        if not isinstance(self.store, FileStore):
            return data.get('locked_by') == self.hostname
        return data.get('locked_by') == self.hostname and data.get('process_id') == self.process_id

    def _read(self, session_dir) -> dict | None:
        """
        The lock file's contents: None when there is no lock file.

        A read that keeps failing is not "no lock": it returns an unreadable lock
        held by an unknown PC, aged by the file's mtime (Phase 12 Bundle 2, B1).
        Invalid contents are returned as they are; the callers decide.
        """
        rel = f"{self._dir_rel(session_dir)}/{LOCK_FILENAME}"
        db = not isinstance(self.store, FileStore)
        data = None
        last_error = None
        for attempt in range(LOCK_READ_ATTEMPTS):
            try:
                if db:
                    # The database has no lock file: its lock is a table row.
                    info = self.store.lock_info(self._dir_rel(session_dir))
                    if info is None:
                        return None
                    hb = info.heartbeat_at.isoformat() if info.heartbeat_at else None
                    return {
                        "locked_by": info.holder_pc, "user_name": info.holder_user,
                        "lock_time": hb, "heartbeat": hb,
                        "process_id": None, "worker_id": None, "worker_name": None,
                    }
                data = self.store.read_json(rel)
                break
            except StoreNotFound:
                return None  # released between the caller's look and this read
            except StoreError as e:
                last_error = e
                if attempt < LOCK_READ_ATTEMPTS - 1:
                    time.sleep(LOCK_READ_RETRY_SECONDS)
        if data is not None:
            return data

        self.logger.warning(
            f"Failed to read lock file: {last_error}",
            extra={"session_dir": str(session_dir)}
        )
        now = datetime.now().astimezone().isoformat()
        try:
            if db:
                raise OSError  # no lock file to age: unreadable counts as fresh
            # A live owner rewrites the lock every 60 s, renewing the mtime; one left
            # empty by a crash mid-create ages out through the stale-lock prompt.
            heartbeat = datetime.fromtimestamp(
                self.store.local_path(rel).stat().st_mtime
            ).astimezone().isoformat()
        except OSError:
            heartbeat = now
        return {
            "unreadable": True,
            "locked_by": "unknown PC",
            "user_name": "unknown",
            "lock_time": now,
            "heartbeat": heartbeat,
            "process_id": None,
            "worker_id": None,
            "worker_name": None,
        }

    def acquire_lock(
        self,
        client_id: str,
        session_dir: Path,
        worker_id: str | None = None,
        worker_name: str | None = None
    ) -> tuple[bool, str | None, dict | None]:
        """
        Attempt to acquire a lock on the session.

        worker_id and worker_name are accepted for the callers. The Store's lock
        does not record them, so they are not written to the lock file.

        Args:
            client_id: Client identifier
            session_dir: Path to session directory
            worker_id: Worker ID (e.g., "worker_001")
            worker_name: Worker display name

        Returns:
            Tuple of (success: bool, error_message: Optional[str], lock_info: Optional[Dict])
            - (True, None, None) if lock acquired successfully
            - (False, error_message, lock_info) if session is locked by another process
              (lock_info is None when the failure is a file I/O error)
        """
        try:
            info = self.store.acquire_list_lock(self._dir_rel(session_dir), self.hostname, self.username)
        except Exception as e:
            self.logger.exception(
                "Failed to acquire lock",
                extra={"client_id": client_id, "session_dir": str(session_dir)},
            )
            return False, f"Failed to create lock file: {e}", None
        if info.acquired:
            self.logger.info(
                "Session lock acquired successfully",
                extra={"client_id": client_id, "session_dir": str(session_dir)}
            )
            return True, None, None

        # Refused: say why, from the lock as it reads now.
        _is_locked, lock_info = self.is_locked(session_dir)
        if lock_info is None:
            return False, LOCK_RACE_MSG, None  # gone or invalid since the Store looked
        if lock_info.get("unreadable") and not self.is_lock_stale(lock_info):
            return False, LOCK_UNREADABLE_MSG, None

        if self.is_lock_stale(lock_info):
            self.logger.warning(
                "Session has stale lock",
                extra={
                    "client_id": client_id,
                    "session_dir": str(session_dir),
                    "original_lock_by": lock_info.get('locked_by'),
                    "stale_for_minutes": self._get_stale_minutes(lock_info)
                }
            )
            return False, self._format_stale_lock_message(lock_info), lock_info

        self.logger.warning(
            "Attempt to open locked session",
            extra={
                "client_id": client_id,
                "session_dir": str(session_dir),
                "locked_by": lock_info.get('locked_by'),
                "attempted_by": self.hostname
            }
        )
        return False, self._format_active_lock_message(lock_info), lock_info

    def release_lock(self, session_dir: Path) -> bool:
        """
        Release the lock on a session.

        Args:
            session_dir: Path to session directory

        Returns:
            True if lock was released, False otherwise
        """
        try:
            if not isinstance(self.store, FileStore):
                # The database has no lock file to read: its release checks the holder
                # itself, and offline BufferedStore queues it (what "finished" means).
                self.store.release_list_lock(self._dir_rel(session_dir), self.hostname)
                self.logger.info("Session lock released", extra={"session_dir": str(session_dir)})
                return True

            data = self._read(session_dir)
            if data is None:
                self.logger.debug(f"No lock file to release: {session_dir / LOCK_FILENAME}")
                return True

            if _is_unreadable(data) or (_is_valid(data) and not self.is_mine(data)):
                # Not ours (or not readable, so not known to be free): don't delete
                self.logger.warning(
                    "Attempted to release lock owned by another process",
                    extra={
                        "session_dir": str(session_dir),
                        "lock_owner": data.get('locked_by'),
                        "attempted_by": self.hostname
                    }
                )
                return False

            if not _is_valid(data):
                # Invalid contents, safe to delete
                self.store.delete(f"{self._dir_rel(session_dir)}/{LOCK_FILENAME}")
                return True

            self.store.release_list_lock(self._dir_rel(session_dir), self.hostname)
            self.logger.info(
                "Session lock released",
                extra={"session_dir": str(session_dir)}
            )
            return True

        except Exception:
            self.logger.exception(
                "Failed to release lock",
                extra={"session_dir": str(session_dir)},
            )
            return False

    def is_locked(self, session_dir: Path) -> tuple[bool, dict | None]:
        """
        Check if a session is locked.

        Args:
            session_dir: Path to session directory

        Returns:
            Tuple of (is_locked: bool, lock_info: Optional[Dict])
            - (False, None) if not locked or lock file invalid
            - (True, lock_info_dict) if locked with lock details; an unreadable
              lock is locked, with "unreadable": True
        """
        data = self._read(session_dir)
        if data is None:
            return False, None
        if _is_unreadable(data):
            return True, data
        if not _is_valid(data):
            self.logger.warning(
                f"Invalid lock file (missing fields): {Path(session_dir) / LOCK_FILENAME}",
                extra={"session_dir": str(session_dir)}
            )
            return False, None

        # Ensure worker fields exist (backward compatibility)
        data.setdefault('worker_id', None)
        data.setdefault('worker_name', None)
        return True, data

    def owns_lock(self, session_dir: Path) -> bool | None:
        """
        Whether this process holds the session's lock.

        True: ours. False: a readable lock naming another PC -- this PC must
        stop writing. None: no readable lock right now; decide nothing. A
        share outage reads as "no lock", and must not end the session.
        """
        is_locked, info = self.is_locked(session_dir)
        if not is_locked or info.get("unreadable"):
            return None
        return self.is_mine(info)

    def update_heartbeat(self, session_dir: Path) -> bool:
        """
        Update the heartbeat timestamp in the lock file.

        This should be called periodically (every 60 seconds) to prove
        the session is still active.

        Args:
            session_dir: Path to session directory

        Returns:
            True if heartbeat updated successfully, False otherwise
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if self.store.heartbeat_list_lock(self._dir_rel(session_dir), self.hostname):
                    self.logger.debug(
                        "Heartbeat updated",
                        extra={"session_dir": str(session_dir)}
                    )
                    return True
                self.logger.warning(
                    "Cannot update heartbeat: no lock of ours in this folder",
                    extra={"session_dir": str(session_dir)}
                )
                return False

            except Exception as e:
                # Network issue or file error - don't crash
                self.logger.warning(
                    f"Failed to update heartbeat (attempt {attempt + 1}/{max_retries}): {e}",
                    extra={"session_dir": str(session_dir)}
                )
                if attempt < max_retries - 1:
                    time.sleep(0.5)

        return False

    def is_lock_stale(self, lock_info: dict) -> bool:
        """
        Check if a lock is stale (no recent heartbeat).

        Args:
            lock_info: Lock information dictionary

        Returns:
            True if lock is stale, False otherwise
        """
        try:
            heartbeat_str = lock_info.get('heartbeat')
            if not heartbeat_str:
                return True

            heartbeat_time = parse_timestamp(heartbeat_str)
            if not heartbeat_time:
                return True  # Can't parse, consider stale
            now = datetime.now().astimezone()
            elapsed = (now - heartbeat_time).total_seconds()

            return elapsed > self.STALE_TIMEOUT

        except (ValueError, TypeError) as e:
            self.logger.warning(f"Failed to parse heartbeat time: {e}")
            return True  # Treat invalid heartbeat as stale

    def force_release_lock(self, session_dir: Path, expected: dict | None = None) -> bool:
        """
        Forcefully release a lock, regardless of who owns it.

        This should only be used for stale locks (crash recovery).

        Args:
            session_dir: Path to session directory
            expected: The stale lock the user was asked about. When given, the
                lock is taken over only if it is still that lock and still stale:
                while the question was open its owner may have come back, or
                another PC may have taken the list (AUDIT-02-1). In DB mode a
                missing expected means the lock as read now, so it is taken only
                if already stale.

        Returns:
            True if lock was released (or taken over), False otherwise
        """
        try:
            lock_info = self._read(session_dir)
            if lock_info is None:
                return True

            if expected is None and not isinstance(self.store, FileStore):
                expected = lock_info  # the database takes the row only if the heartbeat is still this one
            if expected is None or not _is_valid(lock_info):
                # File mode, nothing to check against: remove it (an invalid file has no owner to protect)
                self.store.delete(f"{self._dir_rel(session_dir)}/{LOCK_FILENAME}")
                self._log_forced(lock_info, session_dir)
                return True

            same = all(lock_info.get(k) == expected.get(k) for k in ("locked_by", "process_id", "heartbeat"))
            if not (same and self.is_lock_stale(lock_info)):
                self.logger.warning(
                    "Stale lock changed before force-release; left in place",
                    extra={"session_dir": str(session_dir)},
                )
                return False

            # Store takes the folder only if the heartbeat is still the one asked about.
            taken = self.store.force_take_list_lock(
                self._dir_rel(session_dir),
                self.hostname,
                self.username,
                parse_timestamp(expected.get("heartbeat")),
            )
            if not taken:
                self.logger.warning(
                    "Stale lock changed before force-release; left in place",
                    extra={"session_dir": str(session_dir)},
                )
                return False
            self._log_forced(lock_info, session_dir)
            return True

        except Exception:
            self.logger.exception(
                "Failed to force-release lock",
                extra={"session_dir": str(session_dir)},
            )
            return False

    def _log_forced(self, lock_info: dict, session_dir) -> None:
        if _is_valid(lock_info):
            self.logger.warning(
                "Stale lock force-released",
                extra={
                    "session_dir": str(session_dir),
                    "original_lock_by": lock_info.get('locked_by'),
                    "released_by": self.hostname,
                    "stale_for_minutes": self._get_stale_minutes(lock_info)
                }
            )
        else:
            self.logger.info(
                "Invalid lock file removed",
                extra={"session_dir": str(session_dir)}
            )

    def _get_stale_minutes(self, lock_info: dict) -> int:
        """Get how many minutes the lock has been stale."""
        try:
            heartbeat_str = lock_info.get('heartbeat')
            if not heartbeat_str:
                return 0

            heartbeat_time = parse_timestamp(heartbeat_str)
            if not heartbeat_time:
                return 0
            now = datetime.now().astimezone()
            elapsed_minutes = int((now - heartbeat_time).total_seconds() / 60)
            return elapsed_minutes

        except (ValueError, TypeError):
            return 0

    def _format_active_lock_message(self, lock_info: dict) -> str:
        """Format error message for active lock."""
        locked_by = lock_info.get('locked_by', 'Unknown PC')
        user_name = lock_info.get('user_name', 'Unknown user')

        return (
            f"Session is currently active on another PC:\n"
            f"{user_name} on {locked_by}\n"
            f"Please wait or choose another session."
        )

    def _format_stale_lock_message(self, lock_info: dict) -> str:
        """Format error message for stale lock."""
        locked_by = lock_info.get('locked_by', 'Unknown PC')
        user_name = lock_info.get('user_name', 'Unknown user')
        stale_minutes = self._get_stale_minutes(lock_info)

        return (
            f"Session has a stale lock (no heartbeat for {stale_minutes} minutes):\n"
            f"{user_name} on {locked_by}\n"
            f"The application may have crashed.\n"
            f"You can force-release the lock."
        )
