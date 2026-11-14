"""
Profile Manager - Handles client profiles and centralized network storage.

This module manages client-specific configurations, SKU mappings, and session
directories on a centralized file server. It provides robust file locking for
concurrent access, caching for performance, and connection testing.
"""
import configparser
import contextlib
import fnmatch
import logging
import os
import re
from datetime import datetime
from pathlib import Path

from shared.logger import setup_logging
from shared.server_connection import resolve_server_path, test_path_reachable
from shared.store import FileStore, Store, StoreError

logger = logging.getLogger(__name__)


class ProfileManagerError(Exception):
    """Base exception for ProfileManager errors."""


class NetworkError(ProfileManagerError):
    """Raised when file server is not accessible."""


class ValidationError(ProfileManagerError):
    """Raised when validation fails."""


def resolve_file_server_path(config: configparser.ConfigParser) -> str:
    """The file share root: FULFILLMENT_SERVER_PATH, then the saved connection, then config.ini.

    The env var is the same one shopify-fulfillment-tool's ProfileManager uses, so
    both apps can be pointed at the same file server from one place.
    """
    config_fallback = config.get('Network', 'FileServerPath', fallback=None)
    file_server_path = resolve_server_path("PackingTool-DB", "FULFILLMENT_SERVER_PATH", config_fallback)
    if not file_server_path:
        raise ProfileManagerError(
            "FileServerPath not configured: set FULFILLMENT_SERVER_PATH, "
            "use Settings → Server Connection, or add FileServerPath to config.ini"
        )
    return file_server_path


class ProfileManager:
    """
    Manages client profiles and centralized configuration.

    This class handles:
    - Client profile creation and management
    - SKU mapping with file locking for concurrent access
    - Session directory organization
    - Connection testing and caching
    - Validation of client IDs and data

    Attributes:
        base_path (Path): Root path on file server
        clients_dir (Path): Directory containing client profiles
        sessions_dir (Path): Directory containing session data
        cache_dir (Path): Local cache directory
        connection_timeout (int): Network connection timeout in seconds
        is_network_available (bool): Current network connectivity status
    """

    CACHE_TIMEOUT_SECONDS = 60  # Cache valid for 1 minute

    def __init__(self, config_path: str = "config.ini", store: Store | None = None):
        """
        Initialize ProfileManager with configuration.

        Args:
            config_path: Path to config.ini file
            store: A Store already opened by the caller; used as is. Without one,
                the file share is resolved from config and probed.

        Raises:
            ProfileManagerError: If configuration is invalid or inaccessible
        """
        logger.info("Initializing ProfileManager...")

        # Cache for loaded configurations (client_id -> (data, timestamp)).
        # Per-instance (not class-level) so two managers pointed at different
        # base_path/file servers never share each other's cached data.
        self._config_cache: dict[str, tuple[dict, datetime]] = {}
        self._sku_cache: dict[str, tuple[dict, datetime]] = {}

        # Load configuration
        self.config = self._load_config(config_path)

        # A store the caller already opened (startup chose the backend) is used
        # as is: no server-path resolution, no reachability probe.
        injected = store is not None
        if not injected:
            store = FileStore(resolve_file_server_path(self.config))

        self.store = store
        self.base_path = self.store.local_path("")
        self.clients_dir = self.store.local_path("Clients")
        self.sessions_dir = self.store.local_path("Sessions")
        self.workers_dir = self.store.local_path("Workers")
        self.stats_dir = self.store.local_path("Stats")
        self.logs_dir = self.store.local_path("Logs")

        # Per-process log file on the same server ProfileManager itself
        # resolved (base_path) - previously logger.py re-read config.ini
        # independently, so a saved Server Connection path or
        # FULFILLMENT_SERVER_PATH override could silently point data at
        # one server and logs at another.
        log_level_str = self.config.get('Logging', 'LogLevel', fallback='INFO')
        log_level = getattr(logging, log_level_str.upper(), logging.INFO)
        retention_days = self.config.getint('Logging', 'LogRetentionDays', fallback=30)
        setup_logging("PackingTool-DB", str(self.base_path), level=log_level, retention_days=retention_days)

        # Local cache directory
        cache_path = self.config.get('Network', 'LocalCachePath', fallback='')
        if cache_path:
            self.cache_dir = Path(cache_path)
        else:
            self.cache_dir = Path(os.path.expanduser("~")) / ".packers_assistant" / "cache"

        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Connection timeout
        self.connection_timeout = self.config.getint('Network', 'ConnectionTimeout', fallback=5)

        # Test network connectivity
        self.is_network_available = injected or test_path_reachable(
            str(self.base_path), self.connection_timeout
        )

        if not self.is_network_available:
            logger.error(f"File server not accessible: {self.base_path}")
            raise NetworkError(
                f"Cannot connect to file server at {self.base_path}\n\n"
                f"Please check:\n"
                f"1. Network connection\n"
                f"2. File server is online\n"
                f"3. Path is correct in config.ini"
            )

        # Ensure directory structure exists
        self._ensure_directories()

        logger.info("ProfileManager initialized successfully")
        logger.info(f"Base path: {self.base_path}")
        logger.info(f"Cache dir: {self.cache_dir}")

    @staticmethod
    def _load_config(config_path: str) -> configparser.ConfigParser:
        """Load configuration from config.ini."""
        config = configparser.ConfigParser()

        if not Path(config_path).exists():
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return config

        try:
            config.read(config_path, encoding='utf-8')
            logger.info(f"Configuration loaded from {config_path}")
        except Exception:
            logger.exception("Failed to load config")

        return config

    def _ensure_directories(self):
        """Create base directory structure if it doesn't exist."""
        try:
            # claim_dir is mkdir(parents=True), and says False rather than raising when it exists
            for rel in ("Clients", "Sessions", "Workers", "Stats", "Logs"):
                self.store.claim_dir(rel)
            logger.debug("Directory structure verified")
        except Exception as e:
            logger.exception("Cannot create directories")
            raise ProfileManagerError(f"Cannot create directories on file server: {e}")

    # ========================================================================
    # CLIENT VALIDATION
    # ========================================================================

    @staticmethod
    def validate_client_id(client_id: str) -> tuple[bool, str]:
        """
        Validate client ID format.

        Args:
            client_id: Client ID to validate

        Returns:
            Tuple of (is_valid, error_message)

        Rules:
            - Not empty
            - Max 10 characters
            - Only alphanumeric and underscore
            - No "CLIENT_" prefix
            - Not a Windows reserved name
        """
        if not client_id:
            return False, "Client ID cannot be empty"

        if len(client_id) > 10:
            return False, "Client ID too long (max 10 characters)"

        # Only alphanumeric and underscore
        if not re.match(r'^[A-Z0-9_]+$', client_id):
            return False, "Client ID can only contain letters, numbers, and underscore"

        # Don't allow "CLIENT_" prefix
        if client_id.startswith("CLIENT_"):
            return False, "Don't include 'CLIENT_' prefix, it will be added automatically"

        # Windows reserved names
        reserved = ['CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4',
                    'LPT1', 'LPT2', 'LPT3', 'LPT4']
        if client_id.upper() in reserved:
            return False, f"'{client_id}' is a reserved system name"

        return True, ""

    # ========================================================================
    # CLIENT PROFILE MANAGEMENT
    # ========================================================================

    def get_available_clients(self) -> list[str]:
        """
        Get list of available client IDs.

        Returns:
            List of client IDs (without "CLIENT_" prefix)
        """
        if not self.store.exists("Clients"):
            logger.warning("Clients directory does not exist")
            return []

        try:
            # a client is a folder with something in it (the Store has no is_dir)
            clients = [
                name[len("CLIENT_"):]
                for name in self.store.list_dir("Clients")
                if name.startswith("CLIENT_") and self.store.list_dir(f"Clients/{name}")
            ]

            logger.debug(f"Found {len(clients)} clients: {clients}")
            return sorted(clients)

        except Exception:
            logger.exception("Error listing clients")
            return []

    def create_client_profile(self, client_id: str, client_name: str) -> bool:
        """
        Create a new client profile with default configuration.

        Args:
            client_id: Unique client identifier (e.g., "M", "R")
            client_name: Full client name (e.g., "M Cosmetics")

        Returns:
            True if created successfully, False if already exists

        Raises:
            ValidationError: If client_id is invalid
            ProfileManagerError: If creation fails
        """
        logger.info(f"Creating client profile: {client_id} ({client_name})")

        # Validate client ID
        is_valid, error_msg = self.validate_client_id(client_id)
        if not is_valid:
            raise ValidationError(error_msg)

        client_dir = f"Clients/CLIENT_{client_id}"

        # Claim before the try: a lost race must not clean up the winner's folder
        if not self.store.claim_dir(client_dir):
            logger.warning(f"Client {client_id} already exists")
            return False

        try:
            logger.debug(f"Created client directory: {client_dir}")

            # Create default packer_config with SKU mapping integrated
            default_packer_config = {
                "client_id": client_id,
                "client_name": client_name,
                "created_at": datetime.now().astimezone().isoformat(),
                "barcode_label": {
                    "width_mm": 65,
                    "height_mm": 35,
                    "dpi": 203,
                    "show_quantity": False,
                    "show_client_name": False,
                    "font_size": 10
                },
                "courier_deadlines": {
                    "PostOne": "15:00",
                    "Speedy": "16:00",
                    "DHL": "17:00"
                },
                "required_columns": {
                    "order_number": "Order_Number",
                    "sku": "SKU",
                    "product_name": "Product_Name",
                    "quantity": "Quantity",
                    "courier": "Courier"
                },
                "sku_mapping": {},
                "barcode_settings": {
                    "auto_generate": True,
                    "format": "CODE128"
                },
                "packing_rules": {},
                "last_updated": datetime.now().astimezone().isoformat(),
                "updated_by": os.environ.get('COMPUTERNAME', 'Unknown')
            }

            self.store.write_json(f"{client_dir}/packer_config.json", default_packer_config)
            logger.debug(f"Created packer_config: {client_dir}/packer_config.json")

            # Also create client_config.json for compatibility with Shopify Tool
            client_config = {
                "client_id": client_id,
                "client_name": client_name,
                "created_at": datetime.now().astimezone().isoformat()
            }

            self.store.write_json(f"{client_dir}/client_config.json", client_config)
            logger.debug(f"Created client_config: {client_dir}/client_config.json")

            # Create backups directory
            self.store.claim_dir(f"{client_dir}/backups")

            # Create session directory for this client
            self.store.claim_dir(f"Sessions/CLIENT_{client_id}")

            logger.info(f"Successfully created client profile: {client_id}")
            return True

        except Exception as e:
            logger.exception("Failed to create client profile")
            # Cleanup on failure
            with contextlib.suppress(Exception):
                self.store.delete(client_dir)
            raise ProfileManagerError(f"Failed to create client profile: {e}")

    def load_client_config(self, client_id: str) -> dict | None:
        """
        Load packer configuration for a specific client with caching.

        Args:
            client_id: Client identifier

        Returns:
            Configuration dictionary, or None if not found
        """
        # Check cache first
        cache_key = f"config_{client_id}"
        if cache_key in self._config_cache:
            cached_data, cached_time = self._config_cache[cache_key]
            age_seconds = (datetime.now().astimezone() - cached_time).total_seconds()

            if age_seconds < self.CACHE_TIMEOUT_SECONDS:
                logger.debug(f"Using cached config for {client_id}")
                return cached_data.copy()

        # Load from disk - try packer_config.json first, then fall back to config.json
        packer_config_rel = f"Clients/CLIENT_{client_id}/packer_config.json"
        config_rel = f"Clients/CLIENT_{client_id}/config.json"

        rel_to_use = packer_config_rel if self.store.exists(packer_config_rel) else config_rel

        if not self.store.exists(rel_to_use):
            logger.warning(f"Config not found for client {client_id}")
            return None

        try:
            config = self.store.read_json(rel_to_use)

            # Update cache
            self._config_cache[cache_key] = (config, datetime.now().astimezone())

            logger.debug(f"Loaded config for client {client_id} from {rel_to_use.rsplit('/', 1)[-1]}")
            return config.copy()

        except Exception:
            logger.exception(f"Error loading config for {client_id}")
            return None

    def save_client_config(self, client_id: str, config: dict) -> bool:
        """
        Save packer configuration for a specific client with backup.

        Args:
            client_id: Client identifier
            config: Configuration dictionary

        Returns:
            True if saved successfully
        """
        packer_config_rel = f"Clients/CLIENT_{client_id}/packer_config.json"

        try:
            # Create backup before overwriting
            if self.store.exists(packer_config_rel):
                self._create_backup(client_id, packer_config_rel, "packer_config")

            # Update timestamp
            config['last_updated'] = datetime.now().astimezone().isoformat()
            config['updated_by'] = os.environ.get('COMPUTERNAME', 'Unknown')

            # Save new config
            self.store.write_json(packer_config_rel, config)

            # Invalidate cache
            cache_key = f"config_{client_id}"
            self._config_cache.pop(cache_key, None)

            logger.info(f"Saved packer_config for client {client_id}")
            return True

        except Exception:
            logger.exception(f"Error saving config for {client_id}")
            return False

    def _create_backup(self, client_id: str, file_rel: str, file_type: str):
        """Create timestamped backup of a file."""
        backup_dir = f"Clients/CLIENT_{client_id}/backups"
        self.store.claim_dir(backup_dir)

        timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
        backup_rel = f"{backup_dir}/{file_type}_{timestamp}.json"

        try:
            self.store.write_bytes(backup_rel, self.store.read_bytes(file_rel))
            logger.debug(f"Created backup: {backup_rel}")

            # Keep only last 10 backups
            backups = sorted(
                name for name in self.store.list_dir(backup_dir)
                if fnmatch.fnmatch(name, f"{file_type}_*.json")
            )
            for old_backup in backups[:-10]:
                self.store.delete(f"{backup_dir}/{old_backup}")
                logger.debug(f"Deleted old backup: {old_backup}")

        except Exception as e:
            logger.warning(f"Failed to create backup: {e}")

    # ========================================================================
    # SKU MAPPING WITH FILE LOCKING
    # ========================================================================

    def load_sku_mapping(self, client_id: str, fresh: bool = False) -> dict[str, str]:
        """
        Load SKU mapping for a specific client with caching.
        Now reads from packer_config.json instead of separate sku_mapping.json

        Args:
            client_id: Client identifier
            fresh: Read the file even if a cached copy is under a minute old.

        Returns:
            Dictionary mapping barcode to SKU
        """
        # Check cache
        cache_key = f"sku_{client_id}"
        if not fresh and cache_key in self._sku_cache:
            cached_data, cached_time = self._sku_cache[cache_key]
            age_seconds = (datetime.now().astimezone() - cached_time).total_seconds()

            if age_seconds < self.CACHE_TIMEOUT_SECONDS:
                logger.debug(f"Using cached SKU mapping for {client_id}")
                return cached_data.copy()

        # Load from packer_config.json first, fall back to sku_mapping.json
        packer_config_rel = f"Clients/CLIENT_{client_id}/packer_config.json"
        mapping_rel = f"Clients/CLIENT_{client_id}/sku_mapping.json"

        mappings = {}

        # Try packer_config.json first. A failed read raises, and is never
        # cached: an empty table read mid-rewrite is not "no mappings" (AUDIT-02-3).
        if self.store.exists(packer_config_rel):
            try:
                data = self.store.read_json(packer_config_rel)
                mappings = data.get("sku_mapping", {})
                logger.debug(f"Loaded {len(mappings)} SKU mappings from packer_config for {client_id}")
            except StoreError as e:
                logger.exception(f"Error loading SKU mapping from packer_config for {client_id}")
                raise ProfileManagerError(
                    f"Could not read the SKU mapping from the file server: {e}"
                ) from e

        # Fall back to old sku_mapping.json if packer_config doesn't have mappings
        if not mappings and self.store.exists(mapping_rel):
            try:
                data = self.store.read_json(mapping_rel)
                mappings = data.get("mappings", {})
                logger.debug(f"Loaded {len(mappings)} SKU mappings from sku_mapping.json for {client_id}")
            except Exception:
                logger.exception(f"Error loading SKU mapping from sku_mapping.json for {client_id}")

        self._sku_cache[cache_key] = (mappings, datetime.now().astimezone())

        return mappings.copy()

    def _change_sku_mapping(self, client_id: str, change) -> dict[str, str]:
        """Apply change(mapping) to the mapping as it is on disk now, under the lock.

        An unreadable packer_config.json raises: a mapping read as empty must
        never be saved over the real one.
        """
        packer_config_rel = f"Clients/CLIENT_{client_id}/packer_config.json"
        legacy_rel = f"Clients/CLIENT_{client_id}/sku_mapping.json"

        def edit(current):
            config = current if current is not None else {"client_id": client_id, "sku_mapping": {}}
            mapping = dict(config.get("sku_mapping") or {})
            if not mapping and self.store.exists(legacy_rel):
                mapping = dict(self.store.read_json(legacy_rel).get("mappings", {}))
            change(mapping)
            config["sku_mapping"] = mapping
            config["last_updated"] = datetime.now().astimezone().isoformat()
            config["updated_by"] = os.environ.get("COMPUTERNAME", "Unknown")
            return config

        try:
            config = self.store.update_json(packer_config_rel, edit)
        except StoreError as e:
            logger.exception(f"Could not save SKU mapping for {client_id}")
            raise ProfileManagerError(
                f"Could not save the SKU mapping to the file server: {e}"
            ) from e
        finally:
            self._sku_cache.pop(f"sku_{client_id}", None)
            self._config_cache.pop(f"config_{client_id}", None)
        mapping = config["sku_mapping"]
        logger.info(f"Saved SKU mapping for {client_id}: {len(mapping)} entries")
        return mapping

    def save_sku_mapping(self, client_id: str, mappings: dict[str, str]) -> bool:
        """Replace the whole SKU mapping. Raises ProfileManagerError on failure.

        For a caller that owns the full table. One PC's edits go through
        update_sku_mapping(), which cannot erase another PC's.
        """
        def replace(mapping):
            mapping.clear()
            mapping.update(mappings)

        self._change_sku_mapping(client_id, replace)
        return True

    def update_sku_mapping(
        self, client_id: str, add: dict[str, str] | None = None, remove=()
    ) -> dict[str, str]:
        """Apply one PC's edits to the mapping on disk now; return the whole mapping.

        Built from a cached or minutes-old copy, a full replace erased what other
        PCs had mapped meanwhile (AUDIT-02-3).
        """
        def edit(mapping):
            for barcode in remove:
                mapping.pop(barcode, None)
            mapping.update(add or {})

        return self._change_sku_mapping(client_id, edit)

    # ========================================================================
    # SESSION MANAGEMENT
    # ========================================================================

    def get_sessions_root(self) -> Path:
        """
        Get the root directory for all sessions.

        Returns:
            Path to SESSIONS directory on file server
        """
        return self.sessions_dir

