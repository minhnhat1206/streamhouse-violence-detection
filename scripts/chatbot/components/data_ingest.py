"""
Data Ingest - Background Async Schema Metadata Ingestion

Asynchronously ingests schema metadata into ChromaDB on startup and periodically.
Non-blocking - does not interfere with main application.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime

logger = logging.getLogger(__name__)


class DataIngestor:
    """Async data ingestor for schema metadata."""

    def __init__(
        self,
        chromadb_wrapper,  # ChromaDBWrapper instance
        ingest_interval_seconds: int = 300
    ):
        """Initialize data ingestor.

        Args:
            chromadb_wrapper: ChromaDBWrapper instance for schema ingestion
            ingest_interval_seconds: Interval between ingest cycles (default: 5 minutes)
        """
        self.chromadb = chromadb_wrapper
        self.ingest_interval = ingest_interval_seconds
        self.ingest_task: Optional[asyncio.Task] = None
        self.is_running = False
        self.last_ingest_time: Optional[datetime] = None
        self.ingest_count = 0

        # Define schemas to ingest
        from .chromadb_wrapper import get_default_schemas
        self.schemas = get_default_schemas()

        logger.info(
            f"Initialized DataIngestor with {len(self.schemas)} schemas, "
            f"interval: {ingest_interval_seconds}s"
        )

    async def ingest_schemas(self) -> bool:
        """Ingest all schemas into ChromaDB (idempotent).

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Starting schema ingest cycle (#{self.ingest_count + 1})")

            for table_name, schema_def in self.schemas.items():
                try:
                    # Ingest schema
                    count = self.chromadb.ingest_schema(
                        table_name=table_name,
                        columns=schema_def["columns"],
                        description=schema_def.get("description")
                    )

                    logger.info(f"Ingested {table_name}: {count} documents")

                except Exception as e:
                    logger.error(f"Failed to ingest {table_name}: {e}")
                    # Continue with other tables even if one fails
                    continue

            self.last_ingest_time = datetime.utcnow()
            self.ingest_count += 1

            logger.info(
                f"Schema ingest complete - Total tables: {len(self.schemas)}, "
                f"Cycles: {self.ingest_count}"
            )

            return True

        except Exception as e:
            logger.error(f"Schema ingest failed: {e}")
            return False

    async def background_ingest_loop(self) -> None:
        """Background loop for periodic schema ingestion.

        Runs indefinitely until stopped. Ingest failures don't crash the loop.
        """
        logger.info(f"Starting background ingest loop (interval: {self.ingest_interval}s)")

        # Ingest immediately on startup
        await self.ingest_schemas()

        while self.is_running:
            try:
                # Sleep for the interval
                await asyncio.sleep(self.ingest_interval)

                # Ingest schemas
                if self.is_running:  # Check again after sleep
                    await self.ingest_schemas()

            except asyncio.CancelledError:
                logger.info("Background ingest loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in background ingest loop: {e}")
                # Continue running even if error
                await asyncio.sleep(5)

        logger.info("Background ingest loop stopped")

    def start(self) -> bool:
        """Start the background ingest loop.

        Returns:
            True if started successfully, False if already running
        """
        if self.is_running:
            logger.warning("Data ingest already running")
            return False

        self.is_running = True

        try:
            # Create async task
            self.ingest_task = asyncio.create_task(self.background_ingest_loop())
            logger.info("Started background data ingest task")
            return True

        except Exception as e:
            logger.error(f"Failed to start background ingest: {e}")
            self.is_running = False
            return False

    async def stop(self) -> None:
        """Stop the background ingest loop.

        Cancels the ingest task and waits for cleanup.
        """
        if not self.is_running:
            logger.warning("Data ingest not running")
            return

        logger.info("Stopping background data ingest...")
        self.is_running = False

        if self.ingest_task:
            self.ingest_task.cancel()

            try:
                await self.ingest_task
            except asyncio.CancelledError:
                pass

        logger.info("Background data ingest stopped")

    def is_ready(self) -> bool:
        """Check if schema metadata is ready in ChromaDB.

        Returns:
            True if at least one schema is ingested and available
        """
        return self.chromadb.is_ready()

    def wait_for_readiness(self, timeout_seconds: int = 10) -> bool:
        """Wait for schema metadata to be ready (blocking).

        Args:
            timeout_seconds: Maximum time to wait

        Returns:
            True if ready, False if timeout
        """
        import time

        start_time = time.time()
        check_interval = 0.5

        while time.time() - start_time < timeout_seconds:
            if self.is_ready():
                logger.info("Schema metadata ready")
                return True

            time.sleep(check_interval)

        logger.warning(f"Schema metadata not ready after {timeout_seconds}s")
        return False

    def get_status(self) -> Dict[str, Any]:
        """Get status of the data ingestor.

        Returns:
            Dict with status information
        """
        return {
            "is_running": self.is_running,
            "is_ready": self.is_ready(),
            "ingest_count": self.ingest_count,
            "last_ingest_time": self.last_ingest_time.isoformat() if self.last_ingest_time else None,
            "interval_seconds": self.ingest_interval,
            "total_schemas": len(self.schemas),
            "chromadb_tables": self.chromadb.get_all_tables()
        }


async def initialize_schemas(
    chromadb_wrapper,
    wait_for_ready: bool = True,
    timeout_seconds: int = 10
) -> bool:
    """Convenience function to initialize schemas.

    Blocks until schemas are ingested into ChromaDB.

    Args:
        chromadb_wrapper: ChromaDBWrapper instance
        wait_for_ready: Whether to wait for readiness
        timeout_seconds: Timeout for wait_for_readiness

    Returns:
        True if successful
    """
    try:
        ingestor = DataIngestor(chromadb_wrapper, ingest_interval_seconds=300)

        # Ingest immediately
        success = await ingestor.ingest_schemas()

        if not success:
            logger.error("Initial schema ingest failed")
            return False

        if wait_for_ready:
            ready = ingestor.wait_for_readiness(timeout_seconds)
            if not ready:
                logger.warning("Schemas not ready in time")
                return False

        logger.info("Schemas initialized successfully")
        return True

    except Exception as e:
        logger.error(f"Error initializing schemas: {e}")
        return False
