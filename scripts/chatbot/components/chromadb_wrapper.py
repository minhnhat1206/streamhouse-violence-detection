"""
ChromaDB Wrapper - Schema Metadata Management

Manages ChromaDB persistence and semantic search for table schemas.
Provides anti-hallucination by constraining Gemini to real tables/columns.
"""

import json
import logging
from typing import List, Dict, Optional, Any
from pathlib import Path

import chromadb

logger = logging.getLogger(__name__)


class ChromaDBWrapper:
    """Persistent ChromaDB wrapper for schema metadata."""

    def __init__(self, persist_dir: str = "/app/chroma_data"):
        """Initialize ChromaDB with persistent storage.

        Args:
            persist_dir: Directory for ChromaDB persistence
        """
        self.persist_dir = Path(persist_dir)
        self.persist_dir.mkdir(parents=True, exist_ok=True)

        # Initialize ChromaDB with new PersistentClient API (chromadb >= 0.4.0)
        self.client = chromadb.PersistentClient(path=str(self.persist_dir))
        self.collection_name = "schema_metadata"

        # Get or create collection
        try:
            self.collection = self.client.get_collection(name=self.collection_name)
            logger.info(f"Loaded existing ChromaDB collection: {self.collection_name}")
        except Exception:
            self.collection = self.client.create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"}
            )
            logger.info(f"Created new ChromaDB collection: {self.collection_name}")

    def ingest_schema(
        self,
        table_name: str,
        columns: List[Dict[str, str]],
        description: Optional[str] = None
    ) -> int:
        """Ingest table schema into ChromaDB.

        Args:
            table_name: Name of table (e.g., "violence_incidents")
            columns: List of column definitions with 'name' and 'description' keys
            description: Optional table-level description

        Returns:
            Number of documents added

        Example:
            columns = [
                {"name": "incident_id", "description": "Unique incident identifier"},
                {"name": "camera_id", "description": "Camera that captured the incident"}
            ]
            wrapper.ingest_schema("violence_incidents", columns)
        """
        logger.info(f"Ingesting schema for table: {table_name}")

        # Validate columns
        if not columns or not isinstance(columns, list):
            raise ValueError(f"Invalid columns for {table_name}: {columns}")

        documents = []
        metadatas = []
        ids = []

        # Add table-level document
        table_doc_id = f"{table_name}:table"
        table_text = f"Table {table_name}. {description or ''}"
        documents.append(table_text)
        metadatas.append({
            "table": table_name,
            "type": "table",
            "description": description or ""
        })
        ids.append(table_doc_id)

        # Add column-level documents
        for col in columns:
            col_name = col.get("name")
            col_desc = col.get("description", "")

            if not col_name:
                logger.warning(f"Column missing 'name' in {table_name}: {col}")
                continue

            col_doc_id = f"{table_name}:{col_name}"
            col_text = f"Column {col_name} in table {table_name}. {col_desc}"

            documents.append(col_text)
            metadatas.append({
                "table": table_name,
                "column": col_name,
                "type": "column",
                "description": col_desc
            })
            ids.append(col_doc_id)

        # Upsert to ChromaDB (idempotent - overwrites existing)
        try:
            self.collection.upsert(
                ids=ids,
                documents=documents,
                metadatas=metadatas
            )
            logger.info(f"Ingested {len(documents)} documents for {table_name}")
            return len(documents)
        except Exception as e:
            logger.error(f"Failed to ingest schema for {table_name}: {e}")
            raise

    def search_schema(
        self,
        query: str,
        top_k: int = 3,
        where: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Search for relevant tables/columns using semantic search.

        Args:
            query: Vietnamese user query (e.g., "Các vụ bạo lực hôm nay")
            top_k: Number of results to return
            where: Optional metadata filter (e.g., {"type": "table"})

        Returns:
            List of results with table, columns, and relevance scores

        Example:
            results = wrapper.search_schema("bạo lực quận 1")
            # Returns: [{"table": "violence_incidents", "columns": [...], "score": 0.95}]
        """
        try:
            # Perform semantic search
            raw_results = self.collection.query(
                query_texts=[query],
                n_results=top_k,
                where=where
            )

            if not raw_results or not raw_results.get("ids") or len(raw_results["ids"]) == 0:
                logger.warning(f"No results found for query: {query}")
                return []

            # Parse results and group by table
            results_by_table = {}

            for i, doc_id in enumerate(raw_results["ids"][0]):
                doc_metadata = raw_results["metadatas"][0][i]
                doc_distance = raw_results["distances"][0][i]

                # Convert distance to similarity score (cosine distance to similarity)
                # For cosine distance: similarity = 1 - distance
                relevance_score = max(0.0, 1.0 - doc_distance)

                table_name = doc_metadata.get("table")

                if table_name not in results_by_table:
                    results_by_table[table_name] = {
                        "table": table_name,
                        "columns": [],
                        "relevance_score": relevance_score,
                        "description": doc_metadata.get("description", "")
                    }

                # Add column info if this is a column document
                if doc_metadata.get("type") == "column":
                    results_by_table[table_name]["columns"].append({
                        "name": doc_metadata.get("column"),
                        "description": doc_metadata.get("description"),
                        "relevance": relevance_score
                    })

            # Convert to list, sorted by relevance
            results = list(results_by_table.values())
            results.sort(key=lambda x: x["relevance_score"], reverse=True)

            logger.info(f"Search for '{query}' returned {len(results)} tables")
            return results

        except Exception as e:
            logger.error(f"Search failed for query '{query}': {e}")
            return []

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get full schema information for a specific table.

        Args:
            table_name: Name of table to retrieve

        Returns:
            Dict with table info and all columns, or None if not found
        """
        try:
            # Search for table documents
            results = self.search_schema(
                query=table_name,
                top_k=100,
                where={"table": table_name}
            )

            if not results:
                logger.warning(f"Table not found in ChromaDB: {table_name}")
                return None

            # Get the first result (should be the exact match)
            table_info = results[0]
            logger.info(f"Retrieved schema for table: {table_name}")
            return table_info

        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            return None

    def get_all_tables(self) -> List[str]:
        """Get list of all tables in ChromaDB.

        Returns:
            List of table names
        """
        try:
            results = self.collection.get(
                where={"type": "table"}
            )

            table_names = set()
            for metadata in results.get("metadatas", []):
                table_name = metadata.get("table")
                if table_name:
                    table_names.add(table_name)

            return sorted(list(table_names))

        except Exception as e:
            logger.error(f"Failed to get all tables: {e}")
            return []

    def is_ready(self) -> bool:
        """Check if ChromaDB is ready with schema metadata.

        Returns:
            True if at least one table is ingested
        """
        try:
            tables = self.get_all_tables()
            ready = len(tables) > 0

            if ready:
                logger.info(f"ChromaDB ready with {len(tables)} tables")
            else:
                logger.warning("ChromaDB not yet populated with schema metadata")

            return ready

        except Exception as e:
            logger.error(f"Error checking ChromaDB readiness: {e}")
            return False

    def clear_collection(self) -> None:
        """Clear all data from the collection (useful for testing)."""
        try:
            self.client.delete_collection(name=self.collection_name)
            self.collection = self.client.create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"}
            )
            logger.info("Cleared ChromaDB collection")
        except Exception as e:
            logger.error(f"Failed to clear collection: {e}")
            raise


def get_default_schemas() -> Dict[str, Dict[str, any]]:
    """Return default schema definitions for the violence detection system.

    Returns:
        Dict mapping table names to their column definitions
    """
    return {
        "violence_incidents": {
            "description": "Real-time violence incidents detected by AI",
            "columns": [
                {"name": "incident_id", "description": "Unique incident identifier"},
                {"name": "camera_id", "description": "ID of camera that captured the incident"},
                {"name": "timestamp", "description": "When the incident occurred (UTC timestamp)"},
                {"name": "risk_score", "description": "AI confidence score [0-1] of violence"},
                {"name": "confidence", "description": "Model confidence [0-1]"},
                {"name": "is_violent", "description": "Boolean flag if incident is violent"},
                {"name": "event_type", "description": "Type of event (FIGHTING, ASSAULT, STABBING, etc.)"},
                {"name": "location", "description": "Location/district where incident occurred"},
                {"name": "frame_url", "description": "S3 path to evidence frame JPEG"},
                {"name": "thumbnail_b64", "description": "Base64-encoded low-res thumbnail"},
                {"name": "frame_capture_ts", "description": "Timestamp when frame was captured"},
            ]
        },
        "daily_incident_stats": {
            "description": "Daily aggregated statistics by location",
            "columns": [
                {"name": "stat_date", "description": "Date of statistics"},
                {"name": "location", "description": "Location/district"},
                {"name": "total_incidents", "description": "Total number of incidents"},
                {"name": "violent_incidents", "description": "Number of violent incidents"},
                {"name": "avg_risk_score", "description": "Average risk score"},
            ]
        },
        "camera_stats": {
            "description": "Daily statistics aggregated by camera",
            "columns": [
                {"name": "stat_date", "description": "Date of statistics"},
                {"name": "camera_id", "description": "Camera identifier"},
                {"name": "total_incidents", "description": "Total incidents recorded by camera"},
                {"name": "violent_incidents", "description": "Violent incidents recorded"},
                {"name": "avg_risk_score", "description": "Average risk score"},
                {"name": "avg_confidence", "description": "Average model confidence"},
            ]
        },
    }
