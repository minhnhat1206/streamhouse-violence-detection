"""
Evidence Service - MinIO S3 Frame Retrieval with Caching

Retrieves evidence frames from MinIO S3 with LRU caching.
Handles frame URLs, metadata association, and error recovery.
"""

import logging
import base64
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from pathlib import Path
from collections import OrderedDict

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None
    S3Error = Exception

logger = logging.getLogger(__name__)


class LRUCache:
    """Simple LRU (Least Recently Used) cache implementation."""

    def __init__(self, max_size: int = 100):
        """Initialize LRU cache.

        Args:
            max_size: Maximum number of items to cache
        """
        self.max_size = max_size
        self.cache = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        """Get item from cache and mark as recently used.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found
        """
        if key not in self.cache:
            return None

        # Move to end (mark as recently used)
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key: str, value: Any) -> None:
        """Put item in cache, evicting LRU item if necessary.

        Args:
            key: Cache key
            value: Value to cache
        """
        if key in self.cache:
            self.cache.move_to_end(key)
        else:
            # Evict oldest if at capacity
            if len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)

        self.cache[key] = value

    def clear(self) -> None:
        """Clear the cache."""
        self.cache.clear()

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "keys": list(self.cache.keys())
        }


class EvidenceService:
    """Service for retrieving evidence frames from MinIO S3."""

    def __init__(
        self,
        minio_endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        bucket_name: str = "evidence-frames",
        cache_size: int = 100,
        use_ssl: bool = False
    ):
        """Initialize evidence service.

        Args:
            minio_endpoint: MinIO endpoint (host:port)
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket_name: S3 bucket for evidence frames
            cache_size: LRU cache size
            use_ssl: Use SSL for MinIO connection
        """
        self.minio_endpoint = minio_endpoint
        self.bucket_name = bucket_name
        self.cache = LRUCache(max_size=cache_size)
        self.cache_hits = 0
        self.cache_misses = 0

        # Initialize MinIO client
        if Minio:
            try:
                self.client = Minio(
                    endpoint=minio_endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=use_ssl
                )
                logger.info(f"Initialized MinIO client: {minio_endpoint}")
            except Exception as e:
                logger.error(f"Failed to initialize MinIO client: {e}")
                self.client = None
        else:
            logger.warning("Minio library not available")
            self.client = None

    def frame_url_from_incident(
        self,
        incident_id: str,
        camera_id: str,
        incident_date: str
    ) -> str:
        """Construct S3 frame URL from incident metadata.

        Args:
            incident_id: Incident ID (e.g., "evt_abc123")
            camera_id: Camera ID (e.g., "cam_01")
            incident_date: Incident date (e.g., "2026-04-28")

        Returns:
            S3 path (e.g., "evidence-frames/cam_01/2026-04-28/evt_abc123.jpg")
        """
        s3_path = f"{self.bucket_name}/{camera_id}/{incident_date}/{incident_id}.jpg"
        return s3_path

    def get_frame(
        self,
        incident_id: str,
        camera_id: Optional[str] = None,
        incident_date: Optional[str] = None,
        timeout: int = 10
    ) -> Optional[str]:
        """Retrieve evidence frame from S3 and return as base64.

        Args:
            incident_id: Incident ID
            camera_id: Camera ID (optional, can be inferred from metadata)
            incident_date: Incident date (optional, uses today if not provided)
            timeout: S3 request timeout in seconds

        Returns:
            Base64-encoded JPEG string, or None if not found
        """
        # Check cache first
        cache_key = f"{incident_id}"
        cached_frame = self.cache.get(cache_key)
        if cached_frame is not None:
            self.cache_hits += 1
            logger.debug(f"Cache hit for frame: {incident_id}")
            return cached_frame

        self.cache_misses += 1

        # Default values
        if not camera_id:
            camera_id = "unknown"
        if not incident_date:
            incident_date = datetime.utcnow().strftime("%Y-%m-%d")

        # Construct S3 path
        s3_path = self.frame_url_from_incident(incident_id, camera_id, incident_date)
        object_key = f"{camera_id}/{incident_date}/{incident_id}.jpg"

        if not self.client:
            logger.warning(f"MinIO client not available, cannot retrieve frame: {s3_path}")
            return None

        try:
            # Download frame from MinIO
            logger.info(f"Downloading frame from S3: {s3_path}")

            response = self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=object_key
            )

            frame_data = response.read()
            response.close()

            # Convert to base64
            frame_b64 = base64.b64encode(frame_data).decode('utf-8')

            # Cache the result
            self.cache.put(cache_key, frame_b64)
            logger.info(f"Retrieved and cached frame: {incident_id} ({len(frame_data)} bytes)")

            return frame_b64

        except S3Error as e:
            if "NoSuchKey" in str(e) or "404" in str(e):
                logger.warning(f"Frame not found in S3: {s3_path}")
                return None
            else:
                logger.error(f"S3 error retrieving frame: {e}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving frame: {e}")
            return None

    def batch_get_frames(
        self,
        incident_ids: List[str],
        camera_ids: Optional[List[str]] = None,
        incident_dates: Optional[List[str]] = None
    ) -> Dict[str, Optional[str]]:
        """Retrieve multiple frames in batch.

        Args:
            incident_ids: List of incident IDs
            camera_ids: Optional list of camera IDs (parallel to incident_ids)
            incident_dates: Optional list of incident dates (parallel to incident_ids)

        Returns:
            Dict mapping incident_id → base64 frame (or None if not found)
        """
        results = {}

        for i, incident_id in enumerate(incident_ids):
            camera_id = camera_ids[i] if camera_ids and i < len(camera_ids) else None
            incident_date = incident_dates[i] if incident_dates and i < len(incident_dates) else None

            frame = self.get_frame(incident_id, camera_id, incident_date)
            results[incident_id] = frame

        logger.info(f"Batch retrieved {len([f for f in results.values() if f is not None])}/{len(incident_ids)} frames")
        return results

    def get_frame_url(
        self,
        incident_id: str,
        camera_id: str,
        incident_date: str
    ) -> str:
        """Get S3 URL for a frame (for frontend display).

        Args:
            incident_id: Incident ID
            camera_id: Camera ID
            incident_date: Incident date

        Returns:
            S3 URL or path
        """
        return f"s3://{self.bucket_name}/{camera_id}/{incident_date}/{incident_id}.jpg"

    def cache_hit_ratio(self) -> float:
        """Get cache hit ratio.

        Returns:
            Cache hit ratio (0-1)
        """
        total = self.cache_hits + self.cache_misses
        if total == 0:
            return 0.0
        return self.cache_hits / total

    def cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dict with cache stats
        """
        total = self.cache_hits + self.cache_misses
        hit_ratio = self.cache_hit_ratio()

        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "total_accesses": total,
            "hit_ratio": hit_ratio,
            "cache_size": self.cache.stats()["size"],
            "cache_max_size": self.cache.stats()["max_size"]
        }

    def clear_cache(self) -> None:
        """Clear the cache."""
        self.cache.clear()
        logger.info("Cleared evidence frame cache")

    def get_recent_frame_urls(
        self,
        camera_id: Optional[str] = None,
        date_str: Optional[str] = None,
        limit: int = 20,
        minio_public_url: str = "http://localhost:9000",
        min_size_bytes: int = 1000,
    ) -> List[str]:
        """List most recent evidence frames from MinIO and return public HTTP URLs.

        Fast path (< 1 second) — does not require SQL.  Used as fallback when
        the SQL layer (Fluss/Iceberg) has no frame_url column.

        Args:
            camera_id: Optional camera filter (e.g. "cam_01")
            date_str: Optional date filter in YYYY-MM-DD format
            limit: Maximum number of URLs to return
            minio_public_url: Public MinIO base URL
            min_size_bytes: Skip stub/blank frames smaller than this (default 1 KB)

        Returns:
            List of public HTTP URLs, sorted most-recent first
        """
        if not self.client:
            logger.warning("MinIO client not available for frame listing")
            return []

        try:
            prefix = ""
            if camera_id and date_str:
                prefix = f"{camera_id}/{date_str}/"
            elif camera_id:
                prefix = f"{camera_id}/"

            objects = list(self.client.list_objects(
                self.bucket_name,
                prefix=prefix,
                recursive=True,
            ))

            # Filter out stub/blank frames (e.g. 218-byte placeholder JPEGs)
            objects = [o for o in objects if (o.size or 0) >= min_size_bytes]

            # Sort most-recent first
            objects.sort(key=lambda o: o.last_modified, reverse=True)

            urls = [
                f"{minio_public_url}/{self.bucket_name}/{obj.object_name}"
                for obj in objects[:limit]
            ]
            logger.info(
                f"MinIO listing: {len(urls)} real frames (prefix='{prefix}', "
                f"bucket='{self.bucket_name}', min_size={min_size_bytes}B)"
            )
            return urls

        except Exception as e:
            logger.error(f"Failed to list MinIO objects: {e}")
            return []

    def health_check(self) -> bool:
        """Check if MinIO connection is healthy.

        Returns:
            True if healthy, False otherwise
        """
        if not self.client:
            logger.warning("MinIO client not initialized")
            return False

        try:
            # List buckets as a health check
            self.client.list_buckets()
            logger.info("MinIO health check passed")
            return True
        except Exception as e:
            logger.error(f"MinIO health check failed: {e}")
            return False
