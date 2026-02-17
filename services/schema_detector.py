"""
Database schema detection and management.

Detects available columns in creators table on startup.
Allows worker to gracefully handle schema changes without failures.

Features:
- Schema caching (detects once, uses cached result)
- Intelligent fallback (writes only available columns)
- Clear logging (helpful error messages)
- Auto-generated SQL (suggests missing columns)
"""

import logging
from typing import Set, Optional, Dict, Tuple

logger = logging.getLogger(__name__)


class SchemaDetector:
    """
    Detects available columns in database tables.

    Usage:
        detector = SchemaDetector()
        await detector.detect_schema(supabase_client)
        payload, missing = detector.filter_payload(full_payload)
        if missing:
            detector.log_schema_mismatch(missing)
    """

    def __init__(self):
        self.available_columns: Optional[Set[str]] = None
        self.missing_columns: Set[str] = set()
        self.initialized = False
        self._fallback_columns = {
            "id",
            "channel_id",
            "channel_name",
            "current_subscribers",
            "current_view_count",
            "current_video_count",
            "sync_status",
            "sync_error_message",
            "last_updated_at",
            "last_synced_at",
        }

    def detect_schema_sync(
        self, supabase_client, table_name: str = "creators"
    ) -> Set[str]:
        """
        Synchronously detect available columns (workaround for sync environments).

        Args:
            supabase_client: Supabase client
            table_name: Table to inspect

        Returns:
            Set of available column names
        """
        if self.initialized:
            return self.available_columns

        try:
            # Try to fetch schema via RPC or direct query
            response = supabase_client.table(table_name).select("*").limit(1).execute()

            if response.data and len(response.data) > 0:
                self.available_columns = set(response.data[0].keys())
                self.initialized = True
                logger.info(
                    f"✅ Schema detected for '{table_name}': "
                    f"{len(self.available_columns)} columns available"
                )
                return self.available_columns
            else:
                # Table might be empty, use fallback
                logger.warning(
                    f"Could not detect schema from empty table '{table_name}', "
                    f"using fallback columns"
                )
                self.available_columns = self._fallback_columns.copy()
                self.initialized = True
                return self.available_columns

        except Exception as e:
            logger.error(f"Failed to detect schema: {e}")
            logger.info("Using fallback minimal column set")
            self.available_columns = self._fallback_columns.copy()
            self.initialized = True
            return self.available_columns

    def filter_payload(self, payload: Dict) -> Tuple[Dict, Dict]:
        """
        Filter payload to include only available columns.

        Separates available fields from missing fields, allowing
        graceful degradation and helpful logging.

        Args:
            payload: Full update payload with all fields

        Returns:
            Tuple of (available_payload, missing_payload)

        Example:
            >>> full = {"name": "test", "banner_url": "...", "unknown": "..."}
            >>> available, missing = detector.filter_payload(full)
            >>> # available: {"name": "test"}
            >>> # missing: {"banner_url": "...", "unknown": "..."}
        """
        if not self.available_columns:
            # If schema not detected, return full payload
            # (will try update and fail with good error message)
            return payload, {}

        available_payload = {}
        missing_payload = {}

        for key, value in payload.items():
            if key in self.available_columns:
                available_payload[key] = value
            else:
                missing_payload[key] = value

        return available_payload, missing_payload

    def log_schema_mismatch(self, missing_fields: Dict, table_name: str = "creators"):
        """
        Log missing columns with helpful guidance.

        Generates SQL migration commands and explains what happened.

        Args:
            missing_fields: Dict of fields that couldn't be written
            table_name: Table name for SQL generation
        """
        if not missing_fields:
            return

        missing_cols = list(missing_fields.keys())
        self.missing_columns.update(missing_cols)

        logger.warning(
            f"⚠️  SCHEMA MISMATCH: {len(missing_cols)} field(s) not in '{table_name}' table"
        )
        logger.warning(f"   Missing columns: {', '.join(missing_cols)}")
        logger.warning("   → These fields will be skipped, sync will continue")
        logger.warning("")
        logger.warning("   To enable these features, run this SQL migration:")
        logger.warning("")

        # Generate helpful SQL
        for col in missing_cols:
            sql_type = self._infer_column_type(col)
            logger.warning(
                f"   ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} {sql_type};"
            )

        logger.warning("")
        logger.warning("   After running SQL:")
        logger.warning("   1. Clear Supabase cache")
        logger.warning("   2. Restart the worker")
        logger.warning("   3. Missed syncs will auto-retry")
        logger.warning("")

    @staticmethod
    def _infer_column_type(column_name: str) -> str:
        """Infer SQL type from column name."""
        col_lower = column_name.lower()

        # Count columns
        if "count" in col_lower:
            return "INT"

        # Age/days columns
        if "age" in col_lower or "days" in col_lower:
            return "INT"

        # Upload frequency
        if "upload" in col_lower or "monthly" in col_lower:
            return "FLOAT"

        # URL columns
        if "url" in col_lower or "uri" in col_lower:
            return "TEXT"

        # Timestamp columns
        if "timestamp" in col_lower or "date" in col_lower or "at" in col_lower:
            return "TIMESTAMP"

        # Boolean columns
        if (
            "boolean" in col_lower
            or "flag" in col_lower
            or "hidden" in col_lower
            or "official" in col_lower
        ):
            return "BOOLEAN"

        # JSON/array columns
        if "categories" in col_lower or "urls" in col_lower or "list" in col_lower:
            return "JSONB"

        # Default to TEXT
        return "TEXT"

    def get_status(self) -> Dict:
        """
        Get current detection status.

        Returns:
            Dict with status info
        """
        return {
            "initialized": self.initialized,
            "total_columns": len(self.available_columns or []),
            "available_columns": sorted(self.available_columns or []),
            "missing_columns": sorted(self.missing_columns),
            "missing_count": len(self.missing_columns),
        }

    def reset(self):
        """Reset detector (use to force re-detection)."""
        self.available_columns = None
        self.missing_columns = set()
        self.initialized = False


# Global instance for use across worker
schema_detector = SchemaDetector()
