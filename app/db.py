"""MySQL integration for writing GA4 batch results to Legitrix database.

Opt-in: all functions no-op gracefully when DB_HOST env var is unset.
"""

import logging
import os

from mysql.connector import pooling

logger = logging.getLogger(__name__)

_pool: pooling.MySQLConnectionPool | None = None


def is_enabled() -> bool:
    """True if DB_HOST is set (makes MySQL entirely opt-in)."""
    return bool(os.environ.get("DB_HOST"))


def _get_pool() -> pooling.MySQLConnectionPool:
    """Lazy-create a connection pool on first use."""
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="ga4_pool",
            pool_size=3,
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", "3306")),
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ["DB_DATABASE"],
        )
    return _pool


def get_connection():
    """Get a connection from the pool (use as context manager)."""
    return _get_pool().get_connection()


def load_property_brand_map() -> dict[str, int]:
    """Query brand_google_channels for {ga4_property_id: brand_id} mapping."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ga4_property_id, brand_id FROM brand_google_channels "
            "WHERE ga4_property_id IS NOT NULL AND ga4_property_id != ''"
        )
        mapping = {str(row[0]): int(row[1]) for row in cursor.fetchall()}
        cursor.close()
        logger.info("Loaded %d property->brand mappings from DB", len(mapping))
        return mapping
    finally:
        conn.close()


def upsert_monthly_brand(rows: list[dict]) -> int:
    """INSERT ... ON DUPLICATE KEY UPDATE into ga4_monthly_brand.

    Each row dict must have keys: brand_id, year, month, conversions,
    active_users, new_users, avg_session_duration, bounce_rate.

    Returns the number of rows upserted.
    """
    if not rows:
        return 0

    sql = (
        "INSERT INTO ga4_monthly_brand "
        "(brand_id, year, month, conversions, active_users, new_users, "
        "avg_session_duration, bounce_rate) "
        "VALUES (%(brand_id)s, %(year)s, %(month)s, %(conversions)s, "
        "%(active_users)s, %(new_users)s, %(avg_session_duration)s, %(bounce_rate)s) "
        "ON DUPLICATE KEY UPDATE "
        "conversions = VALUES(conversions), "
        "active_users = VALUES(active_users), "
        "new_users = VALUES(new_users), "
        "avg_session_duration = VALUES(avg_session_duration), "
        "bounce_rate = VALUES(bounce_rate)"
    )

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.executemany(sql, rows)
        conn.commit()
        cursor.close()
        logger.info("Upserted %d rows into ga4_monthly_brand", len(rows))
        return len(rows)
    finally:
        conn.close()
