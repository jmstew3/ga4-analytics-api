"""Batch GA4 report runner for multi-brand portfolio analysis.

Reads batch_config.json, loops through all brands x date ranges x report types,
and outputs consolidated CSVs to output/batch/.

Reuses app.auth, app.report (single source of GA4 query logic).
"""

import calendar
import csv
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.api_core.exceptions import (
    DeadlineExceeded,
    ResourceExhausted,
    ServiceUnavailable,
)

from app import db
from app.auth import load_credentials
from app.report import chunk_metrics, run_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

# Rate limiting: GA4 API has quotas per property per day
REQUEST_DELAY_SECONDS = 1.0

# Retry config for transient API errors
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2
RETRYABLE_EXCEPTIONS = (
    ResourceExhausted,
    ServiceUnavailable,
    DeadlineExceeded,
    ConnectionError,
    TimeoutError,
)

_MONTH_ABBR = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}
_MONTH_NUM_TO_ABBR = {v: k for k, v in _MONTH_ABBR.items()}


def generate_date_ranges(
    start_date_str: str, end_date_str: str | None = None
) -> list[dict]:
    """Generate monthly date ranges from start_date up to end_date (default: yesterday).

    Each range spans a full calendar month (or partial for the final month if
    end_date falls mid-month). Handles leap years via calendar.monthrange().
    """
    start = date.fromisoformat(start_date_str)
    start = start.replace(day=1)  # normalize to 1st of month

    end_ceiling = date.fromisoformat(end_date_str) if end_date_str else date.today() - timedelta(days=1)

    ranges: list[dict] = []
    cursor = start
    while cursor <= end_ceiling:
        _, last_day = calendar.monthrange(cursor.year, cursor.month)
        month_end = cursor.replace(day=last_day)
        # Cap at the ceiling so the final month doesn't exceed it
        if month_end > end_ceiling:
            month_end = end_ceiling
        label = f"{_MONTH_NUM_TO_ABBR[cursor.month]}{cursor.year}"
        ranges.append({
            "label": label,
            "start_date": cursor.isoformat(),
            "end_date": month_end.isoformat(),
        })
        # Advance to 1st of next month
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1, day=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1, day=1)
    return ranges


def _run_report_with_retry(**kwargs) -> list[dict]:
    """Wrap run_report() with retries for transient API errors."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            return run_report(**kwargs)
        except RETRYABLE_EXCEPTIONS as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF_BASE ** (attempt + 1)
            logger.warning(
                "    RETRY %d/%d after %s: %s (waiting %ds)",
                attempt + 1,
                MAX_RETRIES,
                type(exc).__name__,
                exc,
                wait,
            )
            time.sleep(wait)
    return []  # unreachable, satisfies type checker


def load_batch_config(config_path: str) -> dict:
    """Load and validate batch_config.json.

    Filters out brands that still have placeholder property IDs.
    """
    resolved = os.path.realpath(config_path)

    # Validate path is inside the project or container working directory
    allowed_prefixes = (
        os.path.realpath(os.path.dirname(resolved)),  # same dir as config
        os.path.realpath(os.getcwd()),                 # current working dir
        "/app/",                                       # Docker container
    )
    if not any(resolved.startswith(p) for p in allowed_prefixes):
        raise ValueError(
            f"Config path must be within the project directory, got '{resolved}'"
        )

    with open(resolved, "r") as f:
        config = json.load(f)

    # Resolve date ranges: explicit array or auto-generated from start date
    if "date_ranges" in config:
        pass  # use as-is
    elif "date_range_start" in config:
        config["date_ranges"] = generate_date_ranges(
            config["date_range_start"],
            config.get("date_range_end"),
        )
        logger.info(
            "Generated %d monthly date ranges from %s",
            len(config["date_ranges"]),
            config["date_range_start"],
        )
    else:
        logger.error("batch_config.json must have 'date_ranges' or 'date_range_start'")
        sys.exit(1)

    brands = config.get("brands", [])
    active = [b for b in brands if b.get("property_id", "FILL_IN") != "FILL_IN"]
    if not active:
        logger.error("No brands have property_id filled in. Edit batch_config.json first.")
        sys.exit(1)
    skipped = len(brands) - len(active)
    if skipped > 0:
        logger.warning("Skipping %d brands with property_id='FILL_IN'", skipped)
    config["brands"] = active
    return config


def run_batch(config: dict, creds) -> None:
    """Execute all brand x date-range x report-type combinations."""
    client = BetaAnalyticsDataClient(credentials=creds)
    output_dir = Path(os.environ.get("GA4_OUTPUT_DIR", "/app/output")) / "batch"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    brands = config["brands"]
    date_ranges = config["date_ranges"]
    reports = config["reports"]

    total_queries = 0
    errors: list[str] = []
    csv_paths: dict[str, Path] = {}

    for report in reports:
        report_name = report["name"]
        dimensions = report.get("dimensions", [])
        all_metrics = report["metrics"]
        dim_filter = report.get("dimension_filter")
        metric_chunks = chunk_metrics(all_metrics)

        csv_path = output_dir / f"{report_name}_{timestamp}.csv"
        fieldnames = ["brand_name", "property_id", "period"] + dimensions + all_metrics
        csv_file = open(csv_path, "w", newline="")
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        logger.info("=" * 60)
        logger.info("REPORT: %s (%d brands x %d periods)", report_name, len(brands), len(date_ranges))
        logger.info("  Dimensions: %s", dimensions or "(none)")
        logger.info("  Metrics: %s", all_metrics)
        if dim_filter:
            logger.info("  Filter: %s = %s", dim_filter["field"], dim_filter["value"])
        logger.info("=" * 60)

        for brand in brands:
            brand_name = brand["name"]
            property_id = brand["property_id"]

            for dr in date_ranges:
                period_label = dr["label"]
                start = dr["start_date"]
                end = dr["end_date"]

                logger.info("  %s | %s | %s -> %s", brand_name, period_label, start, end)

                combined_rows: dict[str, dict] = {}

                for chunk in metric_chunks:
                    try:
                        rows = _run_report_with_retry(
                            credentials=creds,
                            property_id=property_id,
                            start_date=start,
                            end_date=end,
                            dimensions=dimensions,
                            metrics=chunk,
                            dimension_filter=dim_filter,
                            client=client,
                        )
                        total_queries += 1

                        for row in rows:
                            dim_key = "|".join(row.get(d, "") for d in dimensions) if dimensions else "__total__"
                            if dim_key not in combined_rows:
                                combined_rows[dim_key] = {
                                    "brand_name": brand_name,
                                    "property_id": property_id,
                                    "period": period_label,
                                }
                                for d in dimensions:
                                    combined_rows[dim_key][d] = row.get(d, "")
                            combined_rows[dim_key].update(
                                {m: row[m] for m in chunk if m in row}
                            )

                        time.sleep(REQUEST_DELAY_SECONDS)

                    except Exception as exc:
                        error_msg = f"{brand_name} | {period_label} | {report_name}: {type(exc).__name__}: {exc}"
                        logger.error("    ERROR: %s", error_msg)
                        errors.append(error_msg)
                        time.sleep(REQUEST_DELAY_SECONDS * 2)

                if not combined_rows and not dimensions:
                    combined_rows["__total__"] = {
                        "brand_name": brand_name,
                        "property_id": property_id,
                        "period": period_label,
                    }

                for row_data in combined_rows.values():
                    writer.writerow(row_data)

        csv_file.close()
        csv_paths[report_name] = csv_path
        logger.info("Saved: %s", csv_path)

    logger.info("")
    logger.info("=" * 60)
    logger.info("BATCH COMPLETE")
    logger.info("  Total API queries: %d", total_queries)
    logger.info("  Errors: %d", len(errors))
    logger.info("  Output directory: %s", output_dir)
    if errors:
        logger.warning("  Error summary:")
        for err in errors:
            logger.warning("    - %s", err)
    logger.info("=" * 60)

    # Post-processing: write to MySQL if enabled
    if db.is_enabled():
        try:
            _write_to_database(csv_paths)
        except Exception as exc:
            logger.error("DB post-processing failed: %s: %s", type(exc).__name__, exc)
            logger.error("CSVs are still saved — DB write can be retried manually")
    else:
        logger.info("DB_HOST not set — skipping MySQL write (CSV-only mode)")


def _parse_period(label: str) -> tuple[int, int]:
    """Parse period label like 'Jan2023' into (year, month)."""
    return int(label[3:]), _MONTH_ABBR[label[:3]]


def _write_to_database(csv_paths: dict[str, Path]) -> None:
    """Merge overview + conversion CSVs and upsert into ga4_monthly_brand.

    Called after all CSVs are written. Gated on db.is_enabled() by caller.
    """
    overview_path = csv_paths.get("overview_metrics")
    conversion_path = csv_paths.get("conversion_metrics")

    if not overview_path or not conversion_path:
        logger.warning("DB WRITE: Missing overview or conversion CSV, skipping")
        return

    logger.info("")
    logger.info("=" * 60)
    logger.info("MYSQL POST-PROCESSING")
    logger.info("=" * 60)

    # Load property_id -> brand_id mapping from DB
    prop_brand_map = db.load_property_brand_map()

    # Read overview CSV into dict keyed by (property_id, period)
    overview_data: dict[tuple[str, str], dict] = {}
    with open(overview_path, "r") as f:
        for row in csv.DictReader(f):
            key = (row["property_id"], row["period"])
            overview_data[key] = row

    # Read conversion CSV, join with overview
    db_rows: list[dict] = []
    skipped = 0
    with open(conversion_path, "r") as f:
        for row in csv.DictReader(f):
            key = (row["property_id"], row["period"])
            overview = overview_data.get(key)
            if not overview:
                skipped += 1
                continue

            property_id = row["property_id"]
            brand_id = prop_brand_map.get(property_id)
            if brand_id is None:
                logger.warning(
                    "  No brand_id mapping for property %s (%s), skipping",
                    property_id,
                    row.get("brand_name", "?"),
                )
                skipped += 1
                continue

            # Skip rows where key metrics are empty (brand didn't exist yet)
            total_users = overview.get("totalUsers", "")
            if not total_users or total_users == "0":
                skipped += 1
                continue

            year, month = _parse_period(row["period"])

            # Compute bounce_rate from engagementRate
            engagement_rate_str = overview.get("engagementRate", "")
            bounce_rate = None
            if engagement_rate_str:
                bounce_rate = round(1.0 - float(engagement_rate_str), 2)

            # Parse avg_session_duration
            avg_dur_str = overview.get("averageSessionDuration", "")
            avg_session_duration = round(float(avg_dur_str), 2) if avg_dur_str else None

            # Parse conversions
            conv_str = row.get("conversions", "")
            conversions = int(float(conv_str)) if conv_str else None

            db_rows.append({
                "brand_id": brand_id,
                "year": year,
                "month": month,
                "conversions": conversions,
                "active_users": int(total_users),
                "new_users": int(overview.get("newUsers", 0) or 0),
                "avg_session_duration": avg_session_duration,
                "bounce_rate": bounce_rate,
            })

    logger.info("  Prepared %d rows for DB (%d skipped)", len(db_rows), skipped)

    if db_rows:
        count = db.upsert_monthly_brand(db_rows)
        logger.info("  Upserted %d rows into ga4_monthly_brand", count)

    logger.info("=" * 60)


def main() -> int:
    config_path = os.environ.get("BATCH_CONFIG_PATH", "/app/batch_config.json")
    token_path = os.environ.get("GA4_TOKEN_PATH", "/app/credentials/token.json")

    logger.info("Loading batch config from %s", config_path)
    config = load_batch_config(config_path)
    logger.info(
        "Loaded %d brands, %d date ranges, %d report types",
        len(config["brands"]),
        len(config["date_ranges"]),
        len(config["reports"]),
    )

    logger.info("Loading credentials from %s", token_path)
    try:
        creds = load_credentials(token_path)
    except FileNotFoundError:
        logger.error("Token not found. Run 'python scripts/authenticate.py' first.")
        return 2
    except Exception as exc:
        logger.error("Auth error: %s", exc)
        return 2

    run_batch(config, creds)
    return 0


if __name__ == "__main__":
    sys.exit(main())
