"""Batch GA4 report runner for multi-brand portfolio analysis.

Reads batch_config.json, loops through all brands x date ranges x report types,
and outputs consolidated CSVs to output/batch/.

Reuses app.auth, app.report (single source of GA4 query logic).
"""

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from google.analytics.data_v1beta import BetaAnalyticsDataClient

from app.auth import load_credentials
from app.report import chunk_metrics, run_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

# Rate limiting: GA4 API has quotas per property per day
REQUEST_DELAY_SECONDS = 1.0


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
                        rows = run_report(
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
