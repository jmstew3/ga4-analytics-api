"""CSV export with timestamped filenames."""

import csv
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def export_csv(
    rows: list[dict],
    output_dir: str,
    prefix: str = "ga4_report",
    delimiter: str = ",",
) -> str:
    """Write rows to a timestamped CSV file and return the filepath."""
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    if not rows:
        logger.warning("No rows to export")
        # Write an empty file with no headers
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            f.write("")
        return filepath

    fieldnames = list(rows[0].keys())

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Exported %d rows → %s", len(rows), filepath)
    return filepath
