"""Entrypoint: load config -> authenticate -> query GA4 -> export CSV."""

import logging
import sys

from app.auth import load_credentials
from app.config import Settings
from app.export import export_csv
from app.report import run_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# Exit codes
EXIT_CONFIG_ERROR = 1
EXIT_AUTH_ERROR = 2
EXIT_QUERY_ERROR = 3
EXIT_EXPORT_ERROR = 4
EXIT_UNKNOWN_ERROR = 5


def main() -> int:
    # --- Config ---
    try:
        settings = Settings()  # type: ignore[call-arg]
    except Exception as exc:
        logger.error("Configuration error: %s", type(exc).__name__)
        logger.debug("Detail: %s", exc)
        return EXIT_CONFIG_ERROR

    # --- Auth ---
    try:
        creds = load_credentials(settings.token_path)
    except FileNotFoundError:
        logger.error(
            "Token file not found. Run 'python scripts/authenticate.py' first."
        )
        return EXIT_AUTH_ERROR
    except Exception as exc:
        logger.error("Authentication error: %s", type(exc).__name__)
        logger.debug("Detail: %s", exc)
        return EXIT_AUTH_ERROR

    # --- Query ---
    try:
        rows = run_report(
            credentials=creds,
            property_id=settings.property_id,
            start_date=settings.start_date,
            end_date=settings.end_date,
            dimensions=settings.dimension_list(),
            metrics=settings.metric_list(),
        )
    except Exception as exc:
        logger.error("GA4 query error: %s", type(exc).__name__)
        logger.debug("Detail: %s", exc)
        return EXIT_QUERY_ERROR

    # --- Export ---
    try:
        filepath = export_csv(
            rows=rows,
            output_dir=settings.output_dir,
            prefix=settings.output_prefix,
            delimiter=settings.csv_delimiter,
        )
    except Exception as exc:
        logger.error("CSV export error: %s", type(exc).__name__)
        logger.debug("Detail: %s", exc)
        return EXIT_EXPORT_ERROR

    logger.info("Done — CSV saved to %s", filepath)
    return 0


if __name__ == "__main__":
    sys.exit(main())
