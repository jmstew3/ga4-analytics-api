"""GA4 Data API query logic."""

import logging

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.oauth2.credentials import Credentials

logger = logging.getLogger(__name__)


def run_report(
    credentials: Credentials,
    property_id: str,
    start_date: str,
    end_date: str,
    dimensions: list[str],
    metrics: list[str],
) -> list[dict]:
    """Execute a GA4 report and return rows as a list of dicts."""
    client = BetaAnalyticsDataClient(credentials=credentials)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )

    logger.info(
        "Querying property %s  |  %s → %s  |  dims=%s  metrics=%s",
        property_id,
        start_date,
        end_date,
        dimensions,
        metrics,
    )

    response = client.run_report(request)

    results: list[dict] = []
    for row in response.rows:
        record: dict = {}
        for i, dim in enumerate(dimensions):
            record[dim] = row.dimension_values[i].value
        for i, met in enumerate(metrics):
            record[met] = row.metric_values[i].value
        results.append(record)

    logger.info("Retrieved %d rows", len(results))
    return results
