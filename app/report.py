"""GA4 Data API query logic."""

import logging

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    RunReportRequest,
)
from google.oauth2.credentials import Credentials

logger = logging.getLogger(__name__)

# GA4 API limit: max 10 metrics per request
MAX_METRICS_PER_REQUEST = 10


def chunk_metrics(
    metrics: list[str], max_size: int = MAX_METRICS_PER_REQUEST
) -> list[list[str]]:
    """Split a metrics list into batches that respect the GA4 API limit."""
    return [metrics[i : i + max_size] for i in range(0, len(metrics), max_size)]


def run_report(
    credentials: Credentials,
    property_id: str,
    start_date: str,
    end_date: str,
    dimensions: list[str],
    metrics: list[str],
    dimension_filter: dict | None = None,
    client: BetaAnalyticsDataClient | None = None,
) -> list[dict]:
    """Execute a GA4 report and return rows as a list of dicts.

    Args:
        dimension_filter: Optional dict with keys ``field``, ``match_type``,
            ``value`` to apply a dimension filter to the query.
        client: Optional pre-built client instance (avoids creating a new
            one per call, useful for batch workloads).
    """
    if client is None:
        client = BetaAnalyticsDataClient(credentials=credentials)

    request_kwargs: dict = {
        "property": f"properties/{property_id}",
        "dimensions": [Dimension(name=d) for d in dimensions],
        "metrics": [Metric(name=m) for m in metrics],
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
    }

    if dimension_filter:
        string_filter = Filter.StringFilter(
            match_type=Filter.StringFilter.MatchType[dimension_filter["match_type"]],
            value=dimension_filter["value"],
        )
        request_kwargs["dimension_filter"] = FilterExpression(
            filter=Filter(
                field_name=dimension_filter["field"],
                string_filter=string_filter,
            )
        )

    logger.info(
        "Querying property %s  |  %s → %s  |  dims=%s  metrics=%s",
        property_id,
        start_date,
        end_date,
        dimensions,
        metrics,
    )

    request = RunReportRequest(**request_kwargs)
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
