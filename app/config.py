"""Configuration via environment variables with Pydantic validation."""

import os
import re

from pydantic import field_validator
from pydantic_settings import BaseSettings

_DATE_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2}|today|yesterday|\d+daysAgo)$")


class Settings(BaseSettings):
    model_config = {"env_prefix": "GA4_"}

    # Required
    property_id: str

    # Date range
    start_date: str = "30daysAgo"
    end_date: str = "today"

    # Dimensions / metrics (comma-separated strings)
    dimensions: str = "country,city"
    metrics: str = "activeUsers,sessions"

    # File paths
    token_path: str = "/app/credentials/token.json"
    output_dir: str = "/app/output"

    # CSV options
    output_prefix: str = "ga4_report"
    csv_delimiter: str = ","

    @field_validator("property_id")
    @classmethod
    def validate_property_id(cls, v: str) -> str:
        if not re.fullmatch(r"\d+", v):
            raise ValueError(
                f"GA4_PROPERTY_ID must be numeric, got '{v}'"
            )
        return v

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date(cls, v: str) -> str:
        if not _DATE_PATTERN.match(v):
            raise ValueError(
                f"Invalid date format: '{v}'. "
                "Use 'YYYY-MM-DD', 'today', 'yesterday', or 'NdaysAgo'."
            )
        return v

    @field_validator("dimensions", "metrics")
    @classmethod
    def validate_csv_list(cls, v: str) -> str:
        items = [i.strip() for i in v.split(",") if i.strip()]
        if not items:
            raise ValueError("Must contain at least one value")
        if len(items) > 20:
            raise ValueError("Maximum 20 items allowed")
        pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
        for item in items:
            if not pattern.match(item):
                raise ValueError(
                    f"Invalid dimension/metric name: '{item}'"
                )
        return ",".join(items)

    @field_validator("token_path")
    @classmethod
    def validate_token_path(cls, v: str) -> str:
        resolved = os.path.realpath(v)
        if not resolved.startswith("/app/credentials/"):
            raise ValueError(
                f"token_path must be within /app/credentials/, got '{resolved}'"
            )
        return resolved

    @field_validator("output_dir")
    @classmethod
    def validate_output_dir(cls, v: str) -> str:
        resolved = os.path.realpath(v)
        if not resolved.startswith("/app/output"):
            raise ValueError(
                f"output_dir must be within /app/output, got '{resolved}'"
            )
        return resolved

    @field_validator("output_prefix")
    @classmethod
    def validate_prefix(cls, v: str) -> str:
        if not re.fullmatch(r"[a-zA-Z0-9_\-]+", v):
            raise ValueError(
                "output_prefix must be alphanumeric with dashes/underscores"
            )
        return v

    @field_validator("csv_delimiter")
    @classmethod
    def validate_delimiter(cls, v: str) -> str:
        allowed = {",", ";", "\t", "|"}
        if v not in allowed:
            raise ValueError(
                "csv_delimiter must be one of: comma (,), semicolon (;), tab, or pipe (|)"
            )
        return v

    def dimension_list(self) -> list[str]:
        return [d.strip() for d in self.dimensions.split(",")]

    def metric_list(self) -> list[str]:
        return [m.strip() for m in self.metrics.split(",")]
