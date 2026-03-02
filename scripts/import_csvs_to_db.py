#!/usr/bin/env python3
"""Import GA4 batch CSV data into the Legitrix MySQL database.

Reads CSVs from a batch output directory, resolves property_id → brand_id
via brand_google_channels, parses period labels (e.g. "Jan2023") into
year/month, and performs batched INSERT ... ON DUPLICATE KEY UPDATE.

Usage:
    python scripts/import_csvs_to_db.py [batch_dir]

If batch_dir is not provided, uses the latest directory under output/batch/.
"""

import csv
import os
import subprocess
import sys
from pathlib import Path

# MySQL connection from .env defaults
DB_HOST = os.getenv("DB_HOST", "172.16.0.89")
DB_PORT = os.getenv("DB_PORT", "3308")
DB_USER = os.getenv("DB_USER", "legitrix_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "legitrix_pass_2024")
DB_DATABASE = os.getenv("DB_DATABASE", "legitrix")

MONTH_ABBR = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

BATCH_SIZE = 500


def parse_period(period: str) -> tuple[int, int]:
    """Parse 'Jan2023' → (2023, 1)."""
    month_str = period[:3]
    year = int(period[3:])
    month = MONTH_ABBR[month_str]
    return year, month


def run_sql(sql: str) -> str:
    """Execute SQL via mysql CLI and return output."""
    cmd = [
        "mysql",
        f"-h{DB_HOST}",
        f"-P{DB_PORT}",
        f"-u{DB_USER}",
        f"-p{DB_PASSWORD}",
        "--default-character-set=utf8mb4",
        DB_DATABASE,
        "-e", sql,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"SQL ERROR: {result.stderr}", file=sys.stderr)
        raise RuntimeError(result.stderr)
    return result.stdout


def get_property_brand_map() -> dict[str, int]:
    """Query brand_google_channels to build property_id → brand_id map."""
    output = run_sql(
        "SELECT brand_id, ga4_property_id FROM brand_google_channels "
        "WHERE ga4_property_id IS NOT NULL AND ga4_property_id != ''"
    )
    mapping = {}
    for line in output.strip().split("\n")[1:]:  # skip header
        parts = line.split("\t")
        brand_id = int(parts[0])
        prop_id = parts[1].strip()
        mapping[prop_id] = brand_id
    return mapping


def escape_sql(val: str) -> str:
    """Basic SQL string escaping."""
    return val.replace("\\", "\\\\").replace("'", "\\'")


def find_csv(batch_dir: Path, prefix: str) -> Path:
    """Find a CSV file matching a prefix in the batch directory."""
    matches = list(batch_dir.glob(f"{prefix}_*.csv"))
    if not matches:
        raise FileNotFoundError(f"No CSV matching {prefix}_*.csv in {batch_dir}")
    return matches[0]


def load_overview_conversion(batch_dir: Path, prop_map: dict[str, int]):
    """Load overview_metrics + conversion_metrics into ga4_monthly_brand."""
    overview_file = find_csv(batch_dir, "overview_metrics")
    conversion_file = find_csv(batch_dir, "conversion_metrics")

    # Read overview data keyed by (property_id, period)
    overview_data = {}
    with open(overview_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["property_id"], row["period"])
            overview_data[key] = row

    # Read conversion data and merge
    conversion_data = {}
    with open(conversion_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["property_id"], row["period"])
            conversion_data[key] = row

    # Build insert rows
    rows = []
    skipped = 0
    for key, ov in overview_data.items():
        prop_id, period = key
        if prop_id not in prop_map:
            skipped += 1
            continue

        # Skip empty rows (no data for this period)
        if not ov.get("totalUsers") or ov["totalUsers"] == "0":
            skipped += 1
            continue

        brand_id = prop_map[prop_id]
        year, month = parse_period(period)

        conv = conversion_data.get(key, {})

        def to_int(v):
            return int(v) if v else "NULL"

        def to_dec(v):
            return v if v else "NULL"

        rows.append(
            f"({brand_id},{year},{month},"
            f"{to_int(ov.get('totalUsers'))},"
            f"{to_int(ov.get('newUsers'))},"
            f"{to_int(ov.get('sessions'))},"
            f"{to_int(ov.get('screenPageViews'))},"
            f"{to_dec(ov.get('engagementRate'))},"
            f"{to_dec(ov.get('averageSessionDuration'))},"
            f"{to_int(ov.get('engagedSessions'))},"
            f"{to_int(ov.get('eventCount'))},"
            f"{to_dec(ov.get('screenPageViewsPerSession'))},"
            f"{to_int(conv.get('conversions'))},"
            f"{to_dec(conv.get('userConversionRate'))},"
            f"{to_dec(conv.get('sessionConversionRate'))})"
        )

    print(f"ga4_monthly_brand: {len(rows)} rows to insert ({skipped} skipped)")

    # Batch insert
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        sql = (
            "INSERT INTO ga4_monthly_brand "
            "(brand_id, year, month, total_users, new_users, sessions, "
            "screen_page_views, engagement_rate, avg_session_duration, "
            "engaged_sessions, event_count, page_views_per_session, "
            "conversions, user_conversion_rate, session_conversion_rate) VALUES "
            + ",".join(batch)
            + " ON DUPLICATE KEY UPDATE "
            "total_users=VALUES(total_users), new_users=VALUES(new_users), "
            "sessions=VALUES(sessions), screen_page_views=VALUES(screen_page_views), "
            "engagement_rate=VALUES(engagement_rate), "
            "avg_session_duration=VALUES(avg_session_duration), "
            "engaged_sessions=VALUES(engaged_sessions), "
            "event_count=VALUES(event_count), "
            "page_views_per_session=VALUES(page_views_per_session), "
            "conversions=VALUES(conversions), "
            "user_conversion_rate=VALUES(user_conversion_rate), "
            "session_conversion_rate=VALUES(session_conversion_rate)"
        )
        run_sql(sql)
    print(f"  -> Inserted {len(rows)} rows into ga4_monthly_brand")


def load_channel_sessions(batch_dir: Path, prop_map: dict[str, int]):
    """Load channel_sessions CSV into ga4_monthly_channel."""
    csv_file = find_csv(batch_dir, "channel_sessions")

    rows = []
    skipped = 0
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            prop_id = row["property_id"]
            if prop_id not in prop_map:
                skipped += 1
                continue
            brand_id = prop_map[prop_id]
            year, month = parse_period(row["period"])
            channel = escape_sql(row["sessionDefaultChannelGrouping"])

            def to_int(v):
                return int(v) if v else "NULL"

            def to_dec(v):
                return v if v else "NULL"

            rows.append(
                f"({brand_id},{year},{month},'{channel}',"
                f"{to_int(row.get('sessions'))},"
                f"{to_int(row.get('totalUsers'))},"
                f"{to_int(row.get('conversions'))},"
                f"{to_dec(row.get('engagementRate'))})"
            )

    print(f"ga4_monthly_channel: {len(rows)} rows to insert ({skipped} skipped)")

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        sql = (
            "INSERT INTO ga4_monthly_channel "
            "(brand_id, year, month, channel_grouping, sessions, total_users, "
            "conversions, engagement_rate) VALUES "
            + ",".join(batch)
            + " ON DUPLICATE KEY UPDATE "
            "sessions=VALUES(sessions), total_users=VALUES(total_users), "
            "conversions=VALUES(conversions), engagement_rate=VALUES(engagement_rate)"
        )
        run_sql(sql)
    print(f"  -> Inserted {len(rows)} rows into ga4_monthly_channel")


def load_source_medium(batch_dir: Path, prop_map: dict[str, int]):
    """Load source_medium CSV into ga4_monthly_source_medium."""
    csv_file = find_csv(batch_dir, "source_medium")

    rows = []
    skipped = 0
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            prop_id = row["property_id"]
            if prop_id not in prop_map:
                skipped += 1
                continue
            brand_id = prop_map[prop_id]
            year, month = parse_period(row["period"])
            source = escape_sql(row["sessionSource"][:500])
            medium = escape_sql(row["sessionMedium"][:100])

            def to_int(v):
                return int(v) if v else "NULL"

            rows.append(
                f"({brand_id},{year},{month},'{source}','{medium}',"
                f"{to_int(row.get('sessions'))},"
                f"{to_int(row.get('totalUsers'))},"
                f"{to_int(row.get('conversions'))})"
            )

    print(f"ga4_monthly_source_medium: {len(rows)} rows to insert ({skipped} skipped)")

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        sql = (
            "INSERT INTO ga4_monthly_source_medium "
            "(brand_id, year, month, session_source, session_medium, sessions, "
            "total_users, conversions) VALUES "
            + ",".join(batch)
            + " ON DUPLICATE KEY UPDATE "
            "sessions=VALUES(sessions), total_users=VALUES(total_users), "
            "conversions=VALUES(conversions)"
        )
        run_sql(sql)
    print(f"  -> Inserted {len(rows)} rows into ga4_monthly_source_medium")


def load_referral_sources(batch_dir: Path, prop_map: dict[str, int]):
    """Load referral_sources CSV into ga4_monthly_referral."""
    csv_file = find_csv(batch_dir, "referral_sources")

    rows = []
    skipped = 0
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            prop_id = row["property_id"]
            if prop_id not in prop_map:
                skipped += 1
                continue
            brand_id = prop_map[prop_id]
            year, month = parse_period(row["period"])
            source = escape_sql(row["sessionSource"][:500])

            def to_int(v):
                return int(v) if v else "NULL"

            rows.append(
                f"({brand_id},{year},{month},'{source}',"
                f"{to_int(row.get('sessions'))},"
                f"{to_int(row.get('totalUsers'))})"
            )

    print(f"ga4_monthly_referral: {len(rows)} rows to insert ({skipped} skipped)")

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        sql = (
            "INSERT INTO ga4_monthly_referral "
            "(brand_id, year, month, referral_source, sessions, total_users) VALUES "
            + ",".join(batch)
            + " ON DUPLICATE KEY UPDATE "
            "sessions=VALUES(sessions), total_users=VALUES(total_users)"
        )
        run_sql(sql)
    print(f"  -> Inserted {len(rows)} rows into ga4_monthly_referral")


def main():
    # Determine batch directory
    if len(sys.argv) > 1:
        batch_dir = Path(sys.argv[1])
    else:
        batch_base = Path("output/batch")
        dirs = sorted([d for d in batch_base.iterdir() if d.is_dir()])
        if not dirs:
            print("No batch directories found under output/batch/", file=sys.stderr)
            sys.exit(1)
        batch_dir = dirs[-1]  # latest

    print(f"Loading from: {batch_dir}")

    # Get property_id → brand_id mapping
    prop_map = get_property_brand_map()
    print(f"Property → Brand mapping: {len(prop_map)} entries")

    # Load each table
    load_overview_conversion(batch_dir, prop_map)
    load_channel_sessions(batch_dir, prop_map)
    load_source_medium(batch_dir, prop_map)
    load_referral_sources(batch_dir, prop_map)

    print("\nDone! All tables loaded.")


if __name__ == "__main__":
    main()
