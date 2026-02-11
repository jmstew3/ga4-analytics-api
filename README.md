# GA4 Analytics API

A Dockerized Python tool that queries the Google Analytics 4 Data API and exports report data to CSV files.

## How It Works

The application runs in two phases:

1. **Authenticate (host machine)** — A one-time OAuth2 browser flow (`scripts/authenticate.py`) opens your browser, has you sign in with Google, and saves the resulting token to `credentials/token.json`.

2. **Run reports (Docker container)** — Two modes are available:
   - **Single report** — Query one GA4 property for a single date range. Configured via `.env`.
   - **Batch report** — Query multiple brands, date ranges, and report types in one run. Configured via `batch_config.json`.

### Architecture

```
scripts/authenticate.py   →  credentials/token.json  (one-time, runs on host)

docker compose run ga4-report          (single property)
  app/config.py            →  Load & validate env vars (Pydantic)
  app/auth.py              →  Load token, auto-refresh if expired
  app/report.py            →  Query GA4 Data API (BetaAnalyticsDataClient)
  app/export.py            →  Write results to timestamped CSV
  app/main.py              →  Orchestrates the above pipeline

docker compose run ga4-batch           (multi-brand portfolio)
  app/batch.py             →  Load batch_config.json, loop brands × dates × reports
  app/auth.py              →  Load token, auto-refresh if expired
  app/report.py            →  Query GA4 Data API (reused from single mode)
  output/batch/            →  One CSV per report type, timestamped
```

## Prerequisites

- **Python 3.12+** (for the authentication script)
- **Docker** and **Docker Compose**
- A **Google Cloud project** with the GA4 Data API enabled
- An **OAuth 2.0 Client ID** (Desktop type) downloaded as `client_secret.json`

## Setup

### 1. Get your OAuth client secret

1. Go to [Google Cloud Console > APIs & Services > Credentials](https://console.cloud.google.com/apis/credentials)
2. Create an OAuth 2.0 Client ID (application type: **Desktop app**)
3. Download the JSON and save it as `credentials/client_secret.json`

### 2. Authenticate

Run the authentication script on your host machine (not inside Docker) — it needs a browser:

```bash
pip install google-auth-oauthlib==1.2.4
python scripts/authenticate.py
```

A browser window opens. Sign in with the Google account that has access to your GA4 property. On success, `credentials/token.json` is created.

Optional arguments:

```
--client-secret PATH   Path to client_secret.json (default: credentials/client_secret.json)
--token-output PATH    Output path for token.json (default: credentials/token.json)
```

### 3. Configure

Copy the example env file and set your property ID:

```bash
cp .env.example .env
```

Edit `.env`:

```env
# Required — your GA4 property ID (numeric), found in GA4 Admin > Property Settings
GA4_PROPERTY_ID=123456789

# Optional — all have defaults
GA4_START_DATE=30daysAgo
GA4_END_DATE=today
GA4_DIMENSIONS=country,city
GA4_METRICS=activeUsers,sessions
GA4_OUTPUT_PREFIX=ga4_report
GA4_CSV_DELIMITER=,
```

| Variable | Required | Default | Description |
|---|---|---|---|
| `GA4_PROPERTY_ID` | Yes | — | Numeric GA4 property ID |
| `GA4_START_DATE` | No | `30daysAgo` | `YYYY-MM-DD`, `today`, `yesterday`, or `NdaysAgo` |
| `GA4_END_DATE` | No | `today` | Same format as start date |
| `GA4_DIMENSIONS` | No | `country,city` | Comma-separated GA4 dimension names (max 20) |
| `GA4_METRICS` | No | `activeUsers,sessions` | Comma-separated GA4 metric names (max 20) |
| `GA4_OUTPUT_PREFIX` | No | `ga4_report` | Filename prefix for the CSV |
| `GA4_CSV_DELIMITER` | No | `,` | One of: `,` `;` `\t` `\|` |

For available dimensions and metrics, see the [GA4 API schema](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema).

### 4. Run

There are two ways to pull reports: **single** (one property, one date range) and **batch** (many brands, date ranges, and report types at once).

#### Option A: Single Report

Uses the `.env` file for configuration. Good for quick, ad-hoc pulls from a single GA4 property.

```bash
docker compose run ga4-report
```

The CSV appears in `output/`, named like `ga4_report_20260211_183045.csv`.

#### Option B: Batch Report

Uses `batch_config.json` for configuration. Runs every combination of brand × date range × report type and writes one CSV per report type to `output/batch/`.

1. Edit `batch_config.json` to define your brands, date ranges, and reports:

```jsonc
{
  "date_ranges": [
    { "label": "FY2025", "start_date": "2025-01-01", "end_date": "2025-12-31" },
    { "label": "Jan2026", "start_date": "2026-01-01", "end_date": "2026-01-31" }
  ],
  "reports": [
    {
      "name": "overview_metrics",
      "dimensions": [],
      "metrics": ["totalUsers", "sessions", "engagementRate"]
    },
    {
      "name": "channel_sessions",
      "dimensions": ["sessionDefaultChannelGrouping"],
      "metrics": ["sessions", "totalUsers", "conversions"],
      "dimension_filter": {                          // optional
        "field": "sessionDefaultChannelGrouping",
        "match_type": "EXACT",
        "value": "Referral"
      }
    }
  ],
  "brands": [
    { "name": "Acme Corp", "account_id": "123456", "property_id": "789012345" }
  ]
}
```

2. Run the batch:

```bash
docker compose run ga4-batch
```

CSVs appear in `output/batch/`, one per report type (e.g. `overview_metrics_20260211_201353.csv`). Each row includes `brand_name`, `property_id`, and `period` columns so all brands are in one file.

The batch service has higher resource limits (512 MB / 1 CPU) and includes a 1-second delay between API calls to respect GA4 quotas.

#### Rebuilding

To rebuild after code changes:

```bash
docker compose build && docker compose run ga4-report   # or ga4-batch
```

## Project Structure

```
.
├── app/
│   ├── __init__.py
│   ├── main.py          # Single-report entrypoint — config, auth, query, export
│   ├── batch.py         # Batch-report entrypoint — multi-brand portfolio runner
│   ├── config.py         # Pydantic settings with env var validation
│   ├── auth.py           # OAuth2 credential loading and token refresh
│   ├── report.py         # GA4 Data API query execution (shared by both modes)
│   └── export.py         # Timestamped CSV export
├── credentials/          # OAuth files (gitignored)
│   └── .gitkeep
├── output/               # CSV output directory (gitignored)
│   └── .gitkeep
├── scripts/
│   └── authenticate.py   # One-time host-side OAuth browser flow
├── batch_config.json     # Batch mode configuration (brands, date ranges, reports)
├── .env.example          # Environment variable template (single mode)
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Security

The Docker setup follows a hardened configuration:

- **Non-root user** inside the container
- **Read-only filesystem** with a tmpfs for `/tmp`
- **All Linux capabilities dropped**
- **`no-new-privileges`** security option
- **Resource limits**: 256 MB / 0.5 CPU (single), 512 MB / 1 CPU (batch)
- Token files are written with `0600` permissions (owner read/write only)
- Credentials directory and `.env` are gitignored
- Unsafe serialization formats (`.pkl`) are blocked via `.gitignore` — only JSON tokens are used

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Token file not found` | `credentials/token.json` doesn't exist | Run `python scripts/authenticate.py` |
| `Configuration error` | Missing or invalid `GA4_PROPERTY_ID` | Check your `.env` file |
| `Credentials are invalid and cannot be refreshed` | Expired refresh token or revoked access | Re-run `python scripts/authenticate.py` |
| `GA4 query error` | Invalid dimensions/metrics or no API access | Verify dimension/metric names and that the GA4 Data API is enabled |
| `No brands have property_id filled in` | All brands in `batch_config.json` still have placeholder IDs | Add real GA4 property IDs to the `brands` array |

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `google-analytics-data` | 0.20.0 | GA4 Data API client |
| `google-auth` | 2.48.0 | OAuth2 credential handling |
| `google-auth-oauthlib` | 1.2.4 | OAuth2 browser flow (auth script) |
| `pydantic-settings` | 2.12.0 | Environment variable validation |
