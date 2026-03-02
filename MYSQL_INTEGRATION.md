# GA4 Analytics API — MySQL Integration Instructions

## Context

This app currently queries the GA4 Data API and exports CSVs. These instructions describe how to modify it to write data directly to the Legitrix MySQL database. The database already has GA4 target tables (`ga4_import_raw`, `ga4_detail`, `ga4_monthly_brand`) and a `brand_google_channels` mapping table that links GA4 property IDs to Legitrix brand IDs.

---

## 1. Docker Compose — Join the Legitrix Network

**File:** `docker-compose.yml`

Add the `legitrix-internal` external network and attach both services to it. This lets containers resolve `mysql` hostname to the Legitrix MySQL container.

```yaml
services:
  ga4-report:
    # ... existing config ...
    networks:
      - legitrix-internal

  ga4-batch:
    # ... existing config ...
    networks:
      - legitrix-internal

networks:
  legitrix-internal:
    external: true
```

Also remove `read_only: true` from `ga4-batch` (needed for mysql-connector's temp files), or add a tmpfs for `/tmp`.

## 2. Add MySQL Dependency

**File:** `requirements.txt`

Add:
```
mysql-connector-python==9.1.0
```

Rebuild the image after: `docker compose build`

## 3. Add Database Configuration

**File:** `.env` (add these variables)
```
DB_HOST=mysql
DB_PORT=3306
DB_USER=legitrix_user
DB_PASSWORD=legitrix_pass_2024
DB_DATABASE=legitrix
```

**File:** `.env.example` (add documentation)
```
# MySQL (Legitrix shared database via legitrix-internal Docker network)
DB_HOST=mysql
DB_PORT=3306
DB_USER=legitrix_user
DB_PASSWORD=legitrix_pass_2024
DB_DATABASE=legitrix
```

Pass them through in `docker-compose.yml`:
```yaml
  ga4-batch:
    env_file: .env
    # (add env_file if not already present)
```

## 4. Create Database Module

**File:** `app/db.py` — new module for MySQL connection pooling

The module should:
- Read DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE from env vars
- Create a `mysql.connector.pooling.MySQLConnectionPool` (pool size 3-5)
- Provide a context manager `get_connection()` that yields a connection and auto-returns to pool
- Provide helper functions:
  - `insert_raw_rows(rows: list[dict])` — bulk insert into `ga4_import_raw`
  - `upsert_monthly_brand(rows: list[dict])` — upsert into `ga4_monthly_brand`

## 5. Database Target Tables (already exist)

The Legitrix database already has these tables. **Do NOT create them** — they are already present.

**`ga4_import_raw`** — Raw per-date import rows:
| Column | Type |
|--------|------|
| ga4_property_id | bigint |
| date | date |
| source_medium | varchar(150) |
| landing_page | varchar(255) |
| city | varchar(100) |
| region | varchar(100) |
| conversions | int |
| active_users | int |
| new_users | int |
| avg_session_duration | decimal(10,2) |
| bounce_rate | decimal(5,2) |
| brand_id | int |

**`ga4_monthly_brand`** — Aggregated monthly per-brand:
| Column | Type |
|--------|------|
| brand_id | int (PK) |
| year | int (PK) |
| month | int (PK) |
| conversions | int |
| active_users | int |
| new_users | int |
| avg_session_duration | decimal(10,2) |
| bounce_rate | decimal(5,2) |

**`brand_google_channels`** — Maps GA4 property IDs to Legitrix brand IDs:
| Column | Type |
|--------|------|
| brand_id | int (PK) |
| ga4_property_id | bigint unsigned |
| brand_name_google | varchar(255) |

## 6. Modify `batch.py` — Write to MySQL Instead of CSV

Current flow: GA4 API → CSV files
New flow: GA4 API → MySQL tables (+ CSV backup)

Key changes to `app/batch.py`:
1. Import `app.db` module
2. After querying each brand+period, resolve `brand_id` from `brand_google_channels` using the `property_id`
3. Insert rows into `ga4_import_raw` with the resolved `brand_id`
4. After all brands/periods complete, aggregate and upsert into `ga4_monthly_brand`
5. Keep CSV export as a backup alongside MySQL (controlled by env var `GA4_EXPORT_CSV=true`, default: true)

## 7. Brand ID Resolution

The `brand_google_channels` table maps GA4 property IDs to Legitrix brand IDs:

| GA4 App Brand | property_id | Legitrix brand_id |
|---------------|-------------|-------------------|
| Anthony | 373485153 | 19 |
| Anytime Plumbing | 297219515 | 21 |
| Apollo Home | 361108781 | 22 |
| Arctic Air | 353108280 | 26 |
| Aztec Plumbing | 261931913 | 29 |
| Bell Brothers | 361706527 | 33 |
| Black Hills | 383262078 | 41 |
| Campbell and Company | 319229254 | 54 |
| Cassell Brothers | 361657600 | 60 |
| Charles Stone Mechanical | 403757637 | 62 |
| Dauenhauer | 358767034 | 74 |
| Fayette Heating & Air | 362082264 | 83 |
| GAC Services | 309938017 | 91 |
| Home Comfort Experts | 361703861 | 111 |
| Hunter Super Techs | 373521093 | 114 |
| LimRic | 361717928 | 126 |
| Reliable Power Systems | 361692839 | 152 |
| Roth Home | 325174168 | 155 |
| Schaal | 361659621 | 160 |
| Sunny Service | 361722376 | 169 |
| Webb HVAC | 361711756 | 189 |

At startup, query `brand_google_channels` to build a `property_id → brand_id` lookup dict. Log warnings for any brands not found in the mapping.

**Add missing brand mappings** — Run these INSERTs against the Legitrix database BEFORE the first batch run:

```sql
INSERT INTO brand_google_channels (brand_id, ga4_property_id, brand_name_google) VALUES
  (33, 361706527, 'Bell Brothers - GA4'),
  (62, 403757637, 'Charles Stone Mechanical - GA4'),
  (111, 361703861, 'Home Comfort Experts - GA4'),
  (169, 361722376, 'Sunny Service - GA4')
ON DUPLICATE KEY UPDATE ga4_property_id = VALUES(ga4_property_id), brand_name_google = VALUES(brand_name_google);
```

This brings all 21 GA4 brands into the mapping table.

## 8. Report-to-Column Mapping

The GA4 app pulls 5 report types. Map their metrics to database columns:

**`overview_metrics`** report → `ga4_monthly_brand`:
- `totalUsers` → `active_users`
- `newUsers` → `new_users`
- `averageSessionDuration` → `avg_session_duration`
- `engagementRate` → inverse as `bounce_rate` (1 - engagementRate)

**`conversion_metrics`** report → `ga4_monthly_brand`:
- `conversions` → `conversions`

**`channel_sessions`** and **`source_medium`** reports → `ga4_import_raw`:
- `sessionSource` + `sessionMedium` → `source_medium` (concatenated)
- `sessions` → can map to `active_users` or store separately
- `conversions` → `conversions`

## 9. Prerequisite: Legitrix MySQL Must Be Running

Before running the GA4 batch, ensure the Legitrix infrastructure is up:

```bash
# From the legitrix repo:
cd /share/Coding/Docker/legitrix
./dev.sh infra

# Or manually:
docker compose -f docker-compose.infra.yml up -d
```

The `legitrix-internal` Docker network must exist:
```bash
docker network create legitrix-internal  # only needed once, already exists
```

## 10. Testing

```bash
# Rebuild after changes
cd /share/Coding/Docker/ga4-analytics-api
docker compose build

# Test connectivity
docker compose run --rm ga4-batch python -c "
from app.db import get_connection
with get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM brand_google_channels')
    print(f'Brands mapped: {cursor.fetchone()[0]}')
"

# Run batch with MySQL output
docker compose run --rm ga4-batch
```

## Verification

After implementation:
1. Run `docker compose run --rm ga4-batch` from the ga4-analytics-api directory
2. Check `ga4_import_raw` table has new rows: `SELECT COUNT(*) FROM ga4_import_raw;`
3. Check `ga4_monthly_brand` has aggregated data: `SELECT * FROM ga4_monthly_brand ORDER BY year DESC, month DESC LIMIT 10;`
4. Verify brand_id resolution worked (no NULLs in brand_id column for mapped brands)
