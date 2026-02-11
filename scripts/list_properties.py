#!/usr/bin/env python3
"""List all GA4 property IDs across every account.

Uses the Google Analytics Admin API with existing OAuth2 credentials.

Usage:
    python scripts/list_properties.py
"""

import csv
import os
import sys
from datetime import datetime

from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
TOKEN_PATH = os.path.join("credentials", "token.json")


def load_credentials() -> Credentials:
    if not os.path.exists(TOKEN_PATH):
        print(
            f"ERROR: Token file not found at '{TOKEN_PATH}'.\n"
            "Run 'python scripts/authenticate.py' first.",
            file=sys.stderr,
        )
        sys.exit(1)

    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds.valid:
        print(
            "ERROR: Credentials are invalid and cannot be refreshed.\n"
            "Re-run 'python scripts/authenticate.py'.",
            file=sys.stderr,
        )
        sys.exit(1)

    return creds


def main() -> int:
    creds = load_credentials()
    client = AnalyticsAdminServiceClient(credentials=creds)

    summaries = client.list_account_summaries()

    rows = []
    for account in summaries:
        print(f"\n  Account: {account.display_name} ({account.account})")
        print("  " + "-" * 60)
        for prop in account.property_summaries:
            prop_id = prop.property.split("/")[-1]
            print(f"    {prop_id}  {prop.display_name}")
            rows.append((account.display_name, prop_id, prop.display_name))

    print(f"\n  Total properties: {len(rows)}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join("output", f"ga4_properties_{timestamp}.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["account_name", "property_id", "property_name"])
        writer.writerows(rows)

    print(f"  Saved to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
