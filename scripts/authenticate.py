#!/usr/bin/env python3
"""Host-side OAuth browser flow.

Run this once on your machine (NOT inside Docker) to generate
credentials/token.json, which the container will mount and use.

Usage:
    python scripts/authenticate.py
    python scripts/authenticate.py --client-secret path/to/client_secret.json
"""

import argparse
import json
import os
import stat
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
DEFAULT_SECRET = os.path.join("credentials", "client_secret.json")
DEFAULT_TOKEN = os.path.join("credentials", "token.json")


def main() -> int:
    parser = argparse.ArgumentParser(description="GA4 OAuth2 authentication")
    parser.add_argument(
        "--client-secret",
        default=DEFAULT_SECRET,
        help=f"Path to client_secret.json (default: {DEFAULT_SECRET})",
    )
    parser.add_argument(
        "--token-output",
        default=DEFAULT_TOKEN,
        help=f"Output path for token.json (default: {DEFAULT_TOKEN})",
    )
    args = parser.parse_args()

    use_env_vars = False
    if os.path.exists(args.client_secret):
        print(f"Using client secret from {args.client_secret}")
    elif os.environ.get("GA4_CLIENT_ID") and os.environ.get("GA4_CLIENT_SECRET"):
        use_env_vars = True
        print("Using credentials from GA4_CLIENT_ID / GA4_CLIENT_SECRET env vars")
    else:
        print(
            f"ERROR: Client secret not found at '{args.client_secret}'\n"
            "and GA4_CLIENT_ID / GA4_CLIENT_SECRET env vars are not set.\n\n"
            "Either:\n"
            "  1. Download client_secret.json from Google Cloud Console\n"
            "     and place it in the credentials/ directory, OR\n"
            "  2. Set GA4_CLIENT_ID and GA4_CLIENT_SECRET in your .env file\n"
            "     then run: set -a && source .env && set +a && python scripts/authenticate.py",
            file=sys.stderr,
        )
        return 1

    print("Starting OAuth authentication flow...")
    print("A browser window will open — sign in with your Google account.\n")

    if use_env_vars:
        client_config = {
            "installed": {
                "client_id": os.environ["GA4_CLIENT_ID"],
                "client_secret": os.environ["GA4_CLIENT_SECRET"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost"],
            }
        }
        flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            args.client_secret, SCOPES
        )
    flow.redirect_uri = "http://localhost:8085/"
    creds = flow.run_local_server(
        port=8085,
        access_type="offline",
        prompt="consent",
    )

    # Save as JSON (secure, data-only — no unsafe deserialization)
    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes or []),
    }

    os.makedirs(os.path.dirname(args.token_output), exist_ok=True)
    with open(args.token_output, "w") as f:
        json.dump(token_data, f, indent=2)
    os.chmod(args.token_output, stat.S_IRUSR | stat.S_IWUSR)  # 0o600

    print(f"\nAuthentication successful!")
    print(f"Token saved to {args.token_output}")
    print("You can now run: docker compose run ga4-report")
    return 0


if __name__ == "__main__":
    sys.exit(main())
