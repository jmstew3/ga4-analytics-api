"""Secure credential loading — JSON-based, no unsafe deserialization."""

import json
import logging
import os
import stat

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


def load_credentials(token_path: str) -> Credentials:
    """Load and refresh OAuth2 credentials from a JSON token file.

    Raises FileNotFoundError if the token file is missing (user must run
    scripts/authenticate.py on the host first).
    """
    if not os.path.exists(token_path):
        raise FileNotFoundError(
            f"Token file not found at {token_path}. "
            "Run 'python scripts/authenticate.py' on the host to authenticate first."
        )

    creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if creds.expired and creds.refresh_token:
        logger.info("Token expired — refreshing")
        creds.refresh(Request())
        _save_token(creds, token_path)

    if not creds.valid:
        raise RuntimeError(
            "Credentials are invalid and cannot be refreshed. "
            "Re-run 'python scripts/authenticate.py' on the host."
        )

    logger.info("Credentials loaded successfully")
    return creds


def _save_token(creds: Credentials, path: str) -> None:
    """Write refreshed token as JSON with restrictive permissions."""
    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes or []),
    }
    with open(path, "w") as f:
        json.dump(token_data, f, indent=2)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)  # 0o600
    logger.info("Refreshed token saved to %s", path)
