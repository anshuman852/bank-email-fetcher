"""Fernet encryption helpers for email source credentials.

Provides thin wrappers around Fernet for storing and retrieving email
source credentials (Gmail app password, Fastmail API token) as encrypted
JSON strings in the database.

Note: CC statement passwords are stored directly (not as JSON dicts), so
they use get_fernet() directly rather than these helpers.
"""

import json

from bank_email_fetcher.config import get_fernet


def encrypt_credentials(creds: dict) -> str:
    """Encrypt a credentials dict to a Fernet token string."""
    f = get_fernet()
    return f.encrypt(json.dumps(creds).encode()).decode()


def decrypt_credentials(token: str) -> dict:
    """Decrypt a Fernet token string back to a credentials dict."""
    f = get_fernet()
    return json.loads(f.decrypt(token.encode()))
