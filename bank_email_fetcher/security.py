"""HTTP Basic Auth for bank-email-fetcher.

Enabled when AUTH_USERNAME and AUTH_PASSWORD are both set in the environment.
When disabled, all requests pass through without authentication.
"""

import secrets
from typing import Optional

from fastapi import HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.status import HTTP_401_UNAUTHORIZED

from bank_email_fetcher.config import settings

http_basic = HTTPBasic(auto_error=False)


def check_credentials(credentials: Optional[HTTPBasicCredentials]) -> None:
    """Validate HTTP Basic credentials against configured values.

    No-op when auth is disabled. Raises HTTPException(401) on failure.
    """
    if not settings.auth_enabled:
        return

    if credentials is None:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )

    username_ok = secrets.compare_digest(
        credentials.username.encode("utf-8"),
        settings.auth_username.encode("utf-8"),
    )
    password_ok = secrets.compare_digest(
        credentials.password.encode("utf-8"),
        settings.auth_password.get_secret_value().encode("utf-8"),
    )
    if not (username_ok and password_ok):
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
