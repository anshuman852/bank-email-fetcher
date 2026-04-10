"""FastAPI dependencies for bank-email-fetcher."""

from typing import Optional

from fastapi import Depends, Request
from fastapi.security import HTTPBasicCredentials

from bank_email_fetcher.security import check_credentials, http_basic


def verify_credentials(
    request: Request,
    credentials: Optional[HTTPBasicCredentials] = Depends(http_basic),
) -> None:
    check_credentials(credentials, request)
