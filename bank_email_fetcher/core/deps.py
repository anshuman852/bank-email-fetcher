"""FastAPI dependencies for bank-email-fetcher."""

from typing import Annotated, Optional

from fastapi import Depends, Request
from fastapi.security import HTTPBasicCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from bank_email_fetcher.core.security import check_credentials, http_basic
from bank_email_fetcher.db import async_session


def verify_credentials(
    request: Request,
    credentials: Optional[HTTPBasicCredentials] = Depends(http_basic),
) -> None:
    check_credentials(credentials, request)


async def get_session():
    async with async_session() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
