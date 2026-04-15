from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from bank_email_fetcher.core.deps import get_session
from bank_email_fetcher.schemas.sources import SourceTestResponse
from bank_email_fetcher.services.sources import (
    SourceNotFoundError,
    test_source_connectivity,
)

router = APIRouter()


@router.post("/sources/{source_id}/test", response_model=SourceTestResponse)
async def test_source(
    source_id: int,
    session: AsyncSession = Depends(get_session),
) -> SourceTestResponse:
    try:
        return await test_source_connectivity(session, source_id)
    except SourceNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
