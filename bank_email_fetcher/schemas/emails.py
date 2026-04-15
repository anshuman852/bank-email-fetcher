"""Email endpoint response schemas."""

from pydantic import BaseModel


class ReparseEmailResponse(BaseModel):
    ok: bool
    error: str | None = None
    message: str | None = None
    new_status: str | None = None
    txn_id: int | None = None


class ReparseAllFailedResponse(BaseModel):
    ok: bool
    succeeded: int
    skipped: int
    failed: int
