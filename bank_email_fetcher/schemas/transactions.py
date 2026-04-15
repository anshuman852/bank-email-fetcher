from pydantic import BaseModel


class TransactionNoteUpdate(BaseModel):
    note: str = ""


class TransactionNoteResponse(BaseModel):
    ok: bool
    note: str | None = None
