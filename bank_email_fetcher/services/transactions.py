"""Transaction-domain service helpers."""

from sqlalchemy.ext.asyncio import AsyncSession

from bank_email_fetcher.db import Transaction


async def update_transaction_note(
    session: AsyncSession,
    txn_id: int,
    note: str,
) -> tuple[bool, str | None]:
    cleaned = note.strip()
    txn = await session.get(Transaction, txn_id)
    if not txn:
        return False, None
    txn.note = cleaned or None
    await session.commit()
    return True, cleaned
