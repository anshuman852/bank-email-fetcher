"""Account service helpers."""

from __future__ import annotations

import logging

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from bank_email_fetcher.db import BankStatementUpload, StatementUpload, Transaction
from bank_email_fetcher.services.linker import build_link_context, link_transaction
from bank_email_fetcher.services.statements.shared import (
    retry_bank_statement_upload,
    retry_cc_statement_upload,
)

logger = logging.getLogger(__name__)


async def auto_link_account(session: AsyncSession, account) -> None:
    """Link orphan transactions to a newly created or updated account."""
    bank_lower = account.bank.strip().lower()
    ctx = await build_link_context(session)
    stmt = (
        select(Transaction)
        .where(func.lower(Transaction.bank) == bank_lower)
        .where(Transaction.account_id.is_(None))
    )
    result = await session.execute(stmt)
    for txn in result.scalars().all():
        link_transaction(ctx, txn)
    await session.commit()


async def retry_password_required_statements(
    session: AsyncSession,
    account_id: int,
    password: str,
    *,
    retry_cc_upload=retry_cc_statement_upload,
    retry_bank_upload=retry_bank_statement_upload,
) -> dict:
    result = {"cc_retried": 0, "bank_retried": 0, "cc_failed": 0, "bank_failed": 0}
    cc_ids = (
        (
            await session.execute(
                select(StatementUpload.id).where(
                    StatementUpload.account_id == account_id,
                    StatementUpload.status == "password_required",
                )
            )
        )
        .scalars()
        .all()
    )
    bank_ids = (
        (
            await session.execute(
                select(BankStatementUpload.id).where(
                    BankStatementUpload.account_id == account_id,
                    BankStatementUpload.status == "password_required",
                )
            )
        )
        .scalars()
        .all()
    )

    for upload_id in cc_ids:
        try:
            ok = await retry_cc_upload(upload_id, password)
        except Exception as exc:
            ok = False
            logger.warning(
                "Auto-retry raised for CC statement %d on account %d: %s",
                upload_id,
                account_id,
                exc,
            )
        result["cc_retried" if ok else "cc_failed"] += 1

    for upload_id in bank_ids:
        try:
            ok = await retry_bank_upload(upload_id, password)
        except Exception as exc:
            ok = False
            logger.warning(
                "Auto-retry raised for bank statement %d on account %d: %s",
                upload_id,
                account_id,
                exc,
            )
        result["bank_retried" if ok else "bank_failed"] += 1

    return result
