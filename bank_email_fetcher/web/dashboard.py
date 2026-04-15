"""Dashboard HTML routes."""

from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, Depends, Request as FastAPIRequest
from fastapi.responses import HTMLResponse
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from bank_email_fetcher.core.deps import get_session
from bank_email_fetcher.core.templating import get_templates
from bank_email_fetcher.db import (
    Email,
    FetchRule,
    Transaction,
)
from bank_email_fetcher.integrations.parsers import get_supported_banks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

templates = get_templates()
SUPPORTED_BANKS = get_supported_banks()
router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: FastAPIRequest,
    session: AsyncSession = Depends(get_session),
):
    fetch_service = getattr(request.app.state, "fetch_service", None)
    today = date.today()
    month_start = today.replace(day=1)

    row = (
        await session.execute(
            select(
                func.count(Transaction.id),
                func.coalesce(
                    func.sum(
                        case(
                            (Transaction.direction == "debit", Transaction.amount),
                            else_=0,
                        )
                    ),
                    0,
                ),
                func.coalesce(
                    func.sum(
                        case(
                            (Transaction.direction == "credit", Transaction.amount),
                            else_=0,
                        )
                    ),
                    0,
                ),
            ).where(Transaction.transaction_date >= month_start)
        )
    ).one()
    month_txns, month_debit, month_credit = row
    net_flow = month_credit - month_debit

    total_emails = (await session.execute(select(func.count(Email.id)))).scalar() or 0
    active_rules = (
        await session.execute(
            select(func.count(FetchRule.id)).where(FetchRule.enabled.is_(True))
        )
    ).scalar() or 0

    result = await session.execute(
        select(Transaction)
        .order_by(
            Transaction.transaction_date.desc().nullslast(), Transaction.id.desc()
        )
        .limit(20)
    )
    transactions = result.scalars().all()

    period_label = month_start.strftime("%B %Y")

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "active_page": "dashboard",
            "poll_status": (
                fetch_service.get_poll_status()
                if fetch_service
                else {
                    "state": "idle",
                    "started_at": None,
                    "finished_at": None,
                    "last_stats": None,
                    "last_error": None,
                    "progress": None,
                }
            ),
            "stats": {
                "month_debit": month_debit,
                "month_credit": month_credit,
                "month_transactions": month_txns,
                "net_flow": net_flow,
                "period_label": period_label,
            },
            "ops_stats": {
                "total_emails": total_emails,
                "active_rules": active_rules,
            },
            "transactions": transactions,
        },
    )
