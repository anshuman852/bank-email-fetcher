#!/usr/bin/env python
"""Seed the database with known fetch rules for all supported banks.

Idempotent -- safe to run multiple times. Skips rules that already exist.
Does NOT set up email source credentials -- add those via the web UI at /sources.

On first run, generates a Fernet encryption key and writes it to .env if one
doesn't already exist.

Usage:
    uv run python seed.py
"""
import asyncio
from pathlib import Path

from sqlalchemy import select

from bank_email_fetcher.db import EmailSource, FetchRule, init_db, async_session


def _ensure_fernet_key():
    """Generate EMAIL_SOURCE_MASTER_KEY and write to .env if not already set."""
    from bank_email_fetcher.config import Settings

    if Settings().email_source_master_key:
        return

    from cryptography.fernet import Fernet
    key = Fernet.generate_key().decode()

    env_path = Path(".env")
    with env_path.open("a") as f:
        f.write(f"EMAIL_SOURCE_MASTER_KEY={key}\n")
    print("Generated Fernet key and wrote to .env")

# (provider, bank, sender, subject_filter, folder)
RULES = [
    # Slice (Gmail)
    ("gmail", "slice", "noreply@slice.bank.in", None, None),
    ("gmail", "slice", "noreply@sliceit.com", None, None),

    # ICICI (Gmail)
    ("gmail", "icici", "credit_cards@icicibank.com", None, None),
    ("gmail", "icici", "customernotification@icici.bank.in", None, None),
    ("gmail", "icici", "customercare@icicibank.com", None, None),
    ("gmail", "icici", "customercare@icicibank.com", "Transaction alert", None),
    # CC e-statements (PDF attachments processed by cc-parser)
    ("gmail", "icici", "credit_cards@icici.bank.in", "ICICI Bank Credit Card Statement", None),

    # HDFC (Gmail)
    ("gmail", "hdfc", "alerts@hdfcbank.bank.in", None, None),
    # CC e-statements (PDF attachments processed by cc-parser)
    ("gmail", "hdfc", "Emailstatements.cards@hdfcbank.net", "statement", None),

    # Axis (Gmail)
    ("gmail", "axis", "alerts@axis.bank.in", None, None),
    # CC e-statements (PDF attachments processed by cc-parser)
    ("gmail", "axis", "cc.statements@axis.bank.in", "statement", None),

    # IndusInd (Gmail)
    ("gmail", "indusind", "transactionalert@indusind.com", None, None),
    ("gmail", "indusind", "indusind_bank@indusind.com", None, None),
    ("gmail", "indusind", "indusind_bank@indusind.com", "Transaction", None),
    ("gmail", "indusind", "IndusInd_Bank@indusind.com", "Transaction", None),
    ("gmail", "indusind", "indusind_bank@indusind.com", "Payment Confirmation", None),
    ("gmail", "indusind", "IndusInd_Bank@indusind.com", "Payment Confirmation", None),
    # CC e-statements (PDF attachments processed by cc-parser)
    ("gmail", "indusind", "creditcard.estatements@indusind.com", "statement", None),

    # Kotak (Gmail)
    ("gmail", "kotak", "BankAlerts@kotak.com", None, None),
    ("gmail", "kotak", "no-reply@kotak.com", None, None),

    # SBI Card (Gmail)
    ("gmail", "sbi", "onlinesbicard@sbicard.com", None, None),
    ("gmail", "sbi", "paynet@billdesk.in", None, None),
    # CC e-statements (PDF attachments processed by cc-parser)
    ("gmail", "sbi", "Statements@sbicard.com", "statement", None),

    # HSBC (Gmail)
    ("gmail", "hsbc", "hsbc@mail.hsbc.co.in", None, None),
    ("gmail", "hsbc", "hsbc@mail.hsbc.co.in", "Credit Card", None),
    ("gmail", "hsbc", "alerts@mail.hsbc.co.in", None, None),
    ("gmail", "hsbc", "alerts@mail.hsbc.co.in", "Credit Card", None),
    # CC e-statements (PDF attachments processed by cc-parser)
    ("gmail", "hsbc", "creditcardstatement@mail.hsbc.co.in", "statement", None),
    ("gmail", "hsbc", "campaign@mail.hsbc.co.in", "statement", None),

    # IDFC FIRST (Gmail)
    ("gmail", "idfc", "transaction.alerts@idfcfirstbank.com", None, None),
    ("gmail", "idfc", "noreply@idfcfirstbank.com", None, None),
    # CC e-statements (PDF attachments processed by cc-parser)
    ("gmail", "idfc", "statement@idfcfirst.bank.in", "statement", None),

    # Equitas (Gmail)
    ("gmail", "equitas", "cc-alerts@equitas.bank.in", None, None),

    # OneCard / BOBCARD (Gmail)
    ("gmail", "onecard", "no-reply@getonecard.app", None, None),
    # CC e-statements (PDF attachments processed by cc-parser)
    ("gmail", "onecard", "statement@getonecard.app", "statement", None),

    # Union Bank of India (Gmail)
    ("gmail", "uboi", "noreplyunionbankofindia@unionbankofindia.bank.in", None, None),
    ("gmail", "uboi", "loanemail@unionbankofindia.bank", None, None),
]


async def main():
    await init_db()
    async with async_session() as session:
        existing = await session.execute(select(FetchRule))
        existing_keys = {(r.provider, r.sender, r.subject) for r in existing.scalars().all()}

        added = 0
        for provider, bank, sender, subject, folder in RULES:
            if (provider, sender, subject) in existing_keys:
                continue
            session.add(FetchRule(
                provider=provider,
                bank=bank,
                sender=sender,
                subject=subject,
                folder=folder,
                enabled=True,
            ))
            added += 1

        await session.commit()
        total = len(existing_keys) + added
        print(f"Added {added} rules ({len(existing_keys)} already existed, {total} total)")

    # Auto-assign source_id to rules that don't have one
    async with async_session() as session:
        sources = {s.provider: s.id for s in (await session.execute(select(EmailSource))).scalars().all()}
        if sources:
            orphan_rules = (await session.execute(
                select(FetchRule).where(FetchRule.source_id.is_(None))
            )).scalars().all()
            linked = 0
            for rule in orphan_rules:
                if rule.provider in sources:
                    rule.source_id = sources[rule.provider]
                    linked += 1
            await session.commit()
            if linked:
                print(f"Auto-linked {linked} rules to existing sources")
        else:
            print()
            print("NOTE: No email sources found. Add them at /sources, then re-run seed.py to auto-link.")


if __name__ == "__main__":
    _ensure_fernet_key()
    asyncio.run(main())
