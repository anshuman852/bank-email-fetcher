"""SQLAlchemy async models and database initialisation for bank-email-fetcher.

Defines all ORM models (EmailSource, Account, Card, FetchRule, Email,
StatementUpload, BankStatementUpload, Transaction) and the async
engine/session factory.

init_db() is called at startup (via app.py lifespan). It runs create_all
then applies inline schema migrations for new columns via try/except
ALTER TABLE blocks. There is no Alembic; adding a new column to a model
requires a corresponding migration block in init_db().

A one-time SQLite migration removes a legacy unique constraint
(uq_transaction_dedup) that was replaced by a partial index on
(bank, reference_number) where reference_number IS NOT NULL.
"""

import datetime
import logging
from enum import StrEnum

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    DateTime,
    Time,
    UniqueConstraint,
    ForeignKey,
    text,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, relationship

from bank_email_fetcher.config import settings

engine = create_async_engine(settings.db_url, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class PaymentStatus(StrEnum):
    UNPAID = "unpaid"
    PARTIALLY_PAID = "partially_paid"
    PAID = "paid"
    LATE = "late"


class EmailSource(Base):
    __tablename__ = "email_sources"

    id = Column(Integer, primary_key=True)
    provider = Column(String, nullable=False)  # 'gmail' or 'fastmail'
    label = Column(String, nullable=False)
    account_identifier = Column(String)  # email address, plaintext for display
    credentials = Column(String, nullable=False)  # Fernet-encrypted JSON
    active = Column(Boolean, default=True)
    sync_cursor = Column(String)  # opaque provider state
    last_synced_at = Column(DateTime)
    last_error = Column(String)


class Account(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True)
    bank = Column(String, nullable=False)
    label = Column(String, nullable=False)
    type = Column(String, nullable=False)  # credit_card, debit_card, bank_account
    account_number = Column(String)
    statement_password = Column(String)  # Fernet-encrypted, for statement PDFs
    statement_password_hint = Column(String)  # e.g., "Date of birth in DDMMYYYY format"
    active = Column(Boolean, default=True)

    cards = relationship(
        "Card", lazy="selectin", order_by="Card.is_primary.desc(), Card.id"
    )


class Card(Base):
    __tablename__ = "cards"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    card_mask = Column(String, nullable=False)
    label = Column(String)  # "self", "spouse", etc.
    is_primary = Column(Boolean, default=False)
    active = Column(Boolean, default=True)

    __table_args__ = (
        UniqueConstraint("account_id", "card_mask", name="uq_card_account_mask"),
        Index("ix_cards_card_mask", "card_mask"),
    )


class FetchRule(Base):
    __tablename__ = "fetch_rules"

    id = Column(Integer, primary_key=True)
    provider = Column(String, nullable=False)  # gmail or fastmail
    source_id = Column(Integer, ForeignKey("email_sources.id"), nullable=True)
    sender = Column(String)
    subject = Column(String)
    bank = Column(String, nullable=False)
    folder = Column(String)
    email_kind = Column(String)  # "transaction", "statement", or NULL (try both)
    enabled = Column(Boolean, default=True)
    initial_backfill_done_at = Column(DateTime)  # NULL = needs full historical scan

    source = relationship("EmailSource", lazy="joined")


class Email(Base):
    __tablename__ = "emails"

    id = Column(Integer, primary_key=True)
    provider = Column(String, nullable=False)
    message_id = Column(String, nullable=False, unique=True)
    source_id = Column(Integer, ForeignKey("email_sources.id"), nullable=True)
    remote_id = Column(String)  # provider's message ID
    sender = Column(String)
    subject = Column(String)
    received_at = Column(DateTime)
    fetched_at = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(
        String, default="pending", index=True
    )  # pending, parsed, failed, skipped
    error = Column(Text)
    rule_id = Column(Integer, ForeignKey("fetch_rules.id"))

    __table_args__ = (
        Index("ix_emails_fetched_at", "fetched_at"),
        UniqueConstraint("source_id", "remote_id", name="uq_email_source_remote"),
    )


class StatementUpload(Base):
    __tablename__ = "statement_uploads"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    email_id = Column(Integer, ForeignKey("emails.id"), nullable=True)
    bank = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    status = Column(
        String, nullable=False, default="parsed"
    )  # parsed, password_required, parse_error, imported, partial_import
    card_number = Column(String)
    statement_name = Column(String)
    due_date = Column(String)
    total_amount_due = Column(String)
    parsed_txn_count = Column(Integer, default=0)
    matched_count = Column(Integer, default=0)
    missing_count = Column(Integer, default=0)
    imported_count = Column(Integer, default=0)
    reconciliation_data = Column(Text)  # JSON
    error = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    # Payment tracking (populated when due_date is present)
    payment_status = Column(String)  # PaymentStatus enum value, NULL = no due date
    payment_sent_offsets = Column(
        Text, default="[]"
    )  # JSON list of reminder day-offsets already sent
    payment_last_reminded_at = Column(DateTime)
    payment_paid_at = Column(DateTime)
    payment_paid_amount = Column(Numeric(precision=12, scale=2), default=0)

    account = relationship("Account", lazy="joined")


class BankStatementUpload(Base):
    __tablename__ = "bank_statement_uploads"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    email_id = Column(Integer, ForeignKey("emails.id"), nullable=True)
    bank = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    status = Column(
        String, nullable=False, default="parsed"
    )  # parsed, password_required, parse_error, imported, partial_import
    account_number = Column(String)
    account_holder_name = Column(String)
    opening_balance = Column(String)
    closing_balance = Column(String)
    statement_period_start = Column(String)
    statement_period_end = Column(String)
    parsed_txn_count = Column(Integer, default=0)
    matched_count = Column(Integer, default=0)
    missing_count = Column(Integer, default=0)
    imported_count = Column(Integer, default=0)
    reconciliation_data = Column(Text)  # JSON
    error = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    account = relationship("Account", lazy="joined")


class Setting(Base):
    __tablename__ = "settings"

    key = Column(String, primary_key=True)
    value = Column(Text, nullable=False, default="")
    updated_at = Column(
        DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow
    )


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True)
    email_id = Column(Integer, ForeignKey("emails.id"))
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=True)
    card_id = Column(Integer, ForeignKey("cards.id"), nullable=True)

    statement_upload_id = Column(
        Integer, ForeignKey("statement_uploads.id"), nullable=True
    )
    bank_statement_upload_id = Column(
        Integer, ForeignKey("bank_statement_uploads.id"), nullable=True
    )

    account = relationship("Account", lazy="joined")
    card = relationship("Card", lazy="joined")

    bank = Column(String, nullable=False)
    email_type = Column(String, nullable=False)
    direction = Column(String, nullable=False)  # debit/credit
    amount = Column(Numeric(precision=12, scale=2), nullable=False)
    currency = Column(String, default="INR")
    transaction_date = Column(Date)
    transaction_time = Column(Time)
    counterparty = Column(String)
    card_mask = Column(String)
    account_mask = Column(String)
    reference_number = Column(String)
    channel = Column(String)
    balance = Column(Numeric(precision=12, scale=2))
    raw_description = Column(Text)
    note = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        Index("ix_transactions_transaction_date", "transaction_date"),
        Index("ix_transactions_bank", "bank"),
        # Reference numbers (UTR, UPI ref, etc.) are unique per-transaction within the
        # banking system. NACH/UMRN references are NOT unique per-transaction (one mandate
        # can produce multiple debits), so they are nullified before insertion and stored
        # only in raw_description. SQLite treats NULLs as distinct, so this partial index
        # only applies when reference_number is set.
        Index(
            "uq_transactions_ref",
            "bank",
            "reference_number",
            unique=True,
            sqlite_where=text("reference_number IS NOT NULL"),
        ),
    )


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Add missing columns for schema migrations
    async with engine.begin() as conn:
        try:
            await conn.execute(text("SELECT note FROM transactions LIMIT 0"))
        except Exception:
            await conn.execute(text("ALTER TABLE transactions ADD COLUMN note TEXT"))
        try:
            await conn.execute(
                text("SELECT statement_upload_id FROM transactions LIMIT 0")
            )
        except Exception:
            await conn.execute(
                text(
                    "ALTER TABLE transactions ADD COLUMN statement_upload_id INTEGER REFERENCES statement_uploads(id)"
                )
            )
        try:
            await conn.execute(text("SELECT statement_password FROM accounts LIMIT 0"))
        except Exception:
            await conn.execute(
                text("ALTER TABLE accounts ADD COLUMN statement_password VARCHAR")
            )
        try:
            await conn.execute(
                text("SELECT initial_backfill_done_at FROM fetch_rules LIMIT 0")
            )
        except Exception:
            await conn.execute(
                text(
                    "ALTER TABLE fetch_rules ADD COLUMN initial_backfill_done_at DATETIME"
                )
            )
            # Mark existing rules that already have emails as backfilled
            await conn.execute(
                text(
                    "UPDATE fetch_rules SET initial_backfill_done_at = CURRENT_TIMESTAMP "
                    "WHERE id IN (SELECT DISTINCT rule_id FROM emails WHERE rule_id IS NOT NULL)"
                )
            )
        try:
            await conn.execute(text("SELECT email_id FROM statement_uploads LIMIT 0"))
        except Exception:
            await conn.execute(
                text(
                    "ALTER TABLE statement_uploads ADD COLUMN email_id INTEGER REFERENCES emails(id)"
                )
            )
        for col in (
            "payment_status",
            "payment_sent_offsets",
            "payment_last_reminded_at",
            "payment_paid_at",
        ):
            try:
                await conn.execute(text(f"SELECT {col} FROM statement_uploads LIMIT 0"))
            except Exception:
                default = " DEFAULT '[]'" if col == "payment_sent_offsets" else ""
                await conn.execute(
                    text(
                        f"ALTER TABLE statement_uploads ADD COLUMN {col} {'TEXT' if 'offsets' in col else 'VARCHAR' if col == 'payment_status' else 'DATETIME'}{default}"
                    )
                )
        try:
            await conn.execute(
                text("SELECT payment_paid_amount FROM statement_uploads LIMIT 0")
            )
        except Exception:
            await conn.execute(
                text(
                    "ALTER TABLE statement_uploads ADD COLUMN payment_paid_amount NUMERIC(12,2) DEFAULT 0"
                )
            )
        # Backfill: initialize payment tracking for statements created this month
        await conn.execute(
            text(
                "UPDATE statement_uploads SET payment_status = 'unpaid' "
                "WHERE due_date IS NOT NULL AND due_date != '' "
                "AND total_amount_due IS NOT NULL AND total_amount_due != '' "
                "AND payment_status IS NULL "
                "AND created_at >= date('now', 'start of month')"
            )
        )

        # bank_statement_upload_id FK on transactions
        try:
            await conn.execute(
                text("SELECT bank_statement_upload_id FROM transactions LIMIT 0")
            )
        except Exception:
            await conn.execute(
                text(
                    "ALTER TABLE transactions ADD COLUMN bank_statement_upload_id INTEGER REFERENCES bank_statement_uploads(id)"
                )
            )

        # email_kind on fetch_rules ("transaction", "statement", or NULL)
        try:
            await conn.execute(text("SELECT email_kind FROM fetch_rules LIMIT 0"))
        except Exception:
            await conn.execute(
                text("ALTER TABLE fetch_rules ADD COLUMN email_kind VARCHAR")
            )

        # statement_password_hint on accounts
        try:
            await conn.execute(
                text("SELECT statement_password_hint FROM accounts LIMIT 0")
            )
        except Exception:
            await conn.execute(
                text("ALTER TABLE accounts ADD COLUMN statement_password_hint VARCHAR")
            )

        # NACH/UMRN references are mandate-level identifiers, not per-transaction unique refs.
        # Nullify reference_number for NACH transactions so the partial unique dedup index
        # doesn't reject legitimate repeat debits under the same mandate. Gated behind a
        # one-shot marker so we don't scan the transactions table on every boot.
        nach_marker = (
            await conn.execute(
                text("SELECT 1 FROM settings WHERE key = 'migrations.nach_ref_nullified'")
            )
        ).first()
        if not nach_marker:
            await conn.execute(
                text(
                    "UPDATE transactions SET reference_number = NULL "
                    "WHERE reference_number IS NOT NULL "
                    "AND (channel = 'nach' OR email_type LIKE '%nach%')"
                )
            )
            await conn.execute(
                text(
                    "INSERT INTO settings (key, value) VALUES "
                    "('migrations.nach_ref_nullified', '1')"
                )
            )

    # Populate in-memory settings cache
    from bank_email_fetcher.settings_service import load_all_settings

    await load_all_settings()
