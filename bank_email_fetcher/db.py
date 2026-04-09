"""SQLAlchemy async models and database initialisation for bank-email-fetcher.

Defines all ORM models (EmailSource, Account, Card, FetchRule, Email,
StatementUpload, Transaction) and the async engine/session factory.

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

from sqlalchemy import Boolean, Column, Date, Index, Integer, Numeric, String, Text, DateTime, Time, UniqueConstraint, ForeignKey, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, relationship

from bank_email_fetcher.config import settings

engine = create_async_engine(settings.db_url, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


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
    statement_password = Column(String)  # Fernet-encrypted, for CC statement PDFs
    active = Column(Boolean, default=True)

    cards = relationship("Card", lazy="selectin", order_by="Card.is_primary.desc(), Card.id")


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
    status = Column(String, default="pending", index=True)  # pending, parsed, failed, skipped
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
    status = Column(String, nullable=False, default="parsed")  # parsed, password_required, parse_error, imported, partial_import
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

    account = relationship("Account", lazy="joined")


class Setting(Base):
    __tablename__ = "settings"

    key = Column(String, primary_key=True)
    value = Column(Text, nullable=False, default="")
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True)
    email_id = Column(Integer, ForeignKey("emails.id"))
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=True)
    card_id = Column(Integer, ForeignKey("cards.id"), nullable=True)

    statement_upload_id = Column(Integer, ForeignKey("statement_uploads.id"), nullable=True)

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
        # Reference numbers (UTR, UPI ref, etc.) are unique within the banking system
        # SQLite treats NULLs as distinct, so this only applies when reference_number is set
        Index("uq_transactions_ref", "bank", "reference_number", unique=True, sqlite_where=text("reference_number IS NOT NULL")),
    )


def _sqlite_transactions_has_legacy_dedup_constraint(sync_conn) -> bool:
    row = sync_conn.exec_driver_sql(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'transactions'"
    ).fetchone()
    if row is None or row[0] is None:
        return False
    return "uq_transaction_dedup" in row[0]


def _migrate_sqlite_transactions_table(sync_conn) -> None:
    if sync_conn.dialect.name != "sqlite":
        return
    if not _sqlite_transactions_has_legacy_dedup_constraint(sync_conn):
        return

    logger.warning("Removing legacy transaction dedup constraint from SQLite database")
    sync_conn.exec_driver_sql("PRAGMA foreign_keys=OFF")
    try:
        sync_conn.exec_driver_sql(
            """
            CREATE TABLE transactions__new (
                id INTEGER NOT NULL,
                email_id INTEGER,
                account_id INTEGER,
                card_id INTEGER,
                bank VARCHAR NOT NULL,
                email_type VARCHAR NOT NULL,
                direction VARCHAR NOT NULL,
                amount NUMERIC(12, 2) NOT NULL,
                currency VARCHAR,
                transaction_date DATE,
                counterparty VARCHAR,
                card_mask VARCHAR,
                account_mask VARCHAR,
                reference_number VARCHAR,
                channel VARCHAR,
                balance NUMERIC(12, 2),
                raw_description TEXT,
                created_at DATETIME,
                PRIMARY KEY (id),
                FOREIGN KEY(email_id) REFERENCES emails (id),
                FOREIGN KEY(account_id) REFERENCES accounts (id),
                FOREIGN KEY(card_id) REFERENCES cards (id)
            )
            """
        )
        sync_conn.exec_driver_sql(
            """
            INSERT INTO transactions__new (
                id,
                email_id,
                account_id,
                card_id,
                bank,
                email_type,
                direction,
                amount,
                currency,
                transaction_date,
                counterparty,
                card_mask,
                account_mask,
                reference_number,
                channel,
                balance,
                raw_description,
                created_at
            )
            SELECT
                id,
                email_id,
                account_id,
                card_id,
                bank,
                email_type,
                direction,
                amount,
                currency,
                transaction_date,
                counterparty,
                card_mask,
                account_mask,
                reference_number,
                channel,
                balance,
                raw_description,
                created_at
            FROM transactions
            """
        )
        sync_conn.exec_driver_sql("DROP TABLE transactions")
        sync_conn.exec_driver_sql("ALTER TABLE transactions__new RENAME TO transactions")
        sync_conn.exec_driver_sql(
            "CREATE INDEX ix_transactions_transaction_date ON transactions (transaction_date)"
        )
        sync_conn.exec_driver_sql("CREATE INDEX ix_transactions_bank ON transactions (bank)")
    finally:
        sync_conn.exec_driver_sql("PRAGMA foreign_keys=ON")


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_migrate_sqlite_transactions_table)

    # Add missing columns for schema migrations
    async with engine.begin() as conn:
        try:
            await conn.execute(text("SELECT note FROM transactions LIMIT 0"))
        except Exception:
            await conn.execute(text("ALTER TABLE transactions ADD COLUMN note TEXT"))
        try:
            await conn.execute(text("SELECT statement_upload_id FROM transactions LIMIT 0"))
        except Exception:
            await conn.execute(text("ALTER TABLE transactions ADD COLUMN statement_upload_id INTEGER REFERENCES statement_uploads(id)"))
        try:
            await conn.execute(text("SELECT statement_password FROM accounts LIMIT 0"))
        except Exception:
            await conn.execute(text("ALTER TABLE accounts ADD COLUMN statement_password VARCHAR"))
        try:
            await conn.execute(text("SELECT initial_backfill_done_at FROM fetch_rules LIMIT 0"))
        except Exception:
            await conn.execute(text("ALTER TABLE fetch_rules ADD COLUMN initial_backfill_done_at DATETIME"))
            # Mark existing rules that already have emails as backfilled
            await conn.execute(text(
                "UPDATE fetch_rules SET initial_backfill_done_at = CURRENT_TIMESTAMP "
                "WHERE id IN (SELECT DISTINCT rule_id FROM emails WHERE rule_id IS NOT NULL)"
            ))
        try:
            await conn.execute(text("SELECT email_id FROM statement_uploads LIMIT 0"))
        except Exception:
            await conn.execute(text("ALTER TABLE statement_uploads ADD COLUMN email_id INTEGER REFERENCES emails(id)"))

    # Populate in-memory settings cache
    from bank_email_fetcher.settings_service import load_all_settings
    await load_all_settings()
