# AGENTS.md — AI assistant instructions for bank-email-fetcher

This file documents conventions, patterns, and gotchas for AI assistants working on this codebase.

## Project layout

```
bank_email_fetcher/
  app.py          FastAPI app: all routes, lifespan, background poll loop
  db.py           SQLAlchemy models, async engine, init_db() with inline migrations
  fetcher.py      Email fetching (Gmail IMAP, Fastmail JMAP) + parsing orchestration
  statements.py   CC statement PDF parsing, reconciliation, enrichment
  linker.py       Transaction -> Account/Card linking with preloaded context
  config.py       Pydantic Settings (.env), get_fernet() factory
  crypto.py       encrypt_credentials / decrypt_credentials (thin Fernet wrappers)
  templates/      Jinja2 templates (one per page + partials/)
  static/         CSS and JS assets
  data/
    failed/       Failed parse spools (.eml files, max 7 days old)
    statements/   Saved CC statement PDFs

seed.py           Seed fetch rules for all supported banks (idempotent)
main.py           CLI tool for inspecting raw emails from Gmail/Fastmail
populate.py       Bulk-import transactions from local .eml files
pyproject.toml    uv/hatchling project; git deps for bank-email-parser, cc-parser
```

## How to run

```bash
uv run fastapi dev          # Development server with reload at http://localhost:8000
uv run python seed.py       # (Re-)seed fetch rules
```

There are no automated tests. Manual testing means starting the dev server, adding sources/rules via the UI, and triggering a poll.

## Key patterns

### Async sessions
Every DB operation uses `async with async_session() as session:`. Sessions are short-lived; the session factory (`async_sessionmaker`) is module-level in `db.py`. Never store a session across `await` boundaries beyond its own `async with` block.

```python
async with async_session() as session:
    result = await session.execute(select(Transaction).where(...))
    rows = result.scalars().all()
```

### Inline imports to avoid circular dependencies
`app.py` imports from `fetcher.py` and `linker.py`. `statements.py` and `fetcher.py` both import from `db.py`. To avoid circular imports, some modules defer imports inside functions:

```python
# statements.py
async def enrich_matched_transactions(recon):
    from bank_email_fetcher.db import Transaction, async_session  # deferred
    ...
```

This pattern appears in `statements.py` and in `fetcher.py`'s `poll_all()`. Don't move these to the top of the file.

### Fernet encryption
`EMAIL_SOURCE_MASTER_KEY` must be a valid Fernet key (URL-safe base64-encoded 32 bytes). `config.get_fernet()` handles both str and bytes. `seed.py` auto-generates this key on first run if missing. If the key is absent at runtime, an ephemeral key is generated with a warning — encrypted credentials from a prior run will be unreadable.

Two things are encrypted:
- `EmailSource.credentials` — Gmail app password or Fastmail token (JSON dict)
- `Account.statement_password` — CC statement PDF password

Use `crypto.encrypt_credentials` / `crypto.decrypt_credentials` for email credentials. Use `get_fernet().encrypt(...)` / `get_fernet().decrypt(...)` directly for statement passwords (they're plain strings, not JSON dicts).

### Two-phase IMAP fetch (Gmail)
`_fetch_gmail_source_sync()` in `fetcher.py`:
1. Phase 0: `UID SEARCH` by FROM/SUBJECT/SINCE — collect UIDs
2. Phase 1: batch `UID FETCH` for headers + X-GM-MSGID — dedup across folders
3. Phase 1.5: bulk DB query to filter out already-seen remote_ids
4. Phase 2: `UID FETCH RFC822` for genuinely new messages only

The DB dedup in Phase 1.5 uses a synchronous SQLite connection (not the async engine) because it runs inside a sync function called via `asyncio.to_thread`. The `_check_remote_ids_in_db_sync_params` helper opens a separate sync engine for this.

### Backfill tracking
`FetchRule.initial_backfill_done_at` is NULL for new rules. While NULL, the SINCE/after date filter uses ~3 months ago (90 days) instead of `last_synced_at` — older emails are intentionally skipped. After the first successful search phase completes, the timestamp is set and subsequent polls use the normal incremental SINCE based on `last_synced_at`.

### Link context
`linker.build_link_context(session)` loads all accounts and cards into a `LinkContext` dataclass (Python dicts — no further DB queries). `link_transaction(ctx, txn)` mutates `txn.account_id` and `txn.card_id` in place. The caller commits.

Always build the context once and reuse across a batch:
```python
ctx = await build_link_context(session)
for txn in transactions:
    link_transaction(ctx, txn)
await session.commit()
```

### Poll lock
`fetcher.POLL_LOCK` is an `asyncio.Lock`. `poll_all()` acquires it to prevent overlapping polls. If already locked when `poll_all()` is called, it returns immediately with `status="already_running"`. The background loop and manual trigger both call `poll_all()`.

### Statement reconciliation
`statements.reconcile_statement()` matches by `(date, amount, direction)` with ±1 day tolerance (greedy first-match). Results are stored as JSON in `StatementUpload.reconciliation_data`. Missing transactions are auto-imported during automated processing. Manual import at `/statements/{id}` lets the user choose which missing items to import.

### Schema migrations
There is no Alembic. `db.init_db()` runs `create_all` then tries `ALTER TABLE ... ADD COLUMN` in `try/except` blocks for each new column. Adding a new column to a model requires adding a corresponding migration block in `init_db()`. Also handles one legacy constraint rename via `_migrate_sqlite_transactions_table`.

### Template rendering
All HTML routes return `templates.TemplateResponse(...)`. The `templates` object is a module-level `Jinja2Templates` instance in `app.py`. A custom Jinja2 filter `inr_compact` formats INR amounts compactly (K/L/Cr suffixes).

### Form handling
HTML forms use POST + redirect (PRG pattern). All `@app.post(...)` handlers that modify data end with `return RedirectResponse(url=..., status_code=303)`. API endpoints under `/api/` return `JSONResponse`.

## Integration contracts

- **`email_type` values from `bank-email-parser`**: Stored in DB and matched in `seed.py` fetch rules. Renaming an `email_type` in `bank-email-parser` requires updating `seed.py` and existing DB rows.
- **Date format from `cc-parser`**: Returns DD/MM/YYYY strings. `statements.py` converts to `datetime.date`. Format change is downstream-breaking.
- **Amount format from `cc-parser`**: Returns comma-separated strings like `"25,000.00"`. `statements.py` strips commas and converts to `Decimal`.
- **Statement reconciliation tolerance**: `reconcile_statement()` matches by `(date, amount, direction)` with ±1 day tolerance for posting date lags.

## Known gotchas

- **Linker last-4 lookup is global (not per-bank)**: Two cards from different banks with same last-4 digits will collide.
- **Reconciliation greedy matching**: Two same-day same-amount transactions may be paired incorrectly.
- **Failed email spool expires after 7 days** (`FAILED_SPOOL_MAX_AGE_DAYS`).
- **Statement subject filter**: Only emails with "statement" in subject (case-insensitive) are processed as statements.
- **First PDF attachment only**: Subsequent attachments in the same email are ignored.
- **`application/octet-stream` with `.pdf` extension is also accepted** (some banks misidentify MIME type).
- **Inline imports**: `statements.py` and `fetcher.py` defer imports inside functions to avoid circular deps with `db.py`. Don't move to module level.

## Sensitive files

- `.env`: `EMAIL_SOURCE_MASTER_KEY` (Fernet key). Lost key = all stored credentials unreadable.
- `data/failed/`: Raw `.eml` files with financial data. Auto-expires after 7 days.
- `data/statements/`: CC statement PDFs with full transaction history.

## Known limitations

- **No Alembic**: Adding new columns requires both a model change and a migration block in `init_db()`. Forgetting the migration block silently breaks production (column doesn't exist) until the server restarts and `init_db()` runs.
- **No test suite**: There are no unit or integration tests. Breakage is discovered by running the server manually.
- **Synchronous IMAP/JMAP in threads**: Both `_fetch_gmail_source_sync` and `_fetch_fastmail_source_sync` are blocking functions called via `asyncio.to_thread`. They block the thread for the duration of the network I/O. This is acceptable for a single-user self-hosted app.
- **SQLite only in practice**: The DB_URL can be changed, but migrations and the sync dedup query assume SQLite. The partial index on `transactions` uses `sqlite_where` which is SQLite-specific.
- **Poll progress is in-memory**: `POLL_STATUS` is a module-level dict in `fetcher.py`. Progress is lost on restart and is not shared between processes (single-process deployment only).
- **No pagination on emails list**: `/emails` returns the last 200 rows unconditionally.
- **Statement auto-import is aggressive**: During automated processing, ALL missing transactions are auto-imported. The manual UI lets the user choose. If an automated statement import produces wrong data, the only recourse is to delete the `StatementUpload` and the imported transactions manually.
- **`bank-email-parser` and `cc-parser` are git dependencies**: They are not on PyPI. See `pyproject.toml` `[tool.uv.sources]`.
