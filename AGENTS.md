# AGENTS.md — AI assistant instructions for bank-email-fetcher

## Project layout

```text
bank_email_fetcher/
  main.py                 FastAPI app factory + lifespan wiring
  api/                    JSON routes
  web/
    __init__.py           HTML router aggregation
    dashboard.py
    accounts.py
    sources.py
    rules.py
    transactions.py
    emails.py
    statements.py
    bank_statements.py    Included before statements.py
    settings.py
    polling.py
    forms.py
  services/
    accounts.py
    emails.py
    fetch.py
    linker.py
    reminders.py
    rules.py
    settings.py
    sources.py
    telegram.py
    transactions.py
    statements/
      __init__.py
      cc.py
      bank.py
      shared.py
      dates.py
  integrations/
    parsers.py
    email/
      base.py
      body.py
      parsing.py
      imap_gmail.py
      jmap_fastmail.py
      orchestrator.py
  core/                   Shared templating, date, auth, crypto, deps helpers
  db/                     Engine/session setup, models, enums, init_db glue
  templates/              Jinja templates
  static/                 CSS and JS assets
  data/
    failed/               Failed parse spools (.eml files, max 7 days old)
    statements/           Saved statement PDFs

scripts/
  main.py                 Raw-email dev CLI
  seed.py
  populate.py
```

## How to run

```bash
uv run fastapi dev
uv run python scripts/seed.py
uv run pytest -q
```

## Service conventions

### Session handling

- Route handlers get `session: AsyncSession = Depends(get_session)` from `core/deps.py`.
- Services take `session: AsyncSession` as a required first parameter when the caller already owns the request session.
- Background tasks (fetch polling, Telegram handlers, reminders) open their own session with `async with async_session() as session:` and pass it onward where applicable.
- Do not add `async_session_factory` fallback parameters.

## Compatibility rules

- Preserve current HTTP routes, JSON response shapes, template behavior, parser-derived `email_type` values, and script entrypoints unless a task explicitly allows a breaking change.
- Use `bank_email_fetcher.integrations.parsers` instead of importing sibling parser packages directly from feature code.
- Shared poll state belongs on `app.state.fetch_service`; avoid new module-level poll loops or duplicate status dicts.
- Keep `bank_statements.router` registered before `statements.router`.

## Quality gates

Run all of these before finishing a refactor:

- `uv run ruff check bank_email_fetcher tests scripts`
- `uv run ruff format --check bank_email_fetcher tests scripts`
- `uv run ty check bank_email_fetcher`
- `uv run pytest -q`

Use `uv run` for every command. PEP 758 parenthesis-free `except X, Y:` syntax is valid in this repo. Prefer `python-dateutil` helpers from `core/dates.py`.
