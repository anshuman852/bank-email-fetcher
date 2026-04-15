---
name: add-endpoint
description: Add or update a bank-email-fetcher endpoint using the per-domain router -> service -> schema layout.
---

# Add or Update an Endpoint

## Read first

- `AGENTS.md`
- `README.md`
- the matching files under `bank_email_fetcher/api/`, `bank_email_fetcher/web/`, `bank_email_fetcher/services/`, and `bank_email_fetcher/schemas/`

## Layout

- JSON routes live in `api/{domain}.py`
- HTML routes live in `web/{domain}.py`
- Business logic belongs in `services/{domain}.py` or `services/statements/{facet}.py`
- DTOs live in `schemas/{domain}.py`
- Aggregate HTML routes in `web/__init__.py`

Every handler should take `session: AsyncSession = Depends(get_session)`.

## Route-order pitfall

Keep `web/bank_statements.py` included before `web/statements.py`, otherwise `/statements/bank/{id}` can be shadowed by `/statements/{id}`.

## Compatibility

- Preserve URLs, methods, response shapes, template names, and template context keys.
- Use `bank_email_fetcher.integrations.parsers` for parser calls.
- Keep polling state on `app.state.fetch_service`.

## Validate

Before committing, run:

```bash
uv run ruff check bank_email_fetcher tests scripts
uv run ruff format --check bank_email_fetcher tests scripts
uv run ty check bank_email_fetcher
uv run pytest -q
```
