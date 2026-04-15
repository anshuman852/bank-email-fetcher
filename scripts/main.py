"""Dev tool to fetch and dump bank emails for parser development.

Supports Gmail (IMAP) and Fastmail (JMAP).

Env vars (or .env file):
    GMAIL_USER          - Gmail address
    GMAIL_APP_PASSWORD  - Gmail app password
    FASTMAIL_TOKEN      - Fastmail app password / API token (Bearer token for JMAP)

Usage:
    uv run scripts/main.py list gmail --from "noreply@slice.bank.in" --limit 10
    uv run scripts/main.py list fastmail --from "credit_cards@icicibank.com" --limit 5
    uv run scripts/main.py dump gmail 12345 12346
    uv run scripts/main.py dump fastmail abc123 def456
"""

import argparse
import email
import imaplib
import json
import sys
from email.header import decode_header
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from rich.console import Console
from rich.table import Table

console = Console()
OUTPUT_DIR = Path("data")
PROVIDERS = ("gmail", "fastmail")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    gmail_user: str = ""
    gmail_app_password: SecretStr = SecretStr("")
    fastmail_token: SecretStr = SecretStr("")


settings = Settings()


def decode_header_value(raw: str | None) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(part)
    return " ".join(decoded)


def _save_eml(
    provider: str, uid: str, raw: bytes, subfolder: str | None = None
) -> None:
    out_dir = OUTPUT_DIR / subfolder if subfolder else OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    msg = email.message_from_bytes(raw)
    subject = decode_header_value(msg.get("Subject", "nosubject"))
    safe_subject = "".join(c if c.isalnum() or c in " -_" else "_" for c in subject)[
        :60
    ].strip()
    filename = f"{provider}_{uid}_{safe_subject}.eml"
    path = out_dir / filename
    path.write_bytes(raw)
    console.print(f"[green]Saved:[/green] {path} ({len(raw)} bytes)")


# ---------------------------------------------------------------------------
# Gmail (IMAP with UID commands)
# ---------------------------------------------------------------------------


def _gmail_connect() -> imaplib.IMAP4_SSL:
    if not settings.gmail_user or not settings.gmail_app_password.get_secret_value():
        console.print("[red]Set GMAIL_USER and GMAIL_APP_PASSWORD in env or .env[/red]")
        sys.exit(1)
    try:
        conn = imaplib.IMAP4_SSL("imap.gmail.com")
        conn.login(settings.gmail_user, settings.gmail_app_password.get_secret_value())
    except imaplib.IMAP4.error as e:
        console.print(f"[red]Gmail login failed: {e}[/red]")
        sys.exit(1)
    return conn


def _imap_search_criteria(args: argparse.Namespace) -> str:
    criteria = []
    # Multiple --from values get OR'd together
    if args.sender:
        if len(args.sender) == 1:
            criteria.append(f'FROM "{args.sender[0]}"')
        else:
            # IMAP OR is binary, so chain: OR (FROM a) (OR (FROM b) (FROM c))
            expr = f'FROM "{args.sender[-1]}"'
            for s in reversed(args.sender[:-1]):
                expr = f'OR FROM "{s}" {expr}'
            criteria.append(expr)
    if args.subject:
        criteria.append(f'SUBJECT "{args.subject}"')
    if args.since:
        criteria.append(f"SINCE {args.since}")
    return " ".join(criteria) if criteria else "ALL"


def gmail_list(args: argparse.Namespace) -> None:
    conn = _gmail_connect()
    folder = args.folder or "[Gmail]/All Mail"
    typ, _ = conn.select(f'"{folder}"', readonly=True)
    if typ != "OK":
        console.print(f"[red]Could not select folder: {folder}[/red]")
        conn.logout()
        return

    criteria = _imap_search_criteria(args)
    console.print(f"[dim]Folder: {folder} | Search: {criteria}[/dim]")

    # imaplib's stubs type charset as str, but None is the canonical
    # "no charset" value for SEARCH.
    typ, data = conn.uid("SEARCH", None, criteria)  # ty: ignore[invalid-argument-type]
    if typ != "OK" or not data[0]:
        console.print("[yellow]No emails found[/yellow]")
        conn.logout()
        return

    uids = data[0].split()[-args.limit :]

    table = Table(title=f"gmail - {len(uids)} emails")
    table.add_column("UID", style="cyan")
    table.add_column("Date", style="green")
    table.add_column("From")
    table.add_column("Subject", style="bold")

    for uid in uids:
        typ, msg_data = conn.uid(
            "FETCH", uid, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)])"
        )
        if typ != "OK" or not msg_data or not msg_data[0]:
            continue
        msg = email.message_from_bytes(msg_data[0][1])
        table.add_row(
            uid.decode(),
            decode_header_value(msg.get("Date", ""))[:25],
            decode_header_value(msg.get("From", "")),
            decode_header_value(msg.get("Subject", "")),
        )

    console.print(table)
    conn.logout()


def gmail_dump(args: argparse.Namespace) -> None:
    conn = _gmail_connect()
    folder = args.folder or "[Gmail]/All Mail"
    conn.select(f'"{folder}"', readonly=True)

    for uid in args.uids:
        typ, msg_data = conn.uid("FETCH", uid.encode(), "(RFC822)")
        if typ != "OK" or not msg_data or not msg_data[0]:
            console.print(f"[red]UID {uid}: not found[/red]")
            continue
        _save_eml("gmail", uid, msg_data[0][1], args.output_folder)

    conn.logout()


# ---------------------------------------------------------------------------
# Fastmail (JMAP with Bearer token)
# ---------------------------------------------------------------------------

JMAP_SESSION_URL = "https://api.fastmail.com/jmap/session"


def _jmap_request(token: str, url: str, body: dict | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if body:
        req = Request(
            url, data=json.dumps(body).encode(), headers=headers, method="POST"
        )
    else:
        req = Request(url, headers=headers)
    try:
        with urlopen(req) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        console.print(f"[red]JMAP request failed: {e.code} {e.reason}[/red]")
        if e.code == 401:
            console.print("[red]Check your FASTMAIL_TOKEN[/red]")
        sys.exit(1)
    except URLError as e:
        console.print(f"[red]Network error: {e.reason}[/red]")
        sys.exit(1)


def _fastmail_session() -> tuple[str, str, str]:
    """Returns (token, api_url, account_id)."""
    if not (token := settings.fastmail_token.get_secret_value()):
        console.print("[red]Set FASTMAIL_TOKEN in env or .env[/red]")
        sys.exit(1)
    session = _jmap_request(token, JMAP_SESSION_URL)
    api_url = session["apiUrl"]
    account_id = session["primaryAccounts"]["urn:ietf:params:jmap:mail"]
    return token, api_url, account_id


def _resolve_mailbox_id(
    token: str, api_url: str, account_id: str, folder_name: str
) -> str | None:
    """Look up a JMAP mailbox ID by folder name."""
    body = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            [
                "Mailbox/query",
                {
                    "accountId": account_id,
                    "filter": {"name": folder_name},
                },
                "0",
            ],
            [
                "Mailbox/get",
                {
                    "accountId": account_id,
                    "#ids": {"resultOf": "0", "name": "Mailbox/query", "path": "/ids"},
                    "properties": ["id", "name"],
                },
                "1",
            ],
        ],
    }
    result = _jmap_request(token, api_url, body)
    mailboxes = result["methodResponses"][1][1].get("list", [])
    # Exact match first, then case-insensitive
    for mb in mailboxes:
        if mb["name"] == folder_name:
            return mb["id"]
    for mb in mailboxes:
        if mb["name"].lower() == folder_name.lower():
            return mb["id"]
    return None


def fastmail_list(args: argparse.Namespace) -> None:
    token, api_url, account_id = _fastmail_session()

    jmap_filter: dict = {}
    if args.folder:
        if mailbox_id := _resolve_mailbox_id(token, api_url, account_id, args.folder):
            jmap_filter["inMailbox"] = mailbox_id
        else:
            console.print(f"[red]Folder not found: {args.folder}[/red]")
            return
    if args.sender:
        if len(args.sender) == 1:
            jmap_filter["from"] = args.sender[0]
        else:
            # JMAP FilterOperator: OR across multiple from conditions
            jmap_filter["operator"] = "OR"
            jmap_filter["conditions"] = [{"from": s} for s in args.sender]
    if args.subject:
        jmap_filter["subject"] = args.subject
    if args.since:
        jmap_filter["after"] = args.since

    body = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            [
                "Email/query",
                {
                    "accountId": account_id,
                    "filter": jmap_filter,
                    "sort": [{"property": "receivedAt", "isAscending": False}],
                    "limit": args.limit,
                },
                "0",
            ],
            [
                "Email/get",
                {
                    "accountId": account_id,
                    "#ids": {"resultOf": "0", "name": "Email/query", "path": "/ids"},
                    "properties": ["id", "receivedAt", "from", "subject"],
                },
                "1",
            ],
        ],
    }

    result = _jmap_request(token, api_url, body)
    emails = result["methodResponses"][1][1].get("list", [])

    if not emails:
        console.print("[yellow]No emails found[/yellow]")
        return

    table = Table(title=f"fastmail - {len(emails)} emails")
    table.add_column("ID", style="cyan")
    table.add_column("Date", style="green")
    table.add_column("From")
    table.add_column("Subject", style="bold")

    for em in emails:
        from_addr = em["from"][0].get("email", "") if em.get("from") else ""
        table.add_row(
            em["id"],
            (em.get("receivedAt") or "")[:25],
            from_addr,
            em.get("subject", ""),
        )

    console.print(table)


def fastmail_dump(args: argparse.Namespace) -> None:
    token, api_url, account_id = _fastmail_session()

    body = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            [
                "Email/get",
                {
                    "accountId": account_id,
                    "ids": args.uids,
                    "properties": ["blobId", "subject"],
                },
                "0",
            ],
        ],
    }
    result = _jmap_request(token, api_url, body)
    response = result["methodResponses"][0][1]
    emails = response.get("list", [])

    if not_found := response.get("notFound"):
        for nf in not_found:
            console.print(f"[red]ID {nf}: not found[/red]")

    session = _jmap_request(token, JMAP_SESSION_URL)
    download_url = session["downloadUrl"]

    for em in emails:
        blob_url = (
            download_url.replace("{accountId}", account_id)
            .replace("{blobId}", em["blobId"])
            .replace("{type}", "application/octet-stream")
            .replace("{name}", "email.eml")
        )
        req = Request(blob_url, headers={"Authorization": f"Bearer {token}"})
        with urlopen(req) as resp:
            raw = resp.read()
        _save_eml("fastmail", em["id"], raw, args.output_folder)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def cmd_list(args: argparse.Namespace) -> None:
    {"gmail": gmail_list, "fastmail": fastmail_list}[args.provider](args)


def cmd_dump(args: argparse.Namespace) -> None:
    {"gmail": gmail_dump, "fastmail": fastmail_dump}[args.provider](args)


def app() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch and dump bank emails for parser development"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    ls = sub.add_parser("list", aliases=["ls"], help="List emails matching criteria")
    ls.add_argument("provider", choices=PROVIDERS)
    ls.add_argument(
        "--from",
        dest="sender",
        nargs="+",
        help="Filter by sender (substring match, multiple OK)",
    )
    ls.add_argument("--subject", help="Filter by subject")
    ls.add_argument(
        "--since", help="Gmail: 01-Mar-2026, Fastmail: 2026-03-01T00:00:00Z"
    )
    ls.add_argument(
        "--limit", type=int, default=50, help="Max emails to show (default: 50)"
    )
    ls.add_argument("--folder", help="Gmail IMAP folder (default: [Gmail]/All Mail)")
    ls.set_defaults(func=cmd_list)

    dm = sub.add_parser("dump", help="Dump specific emails as .eml files")
    dm.add_argument("provider", choices=PROVIDERS)
    dm.add_argument(
        "uids", nargs="+", help="Gmail: IMAP UIDs, Fastmail: JMAP email IDs"
    )
    dm.add_argument("--folder", help="Gmail IMAP folder (default: [Gmail]/All Mail)")
    dm.add_argument(
        "--output",
        dest="output_folder",
        help="Subfolder under data/ (e.g. --output hdfc → data/hdfc/)",
    )
    dm.set_defaults(func=cmd_dump)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    app()
