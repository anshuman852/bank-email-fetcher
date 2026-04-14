"""Email fetching and parsing orchestration for bank-email-fetcher.

Provides:
- poll_all(): the main poll function called by the background loop and manual
  trigger. Acquires POLL_LOCK, groups enabled FetchRules by EmailSource,
  opens one provider connection per source, fetches new emails, parses them,
  and stores Email + Transaction rows in the DB. Updates POLL_STATUS with
  live progress. Falls back to CC statement processing for emails that fail
  bank-email-parser.

- _fetch_gmail_source_sync(): synchronous Gmail IMAP fetcher run via
  asyncio.to_thread. Two-phase fetch: Phase 0 searches by FROM/SUBJECT/SINCE,
  Phase 1 fetches headers + X-GM-MSGID for deduplication, Phase 2 fetches
  full RFC822 bodies only for genuinely new messages.

- _fetch_fastmail_source_sync(): synchronous Fastmail JMAP fetcher run via
  asyncio.to_thread. Queries email metadata (including blobId), deduplicates
  against the DB, then downloads only new message blobs.

- _fetch_gmail_single_sync() / _fetch_fastmail_single_sync(): single-message
  fetch helpers used by app.py to re-fetch an email for the original-email
  viewer.

- get_poll_status(): returns a snapshot of POLL_STATUS for the API endpoint.

Both provider fetchers honour the initial_backfill_done_at flag on FetchRule:
rules without it skip the SINCE/after date filter to do a full historical scan.
"""

import asyncio
import datetime
import email as email_lib
import email.utils
import imaplib
import json
import logging
import re
import time
from collections import defaultdict
from email.header import decode_header
from pathlib import Path
from urllib.request import Request, urlopen

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from bank_email_parser.api import parse_email
from bank_email_parser.exceptions import ParseError, UnsupportedEmailTypeError

from bank_email_fetcher.crypto import decrypt_credentials
from bank_email_fetcher.db import (
    Account,
    async_session,
    Card,
    Email,
    EmailSource,
    FetchRule,
    Transaction,
)
from bank_email_fetcher.linker import build_link_context, link_transaction

logger = logging.getLogger(__name__)

# How far back an initial backfill reaches. Used by both Gmail (IMAP SINCE)
# and Fastmail (JMAP after) paths.
INITIAL_BACKFILL_DAYS = 90

JMAP_SESSION_URL = "https://api.fastmail.com/jmap/session"
FAILED_SPOOL_DIR = Path(__file__).parent / "data" / "failed"
FAILED_SPOOL_MAX_AGE_DAYS = 7
POLL_LOCK = asyncio.Lock()
POLL_STATUS = {
    "state": "idle",
    "started_at": None,
    "finished_at": None,
    "last_stats": None,
    "last_error": None,
    "progress": None,  # e.g. {"source": "Personal Gmail", "rule": "1/3", "email": "15/50", "detail": "..."}
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _save_failed_email(provider: str, message_id: str, raw_bytes: bytes) -> None:
    """Save raw .eml to the failed spool directory for debugging."""
    FAILED_SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    # Sanitize message_id for use as filename
    safe_id = re.sub(r"[^\w\-.]", "_", message_id)
    path = FAILED_SPOOL_DIR / f"{provider}_{safe_id}.eml"
    path.write_bytes(raw_bytes)
    logger.info("Saved failed email to %s", path)


def _cleanup_failed_spool() -> None:
    """Delete .eml files in the failed spool older than FAILED_SPOOL_MAX_AGE_DAYS."""
    if not FAILED_SPOOL_DIR.exists():
        return
    cutoff = time.time() - (FAILED_SPOOL_MAX_AGE_DAYS * 86400)
    for path in FAILED_SPOOL_DIR.glob("*.eml"):
        if path.stat().st_mtime < cutoff:
            path.unlink()
            logger.debug("Cleaned up old failed email: %s", path.name)


def _parse_email_date(raw_bytes: bytes) -> datetime.datetime | None:
    """Extract and parse the Date header from raw email bytes."""
    msg = email_lib.message_from_bytes(raw_bytes)
    date_str = msg.get("Date")
    if not date_str:
        return None
    try:
        return email.utils.parsedate_to_datetime(date_str)
    except ValueError, TypeError:
        return None


def _decode_header_value(raw: str | None) -> str:
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


def _extract_html_body(raw_bytes: bytes) -> str | None:
    """Extract the HTML body from raw email bytes."""
    msg = email_lib.message_from_bytes(raw_bytes)
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
    else:
        if msg.get_content_type() == "text/html":
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace")
    return None


def _extract_text_body(raw_bytes: bytes) -> str | None:
    """Extract the plain-text body from raw email bytes."""
    msg = email_lib.message_from_bytes(raw_bytes)
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
    else:
        if msg.get_content_type() == "text/plain":
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace")
    return None


def _extract_message_metadata(raw_bytes: bytes) -> dict:
    """Extract sender, subject, date from raw email bytes."""
    msg = email_lib.message_from_bytes(raw_bytes)
    return {
        "sender": _decode_header_value(msg.get("From", "")),
        "subject": _decode_header_value(msg.get("Subject", "")),
        "date": _decode_header_value(msg.get("Date", "")),
    }


def _clean_rule_value(value: str | None) -> str:
    return (value or "").strip()


def _normalize_text(value: str | None) -> str:
    return _clean_rule_value(value).lower()


def _sender_matches(rule_sender: str, actual_sender: str) -> bool:
    if not rule_sender:
        return True
    actual = _normalize_text(actual_sender)
    expected = _normalize_text(rule_sender)
    if not actual:
        return False
    addr = email.utils.parseaddr(actual_sender)[1].lower()
    return expected in actual or expected == addr


def _subject_matches(rule_subject: str, actual_subject: str) -> bool:
    if not rule_subject:
        return True
    actual = _normalize_text(actual_subject)
    expected = _normalize_text(rule_subject)
    return bool(actual) and expected in actual


def _matches_rule_filters(
    rule: FetchRule, *, sender: str | None, subject: str | None
) -> bool:
    return _sender_matches(
        _clean_rule_value(rule.sender), sender or ""
    ) and _subject_matches(_clean_rule_value(rule.subject), subject or "")


def _format_jmap_from_field(from_field: list[dict] | None) -> str:
    if not from_field:
        return ""
    parts = []
    for sender in from_field:
        name = _clean_rule_value(sender.get("name"))
        email_addr = _clean_rule_value(sender.get("email"))
        if name and email_addr:
            parts.append(f"{name} <{email_addr}>")
        elif email_addr:
            parts.append(email_addr)
        elif name:
            parts.append(name)
    return ", ".join(parts)


def _serialize_datetime(value: datetime.datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() + "Z"


def _is_duplicate_transaction_error(exc: IntegrityError) -> bool:
    message = str(exc.orig)
    return "uq_transaction_dedup" in message or (
        "UNIQUE constraint failed:" in message and "transactions." in message
    )


def get_poll_status() -> dict:
    return {
        "state": POLL_STATUS["state"],
        "started_at": _serialize_datetime(POLL_STATUS["started_at"]),
        "finished_at": _serialize_datetime(POLL_STATUS["finished_at"]),
        "last_stats": POLL_STATUS["last_stats"],
        "last_error": POLL_STATUS["last_error"],
        "progress": POLL_STATUS["progress"],
    }


# ---------------------------------------------------------------------------
# Gmail (IMAP)
# ---------------------------------------------------------------------------


def _imap_since_date(last_synced_at: datetime.datetime | None) -> str | None:
    """Return IMAP SINCE date string with 2-day margin, or None."""
    if last_synced_at is None:
        # For initial backfill, use 3 months ago
        since = datetime.datetime.utcnow() - datetime.timedelta(days=INITIAL_BACKFILL_DAYS)
        return since.strftime("%d-%b-%Y")
    since = last_synced_at - datetime.timedelta(days=2)
    return since.strftime("%d-%b-%Y")


def _check_remote_ids_in_db_sync_params(
    source_id: int, remote_ids: set[str]
) -> set[str]:
    """Check which remote_ids already exist for a source."""
    from bank_email_fetcher.db import engine as async_engine
    from sqlalchemy import create_engine, text

    sync_url = (
        str(async_engine.url)
        .replace("+aiosqlite", "")
        .replace("sqlite+aiosqlite", "sqlite")
    )
    sync_engine = create_engine(sync_url)
    existing = set()
    try:
        with sync_engine.connect() as conn:
            batch_list = list(remote_ids)
            for batch_start in range(0, len(batch_list), 500):
                batch = batch_list[batch_start : batch_start + 500]
                placeholders = ",".join([f":r{i}" for i in range(len(batch))])
                params = {"sid": source_id}
                for i, rid in enumerate(batch):
                    params[f"r{i}"] = rid
                rows = conn.execute(
                    text(
                        f"SELECT remote_id FROM emails WHERE source_id = :sid "
                        f"AND remote_id IN ({placeholders})"
                    ),
                    params,
                ).fetchall()
                existing.update(row[0] for row in rows)
    finally:
        sync_engine.dispose()
    return existing


def _fetch_gmail_source_sync(
    rules,
    *,
    user: str,
    password: str,
    fetch_limit: int,
    source_id: int,
    last_synced_at: datetime.datetime | None = None,
) -> tuple[dict[int, list[tuple]], bool, set[int]]:
    """Fetch emails from Gmail via IMAP for all rules on one source.

    Opens a single IMAP connection, runs SEARCH for each rule, deduplicates
    by X-GM-MSGID, does a two-phase fetch (metadata then RFC822), and returns
    ``(results_by_rule, fetch_ok, backfill_ready_rule_ids)`` where
    *backfill_ready_rule_ids* is the set of rule IDs whose Phase-0 SEARCH
    completed (even if it returned zero results).  A rule whose search was
    skipped due to an IMAP error should **not** appear in this set, so the
    caller knows not to mark its backfill as complete.
    """
    if not user or not password:
        logger.warning("Gmail credentials missing for source %s", source_id)
        return {}, False, set()

    results_by_rule: dict[int, list[tuple]] = {r.id: [] for r in rules}
    # Track rule IDs whose Phase-0 SEARCH completed successfully (OK
    # response, even if zero results).  Only these should be candidates
    # for marking backfill complete.
    backfill_searched_rule_ids: set[int] = set()
    conn = imaplib.IMAP4_SSL("imap.gmail.com")
    try:
        logger.info("Gmail: Connecting to imap.gmail.com for source %s", source_id)
        conn.login(user, password)
        logger.info("Gmail: Logged in successfully for source %s", source_id)

        since_str = _imap_since_date(last_synced_at)

        # Phase 0: collect UIDs per rule, track which rules each UID belongs to
        uid_to_rules: dict[tuple, list] = {}  # (folder, uid) -> [rule, ...]
        current_folder = None

        for rule in rules:
            folder = rule.folder or "[Gmail]/All Mail"
            if folder != current_folder:
                typ, _ = conn.select(f'"{folder}"', readonly=True)
                if typ != "OK":
                    logger.error("Could not select Gmail folder: %s", folder)
                    continue
                current_folder = folder

            # For rules that haven't completed their initial backfill, cap the
            # historical scan at 3 months rather than a full-history search.
            needs_backfill = getattr(rule, "initial_backfill_done_at", None) is None
            if needs_backfill:
                backfill_since = datetime.datetime.now(
                    datetime.timezone.utc
                ) - datetime.timedelta(days=INITIAL_BACKFILL_DAYS)
                rule_since_str = backfill_since.strftime("%d-%b-%Y")
                logger.info(
                    "Rule %s (bank=%s, sender=%s) initial backfill — SINCE %s",
                    rule.id,
                    rule.bank,
                    rule.sender,
                    rule_since_str,
                )
            else:
                rule_since_str = since_str

            criteria_parts = []
            if rule.sender:
                criteria_parts.append(f'FROM "{rule.sender}"')
            if rule.subject:
                criteria_parts.append(f'SUBJECT "{rule.subject}"')
            if rule_since_str:
                criteria_parts.append(f"SINCE {rule_since_str}")
            criteria = " ".join(criteria_parts) if criteria_parts else "ALL"

            typ, data = conn.uid("SEARCH", None, criteria)
            logger.info(
                "Gmail: SEARCH for rule %s (bank=%s): criteria=%s -> result=%s count=%s",
                rule.id,
                rule.bank,
                criteria,
                typ,
                len(data[0].split()) if typ == "OK" and data[0] else 0,
            )
            if typ != "OK" or not data[0]:
                # SEARCH completed OK but returned zero results — this
                # rule genuinely has no matching emails, so its backfill
                # is complete.
                if needs_backfill and typ == "OK":
                    backfill_searched_rule_ids.add(rule.id)
                continue

            rule_uids = data[0].split()
            for uid in rule_uids:
                uid_to_rules.setdefault((folder, uid), []).append(rule)

        if not uid_to_rules:
            return results_by_rule, True, backfill_searched_rule_ids

        # Phase 1: batch metadata fetch to get X-GM-MSGID + headers
        # Group UIDs by folder for metadata fetch
        folder_uids: dict[str, list[bytes]] = defaultdict(list)
        for folder, uid in uid_to_rules:
            folder_uids[folder].append(uid)

        uid_meta: dict[tuple, tuple] = {}  # (folder, uid) -> (remote_id, metadata, xgm)
        current_folder = None

        for folder, uids in folder_uids.items():
            if folder != current_folder:
                typ, _ = conn.select(f'"{folder}"', readonly=True)
                if typ != "OK":
                    continue
                current_folder = folder

            # Batch metadata fetch in chunks of 500
            for batch_start in range(0, len(uids), 500):
                batch = uids[batch_start : batch_start + 500]
                uid_set = b",".join(batch)
                try:
                    typ, msg_data = conn.uid(
                        "FETCH",
                        uid_set,
                        "(X-GM-MSGID BODY.PEEK[HEADER.FIELDS (MESSAGE-ID FROM SUBJECT DATE)])",
                    )
                except imaplib.IMAP4.error as e:
                    logger.error("Gmail metadata fetch error: %s", e)
                    continue
                if typ != "OK" or not msg_data:
                    continue

                # Parse the batch response — each message is a tuple in msg_data
                for part in msg_data:
                    if isinstance(part, tuple):
                        header_line = part[0]
                        if isinstance(header_line, bytes):
                            # Extract UID from the response line
                            uid_match = re.search(
                                rb"^\*\s+\d+\s+FETCH\s+\(UID\s+(\d+)", header_line
                            )
                            m = re.search(rb"X-GM-MSGID\s+(\d+)", header_line)
                            xgm = m.group(1).decode() if m else None
                            headers_raw = (
                                part[1]
                                if len(part) > 1 and isinstance(part[1], bytes)
                                else None
                            )
                            if headers_raw:
                                msg = email_lib.message_from_bytes(headers_raw)
                                meta = {
                                    "message_id": (msg.get("Message-ID") or "").strip(),
                                    "sender": _decode_header_value(msg.get("From", "")),
                                    "subject": _decode_header_value(
                                        msg.get("Subject", "")
                                    ),
                                    "date": _decode_header_value(msg.get("Date", "")),
                                }
                            else:
                                meta = None

                            # Find which UID this response belongs to
                            resp_uid = None
                            if uid_match:
                                resp_uid = uid_match.group(1)
                            else:
                                # For UID FETCH, the UID is in the response
                                uid_in_resp = re.search(rb"UID\s+(\d+)", header_line)
                                if uid_in_resp:
                                    resp_uid = uid_in_resp.group(1)

                            if resp_uid and meta:
                                remote_id = (
                                    xgm
                                    if xgm
                                    else (
                                        meta["message_id"]
                                        or f"{resp_uid.decode()}:{folder}"
                                    )
                                )
                                uid_meta[(folder, resp_uid)] = (remote_id, meta, xgm)

        # Deduplicate by X-GM-MSGID across rules (same message in multiple folders)
        seen_xgm_set: set[str] = set()
        deduped_keys: list[tuple] = []
        for key in uid_to_rules:
            meta_entry = uid_meta.get(key)
            if not meta_entry:
                # Metadata fetch missed this key; include it to be safe
                deduped_keys.append(key)
                continue
            remote_id, meta, xgm = meta_entry
            if xgm:
                if xgm in seen_xgm_set:
                    continue
                seen_xgm_set.add(xgm)
            deduped_keys.append(key)

        # Phase 1.5: scoped dedup against DB
        candidate_remote_ids = {}
        for key in deduped_keys:
            meta_entry = uid_meta.get(key)
            if meta_entry:
                candidate_remote_ids[key] = meta_entry[0]

        if candidate_remote_ids:
            existing_remote_ids = _check_remote_ids_in_db_sync_params(
                source_id,
                set(candidate_remote_ids.values()),
            )
        else:
            existing_remote_ids = set()

        # Filter to genuinely new keys
        new_keys = []
        for key in deduped_keys:
            remote_id = candidate_remote_ids.get(key)
            if remote_id and remote_id in existing_remote_ids:
                continue
            new_keys.append(key)

        if not new_keys:
            # All UIDs from SEARCH were already in DB — backfill search
            # completed for rules that found UIDs (their data is already
            # persisted from a prior run).
            for key in uid_to_rules:
                for r in uid_to_rules[key]:
                    if getattr(r, "initial_backfill_done_at", None) is None:
                        backfill_searched_rule_ids.add(r.id)
            return results_by_rule, True, backfill_searched_rule_ids

        # Apply per-rule fetch limit (scale by number of rules)
        source_limit = fetch_limit * len(rules) if fetch_limit > 0 else 0
        if source_limit > 0:
            new_keys = new_keys[:source_limit]

        # Phase 2: fetch full RFC822 for new emails only
        # Group by folder
        folder_new_keys: dict[str, list[tuple]] = defaultdict(list)
        for key in new_keys:
            folder_new_keys[key[0]].append(key)

        current_folder = None
        for folder, keys in folder_new_keys.items():
            if folder != current_folder:
                typ, _ = conn.select(f'"{folder}"', readonly=True)
                if typ != "OK":
                    continue
                current_folder = folder

            for key in keys:
                uid = key[1]
                try:
                    typ, msg_data = conn.uid("FETCH", uid, "(X-GM-MSGID RFC822)")
                except imaplib.IMAP4.error as e:
                    logger.error("Gmail RFC822 fetch error for uid %s: %s", uid, e)
                    continue
                if typ != "OK" or not msg_data:
                    continue

                xgm_msgid = None
                raw_bytes = None
                for part in msg_data:
                    if isinstance(part, tuple):
                        header_line = part[0]
                        if isinstance(header_line, bytes):
                            m = re.search(rb"X-GM-MSGID\s+(\d+)", header_line)
                            if m:
                                xgm_msgid = m.group(1).decode()
                        raw_bytes = part[1] if len(part) > 1 else raw_bytes

                if raw_bytes is None:
                    continue

                if xgm_msgid:
                    remote_id = xgm_msgid
                else:
                    parsed_msg = email_lib.message_from_bytes(raw_bytes)
                    rfc_msg_id = parsed_msg.get("Message-ID", "").strip()
                    if rfc_msg_id:
                        remote_id = rfc_msg_id
                    else:
                        remote_id = f"{uid.decode()}:{folder}"

                msg_id = f"src{source_id}:gmail:{remote_id}"
                metadata = _extract_message_metadata(raw_bytes)

                # Dispatch to matching rules
                for rule in uid_to_rules.get(key, []):
                    if not _matches_rule_filters(
                        rule,
                        sender=metadata["sender"],
                        subject=metadata["subject"],
                    ):
                        logger.debug(
                            "Skipping Gmail message %s after local rule filter check for rule %s",
                            msg_id,
                            rule.id,
                        )
                        continue
                    results_by_rule[rule.id].append((msg_id, remote_id, raw_bytes))

        # Mark backfill search complete for rules that actually got results
        # through the entire pipeline.  Rules whose SEARCH found UIDs but
        # whose results were dropped (source_limit, filter mismatch, etc.)
        # are intentionally NOT included so their backfill will be retried.
        for rid, results in results_by_rule.items():
            if results:
                backfill_searched_rule_ids.add(rid)

        return results_by_rule, True, backfill_searched_rule_ids
    except imaplib.IMAP4.error as e:
        logger.error("Gmail IMAP error for source %s: %s", source_id, e)
        return results_by_rule, False, set()
    finally:
        try:
            conn.logout()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Fastmail (JMAP)
# ---------------------------------------------------------------------------


def _jmap_request(token: str, url: str, body: dict | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if body:
        req = Request(
            url, data=json.dumps(body).encode(), headers=headers, method="POST"
        )
    else:
        req = Request(url, headers=headers)
    with urlopen(req) as resp:
        return json.loads(resp.read())


def _resolve_mailbox_id(
    token: str, api_url: str, account_id: str, folder_name: str
) -> str | None:
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
    for mb in mailboxes:
        if mb["name"] == folder_name:
            return mb["id"]
    for mb in mailboxes:
        if mb["name"].lower() == folder_name.lower():
            return mb["id"]
    return None


def _fetch_fastmail_source_sync(
    rules,
    *,
    token: str,
    fetch_limit: int,
    source_id: int,
    last_synced_at: datetime.datetime | None = None,
) -> tuple[dict[int, list[tuple]], bool, set[int]]:
    """Fetch emails from Fastmail via JMAP for all rules on one source.

    Establishes the JMAP session once, resolves mailbox IDs once, runs all
    rules' queries on the shared session. Returns
    ``(results_by_rule, fetch_ok, backfill_ready_rule_ids)``.
    """
    results_by_rule: dict[int, list[tuple]] = {r.id: [] for r in rules}

    if not token:
        logger.warning("Fastmail token missing for source %s", source_id)
        return results_by_rule, False, set()

    try:
        logger.info("Fastmail: Starting fetch for source %s", source_id)
        session = _jmap_request(token, JMAP_SESSION_URL)
        api_url = session["apiUrl"]
        account_id = session["primaryAccounts"]["urn:ietf:params:jmap:mail"]
        download_url = session["downloadUrl"]
        logger.info("Fastmail: Connected successfully for source %s", source_id)

        # Cache mailbox IDs by folder name
        mailbox_cache: dict[str, str | None] = {}

        # Date filter: 2-day margin for regular sync, 3 months for initial backfill
        after_date = None
        if last_synced_at is not None:
            since = last_synced_at - datetime.timedelta(days=2)
            after_date = since.strftime("%Y-%m-%dT00:00:00Z")
        else:
            # For initial backfill, use 3 months ago
            since = datetime.datetime.utcnow() - datetime.timedelta(days=INITIAL_BACKFILL_DAYS)
            after_date = since.strftime("%Y-%m-%dT00:00:00Z")

        # Collect all candidate remote_ids per rule before downloading blobs
        # rule_id -> [(remote_id, blob_id, sender_text, subject_text, mailbox_id_filter), ...]
        rule_candidates: dict[int, list[tuple]] = {r.id: [] for r in rules}

        for rule in rules:
            mailbox_id = None
            jmap_filter: dict = {}

            if rule.folder:
                if rule.folder not in mailbox_cache:
                    mailbox_cache[rule.folder] = _resolve_mailbox_id(
                        token,
                        api_url,
                        account_id,
                        rule.folder,
                    )
                mailbox_id = mailbox_cache[rule.folder]
                if mailbox_id:
                    jmap_filter["inMailbox"] = mailbox_id
                else:
                    logger.error("Fastmail folder not found: %s", rule.folder)
                    continue
            if rule.sender:
                jmap_filter["from"] = rule.sender
            if rule.subject:
                jmap_filter["subject"] = rule.subject
            # For rules that haven't completed initial backfill, cap the
            # historical scan at 3 months rather than a full-history search.
            needs_backfill = getattr(rule, "initial_backfill_done_at", None) is None
            if needs_backfill:
                backfill_after = (
                    datetime.datetime.now(datetime.timezone.utc)
                    - datetime.timedelta(days=INITIAL_BACKFILL_DAYS)
                ).strftime("%Y-%m-%dT00:00:00Z")
                jmap_filter["after"] = backfill_after
                logger.info(
                    "Rule %s (bank=%s, sender=%s) initial backfill — after %s",
                    rule.id,
                    rule.bank,
                    rule.sender,
                    backfill_after,
                )
            elif after_date:
                jmap_filter["after"] = after_date

            position = 0
            page_size = max(fetch_limit, 50)
            rule_count = 0
            while fetch_limit <= 0 or rule_count < fetch_limit:
                body = {
                    "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
                    "methodCalls": [
                        [
                            "Email/query",
                            {
                                "accountId": account_id,
                                "filter": jmap_filter,
                                "sort": [
                                    {"property": "receivedAt", "isAscending": False}
                                ],
                                "position": position,
                                "limit": page_size,
                            },
                            "0",
                        ],
                        [
                            "Email/get",
                            {
                                "accountId": account_id,
                                "#ids": {
                                    "resultOf": "0",
                                    "name": "Email/query",
                                    "path": "/ids",
                                },
                                "properties": [
                                    "id",
                                    "blobId",
                                    "receivedAt",
                                    "from",
                                    "subject",
                                    "mailboxIds",
                                ],
                            },
                            "1",
                        ],
                    ],
                }

                result = _jmap_request(token, api_url, body)
                query_result = result["methodResponses"][0][1]
                emails = result["methodResponses"][1][1].get("list", [])
                query_ids = query_result.get("ids", [])
                if not query_ids:
                    break

                for em in emails:
                    if fetch_limit > 0 and rule_count >= fetch_limit:
                        break

                    remote_id = em["id"]
                    sender_text = _format_jmap_from_field(em.get("from"))
                    subject_text = em.get("subject", "")

                    if mailbox_id and mailbox_id not in em.get("mailboxIds", {}):
                        continue
                    if not _matches_rule_filters(
                        rule, sender=sender_text, subject=subject_text
                    ):
                        continue

                    rule_candidates[rule.id].append(
                        (
                            remote_id,
                            em["blobId"],
                            sender_text,
                            subject_text,
                        )
                    )
                    rule_count += 1

                position += len(query_ids)
                if len(query_ids) < page_size:
                    break

        # Scoped dedup: check all candidate remote_ids against DB
        all_remote_ids = set()
        for candidates in rule_candidates.values():
            for remote_id, _, _, _ in candidates:
                all_remote_ids.add(remote_id)

        if all_remote_ids:
            existing_remote_ids = _check_remote_ids_in_db_sync_params(
                source_id, all_remote_ids
            )
        else:
            existing_remote_ids = set()

        # Download blobs only for new emails
        for rule in rules:
            for remote_id, blob_id, sender_text, subject_text in rule_candidates[
                rule.id
            ]:
                if remote_id in existing_remote_ids:
                    continue

                msg_id = f"src{source_id}:fastmail:{remote_id}"

                blob_url = (
                    download_url.replace("{accountId}", account_id)
                    .replace("{blobId}", blob_id)
                    .replace("{type}", "application/octet-stream")
                    .replace("{name}", "email.eml")
                )
                req = Request(blob_url, headers={"Authorization": f"Bearer {token}"})
                with urlopen(req) as resp:
                    raw = resp.read()
                results_by_rule[rule.id].append((msg_id, remote_id, raw))

        # Build backfill-ready set: rules that got results or had
        # genuinely zero candidates.
        backfill_ready: set[int] = set()
        for rule in rules:
            if getattr(rule, "initial_backfill_done_at", None) is not None:
                continue  # already backfilled, skip
            if results_by_rule.get(rule.id):
                backfill_ready.add(rule.id)
            elif not rule_candidates.get(rule.id):
                # JMAP query returned zero candidates — genuinely empty
                backfill_ready.add(rule.id)
        return results_by_rule, True, backfill_ready
    except Exception as e:
        logger.error("Fastmail JMAP error for source %s: %s", source_id, e)
        return results_by_rule, False, set()


# ---------------------------------------------------------------------------
# Single-message fetch helpers (for viewing original emails)
# ---------------------------------------------------------------------------


def _fetch_gmail_single_sync(user: str, password: str, remote_id: str) -> bytes | None:
    """Fetch a single email from Gmail by X-GM-MSGID."""
    try:
        conn = imaplib.IMAP4_SSL("imap.gmail.com")
        conn.login(user, password)
        conn.select('"[Gmail]/All Mail"', readonly=True)
        # Search by X-GM-MSGID
        typ, data = conn.uid("SEARCH", None, f"X-GM-MSGID {remote_id}")
        if typ != "OK" or not data[0]:
            conn.logout()
            return None
        uid = data[0].split()[-1]
        typ, msg_data = conn.uid("FETCH", uid, "(RFC822)")
        conn.logout()
        if typ != "OK" or not msg_data or not msg_data[0]:
            return None
        return msg_data[0][1]
    except Exception as e:
        logger.error("Failed to fetch Gmail message %s: %s", remote_id, e)
        return None


def _fetch_fastmail_single_sync(token: str, remote_id: str) -> bytes | None:
    """Fetch a single email from Fastmail by JMAP email ID."""
    try:
        session = _jmap_request(token, JMAP_SESSION_URL)
        api_url = session["apiUrl"]
        account_id = session["primaryAccounts"]["urn:ietf:params:jmap:mail"]
        download_url = session["downloadUrl"]

        body = {
            "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
            "methodCalls": [
                [
                    "Email/get",
                    {
                        "accountId": account_id,
                        "ids": [remote_id],
                        "properties": ["blobId"],
                    },
                    "0",
                ]
            ],
        }
        result = _jmap_request(token, api_url, body)
        emails = result["methodResponses"][0][1].get("list", [])
        if not emails:
            return None
        blob_url = (
            download_url.replace("{accountId}", account_id)
            .replace("{blobId}", emails[0]["blobId"])
            .replace("{type}", "application/octet-stream")
            .replace("{name}", "email.eml")
        )
        req = Request(blob_url, headers={"Authorization": f"Bearer {token}"})
        with urlopen(req) as resp:
            return resp.read()
    except Exception as e:
        logger.error("Failed to fetch Fastmail message %s: %s", remote_id, e)
        return None


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------


def _process_email(
    bank: str, raw_bytes: bytes
) -> tuple[str | None, dict | None, str | None]:
    """Parse raw email bytes. Returns (error, txn_dict, password_hint)."""
    html = _extract_html_body(raw_bytes)
    if not html:
        html = _extract_text_body(raw_bytes)
    if not html:
        return "No HTML or text body found in email", None, None

    try:
        parsed = parse_email(bank, html)
    except (ParseError, UnsupportedEmailTypeError) as e:
        return str(e), None, None

    if (txn := parsed.transaction) is None:
        return None, None, parsed.password_hint
    return (
        None,
        {
            "bank": parsed.bank,
            "email_type": parsed.email_type,
            "direction": txn.direction,
            "amount": float(txn.amount.amount),
            "currency": txn.amount.currency,
            "transaction_date": txn.transaction_date,
            "transaction_time": txn.transaction_time,
            "counterparty": txn.counterparty,
            "card_mask": txn.card_mask,
            "account_mask": txn.account_mask,
            "reference_number": txn.reference_number,
            "channel": txn.channel,
            "balance": float(txn.balance.amount) if txn.balance else None,
            "raw_description": txn.raw_description,
        },
        None,
    )


# ---------------------------------------------------------------------------
# Main poll function (called by scheduler)
# ---------------------------------------------------------------------------


async def poll_all() -> dict:
    """Run one full poll cycle across all enabled rules. Returns stats."""
    if POLL_LOCK.locked():
        logger.info(
            "Poll requested while another poll is already running; skipping overlap"
        )
        return {
            "rules": 0,
            "fetched": 0,
            "parsed": 0,
            "failed": 0,
            "skipped": 0,
            "status": "already_running",
        }

    async with POLL_LOCK:
        POLL_STATUS["state"] = "polling"
        POLL_STATUS["started_at"] = datetime.datetime.utcnow()
        POLL_STATUS["finished_at"] = None
        POLL_STATUS["last_error"] = None
        POLL_STATUS["progress"] = {
            "source": "",
            "rule": "",
            "email": "",
            "detail": "Initializing...",
        }

        stats = {"rules": 0, "fetched": 0, "parsed": 0, "failed": 0, "skipped": 0}
        from bank_email_fetcher.settings_service import get_setting_int

        fetch_limit = max(1, get_setting_int("poll_fetch_limit_per_rule", 50))

        try:
            try:
                _cleanup_failed_spool()
            except Exception as e:
                logger.warning("Failed spool cleanup error: %s", e)

            async with async_session() as session:
                result = await session.execute(
                    select(FetchRule).where(FetchRule.enabled.is_(True))
                )
                rules = result.scalars().all()

            if not rules:
                logger.info("No enabled fetch rules, nothing to poll")
                POLL_STATUS["last_stats"] = stats
                return stats

            async with async_session() as session:
                result = await session.execute(
                    select(EmailSource).where(EmailSource.active.is_(True))
                )
                sources_by_id = {src.id: src for src in result.scalars().all()}

            # Build account/card lookup once for the entire poll cycle so
            # link_transaction() below never issues per-transaction queries.
            async with async_session() as session:
                _link_ctx = await build_link_context(session)

            inserted_msg_ids: set[str] = set()

            # Group rules by source_id for connection pooling
            rules_by_source: dict[int, list] = defaultdict(list)
            for rule in rules:
                if rule.source_id:
                    source = sources_by_id.get(rule.source_id)
                    if not source:
                        logger.warning(
                            "Rule %s references source_id=%s which is missing or inactive, skipping",
                            rule.id,
                            rule.source_id,
                        )
                        continue
                    rules_by_source[rule.source_id].append(rule)
                else:
                    logger.warning(
                        "Rule %s has no source_id (legacy rule with provider=%s), skipping. "
                        "Please assign an email source to this rule.",
                        rule.id,
                        rule.provider,
                    )

            total_rules = len(rules)
            rule_counter = 0

            for source_id, source_rules in rules_by_source.items():
                source = sources_by_id[source_id]
                provider = source.provider
                source_label = source.label

                for rule in source_rules:
                    stats["rules"] += 1

                try:
                    creds = decrypt_credentials(source.credentials)
                except Exception as e:
                    logger.error(
                        "Failed to decrypt credentials for source %s: %s", source_id, e
                    )
                    continue

                POLL_STATUS["progress"] = {
                    "source": source_label,
                    "rule": f"{rule_counter + 1}/{total_rules}",
                    "email": "0/?",
                    "detail": f"Fetching from {source_label} ({len(source_rules)} rules)",
                }
                logger.info(
                    "Starting fetch for source %s (%s) with %d rules",
                    source_id,
                    source_label,
                    len(source_rules),
                )

                for rule in source_rules:
                    logger.info(
                        "Polling rule %s: source=%s provider=%s bank=%s sender=%s subject=%s "
                        "folder=%s limit=%s",
                        rule.id,
                        source_id,
                        provider,
                        rule.bank,
                        rule.sender,
                        rule.subject,
                        rule.folder,
                        fetch_limit,
                    )

                fetch_ok = False
                backfill_ready_rule_ids: set[int] = set()
                logger.info(
                    "Gmail: Starting fetch for source %s (%d rules)",
                    source_id,
                    len(source_rules),
                )
                if provider == "gmail":
                    (
                        results_by_rule,
                        fetch_ok,
                        backfill_ready_rule_ids,
                    ) = await asyncio.to_thread(
                        _fetch_gmail_source_sync,
                        source_rules,
                        user=creds.get("user", ""),
                        password=creds.get("app_password", ""),
                        fetch_limit=fetch_limit,
                        source_id=source_id,
                        last_synced_at=source.last_synced_at,
                    )
                elif provider == "fastmail":
                    (
                        results_by_rule,
                        fetch_ok,
                        backfill_ready_rule_ids,
                    ) = await asyncio.to_thread(
                        _fetch_fastmail_source_sync,
                        source_rules,
                        token=creds.get("token", ""),
                        fetch_limit=fetch_limit,
                        source_id=source_id,
                        last_synced_at=source.last_synced_at,
                    )
                else:
                    logger.warning(
                        "Unknown provider %s for source %s", provider, source_id
                    )
                    continue

                logger.info(
                    "Gmail/Fastmail fetch completed for source %s, fetched %d total emails",
                    source_id,
                    sum(len(v) for v in results_by_rule.values()),
                )

                # Process results per rule
                for rule in source_rules:
                    rule_counter += 1
                    new_emails = results_by_rule.get(rule.id, [])

                    # Track transactions to notify via Telegram after commit
                    pending_notifications: list[
                        tuple[int, dict]
                    ] = []  # (txn_id, txn_data_for_display)
                    pending_payment_checks: list[
                        tuple[int, int, object]
                    ] = []  # (txn_id, account_id, amount)
                    from bank_email_fetcher.settings_service import (
                        should_notify_transactions,
                    )

                    should_notify = (
                        should_notify_transactions()
                        and getattr(rule, "initial_backfill_done_at", None) is not None
                    )

                    total_emails = len(new_emails)
                    for email_idx, (msg_id, remote_id, raw_bytes) in enumerate(
                        new_emails, 1
                    ):
                        if msg_id in inserted_msg_ids:
                            continue
                        inserted_msg_ids.add(msg_id)
                        stats["fetched"] += 1
                        POLL_STATUS["progress"] = {
                            "source": source_label,
                            "rule": f"{rule_counter}/{total_rules}",
                            "email": f"{email_idx}/{total_emails}",
                            "detail": f"Processing email {email_idx}/{total_emails} from {source_label}",
                        }
                        metadata = _extract_message_metadata(raw_bytes)
                        received_at = _parse_email_date(raw_bytes)

                        email_kind = getattr(rule, "email_kind", None)

                        # Decide processing path based on rule.email_kind:
                        #   "statement"   -> skip transaction parsing, go straight to statements
                        #   "transaction" -> only try transaction parsing, no statement fallback
                        #   None          -> try transaction first, fall back to statements (legacy)
                        error = None
                        txn_data = None
                        stmt_result = None

                        password_hint = None
                        if email_kind != "statement":
                            error, txn_data, password_hint = _process_email(
                                rule.bank, raw_bytes
                            )

                        # Statement pipeline: try CC then bank account statement
                        # Triggers when: explicit statement rule, parse failure, or
                        # parse succeeded but found a statement email (no transaction)
                        should_try_statement = email_kind == "statement" or (
                            email_kind is None and not txn_data
                        )
                        if should_try_statement:
                            subject = metadata.get("subject", "")
                            logger.info(
                                "Email %s %s (bank=%s, subject=%r), trying statement path",
                                msg_id,
                                "routed to statement pipeline"
                                if email_kind == "statement"
                                else "failed parsing",
                                rule.bank,
                                subject[:80],
                            )
                            # Try CC statement first
                            try:
                                from bank_email_fetcher.statements import (
                                    process_statement_email,
                                )

                                stmt_result = await process_statement_email(
                                    rule.bank,
                                    raw_bytes,
                                    subject,
                                    source_id=source_id,
                                )
                            except Exception as stmt_err:
                                logger.warning(
                                    "CC statement processing error for %s: %s",
                                    msg_id,
                                    stmt_err,
                                )

                            # If CC statement didn't match, try bank account statement
                            if stmt_result is None:
                                try:
                                    from bank_email_fetcher.bank_statements import (
                                        process_bank_statement_email,
                                    )

                                    stmt_result = await process_bank_statement_email(
                                        rule.bank,
                                        raw_bytes,
                                        subject,
                                        source_id=source_id,
                                        password_hint=password_hint,
                                    )
                                except Exception as stmt_err:
                                    logger.warning(
                                        "Bank statement processing error for %s: %s",
                                        msg_id,
                                        stmt_err,
                                    )

                            if stmt_result is None:
                                logger.info(
                                    "Statement processing returned None for %s (no PDF or subject mismatch)",
                                    msg_id,
                                )
                                # For statement-only rules, set error if no result
                                if email_kind == "statement":
                                    error = "Statement processing returned no result"

                        if stmt_result:
                            error = None
                            stats["parsed"] += 1
                            stmt_type = (
                                "bank"
                                if stmt_result.get("bank_statement_upload_id")
                                else "CC"
                            )
                            logger.info(
                                "Processed %s statement from email %s: matched=%d imported=%d",
                                stmt_type,
                                msg_id,
                                stmt_result["matched"],
                                stmt_result["imported"],
                            )
                        elif error:
                            try:
                                _save_failed_email(provider, msg_id, raw_bytes)
                            except Exception as save_err:
                                logger.warning(
                                    "Could not save failed email to spool: %s", save_err
                                )

                        async with async_session() as session:
                            async with session.begin():
                                if stmt_result:
                                    initial_status = "parsed"
                                else:
                                    initial_status = (
                                        "pending"
                                        if txn_data
                                        else ("failed" if error else "skipped")
                                    )
                                email_row = Email(
                                    provider=provider,
                                    message_id=msg_id,
                                    source_id=source_id,
                                    remote_id=remote_id,
                                    sender=metadata["sender"],
                                    subject=metadata["subject"],
                                    received_at=received_at,
                                    status=initial_status,
                                    error=error,
                                    rule_id=rule.id,
                                )
                                session.add(email_row)
                                await session.flush()

                                # Link statement upload to this email
                                if stmt_result and stmt_result.get(
                                    "statement_upload_id"
                                ):
                                    from bank_email_fetcher.db import StatementUpload

                                    su = await session.get(
                                        StatementUpload,
                                        stmt_result["statement_upload_id"],
                                    )
                                    if su:
                                        su.email_id = email_row.id
                                elif stmt_result and stmt_result.get(
                                    "bank_statement_upload_id"
                                ):
                                    from bank_email_fetcher.db import (
                                        BankStatementUpload,
                                    )

                                    su = await session.get(
                                        BankStatementUpload,
                                        stmt_result["bank_statement_upload_id"],
                                    )
                                    if su:
                                        su.email_id = email_row.id

                                # Informational-only email types — no funds moved
                                _SKIP_TXN_TYPES = {"sbi_cc_transaction_declined"}

                                if (
                                    txn_data
                                    and txn_data.get("email_type") in _SKIP_TXN_TYPES
                                ):
                                    email_row.status = "parsed"
                                    email_row.error = None
                                    stats["parsed"] += 1
                                    if should_notify:
                                        from bank_email_fetcher.telegram_bot import (
                                            send_transaction_notification,
                                        )

                                        # Tag it so the TG message shows "DECLINED"
                                        txn_data["_declined"] = True
                                        pending_notifications.append((0, txn_data))
                                elif txn_data:
                                    try:
                                        async with session.begin_nested():
                                            txn_row = Transaction(
                                                email_id=email_row.id, **txn_data
                                            )
                                            session.add(txn_row)
                                            await session.flush()
                                    except IntegrityError as exc:
                                        if not _is_duplicate_transaction_error(exc):
                                            raise
                                        email_row.status = "skipped"
                                        email_row.error = (
                                            "Duplicate transaction skipped because an identical "
                                            "transaction row already exists"
                                        )
                                        stats["skipped"] += 1
                                        logger.warning(
                                            "Skipping duplicate transaction for email %s (rule=%s, source=%s): %s",
                                            msg_id,
                                            rule.id,
                                            source_id,
                                            exc.orig,
                                        )
                                    else:
                                        email_row.status = "parsed"
                                        email_row.error = None
                                        stats["parsed"] += 1
                                        # Auto-link to account/card using preloaded context
                                        link_transaction(_link_ctx, txn_row)
                                        await session.flush()
                                        if should_notify:
                                            from bank_email_fetcher.telegram_bot import (
                                                build_account_label,
                                            )
                                            account_obj = (
                                                await session.get(Account, txn_row.account_id)
                                                if txn_row.account_id
                                                else None
                                            )
                                            card_obj = (
                                                await session.get(Card, txn_row.card_id)
                                                if txn_row.card_id
                                                else None
                                            )
                                            pending_notifications.append(
                                                (
                                                    txn_row.id,
                                                    {
                                                        "bank": txn_row.bank,
                                                        "direction": txn_row.direction,
                                                        "amount": txn_row.amount,
                                                        "counterparty": txn_row.counterparty,
                                                        "transaction_date": txn_row.transaction_date,
                                                        "transaction_time": txn_row.transaction_time,
                                                        "card_mask": txn_row.card_mask,
                                                        "account_label": build_account_label(
                                                            account_obj, card_obj
                                                        ),
                                                        "channel": txn_row.channel,
                                                    },
                                                )
                                            )
                                        if (
                                            txn_row.direction == "credit"
                                            and txn_row.account_id
                                        ):
                                            pending_payment_checks.append(
                                                (
                                                    txn_row.id,
                                                    txn_row.account_id,
                                                    txn_row.amount,
                                                )
                                            )
                                elif error:
                                    stats["failed"] += 1
                                else:
                                    stats["skipped"] += 1

                    # Send Telegram notifications AFTER commits (outside DB transaction)
                    if pending_notifications:
                        from bank_email_fetcher.settings_service import (
                            get_telegram_chat_id,
                            get_setting_int,
                        )

                        _chat_id = get_telegram_chat_id()
                        bulk_threshold = get_setting_int("telegram.bulk_threshold", 5)
                        if len(pending_notifications) <= bulk_threshold:
                            from bank_email_fetcher.telegram_bot import (
                                send_transaction_notification,
                            )

                            for txn_id, txn_info in pending_notifications:
                                await send_transaction_notification(
                                    txn_id, txn_info, _chat_id
                                )
                        else:
                            from bank_email_fetcher.telegram_bot import (
                                send_bulk_summary,
                            )

                            await send_bulk_summary(
                                len(pending_notifications),
                                _chat_id,
                                source="email",
                                txns=pending_notifications,
                            )

                    # Check if any credit transactions satisfy pending payment reminders
                    if pending_payment_checks:
                        from bank_email_fetcher.reminders import check_payment_received

                        for txn_id, acct_id, amt in pending_payment_checks:
                            try:
                                await check_payment_received(txn_id, acct_id, amt)
                            except Exception as e:
                                logger.warning(
                                    "Payment-received check failed for txn %s: %s",
                                    txn_id,
                                    e,
                                )

                # Mark initial backfill complete only for rules whose search
                # phase completed AND either produced results or genuinely
                # found nothing.  Rules whose SEARCH found UIDs but whose
                # results were lost (source_limit, filter mismatch, etc.)
                # will NOT be marked, so their backfill is retried next poll.
                if fetch_ok and backfill_ready_rule_ids:
                    async with async_session() as session:
                        for rule in source_rules:
                            if rule.id not in backfill_ready_rule_ids:
                                continue
                            if getattr(rule, "initial_backfill_done_at", None) is None:
                                db_rule = await session.get(FetchRule, rule.id)
                                if db_rule:
                                    db_rule.initial_backfill_done_at = (
                                        datetime.datetime.utcnow()
                                    )
                                    logger.info(
                                        "Marked rule %s (bank=%s, sender=%s) as backfill complete",
                                        rule.id,
                                        rule.bank,
                                        rule.sender,
                                    )
                        await session.commit()

                # Update last_synced_at ONCE per source — only on successful fetch
                async with async_session() as session:
                    src = await session.get(EmailSource, source_id)
                    if src:
                        if fetch_ok:
                            src.last_synced_at = datetime.datetime.utcnow()
                            src.last_error = None
                        else:
                            src.last_error = (
                                f"Fetch failed for {provider} source {source_id}"
                            )
                        await session.commit()

            logger.info("Poll complete: %s", stats)
            POLL_STATUS["last_stats"] = stats
            return stats
        except Exception as e:
            POLL_STATUS["last_error"] = str(e)
            raise
        finally:
            POLL_STATUS["state"] = "idle"
            POLL_STATUS["finished_at"] = datetime.datetime.utcnow()
            POLL_STATUS["progress"] = None
