"""Telegram bot for transaction notifications and note replies.

Sends a notification for each new transaction (post-backfill only).
If the user replies to a notification, the reply text is saved as the
transaction's note. Only the configured chat_id is authorized.
"""

import html
import logging
import re

from telegram import Update
from telegram.ext import Application, CallbackQueryHandler, MessageHandler, filters

logger = logging.getLogger(__name__)

tg_app: Application | None = None


async def init_telegram(token: str):
    """Initialize the Telegram bot application."""
    global tg_app
    app = Application.builder().token(token).build()
    app.add_handler(MessageHandler(filters.TEXT & filters.REPLY, _handle_reply))
    app.add_handler(CallbackQueryHandler(_handle_callback))
    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(
            drop_pending_updates=True, allowed_updates=["message", "callback_query"]
        )
    except Exception:
        try:
            if app.updater.running:
                await app.updater.stop()
            if app.running:
                await app.stop()
            await app.shutdown()
        except Exception:
            pass
        raise
    tg_app = app
    logger.info("Telegram bot started")


async def shutdown_telegram():
    """Shutdown the Telegram bot."""
    global tg_app
    if tg_app:
        app = tg_app
        tg_app = None
        try:
            if app.updater.running:
                await app.updater.stop()
            if app.running:
                await app.stop()
            await app.shutdown()
        except Exception as e:
            logger.warning("Error during Telegram shutdown: %s", e)
        logger.info("Telegram bot stopped")


async def send_transaction_notification(
    txn_id: int, txn_info: dict, chat_id: int
) -> None:
    """Send a transaction notification. Includes #txn_id for reply matching."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    app = tg_app
    if not app:
        return
    try:
        is_declined = txn_info.get("_declined", False)
        direction = txn_info.get("direction", "")
        if is_declined:
            direction_emoji = "\U0001f6ab"
            direction_label = "DECLINED"
        elif direction == "debit":
            direction_emoji = "\U0001f534"
            direction_label = "DEBIT"
        else:
            direction_emoji = "\U0001f7e2"
            direction_label = "CREDIT"
        sign = "-" if direction == "debit" else "+"
        amount = txn_info.get("amount", 0)
        amount_str = f"{amount:,.2f}"
        bank = html.escape(str(txn_info.get("bank", "")).upper())
        counterparty = html.escape(str(txn_info.get("counterparty", "") or ""))
        card_mask = html.escape(str(txn_info.get("card_mask", "") or ""))
        txn_date = txn_info.get("transaction_date", "")
        txn_time = txn_info.get("transaction_time", "")
        channel = txn_info.get("channel", "")

        # Build account/card label string
        account_label = ""
        account_id = txn_info.get("account_id")
        card_id = txn_info.get("card_id")
        if account_id or card_id:
            from bank_email_fetcher.db import Account, Card, async_session
            async with async_session() as session:
                if card_id:
                    card = await session.get(Card, card_id)
                    if card:
                        account = await session.get(Account, card.account_id)
                        card_label = card.label or card.card_mask
                        account_label = f"{account.label} - {card_label}"
                elif account_id:
                    account = await session.get(Account, account_id)
                    if account:
                        account_label = account.label

        # Build notification text
        id_suffix = f"  #{txn_id}" if txn_id else ""
        lines = [
            f"{direction_emoji} <b>{bank}</b> {direction_label}{id_suffix}",
            f"<b>{sign}\u20b9{amount_str}</b>",
        ]
        if counterparty:
            # Add channel badge if present
            if channel:
                lines.append(f"{counterparty} \u00b7 <code>{channel}</code>")
            else:
                lines.append(counterparty)

        # Date line with account/card info
        details_parts = []
        if txn_date:
            date_str = html.escape(str(txn_date))
            if txn_time:
                date_str += f" {html.escape(str(txn_time)[:5])}"
            details_parts.append(date_str)
        if account_label:
            details_parts.append(f"Account: {html.escape(account_label)}")
        elif card_mask:
            details_parts.append(f"Card: {card_mask}")
        if details_parts:
            lines.append(" \u00b7 ".join(details_parts))

        text = "\n".join(lines)

        # Add inline keyboard for quick actions
        keyboard = [
            [
                InlineKeyboardButton(
                    "\U0001f4dd Add Note", callback_data=f"note:{txn_id}"
                )
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await app.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
    except Exception as e:
        logger.warning(
            "Failed to send Telegram notification for txn #%s: %s", txn_id, e
        )


async def send_bulk_summary(
    count: int,
    chat_id: int,
    *,
    account_label: str | None = None,
    source: str | None = None,
    txns: list[tuple[int, dict]] | None = None,
) -> None:
    """Send a single summary when too many transactions arrive at once."""
    app = tg_app
    if not app:
        return
    try:
        lines = [f"\U0001f4e5 Imported <b>{count}</b> transactions"]

        detail_parts = []
        if account_label:
            detail_parts.append(html.escape(account_label))
        if source:
            _source_display = {"cc_statement": "CC statement", "email": "Email"}
            detail_parts.append(_source_display.get(source, source))
        if detail_parts:
            lines.append(" \u00b7 ".join(detail_parts))

        if txns:
            debits = [t for _, t in txns if t.get("direction") == "debit"]
            credits = [t for _, t in txns if t.get("direction") == "credit"]
            parts = []
            if debits:
                total = sum(float(t.get("amount", 0)) for t in debits)
                parts.append(f"{len(debits)} debits (\u20b9{total:,.2f})")
            if credits:
                total = sum(float(t.get("amount", 0)) for t in credits)
                parts.append(f"{len(credits)} credits (\u20b9{total:,.2f})")
            if parts:
                lines.append(" \u00b7 ".join(parts))

        text = "\n".join(lines)
        await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
    except Exception as e:
        logger.warning("Failed to send Telegram bulk summary: %s", e)


async def _handle_callback(update: Update, context) -> None:
    """Route callback queries to appropriate handlers."""
    query = update.callback_query
    if not query or not query.data:
        return

    if query.data.startswith("paid:"):
        from bank_email_fetcher.reminders import handle_mark_paid_callback

        await handle_mark_paid_callback(update, context)
    elif query.data.startswith("note:"):
        # Handle "Add Note" button click — prompt user to reply with note
        txn_id = query.data.split(":", 1)[1]
        await query.answer("Reply to this message with your note", show_alert=True)
        return
    else:
        await query.answer("Unknown action")


async def _handle_reply(update: Update, context) -> None:
    """Handle reply messages — save as transaction note. Only authorized chat."""
    from bank_email_fetcher.settings_service import get_telegram_chat_id

    msg = update.message
    if not msg or not msg.text:
        return
    # Only accept from configured chat
    if msg.chat_id != get_telegram_chat_id():
        return
    if not msg.reply_to_message or not msg.reply_to_message.text:
        return
    # Only accept replies to messages sent by this bot
    if (
        not msg.reply_to_message.from_user
        or msg.reply_to_message.from_user.id != context.bot.id
    ):
        return

    # Parse transaction ID from the first line of the notification (e.g., "#1234")
    original_text = msg.reply_to_message.text
    first_line = original_text.splitlines()[0] if original_text else ""
    match = re.search(r"#(\d+)\s*$", first_line)
    if not match:
        return
    txn_id = int(match.group(1))

    note_text = msg.text.strip()
    if not note_text:
        return

    from bank_email_fetcher.db import Transaction, async_session

    async with async_session() as session:
        txn = await session.get(Transaction, txn_id)
        if txn:
            txn.note = note_text
            await session.commit()
            await msg.reply_text(f"Saved note for #{txn_id}")
        else:
            await msg.reply_text(f"Transaction #{txn_id} not found")
