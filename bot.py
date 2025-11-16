import os
import asyncio
import logging
from pathlib import Path
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters
)
from processor import QueueProcessor

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

WORKDIR = Path(os.environ.get("WORKDIR", "/work"))
WORKDIR.mkdir(parents=True, exist_ok=True)

PUBLIC_DIR = WORKDIR / "public"
PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

THUMB_PATH = os.environ.get("THUMB_PATH", str(WORKDIR / "thumb.jpg"))
WATERMARK_TEXT = os.environ.get("WATERMARK_TEXT", "Stark JR. 😎🔥 | Extracted / Done By :- https://t.me/tonystark_jr")
CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "https://t.me/tonystark_jr")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

processor = None
PENDING_BATCH = {}

def is_admin(uid):
    return uid == ADMIN_ID

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Stark JR. Batch Bot Online 😎🔥")

async def batch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return await update.message.reply_text("Unauthorized")

    if "|" not in " ".join(context.args):
        return await update.message.reply_text("Usage: /batch BatchName|Subject")

    batch, subject = " ".join(context.args).split("|", 1)
    batch, subject = batch.strip(), subject.strip()
    chat_id = update.effective_chat.id

    PENDING_BATCH[chat_id] = {"batch": batch, "subject": subject, "links": []}

    await update.message.reply_text(
        f"Batch: {batch}\nSubject: {subject}\n\nPaste all m3u8 links.\nSend DONE when finished."
    )

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    chat_id = update.effective_chat.id
    if chat_id not in PENDING_BATCH:
        return await update.message.reply_text("Start with /batch")

    text = update.message.text.strip()

    if text.upper() == "DONE":
        batch = PENDING_BATCH.pop(chat_id)
        links = batch["links"]

        if not links:
            return await update.message.reply_text("No links given.")

        await update.message.reply_text(f"Queued {len(links)} lectures. Processing...")

        for i, link in enumerate(links, 1):
            meta = {
                "batch": batch["batch"],
                "subject": batch["subject"],
                "lecture_no": i,
                "total": len(links),
                "m3u8": link,
                "requester_chat": chat_id
            }
            await processor.enqueue(meta)
        return

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    PENDING_BATCH[chat_id]["links"].extend(lines)

    await update.message.reply_text(f"Added {len(lines)} link(s). Total: {len(PENDING_BATCH[chat_id]['links'])}")

async def status_cmd(update, context):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(f"Queue: {processor.queue_size()}")

async def main():
    global processor

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    processor = QueueProcessor(
        bot_application=app,
        public_dir=str(PUBLIC_DIR),
        thumb_path=THUMB_PATH,
        watermark_text=WATERMARK_TEXT,
        channel_link=CHANNEL_LINK
    )

    await processor.start()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("batch", batch_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
