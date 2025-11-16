import os
import asyncio
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
PUBLIC_DIR.mkdir(exist_ok=True)

THUMB_PATH = os.environ.get("THUMB_PATH", str(WORKDIR / "thumb.jpg"))
WATERMARK_TEXT = os.environ.get("WATERMARK_TEXT", "Stark JR. 😎🔥 | Extracted / Done By :- https://t.me/tonystark_jr")
CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "https://t.me/tonystark_jr")

processor = None
PENDING_BATCH = {}

def is_admin(uid):
    return uid == ADMIN_ID

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔥 Stark JR. Batch Bot Online 😎")

async def batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return await update.message.reply_text("Unauthorized 💀")

    if "|" not in " ".join(context.args):
        return await update.message.reply_text("Use: /batch BatchName|Subject")

    batch, subject = " ".join(context.args).split("|", 1)
    batch, subject = batch.strip(), subject.strip()

    chat = update.effective_chat.id
    PENDING_BATCH[chat] = {"batch": batch, "subject": subject, "links": []}

    await update.message.reply_text(
        f"Batch: {batch}\nSubject: {subject}\n\nPaste ALL m3u8 links.\nSend *DONE* when finished.",
        parse_mode="Markdown"
    )

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    chat = update.effective_chat.id
    if chat not in PENDING_BATCH:
        return

    text = update.message.text.strip()

    if text.upper() == "DONE":
        info = PENDING_BATCH.pop(chat)
        links = info["links"]
        if not links:
            return await update.message.reply_text("No links added.")

        await update.message.reply_text(f"🔥 {len(links)} lectures queued. Processing…")

        for i, link in enumerate(links, 1):
            meta = {
                "batch": info["batch"],
                "subject": info["subject"],
                "lecture_no": i,
                "total": len(links),
                "m3u8": link,
                "requester_chat": chat
            }
            await processor.enqueue(meta)
        return

    # otherwise it's link(s)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    PENDING_BATCH[chat]["links"].extend(lines)

    await update.message.reply_text(f"Added {len(lines)} link(s). Total: {len(PENDING_BATCH[chat]['links'])}")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(f"Queue size: {processor.queue_size()}")

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
    app.add_handler(CommandHandler("batch", batch))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
