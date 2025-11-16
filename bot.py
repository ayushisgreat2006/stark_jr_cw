import os
import asyncio
from pathlib import Path
from telegram import Update
from telegram.ext import (
    Application, ApplicationBuilder,
    CommandHandler, MessageHandler,
    ContextTypes, filters
)
from processor import QueueProcessor

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()

ADMIN_ENV = os.environ.get("ADMIN_ID", "").strip()
ADMIN_ID = int(ADMIN_ENV) if ADMIN_ENV else None

WORKDIR = Path(os.environ.get("WORKDIR", "/work"))
WORKDIR.mkdir(parents=True, exist_ok=True)
PUBLIC_DIR = WORKDIR / "public"
PUBLIC_DIR.mkdir(exist_ok=True)

THUMB_PATH = os.environ.get("THUMB_PATH", str(WORKDIR / "thumb.jpg"))
WATERMARK_TEXT = os.environ.get("WATERMARK_TEXT", "Stark JR. 😎🔥 | Extracted / Done By :- https://t.me/tonystark_jr")
CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "https://t.me/tonystark_jr")

processor = None
PENDING_BATCH = {}

def is_admin(uid): return ADMIN_ID is None or uid == ADMIN_ID

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔥 Stark JR Bot Online")

async def batch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("Unauthorized")

    if "|" not in " ".join(context.args):
        return await update.message.reply_text("Use: /batch Batch|Subject")

    batch, subject = " ".join(context.args).split("|", 1)
    chat = update.effective_chat.id

    PENDING_BATCH[chat] = {
        "batch": batch.strip(),
        "subject": subject.strip(),
        "links": []
    }

    await update.message.reply_text("Send links. Type DONE when finished.")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    chat = update.effective_chat.id
    if chat not in PENDING_BATCH:
        return

    text = update.message.text.strip()

    if text.upper() == "DONE":
        info = PENDING_BATCH.pop(chat)
        links = info["links"]

        if not links:
            return await update.message.reply_text("No links.")

        await update.message.reply_text(f"Queued {len(links)} lectures.")

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
    
    PENDING_BATCH[chat]["links"].extend(
        [x.strip() for x in text.splitlines() if x.strip()]
    )

    await update.message.reply_text(f"Added links. Total now: {len(PENDING_BATCH[chat]['links'])}")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    await update.message.reply_text(f"Queue size: {processor.queue_size()}")

async def main():
    global processor

    app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

    processor = QueueProcessor(
        bot_application=app,
        public_dir=str(PUBLIC_DIR),
        thumb_path=THUMB_PATH,
        watermark_text=WATERMARK_TEXT,
        channel_link=CHANNEL_LINK
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("batch", batch_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # PTB21 required startup sequence
    await app.initialize()

    # NOW event loop is running → safe to start processor
    app.create_task(processor.start())

    await app.start()
    await app.updater.start_polling()
    await app.updater.idle()

if __name__ == "__main__":
    asyncio.run(main())
