# bot.py — RAILWAY-SAFE, no asyncio.run(), ADMIN optional
import os
from pathlib import Path
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters
)
from processor import QueueProcessor

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
# ADMIN_ID optional: if empty -> open to anyone (only for quick testing). Set to your numeric id in production.
ADMIN_ENV = os.environ.get("ADMIN_ID", "").strip()
ADMIN_ID = int(ADMIN_ENV) if ADMIN_ENV else None

WORKDIR = Path(os.environ.get("WORKDIR", "/work"))
WORKDIR.mkdir(parents=True, exist_ok=True)
PUBLIC_DIR = WORKDIR / "public"
PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

THUMB_PATH = os.environ.get("THUMB_PATH", str(WORKDIR / "thumb.jpg"))
WATERMARK_TEXT = os.environ.get("WATERMARK_TEXT", "Stark JR. 😎🔥 | Extracted / Done By :- https://t.me/tonystark_jr")
CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "https://t.me/tonystark_jr")

# In-memory state
processor = None
PENDING_BATCH = {}

def is_admin(uid: int) -> bool:
    # If ADMIN_ID is not set, allow any user (useful for testing). In production ALWAYS set ADMIN_ID.
    return ADMIN_ID is None or uid == ADMIN_ID

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔥 Stark JR. Batch Bot Online 😎")

async def batch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return await update.message.reply_text("Unauthorized 💀")

    argtext = " ".join(context.args).strip()
    if "|" not in argtext:
        return await update.message.reply_text("Use: /batch BatchName|Subject")

    batch, subject = argtext.split("|", 1)
    batch, subject = batch.strip(), subject.strip()
    chat = update.effective_chat.id

    PENDING_BATCH[chat] = {"batch": batch, "subject": subject, "links": []}
    await update.message.reply_text(
        f"Batch: {batch}\nSubject: {subject}\n\nPaste ALL m3u8 links (one per line).\nSend DONE when finished."
    )

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    chat = update.effective_chat.id
    if chat not in PENDING_BATCH:
        return await update.message.reply_text("Start with /batch <Batch>|<Subject>")

    text = update.message.text.strip()

    if text.upper() == "DONE":
        info = PENDING_BATCH.pop(chat)
        links = info["links"]
        if not links:
            return await update.message.reply_text("No links added. Cancelled.")
        await update.message.reply_text(f"🔥 {len(links)} lectures queued. Processing…")
        # enqueue tasks
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

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(f"Queue size: {processor.queue_size()}")

def main():
    global processor

    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN environment variable is required.")

    # Build application
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # create processor instance
    processor = QueueProcessor(
        bot_application=app,
        public_dir=str(PUBLIC_DIR),
        thumb_path=THUMB_PATH,
        watermark_text=WATERMARK_TEXT,
        channel_link=CHANNEL_LINK
    )

    # schedule processor.start() to run when the app loop runs
    # app.create_task is safe to call before run_polling; PTB will run the task on its loop.
    app.create_task(processor.start())

    # handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("batch", batch_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # blocking call, starts event loop internally and runs registered tasks
    app.run_polling()

if __name__ == "__main__":
    main()
