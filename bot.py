# bot.py
import os
import asyncio
import logging
from pathlib import Path
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)
from processor import QueueProcessor

# ---------- CONFIG from env ----------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "7941244038"))
WORKDIR = Path(os.environ.get("WORKDIR", "/work"))
THUMB_PATH = os.environ.get("THUMB_PATH", str(WORKDIR / "thumb.jpg"))
WATERMARK_TEXT = os.environ.get("WATERMARK_TEXT", "Stark JR. 😎🔥 | Extracted / Done By :- https://t.me/tonystark_jr")
CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "https://t.me/tonystark_jr")
# --------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WORKDIR.mkdir(parents=True, exist_ok=True)
PUBLIC_DIR = WORKDIR / "public"
PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

# single global processor instance (background worker)
processor = None

def is_admin(user_id: int):
    return user_id == ADMIN_ID

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Stark JR. Batch Bot online. Use /batch <Batch>|<Subject> to start.")

# state per chat for collecting links
PENDING_BATCH = {}  # chat_id -> {batch,subject,links:list}

async def batch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user.id
    if not is_admin(user):
        return await update.message.reply_text("Not allowed.")
    text = " ".join(context.args)
    if "|" not in text:
        return await update.message.reply_text("Usage: /batch BatchName|SubjectName")
    batch_name, subject_name = [t.strip() for t in text.split("|", 1)]
    chat_id = update.effective_chat.id
    PENDING_BATCH[chat_id] = {"batch": batch_name, "subject": subject_name, "links": []}
    await update.message.reply_text(
        f"Batch set: *{batch_name}* | Subject: *{subject_name}*\nNow paste m3u8 links (one per line). Send `DONE` when finished.",
        parse_mode="Markdown"
    )

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user.id
    if not is_admin(user):
        return
    chat_id = update.effective_chat.id
    if chat_id not in PENDING_BATCH:
        await update.message.reply_text("No batch in progress. Use /batch first.")
        return

    text = update.message.text.strip()
    if text.upper() == "DONE":
        batch = PENDING_BATCH.pop(chat_id)
        links = [ln for ln in batch["links"] if ln.strip()]
        if not links:
            return await update.message.reply_text("No links provided. Cancelled.")
        await update.message.reply_text(f"Queued {len(links)} lectures. Processing will start in background. I'll DM progress.")
        # queue them
        for idx, link in enumerate(links, start=1):
            meta = {
                "batch": batch["batch"],
                "subject": batch["subject"],
                "lecture_no": idx,
                "total": len(links),
                "m3u8": link.strip(),
                "requester_chat": chat_id
            }
            await processor.enqueue(meta)
        return
    # otherwise treat each non-empty line as a link (or a multi-line message)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    PENDING_BATCH[chat_id]["links"].extend(lines)
    await update.message.reply_text(f"Added {len(lines)} link(s). Total so far: {len(PENDING_BATCH[chat_id]['links'])}")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user.id
    if not is_admin(user):
        return
    qsz = processor.queue_size()
    await update.message.reply_text(f"Queue size: {qsz}")

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
    # start worker
    await processor.start()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("batch", batch_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # run bot (blocking)
    await app.run_polling()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
