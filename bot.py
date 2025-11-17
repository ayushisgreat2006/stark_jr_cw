import os
import asyncio
from pathlib import Path
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters
)
from processor import QueueProcessor

ADMIN_ENV = os.getenv("ADMIN_ID", "").strip()
ADMIN_ID = int(ADMIN_ENV) if ADMIN_ENV else None

WORKDIR = Path("/work")
WORKDIR.mkdir(exist_ok=True)

PUBLIC_DIR = WORKDIR / "public"
PUBLIC_DIR.mkdir(exist_ok=True)

THUMB_PATH = os.getenv("THUMB_PATH", "/work/thumb.jpg")
WATERMARK_TEXT = os.getenv(
    "WATERMARK_TEXT",
    "Extracted By tonystark_jr"
)
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "tonystark_jr")

processor = None
PENDING = {}

def is_admin(uid): 
    return ADMIN_ID is None or uid == ADMIN_ID

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔥 Stark JR Bot Online")

async def batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return await update.message.reply_text("Unauthorized")

    txt = " ".join(context.args)
    if "|" not in txt:
        return await update.message.reply_text("Use: /batch Batch|Subject")

    batch_name, subject = txt.split("|", 1)
    cid = update.effective_chat.id

    PENDING[cid] = {"batch": batch_name.strip(), "subject": subject.strip(), "links": []}
    await update.message.reply_text("Send links, type DONE when finished.")

async def text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    if cid not in PENDING: return

    msg = update.message.text.strip()

    if msg.upper() == "DONE":
        info = PENDING.pop(cid)
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
                "requester_chat": cid,
            }
            await processor.enqueue(meta)

        return

    # otherwise add links
    PENDING[cid]["links"].extend(
        [x.strip() for x in msg.splitlines() if x.strip()]
    )
    await update.message.reply_text(f"Added. Total: {len(PENDING[cid]['links'])}")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    await update.message.reply_text(f"Queue size: {processor.queue_size()}")

# ---------------------------
# MAIN PTB21-CORRECT LAUNCH
# ---------------------------
async def background_worker(app):
    """Run processor forever in background."""
    asyncio.create_task(processor.start())

# In bot.py, inside the main() function:

# bot.py (in main function)

def main():
    global processor

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(background_worker).build()

    # Add these environment variables
    SESSION_STRING = os.getenv("SESSION_STRING", "")
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH", "")

    processor = QueueProcessor(
        bot_application=app,
        public_dir=str(PUBLIC_DIR),
        thumb_path=THUMB_PATH,
        watermark_text=WATERMARK_TEXT,
        channel_link=CHANNEL_LINK,
        session_string=SESSION_STRING,
        api_id=API_ID,
        api_hash=API_HASH,
        max_concurrent=1,      # Critical for Railway
        max_file_size_gb=1.5,  # Adjust based on your plan
    )

    # ... rest of your handlers
    app.run_polling()

if __name__ == "__main__":
    main()
