# processor.py
import os
import shlex
import subprocess
import asyncio
import uuid
from pathlib import Path
from telegram import InputFile

class QueueProcessor:
    def __init__(self, bot_application, public_dir="public", thumb_path=None, watermark_text="", channel_link=""):
        self.app = bot_application
        self.public_dir = Path(public_dir)
        self.thumb_path = thumb_path
        self.watermark_text = watermark_text
        self.channel_link = channel_link
        self.q = asyncio.Queue()
        self.worker_task = None
        self.ffmpeg = os.environ.get("FFMPEG_BIN", "ffmpeg")

    async def start(self):
        # start worker
        self.worker_task = asyncio.create_task(self.worker_loop())

    async def enqueue(self, meta: dict):
        await self.q.put(meta)

    def queue_size(self):
        return self.q.qsize()

    async def worker_loop(self):
        while True:
            meta = await self.q.get()
            try:
                await self.process_item(meta)
            except Exception as e:
                # notify requester
                chat = meta.get("requester_chat")
                if chat:
                    try:
                        await self.app.bot.send_message(chat, f"Lecture {meta.get('lecture_no')} failed: {e}")
                    except Exception:
                        pass
            finally:
                self.q.task_done()

    async def process_item(self, meta: dict):
        m3u8 = meta["m3u8"]
        lecture_no = meta["lecture_no"]
        batch = meta["batch"]
        subject = meta["subject"]
        total = meta.get("total", "?")
        # create unique filenames
        uid = uuid.uuid4().hex[:8]
        tmp_base = Path(self.public_dir) / f"{subject}_{lecture_no}_{uid}"
        tmp_base.parent.mkdir(parents=True, exist_ok=True)
        tmp_mp4 = str(tmp_base) + ".tmp.mp4"
        water_mp4 = str(tmp_base) + ".water.mp4"
        final_mp4 = str(tmp_base) + ".mp4"

        # step 1: fetch/convert (copy)
        cmd_fetch = [
            self.ffmpeg, "-hide_banner", "-loglevel", "error",
            "-i", m3u8, "-c", "copy", "-bsf:a", "aac_adtstoasc", tmp_mp4
        ]
        # run fetch
        proc = await asyncio.create_subprocess_exec(*cmd_fetch)
        await proc.communicate()
        if not Path(tmp_mp4).exists():
            raise RuntimeError("ffmpeg failed to produce mp4")

        # step 2: watermark via drawtext
        drawtext = f"drawtext=text='{self.watermark_text}':fontsize=28:fontcolor=white@0.9:x=20:y=20:box=1:boxcolor=black@0.5:boxborderw=5"
        cmd_water = [
            self.ffmpeg, "-hide_banner", "-loglevel", "error",
            "-i", tmp_mp4, "-vf", drawtext, "-preset", "ultrafast", water_mp4
        ]
        proc = await asyncio.create_subprocess_exec(*cmd_water)
        await proc.communicate()
        if not Path(water_mp4).exists():
            raise RuntimeError("watermarking failed")

        # step 3: attach thumbnail if exists
        if self.thumb_path and Path(self.thumb_path).exists():
            cmd_attach = [
                self.ffmpeg, "-hide_banner", "-loglevel", "error",
                "-i", water_mp4, "-i", self.thumb_path,
                "-map", "0", "-map", "1", "-c", "copy", "-disposition:v:1", "attached_pic", final_mp4
            ]
            proc = await asyncio.create_subprocess_exec(*cmd_attach)
            await proc.communicate()
            if not Path(final_mp4).exists():
                # fallback: use water_mp4 as final
                Path(water_mp4).rename(final_mp4)
        else:
            Path(water_mp4).rename(final_mp4)

        # cleanup tmp mp4
        try:
            Path(tmp_mp4).unlink(missing_ok=True)
            Path(water_mp4).unlink(missing_ok=True)
        except Exception:
            pass

        # upload to requester chat as document with caption
        caption = (f"🔥 Stark JR. Batch Engine Activated\n"
                   f"🎯 Batch: {batch}\n"
                   f"📘 Subject: {subject}\n"
                   f"📚 Lecture {lecture_no} / {total}\n"
                   f"⚡ Extracted / Done By :- {self.channel_link}")

        chat = meta.get("requester_chat")
        if chat:
            # send progress start
            await self.app.bot.send_message(chat, f"Uploading Lecture {lecture_no} ...")
            with open(final_mp4, "rb") as f:
                await self.app.bot.send_document(chat_id=chat, document=InputFile(f, filename=os.path.basename(final_mp4)),
                                                 timeout=1000, caption=caption)
            await self.app.bot.send_message(chat, f"Lecture {lecture_no} uploaded ✅")
        # keep final mp4 for public access (optional). If you want to remove, uncomment:
        # Path(final_mp4).unlink(missing_ok=True)
