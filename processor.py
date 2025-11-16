import os
import uuid
import asyncio
from pathlib import Path
from telegram import InputFile

class QueueProcessor:
    def __init__(self, bot_application, public_dir, thumb_path, watermark_text, channel_link):
        self.app = bot_application
        self.public_dir = Path(public_dir)
        self.thumb_path = thumb_path
        self.watermark_text = watermark_text
        self.channel_link = channel_link
        self.q = asyncio.Queue()
        self.ffmpeg = "ffmpeg"

    async def start(self):
        asyncio.create_task(self.worker())

    async def enqueue(self, meta):
        await self.q.put(meta)

    def queue_size(self):
        return self.q.qsize()

    async def worker(self):
        while True:
            meta = await self.q.get()
            try:
                await self.process(meta)
            except Exception as e:
                await self.app.bot.send_message(meta["requester_chat"], f"Error: {e}")
            self.q.task_done()

    async def process(self, m):
        chat = m["requester_chat"]
        await self.app.bot.send_message(chat, f"Processing Lecture {m['lecture_no']}/{m['total']}…")

        uid = uuid.uuid4().hex[:6]
        base = Path(self.public_dir) / f"lec_{m['lecture_no']}_{uid}"

        tmp = f"{base}.tmp.mp4"
        water = f"{base}.water.mp4"
        final = f"{base}.mp4"

        # Step 1: Fetch stream
        cmd1 = [
            self.ffmpeg, "-loglevel", "error",
            "-i", m["m3u8"],
            "-c", "copy",
            "-bsf:a", "aac_adtstoasc",
            tmp
        ]
        await self.run(cmd1)

                # Step 2: Watermark (SAFE URL TEXT)
        safe_text = (
            self.watermark_text
            .replace(":", "_")
            .replace("/", "_")
            .replace("-", "_")
        )

        draw = (
            f"drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:"
            f"text='{safe_text}':"
            f"fontsize=22:fontcolor=white@0.9:x=20:y=20:box=1:boxcolor=black@0.4"
        )

        cmd2 = [
            self.ffmpeg, "-y",
            "-i", tmp,
            "-filter_complex", draw,
            "-preset", "ultrafast",
            water
        ]
        await self.run(cmd2)


        # Step 3: Thumbnail
        if Path(self.thumb_path).exists():
            cmd3 = [
                self.ffmpeg, "-i", water, "-i", self.thumb_path,
                "-map", "0", "-map", "1",
                "-c", "copy",
                "-disposition:v:1", "attached_pic",
                final
            ]
            await self.run(cmd3)
        else:
            Path(water).rename(final)

        Path(tmp).unlink(missing_ok=True)
        Path(water).unlink(missing_ok=True)

        caption = (
            f"🔥 Stark JR. Batch Engine\n"
            f"🎯 Batch: {m['batch']}\n"
            f"📘 Subject: {m['subject']}\n"
            f"📚 Lecture {m['lecture_no']}/{m['total']}\n"
            f"⚡ Extracted / Done By :- {self.channel_link}"
        )

        await self.app.bot.send_document(chat, InputFile(final), caption=caption, timeout=20000)

    async def run(self, cmd):
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()
