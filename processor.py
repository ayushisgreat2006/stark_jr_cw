import os
import uuid
import asyncio
import shlex
import logging
import shutil
from pathlib import Path
from typing import Dict, Any

import aiofiles
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class QueueProcessor:
    def __init__(
        self,
        bot_application,
        public_dir: str,
        thumb_path: str,
        watermark_text: str,
        channel_link: str,
        session_string: str,
        api_id: int,
        api_hash: str,
    ):
        self.app = bot_application
        self.public_dir = Path(public_dir)
        self.thumb_path = Path(thumb_path) if thumb_path else None
        self.watermark_text = watermark_text
        self.channel_link = channel_link
        self.session_string = session_string
        self.api_id = api_id
        self.api_hash = api_hash
        
        self.q = asyncio.Queue()
        self.telethon_client = None
        
    async def start(self):
        """Start the background worker."""
        logger.info("Starting queue processor...")
        
        # Initialize Telethon
        if self.session_string and self.api_id and self.api_hash:
            logger.info("Connecting Telethon...")
            self.telethon_client = TelegramClient(
                StringSession(self.session_string),
                self.api_id,
                self.api_hash
            )
            await self.telethon_client.connect()
            if not await self.telethon_client.is_user_authorized():
                logger.error("❌ Telethon session invalid! Check your SESSION_STRING, API_ID, API_HASH")
                self.telethon_client = None
            else:
                logger.info("✅ Telethon connected!")
        else:
            logger.warning("⚠️ No Telethon session. Bot uploads will fail for large files.")
            
        asyncio.create_task(self.worker())
        
    async def stop(self):
        if self.telethon_client:
            await self.telethon_client.disconnect()
            
    async def enqueue(self, meta: Dict[str, Any]):
        await self.q.put(meta)
        logger.info(f"📥 Enqueued L{meta['lecture_no']}/{meta['total']}")
        
    def queue_size(self) -> int:
        return self.q.qsize()
        
    async def worker(self):
        while True:
            meta = await self.q.get()
            try:
                await self.process(meta)
            except Exception as e:
                logger.error(f"❌ Failed L{meta['lecture_no']}: {e}")
                await self.app.bot.send_message(
                    meta["requester_chat"], 
                    f"❌ Error L{meta['lecture_no']}: {str(e)}"
                )
            finally:
                self.q.task_done()
                
    def _get_font(self) -> str:
        fonts = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "C:\\\\Windows\\\\Fonts\\\\arialbd.ttf"
        ]
        for f in fonts:
            if Path(f).exists():
                return f
        return "sans"
        
    async def _run_ffmpeg(self, cmd: list) -> None:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise Exception(f"FFmpeg error: {stderr.decode()[-500:]}")
            
    async def process(self, meta: Dict[str, Any]) -> None:
        chat = meta["requester_chat"]
        no, total = meta["lecture_no"], meta["total"]
        
        msg = await self.app.bot.send_message(chat, f"📥 L{no}/{total} starting...")
        
        # Setup paths
        base = self.public_dir / f"lec_{no}_{uuid.uuid4().hex[:6]}"
        tmp, water, final = base.with_suffix(".tmp.mp4"), base.with_suffix(".water.mp4"), base.with_suffix(".mp4")
        watermark_txt = base.with_suffix(".txt")
        
        try:
            # Step 1: Download
            await msg.edit_text(f"📥 Step 1/3: Downloading...")
            await self._run_ffmpeg([
                "ffmpeg", "-loglevel", "error", "-stats",
                "-i", meta["m3u8"], "-c", "copy", "-bsf:a", "aac_adtstoasc", str(tmp)
            ])
            
            # Step 2: Watermark
            await msg.edit_text(f"🎨 Step 2/3: Watermarking...")
            async with aiofiles.open(watermark_txt, "w", encoding="utf-8") as f:
                await f.write(self.watermark_text)
            
            font = self._get_font()
            draw = f"drawtext=fontfile={shlex.quote(font)}:textfile={shlex.quote(str(watermark_txt))}:fontsize=22:fontcolor=white@0.9:x=20:y=20:box=1:boxcolor=black@0.4:boxborderw2"
            await self._run_ffmpeg([
                "ffmpeg", "-y", "-loglevel", "error",
                "-i", str(tmp), "-filter_complex", draw,
                "-preset", "ultrafast", "-crf", "28", "-movflags", "+faststart", str(water)
            ])
            
            # Step 3: Thumbnail
            if self.thumb_path and self.thumb_path.exists():
                await msg.edit_text(f"🖼️ Step 3/3: Adding thumbnail...")
                await self._run_ffmpeg([
                    "ffmpeg", "-y", "-loglevel", "error",
                    "-i", str(water), "-i", str(self.thumb_path),
                    "-map", "0", "-map", "1", "-c", "copy",
                    "-disposition:v:1", "attached_pic", str(final)
                ])
            else:
                water.rename(final)
                
            # Step 4: Upload via Telethon
            await msg.edit_text(f"📤 Uploading...")
            caption = (
                f"🔥 Stark JR. Batch Engine\n"
                f"🎯 Batch: {meta['batch']}\n"
                f"📘 Subject: {meta['subject']}\n"
                f"📚 Lecture {no}/{total}\n"
                f"⚡ Extracted By: {self.channel_link}"
            )
            
            if self.telethon_client:
                logger.info(f"Uploading {final.name} via Telethon...")
                await self.telethon_client.send_file(
                    chat,
                    str(final),
                    caption=caption,
                    allow_cache=False
                )
            else:
                logger.error("No Telethon client! Upload cannot proceed.")
                raise Exception("Telethon session not configured. Cannot upload large files.")
            
            await msg.edit_text(f"✅ L{no}/{total} completed!")
            
        finally:
            # Cleanup
            for p in [tmp, water, final, watermark_txt]:
                if p.exists():
                    p.unlink()
