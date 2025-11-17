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
        max_concurrent: int = 1,
        max_file_size_gb: float = 1.5,
    ):
        self.app = bot_application
        self.public_dir = Path(public_dir)
        self.thumb_path = Path(thumb_path) if thumb_path else None
        self.watermark_text = watermark_text
        self.channel_link = channel_link
        self.session_string = session_string
        self.api_id = api_id
        self.api_hash = api_hash
        self.max_concurrent = max_concurrent
        self.max_file_size_bytes = int(max_file_size_gb * 1024**3)
        
        self.q = asyncio.Queue()
        self.telethon_client = None
                
    async def start(self):
        """Start the background worker."""
        logger.info("Starting queue processor...")
        
        # Initialize Telethon
        has_creds = bool(self.session_string and self.api_id and self.api_hash)
        logger.info(f"Telethon credentials present: {has_creds}")
        
        if has_creds:
            try:
                self.telethon_client = TelegramClient(
                    StringSession(self.session_string),
                    self.api_id,
                    self.api_hash,
                    sequential_updates=True
                )
                await self.telethon_client.connect()
                
                is_authorized = await self.telethon_client.is_user_authorized()
                logger.info(f"Telethon authorized: {is_authorized}")
                
                if not is_authorized:
                    logger.error("❌ Telethon session NOT authorized!")
                    self.telethon_client = None
                else:
                    logger.info("✅ Telethon authorized!")
                    
            except Exception as e:
                logger.error(f"Telethon init failed: {e}")
                self.telethon_client = None
        else:
            logger.warning("⚠️ No Telethon credentials")
            
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
            # Step 1: Download (fast, no re-encoding)
            await msg.edit_text(f"📥 Step 1/3: Downloading...")
            await self._run_ffmpeg([
                "ffmpeg", "-loglevel", "error", "-stats",
                "-i", meta["m3u8"], "-c", "copy", "-bsf:a", "aac_adtstoasc", str(tmp)
            ])
            
            # In processor.py, replace the FFmpeg watermark command:

# Step 2: Watermark + **CRITICAL: Force yuv420p IN FILTER CHAIN**
await msg.edit_text(f"🎨 Step 2/3: Watermarking...")
async with aiofiles.open(watermark_txt, "w", encoding="utf-8") as f:
    await f.write(self.watermark_text)

font = self._get_font()

# **THE MAGIC: format=yuv420p MUST be in the filter, not after**
# Also scale to reasonable resolution to prevent iPad zoom
scale = "scale=trunc(iw/2)*2:trunc(ih/2)*2:force_original_aspect_ratio=decrease"
pad = "pad=ceil(iw/2)*2:ceil(ih/2)*2:(ow-iw)/2:(oh-ih)/2"
force_format = "format=yuv420p"  # ✅ THIS IS THE FIX
watermark = f"drawtext=fontfile={shlex.quote(font)}:textfile={shlex.quote(str(watermark_txt))}:fontsize=22:fontcolor=white@0.9:x=20:y=20:box=1:boxcolor=black@0.4:boxborderw=2"

# Combine filters: scale → pad → force format → watermark
full_filter = f"{scale},{pad},{force_format},{watermark}"

await self._run_ffmpeg([
    "ffmpeg", "-y", "-loglevel", "error",
    "-i", str(tmp),
    "-filter_complex", full_filter,
    "-c:v", "libx264",
    "-preset", "medium",
    "-crf", "20",
    "-movflags", "+faststart",
    "-c:a", "aac",
    "-b:a", "192k",
    str(water)
])
            
            # Step 3: Extract thumbnail from video itself
            await msg.edit_text(f"🖼️ Step 3/3: Adding thumbnail...")
            
            # Extract a frame at 5 seconds for thumbnail
            thumbnail_path = base.with_suffix(".thumb.jpg")
            await self._run_ffmpeg([
                "ffmpeg", "-y", "-loglevel", "error",
                "-i", str(water),
                "-ss", "5",  # Take frame at 5 seconds
                "-vframes", "1",
                "-vf", "scale=320:180:force_original_aspect_ratio=decrease,pad=320:180:(ow-iw)/2:(oh-ih)/2",
                str(thumbnail_path)
            ])
            
            # Attach extracted thumbnail to video
            await self._run_ffmpeg([
                "ffmpeg", "-y", "-loglevel", "error",
                "-i", str(water), "-i", str(thumbnail_path),
                "-map", "0", "-map", "1",
                "-c", "copy",
                "-disposition:v:1", "attached_pic",
                str(final)
            ])
                
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
                logger.info(f"Uploading {final.name} ({final.stat().st_size/1024**2:.1f}MB) via Telethon...")
                await self.telethon_client.send_file(
                    chat,
                    str(final),
                    caption=caption,
                    allow_cache=False
                )
            else:
                raise Exception("Telethon client not available. Cannot upload.")
            
            await msg.edit_text(f"✅ L{no}/{total} completed!")
            
        finally:
            # Cleanup
            for p in [tmp, water, final, watermark_txt]:
                if p.exists():
                    p.unlink()
