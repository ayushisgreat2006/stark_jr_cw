import os
import uuid
import asyncio
import shlex
import logging
import shutil
import traceback
from pathlib import Path
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor

# CRITICAL: Must import aiofiles and telethon
import aiofiles
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
        max_file_size_gb: float = 2.0
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
        self.ffmpeg = self._find_ffmpeg()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._shutdown = False
        
        # Initialize Telethon client for uploads
        self.telethon_client = None
        
        # Validate paths
        self.public_dir.mkdir(parents=True, exist_ok=True)
        
        # Check disk space
        free_space = self._get_free_space()
        logger.info(f"Free disk space: {free_space / 1024**3:.2f}GB")
        if free_space < self.max_file_size_bytes * 2:
            logger.warning("Low disk space detected")
                
    def _find_ffmpeg(self) -> str:
        """Find ffmpeg executable."""
        for cmd in ["ffmpeg", "avconv"]:
            if shutil.which(cmd):
                logger.info(f"Found {cmd}")
                return cmd
        raise Exception("FFmpeg not found. Please install FFmpeg and ensure it's in PATH.")
        
    def _get_free_space(self) -> int:
        """Get free disk space in bytes."""
        try:
            stat = self.public_dir.statvfs()
            return stat.f_frsize * stat.f_bavail
        except AttributeError:
            # Windows fallback
            usage = shutil.disk_usage(self.public_dir)
            return usage.free
            
    async def start(self):
        """Start the background worker tasks."""
        logger.info(f"Starting queue processor with {self.max_concurrent} workers")
        
        # Initialize Telethon client
        if self.session_string and self.api_id and self.api_hash:
            logger.info("Initializing Telethon client...")
            self.telethon_client = TelegramClient(
                StringSession(self.session_string),
                self.api_id,
                self.api_hash,
                sequential_updates=True
            )
            await self.telethon_client.connect()
            if not await self.telethon_client.is_user_authorized():
                logger.error("Telethon session is not authorized! Check your SESSION_STRING, API_ID, and API_HASH.")
                self.telethon_client = None
            else:
                logger.info("Telethon client connected successfully!")
        else:
            logger.warning("No Telethon session provided. Using bot token for uploads (may fail for large files).")
        
        asyncio.create_task(self.worker(), name="queue_worker")
        
    async def stop(self):
        """Graceful shutdown."""
        logger.info("Shutting down queue processor...")
        self._shutdown = True
        await self.q.join()
        
        if self.telethon_client:
            await self.telethon_client.disconnect()
            logger.info("Telethon client disconnected")
            
        logger.info("Queue processor stopped")
        
    async def enqueue(self, meta: Dict[str, Any]):
        """Add a job to the queue."""
        await self.q.put(meta)
        logger.info(f"Enqueued lecture {meta.get('lecture_no', '?')}/{meta.get('total', '?')}")
        
    def queue_size(self) -> int:
        """Get current queue size."""
        return self.q.qsize()
        
    async def worker(self):
        """Main worker loop that processes queue items."""
        while not self._shutdown:
            try:
                meta = await asyncio.wait_for(self.q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
                
            async with self.semaphore:
                try:
                    await self.process(meta)
                except Exception as e:
                    logger.error(f"Processing failed for lecture {meta.get('lecture_no', '?')}: {e}\n{traceback.format_exc()}")
                    try:
                        await self.app.bot.send_message(
                            meta["requester_chat"], 
                            f"❌ Error processing lecture {meta.get('lecture_no', '?')}: {str(e)}"
                        )
                    except Exception as notify_error:
                        logger.error(f"Failed to send error notification: {notify_error}")
                finally:
                    self.q.task_done()
                    
    async def run_ffmpeg(self, cmd: list[str], status_msg: Optional[Any] = None) -> None:
        """
        Run FFmpeg with proper error handling and optional progress updates.
        """
        logger.info(f"FFmpeg command: {' '.join(cmd[:8])}...")
        
        proc = await asyncio.create_subprocess_exec(
            *cmd, 
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            error_str = stderr.decode()[-1000:]
            logger.error(f"FFmpeg failed (code {proc.returncode}): {error_str}")
            raise Exception(f"FFmpeg error: {error_str}")
            
        logger.info("FFmpeg completed successfully")
        
    async def _cleanup_files(self, *paths: Path):
        """Safely delete temporary files."""
        for path in paths:
            try:
                if path.exists():
                    await asyncio.to_thread(path.unlink)
                    logger.debug(f"Cleaned up {path}")
            except Exception as e:
                logger.warning(f"Failed to delete {path}: {e}")
                
    def _get_font_path(self) -> str:
        """Get a valid font path for watermark."""
        candidates = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
            "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "C:\\\\Windows\\\\Fonts\\\\arialbd.ttf"
        ]
        for font in candidates:
            if Path(font).exists():
                return font
                
        logger.warning("No suitable font found, using FFmpeg default")
        return "sans"
        
    def _sanitize_filename(self, text: str) -> str:
        """Sanitize text for use in filename."""
        return "".join(c for c in text if c.isalnum() or c in (' ', '-', '_')).rstrip()[:50]
        
    async def _check_file_size(self, path: Path):
        """Check if file size is within limits."""
        try:
            size = await asyncio.to_thread(lambda: path.stat().st_size)
            if size > self.max_file_size_bytes:
                raise Exception(f"File too large: {size/1024**2:.1f}MB > {self.max_file_size_bytes/1024**3:.1f}GB limit")
            if size == 0:
                raise Exception("Output file is empty")
        except Exception as e:
            raise Exception(f"Size check failed: {e}")
            
    async def process(self, meta: Dict[str, Any]) -> None:
        """Process a single lecture from start to finish."""
        chat = meta["requester_chat"]
        lecture_no = meta["lecture_no"]
        total = meta["total"]
        
        # Validate inputs
        if not (meta["m3u8"] and meta["m3u8"].startswith("http") and ".m3u8" in meta["m3u8"].lower()):
            raise ValueError("Invalid m3u8 URL (must be HTTP and contain .m3u8)")
            
        # Check disk space
        free_space = self._get_free_space()
        if free_space < self.max_file_size_bytes * 1.5:
            raise Exception(f"Insufficient disk space: {free_space/1024**3:.2f}GB available")
            
        # Send initial status
        status_msg = await self.app.bot.send_message(
            chat, 
            f"📥 Processing Lecture {lecture_no}/{total}…"
        )
        
        uid = uuid.uuid4().hex[:6]
        base = self.public_dir / f"lec_{lecture_no}_{uid}"
        tmp = base.with_suffix(".tmp.mp4")
        water = base.with_suffix(".water.mp4")
        final = base.with_suffix(".mp4")
        watermark_txt = base.with_suffix(".txt")
        
        try:
            # Step 1: Download stream
            await status_msg.edit_text(f"📥 Step 1/3: Downloading...")
            cmd1 = [
                self.ffmpeg, "-loglevel", "error", "-stats",
                "-i", meta["m3u8"],
                "-c", "copy",
                "-bsf:a", "aac_adtstoasc",
                str(tmp)
            ]
            await self.run_ffmpeg(cmd1)
            await self._check_file_size(tmp)
            
            # Step 2: Write watermark to file & apply
            await status_msg.edit_text(f"🎨 Step 2/3: Watermarking...")
            async with aiofiles.open(watermark_txt, "w", encoding="utf-8") as f:
                await f.write(self.watermark_text)
            
            font_path = self._get_font_path()
            draw = (
                f"drawtext=fontfile={shlex.quote(font_path)}:"
                f"textfile={shlex.quote(str(watermark_txt))}:"
                f"fontsize=22:fontcolor=white@0.9:"
                f"x=20:y=20:box=1:boxcolor=black@0.4:boxborderw=2"
            )
            
            cmd2 = [
                self.ffmpeg, "-y", "-loglevel", "error",
                "-i", str(tmp), "-filter_complex", draw,
                "-preset", "ultrafast", "-crf", "28",
                "-movflags", "+faststart",
                str(water)
            ]
            await self.run_ffmpeg(cmd2)
            await self._check_file_size(water)
            
            # Step 3: Add thumbnail (if exists)
            if self.thumb_path and self.thumb_path.exists():
                await status_msg.edit_text(f"🖼️ Step 3/3: Adding thumbnail...")
                cmd3 = [
                    self.ffmpeg, "-y", "-loglevel", "error",
                    "-i", str(water), "-i", str(self.thumb_path),
                    "-map", "0", "-map", "1",
                    "-c", "copy",
                    "-disposition:v:1", "attached_pic",
                    str(final)
                ]
                await self.run_ffmpeg(cmd3)
            else:
                logger.warning(f"Thumbnail not found at {self.thumb_path}, copying watermarked file")
                if water.exists():
                    await asyncio.to_thread(water.rename, final)
                else:
                    raise Exception("Watermark file missing after processing")
                    
            await self._check_file_size(final)
            
            # Step 4: Upload using Telethon
            await status_msg.edit_text(f"📤 Uploading...")
            safe_batch = self._sanitize_filename(meta['batch'])
            safe_subject = self._sanitize_filename(meta['subject'])
            filename = f"{safe_batch}_{safe_subject}_L{lecture_no}.mp4"
            
            file_size_mb = await asyncio.to_thread(lambda: final.stat().st_size / 1024**2)
            caption = (
                f"🔥 Stark JR. Batch Engine\n"
                f"🎯 Batch: {meta['batch']}\n"
                f"📘 Subject: {meta['subject']}\n"
                f"📚 Lecture {lecture_no}/{total}\n"
                f"💾 Size: {file_size_mb:.1f}MB\n"
                f"⚡ Extracted By: {self.channel_link}"
            )
            
            if self.telethon_client:
                # Use Telethon for upload (much more reliable for large files)
                logger.info(f"Uploading via Telethon: {final.name} ({file_size_mb:.1f}MB)")
                await self.telethon_client.send_file(
                    chat,
                    str(final),
                    caption=caption,
                    allow_cache=False,
                    progress_callback=lambda current, total: logger.info(
                        f"Upload progress: {current/1024**2:.1f}/{total/1024**2:.1f}MB"
                    )
                )
            else:
                # Fallback to bot token if Telethon not available
                logger.warning("Telethon not available, using bot token for upload")
                await self.app.bot.send_document(
                    chat_id=chat,
                    document=str(final),
                    filename=filename,
                    caption=caption,
                    write_timeout=600,
                    read_timeout=600,
                    connect_timeout=600,
                    pool_timeout=600,
                )
            
            await status_msg.edit_text(f"✅ Lecture {lecture_no}/{total} completed!")
            logger.info(f"Successfully processed and uploaded lecture {lecture_no}/{total}")
            
        except Exception as e:
            logger.error(f"Processing failed: {e}\n{traceback.format_exc()}")
            raise
        finally:
            # Always cleanup
            await self._cleanup_files(tmp, water, final, watermark_txt)
