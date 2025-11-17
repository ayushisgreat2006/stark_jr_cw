import os
import uuid
import asyncio
import aiofiles
import shlex
import logging
import shutil
from pathlib import Path
from typing import Optional, Dict, Any
from telegram import InputFile

# Configure logging
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
        max_concurrent: int = 1,
        max_file_size_gb: float = 2.0
    ):
        self.app = bot_application
        self.public_dir = Path(public_dir)
        self.thumb_path = Path(thumb_path) if thumb_path else None
        self.watermark_text = watermark_text
        self.channel_link = channel_link
        self.max_concurrent = max_concurrent
        self.max_file_size_bytes = int(max_file_size_gb * 1024**3)
        
        self.q = asyncio.Queue()
        self.ffmpeg = self._find_ffmpeg()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._shutdown = False
        
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
        asyncio.create_task(self.worker(), name="queue_worker")
        
    async def stop(self):
        """Graceful shutdown."""
        logger.info("Shutting down queue processor...")
        self._shutdown = True
        await self.q.join()
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
                    logger.error(f"Processing failed for lecture {meta.get('lecture_no', '?')}: {e}", exc_info=True)
                    try:
                        await self.app.bot.send_message(
                            meta["requester_chat"], 
                            f"❌ Error processing lecture {meta.get('lecture_no', '?')}: {str(e)}"
                        )
                    except Exception as notify_error:
                        logger.error(f"Failed to send error notification: {notify_error}")
                finally:
                    self.q.task_done()
                    
            # Rate limiting
            await asyncio.sleep(0.5)
                    
    def _sanitize_watermark(self, text: str) -> str:
        """Sanitize text for FFmpeg drawtext filter."""
        text = text.replace("\n", " ").strip()
        return shlex.quote(text)
        
    def _get_font_path(self) -> str:
        """Get a valid font path for watermark."""
        candidates = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "C:\\\\Windows\\\\Fonts\\\\arialbd.ttf"
        ]
        for font in candidates:
            if Path(font).exists():
                return font
                
        logger.warning("No suitable font found, using FFmpeg default")
        return "sans"
        
    async def _cleanup_files(self, *paths: Path):
        """Safely delete temporary files."""
        for path in paths:
            try:
                if path.exists():
                    await asyncio.to_thread(path.unlink)
                    logger.debug(f"Cleaned up {path}")
            except Exception as e:
                logger.warning(f"Failed to delete {path}: {e}")
                
    async def run_ffmpeg(self, cmd: list[str], progress_msg: Optional[Any] = None) -> None:
        """
        Run FFmpeg with proper error handling and optional progress updates.
        """
        logger.info(f"FFmpeg command: {' '.join(cmd[:5])}...")
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        if progress_msg:
            asyncio.create_task(self._send_progress_updates(progress_msg))
            
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            error_str = stderr.decode()[-1000:]
            logger.error(f"FFmpeg failed (code {proc.returncode}): {error_str}")
            raise Exception(f"FFmpeg error: {error_str}")
            
        logger.info("FFmpeg completed successfully")
        
    async def _send_progress_updates(self, message, interval=30):
        """Send periodic progress updates."""
        try:
            for i in range(1, 20):
                await asyncio.sleep(interval)
                await message.edit_text(
                    f"{message.text.split('...')[0]}... ({i*interval}s elapsed)"
                )
        except Exception:
            pass
            
    async def _validate_url(self, url: str) -> bool:
        """Basic validation of m3u8 URL."""
        return bool(url and url.startswith("http") and ".m3u8" in url.lower())
        
    async def _check_file_size(self, path: Path):
        """Check if file size is within limits."""
        try:
            size = await asyncio.to_thread(lambda: path.stat().st_size)
            if size > self.max_file_size_bytes:
                raise Exception(
                    f"File too large: {size/1024**2:.1f}MB > {self.max_file_size_bytes/1024**3:.1f}GB limit"
                )
            if size == 0:
                raise Exception("Output file is empty")
        except Exception as e:
            if "File too large" in str(e):
                raise
            raise Exception(f"Cannot check file size: {e}")
            
    def _sanitize_filename(self, text: str) -> str:
        """Sanitize text for use in filename."""
        return "".join(c for c in text if c.isalnum() or c in (' ', '-', '_')).rstrip()
        
    async def process(self, meta: Dict[str, Any]) -> None:
        """Process a single lecture from start to finish."""
        chat = meta["requester_chat"]
        lecture_no = meta["lecture_no"]
        total = meta["total"]
        
        # Validate inputs
        if not await self._validate_url(meta["m3u8"]):
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
            await self.run_ffmpeg(cmd1, status_msg)
            await self._check_file_size(tmp)
            
            # Step 2: Add watermark
            await status_msg.edit_text(f"🎨 Step 2/3: Adding watermark...")
            safe_text = self._sanitize_watermark(self.watermark_text)
            font_path = self._get_font_path()
            
            draw = (
                f"drawtext=fontfile={shlex.quote(font_path)}:"
                f"text={safe_text}:"
                f"fontsize=22:fontcolor=white@0.9:"
                f"x=20:y=20:box=1:boxcolor=black@0.4:boxborderw=2"
            )
            
            cmd2 = [
                self.ffmpeg, "-y", "-loglevel", "error",
                "-i", str(tmp),
                "-filter_complex", draw,
                "-preset", "ultrafast",
                "-crf", "28",
                "-movflags", "+faststart",
                str(water)
            ]
            await self.run_ffmpeg(cmd2, status_msg)
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
                await self.run_ffmpeg(cmd3, status_msg)
            else:
                logger.warning(f"Thumbnail not found at {self.thumb_path}")
                await asyncio.to_thread(water.rename, final)
                
            await self._check_file_size(final)
            
            # Generate caption
            file_size_mb = await asyncio.to_thread(lambda: final.stat().st_size / 1024**2)
            caption = (
                f"🔥 Stark JR. Batch Engine\n"
                f"🎯 Batch: {meta['batch']}\n"
                f"📘 Subject: {meta['subject']}\n"
                f"📚 Lecture {lecture_no}/{total}\n"
                f"💾 Size: {file_size_mb:.1f}MB\n"
                f"⚡ Extracted By :- {self.channel_link}"
            )
            
            # Upload
            await status_msg.edit_text(f"📤 Uploading...")
            safe_batch = self._sanitize_filename(meta['batch'])
            safe_subject = self._sanitize_filename(meta['subject'])
            filename = f"{safe_batch}_{safe_subject}_L{lecture_no}.mp4"
            
            async with aiofiles.open(final, "rb") as f:
                await self.app.bot.send_document(
                    chat_id=chat,
                    document=f,
                    filename=filename,
                    caption=caption,
                    write_timeout=2000,
                    read_timeout=2000,
                    connect_timeout=2000,
                )
                
            await status_msg.edit_text(f"✅ Lecture {lecture_no}/{total} completed!")
            logger.info(f"Successfully processed lecture {lecture_no}/{total}")
            
        except Exception as e:
            logger.error(f"Processing failed: {e}", exc_info=True)
            raise
        finally:
            # Always clean up
            await self._cleanup_files(tmp, water, final)
