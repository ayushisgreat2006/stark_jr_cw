import os
import uuid
import asyncio
import aiofiles
import shlex
import logging
import shutil
from pathlib import Path
from typing import Optional, Dict, Any

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
        
        self.public_dir.mkdir(parents=True, exist_ok=True)
        free_space = self._get_free_space()
        logger.info(f"Free space: {free_space / 1024**3:.2f}GB")
        
    def _find_ffmpeg(self) -> str:
        for cmd in ["ffmpeg", "avconv"]:
            if shutil.which(cmd):
                logger.info(f"FFmpeg found: {cmd}")
                return cmd
        raise Exception("FFmpeg not found in PATH")
        
    def _get_free_space(self) -> int:
        try:
            stat = self.public_dir.statvfs()
            return stat.f_frsize * stat.f_bavail
        except AttributeError:
            return shutil.disk_usage(self.public_dir).free
            
    async def start(self):
        logger.info(f"Starting {self.max_concurrent} workers")
        asyncio.create_task(self.worker(), name="queue_worker")
        
    async def stop(self):
        self._shutdown = True
        await self.q.join()
        
    async def enqueue(self, meta: Dict[str, Any]):
        await self.q.put(meta)
        logger.info(f"Enqueued L{meta.get('lecture_no', '?')}/{meta.get('total', '?')}")
        
    def queue_size(self) -> int:
        return self.q.qsize()
        
    async def worker(self):
        while not self._shutdown:
            try:
                meta = await asyncio.wait_for(self.q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
                
            async with self.semaphore:
                try:
                    await self.process(meta)
                except Exception as e:
                    logger.error(f"Failed L{meta.get('lecture_no')}: {e}")
                    try:
                        await self.app.bot.send_message(
                            meta["requester_chat"], 
                            f"❌ Error L{meta.get('lecture_no')}: {e}"
                        )
                    except:
                        pass
                finally:
                    self.q.task_done()
                    
    async def run_ffmpeg(self, cmd: list[str], status_msg: Optional[Any] = None) -> None:
        logger.info(f"Running FFmpeg: {' '.join(cmd[:8])}...")
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            error = stderr.decode()[-500:]
            logger.error(f"FFmpeg error (code {proc.returncode}): {error}")
            raise Exception(f"FFmpeg failed: {error}")
            
    async def _cleanup_files(self, *paths: Path):
        for path in paths:
            try:
                if path.exists():
                    await asyncio.to_thread(path.unlink)
            except Exception as e:
                logger.warning(f"Failed to delete {path}: {e}")
                
    def _get_font_path(self) -> str:
        candidates = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
            "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",  # Arch
            "/System/Library/Fonts/Helvetica.ttc",
            "C:\\\\Windows\\\\Fonts\\\\arialbd.ttf"
        ]
        for font in candidates:
            if Path(font).exists():
                return font
        logger.warning("No font found, using default")
        return "sans"
        
    def _sanitize_filename(self, text: str) -> str:
        return "".join(c for c in text if c.isalnum() or c in (' ', '-', '_')).rstrip()[:50]
        
    async def _check_file_size(self, path: Path):
        try:
            size = await asyncio.to_thread(lambda: path.stat().st_size)
            if size > self.max_file_size_bytes:
                raise Exception(f"File too large: {size/1024**2:.1f}MB")
            if size == 0:
                raise Exception("Output file is empty")
        except Exception as e:
            raise Exception(f"Size check failed: {e}")
            
    async def process(self, meta: Dict[str, Any]) -> None:
        chat = meta["requester_chat"]
        lecture_no = meta["lecture_no"]
        total = meta["total"]
        
        # Validate URL
        url = meta["m3u8"]
        if not (url and url.startswith("http") and ".m3u8" in url.lower()):
            raise ValueError("Invalid m3u8 URL")
            
        # Check space
        free = self._get_free_space()
        if free < self.max_file_size_bytes * 1.5:
            raise Exception("Insufficient disk space")
            
        status_msg = await self.app.bot.send_message(chat, f"📥 L{lecture_no}/{total} starting…")
        
        uid = uuid.uuid4().hex[:6]
        base = self.public_dir / f"lec_{lecture_no}_{uid}"
        tmp = base.with_suffix(".tmp.mp4")
        water = base.with_suffix(".water.mp4")
        final = base.with_suffix(".mp4")
        
        # Watermark text file (AVOIDS COLON ESCAPING ISSUES!)
        watermark_txt = base.with_suffix(".txt")
        
        try:
            # Step 1: Download
            await status_msg.edit_text(f"📥 Downloading...")
            cmd1 = [
                self.ffmpeg, "-loglevel", "error", "-stats",
                "-i", url, "-c", "copy", "-bsf:a", "aac_adtstoasc",
                str(tmp)
            ]
            await self.run_ffmpeg(cmd1)
            await self._check_file_size(tmp)
            
            # Step 2: Write watermark to file & apply
            await status_msg.edit_text(f"🎨 Watermarking...")
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
                "-preset", "ultrafast", "-crf", "28", "-movflags", "+faststart",
                str(water)
            ]
            await self.run_ffmpeg(cmd2)
            await self._check_file_size(water)
            
            # Step 3: Thumbnail
            if self.thumb_path and self.thumb_path.exists():
                await status_msg.edit_text(f"🖼️ Adding thumbnail...")
                cmd3 = [
                    self.ffmpeg, "-y", "-loglevel", "error",
                    "-i", str(water), "-i", str(self.thumb_path),
                    "-map", "0", "-map", "1", "-c", "copy",
                    "-disposition:v:1", "attached_pic",
                    str(final)
                ]
                await self.run_ffmpeg(cmd3)
            else:
                if water.exists():
                    await asyncio.to_thread(water.rename, final)
                else:
                    raise Exception("Watermark file missing")
                    
            await self._check_file_size(final)
            
            # Upload
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
                
            await status_msg.edit_text(f"✅ L{lecture_no}/{total} complete!")
            logger.info(f"Success L{lecture_no}/{total}")
            
        except Exception:
            raise
        finally:
            await self._cleanup_files(tmp, water, final, watermark_txt)
