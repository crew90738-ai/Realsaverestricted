"""
Save Restricted Content Bot — single-file edition
===================================================
All logic lives here: config, Redis helpers, progress bars, forwarding,
batch download, yt-dlp, thumbnail management, admin commands, health server.

Deploy on Render.com as a Web Service:
  Build:  pip install -r requirements.txt
  Start:  python main.py
"""

# ═══════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import signal
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Optional, Tuple, Union

import redis.asyncio as aioredis
from dotenv import load_dotenv
from pyrogram import Client, StopPropagation, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    ChannelInvalid,
    ChannelPrivate,
    ChatAdminRequired,
    FloodWait,
    InputUserDeactivated,
    MessageIdInvalid,
    UserIsBlocked,
    UsernameInvalid,
    UsernameNotOccupied,
    UserNotParticipant,
)
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

load_dotenv()

# ── yt-dlp (optional) ─────────────────────────────────────────────────────────
try:
    import yt_dlp  # type: ignore
    YTDLP_AVAILABLE = True
except ImportError:
    YTDLP_AVAILABLE = False

# ── aiohttp for thumbnail fetching only (NOT for health server) ───────────────
try:
    import aiohttp as _aiohttp_mod
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False


# ═══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("yt_dlp").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise ValueError(f"[Config] Required env var '{name}' is not set.")
    return val


class Config:
    API_ID: int         = int(_require("API_ID"))
    API_HASH: str       = _require("API_HASH")
    BOT_TOKEN: str      = _require("BOT_TOKEN")
    SESSION_STRING: str = _require("SESSION_STRING")
    OWNER_ID: int       = int(_require("OWNER_ID"))
    REDIS_URL: str      = os.environ.get("REDIS_URL", "redis://localhost:6379")
    LOG_CHANNEL: Optional[int] = (
        int(os.environ["LOG_CHANNEL"])
        if os.environ.get("LOG_CHANNEL", "").strip()
        else None
    )
    FORCE_SUB_CHANNEL: str = os.environ.get("FORCE_SUB_CHANNEL", "").strip()
    BOT_USERNAME: str      = os.environ.get("BOT_USERNAME", "SaveRestrictedBot")
    MAX_FILE_SIZE: int     = 4 * 1024 * 1024 * 1024
    DOWNLOAD_DIR: str      = os.environ.get("DOWNLOAD_DIR", "/tmp/dl")
    PROGRESS_UPDATE_INTERVAL: int = 5
    AUTO_DELETE_DELAY: int = int(os.environ.get("AUTO_DELETE_DELAY", "60"))
    BATCH_MAX: int         = int(os.environ.get("BATCH_MAX", "50"))
    YTDLP_FORMAT: str      = os.environ.get(
        "YTDLP_FORMAT",
        "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
    )
    PORT: int = int(os.environ.get("PORT", "8080"))


os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  HEALTH-CHECK SERVER
#  Pure Python threading — zero asyncio, zero event-loop conflict.
#  Render requires the service to bind a port; this satisfies that.
# ═══════════════════════════════════════════════════════════════════════════════

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass  # suppress HTTP access logs


def start_health_server() -> None:
    """Start a plain HTTP health-check server in a background daemon thread."""
    server = HTTPServer(("0.0.0.0", Config.PORT), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health server running on port %d ✓", Config.PORT)


# ═══════════════════════════════════════════════════════════════════════════════
#  REDIS HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_redis: aioredis.Redis | None = None


def _get_redis() -> aioredis.Redis:
    if _redis is None:
        raise RuntimeError("Redis not initialised yet.")
    return _redis


async def redis_add_premium(user_id: int) -> None:
    await _get_redis().sadd("premium_users", str(user_id))

async def redis_remove_premium(user_id: int) -> None:
    await _get_redis().srem("premium_users", str(user_id))

async def redis_is_premium(user_id: int) -> bool:
    return await _get_redis().sismember("premium_users", str(user_id))

async def redis_get_all_premium() -> list[int]:
    return [int(m) for m in await _get_redis().smembers("premium_users")]

async def redis_register_user(user_id: int) -> bool:
    added = await _get_redis().sadd("bot_users", str(user_id))
    if added:
        await _get_redis().incr("stats:total_users")
    return bool(added)

async def redis_get_all_users() -> list[int]:
    return [int(m) for m in await _get_redis().smembers("bot_users")]

async def redis_inc_downloads() -> None:
    await _get_redis().incr("stats:total_downloads")

async def redis_inc_files() -> None:
    await _get_redis().incr("stats:total_files")

async def redis_get_stats() -> dict:
    r = _get_redis()
    return {
        "users":     await r.scard("bot_users"),
        "downloads": int(await r.get("stats:total_downloads") or 0),
        "files":     int(await r.get("stats:total_files") or 0),
        "premium":   await r.scard("premium_users"),
    }

async def redis_set_thumb(user_id: int, file_id: str) -> None:
    await _get_redis().set(f"user_thumb:{user_id}", file_id)

async def redis_get_thumb(user_id: int) -> Optional[str]:
    return await _get_redis().get(f"user_thumb:{user_id}")

async def redis_del_thumb(user_id: int) -> None:
    await _get_redis().delete(f"user_thumb:{user_id}")

async def redis_set_caption(user_id: int, caption: str) -> None:
    await _get_redis().set(f"user_caption:{user_id}", caption)

async def redis_get_caption(user_id: int) -> Optional[str]:
    return await _get_redis().get(f"user_caption:{user_id}")

async def redis_del_caption(user_id: int) -> None:
    await _get_redis().delete(f"user_caption:{user_id}")

async def redis_set_batch(user_id: int, state: dict) -> None:
    await _get_redis().set(f"batch_state:{user_id}", json.dumps(state), ex=600)

async def redis_get_batch(user_id: int) -> Optional[dict]:
    raw = await _get_redis().get(f"batch_state:{user_id}")
    return json.loads(raw) if raw else None

async def redis_clear_batch(user_id: int) -> None:
    await _get_redis().delete(f"batch_state:{user_id}")


# ═══════════════════════════════════════════════════════════════════════════════
#  PROGRESS BAR HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def humanbytes(size: int) -> str:
    if not size:
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB")
    i = 0
    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1
    return f"{size:.2f} {units[i]}"


def time_formatter(seconds: float) -> str:
    seconds = int(seconds)
    parts = []
    for unit, div in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
        v, seconds = divmod(seconds, div)
        if v:
            parts.append(f"{v}{unit}")
    return " ".join(parts) if parts else "0s"


def progress_bar(current: int, total: int, length: int = 10) -> str:
    if total == 0:
        return "░" * length + "  0%"
    filled = int(length * current / total)
    return "█" * filled + "░" * (length - filled) + f"  {current * 100 / total:.1f}%"


def make_progress_callback(
    message: Message,
    action: str = "Processing",
    start_time: Optional[float] = None,
    update_interval: int = Config.PROGRESS_UPDATE_INTERVAL,
):
    last_update: list[float] = [0.0]
    if start_time is None:
        start_time = time.time()

    async def _cb(current: int, total: int) -> None:
        now = time.time()
        if now - last_update[0] < update_interval and current != total:
            return
        last_update[0] = now
        elapsed = now - start_time
        speed   = current / elapsed if elapsed > 0 else 0
        eta     = (total - current) / speed if speed > 0 else 0
        text = (
            f"<b>{action}</b>\n\n"
            f"<code>{progress_bar(current, total)}</code>\n\n"
            f"📦 {humanbytes(current)} / {humanbytes(total)}\n"
            f"🚀 Speed: {humanbytes(speed)}/s\n"
            f"⏳ ETA: {time_formatter(eta)}"
        )
        try:
            await message.edit_text(text)
        except Exception:
            pass

    return _cb


# ═══════════════════════════════════════════════════════════════════════════════
#  GENERAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_PRIVATE_RE = re.compile(r"t\.me/c/(\d+)/(\d+)")
_PUBLIC_RE  = re.compile(r"t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)")


def parse_telegram_link(url: str) -> Optional[Tuple[Union[int, str], int]]:
    url = url.strip().replace("https://", "").replace("http://", "")
    m = _PRIVATE_RE.search(url)
    if m:
        return int("-100" + m.group(1)), int(m.group(2))
    m = _PUBLIC_RE.search(url)
    if m:
        return m.group(1), int(m.group(2))
    return None


def is_telegram_link(text: str) -> bool:
    return bool(_PRIVATE_RE.search(text) or _PUBLIC_RE.search(text))


def is_ytdlp_url(url: str) -> bool:
    patterns = [
        r"youtube\.com/watch", r"youtu\.be/",
        r"instagram\.com/", r"facebook\.com/",
        r"tiktok\.com/", r"twitter\.com/status", r"x\.com/",
        r"vimeo\.com/", r"dailymotion\.com/", r"twitch\.tv/",
        r"reddit\.com/r/.*/comments",
        r"pinterest\.", r"linkedin\.com/",
        r"soundcloud\.com/", r"mixcloud\.com/",
        r"bilibili\.com/", r"nicovideo\.jp/",
    ]
    return any(re.search(p, url.lower()) for p in patterns)


async def _auto_delete(message: Message, delay: int) -> None:
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass


def schedule_delete(message: Message, delay: int = Config.AUTO_DELETE_DELAY) -> None:
    if delay > 0:
        asyncio.create_task(_auto_delete(message, delay))


def get_media_type(message: Message) -> Optional[str]:
    for attr in ("document", "video", "audio", "photo",
                 "animation", "voice", "video_note", "sticker"):
        if getattr(message, attr, None):
            return attr
    return None


async def log_to_channel(bot: Client, text: str) -> None:
    if not Config.LOG_CHANNEL:
        return
    try:
        await bot.send_message(Config.LOG_CHANNEL, text)
    except Exception:
        pass


async def check_force_sub(bot: Client, user_id: int) -> bool:
    ch = Config.FORCE_SUB_CHANNEL
    if not ch:
        return True
    try:
        member = await bot.get_chat_member(ch, user_id)
        return member.status.value not in ("left", "banned", "kicked")
    except UserNotParticipant:
        return False
    except (ChatAdminRequired, Exception):
        return True


async def send_force_sub_message(bot: Client, message: Message) -> None:
    ch = Config.FORCE_SUB_CHANNEL
    invite = (
        f"https://t.me/c/{ch.lstrip('-').removeprefix('100')}"
        if ch.lstrip("-").isdigit()
        else f"https://t.me/{ch.lstrip('@')}"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("📢 Join Channel", url=invite),
        InlineKeyboardButton("✅ I've Joined",  callback_data="check_sub"),
    ]])
    await message.reply(
        "⚠️ <b>You must join our channel to use this bot.</b>\n\n"
        "Click below, join, then press <b>✅ I've Joined</b>.",
        reply_markup=kb,
    )


def _is_owner(_, __, msg: Message) -> bool:
    return bool(msg.from_user and msg.from_user.id == Config.OWNER_ID)

owner_filter = filters.create(_is_owner)


# ═══════════════════════════════════════════════════════════════════════════════
#  PYROGRAM CLIENTS
# ═══════════════════════════════════════════════════════════════════════════════

bot = Client(
    name=":memory:",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    parse_mode=ParseMode.HTML,
    workers=8,
)

user = Client(
    name="user_session",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    session_string=Config.SESSION_STRING,
    parse_mode=ParseMode.HTML,
    workers=4,
    no_updates=True,
)


# ═══════════════════════════════════════════════════════════════════════════════
#  CORE: PROCESS A SINGLE RESTRICTED MESSAGE
# ═══════════════════════════════════════════════════════════════════════════════

async def process_single_message(
    dest_chat_id: int,
    source_chat: Union[int, str],
    msg_id: int,
    status_msg: Optional[Message] = None,
) -> Optional[Message]:
    try:
        src: Message = await user.get_messages(source_chat, msg_id)
    except ChannelPrivate:
        if status_msg:
            await status_msg.edit_text("❌ User account is not in this private channel.")
        return None
    except (ChannelInvalid, UsernameInvalid, UsernameNotOccupied):
        if status_msg:
            await status_msg.edit_text("❌ Invalid channel — check the link.")
        return None
    except MessageIdInvalid:
        if status_msg:
            await status_msg.edit_text(f"❌ Message #{msg_id} not found.")
        return None
    except FloodWait as fw:
        await asyncio.sleep(fw.value)
        return await process_single_message(dest_chat_id, source_chat, msg_id, status_msg)

    if src.empty:
        return None

    uid        = dest_chat_id
    custom_cap = await redis_get_caption(uid)
    thumb_fid  = await redis_get_thumb(uid)
    raw_cap    = src.caption or src.text or ""
    caption    = (custom_cap if custom_cap else raw_cap)[:1024]

    media_type = get_media_type(src)
    if not media_type:
        sent = await bot.send_message(dest_chat_id, caption or "​")
        await redis_inc_downloads()
        return sent

    if status_msg:
        await status_msg.edit_text("📥 <b>Downloading…</b>")

    dl_start = time.time()
    try:
        dl_cb = make_progress_callback(status_msg, "Downloading", dl_start) if status_msg else None
        file_path = await user.download_media(
            src, file_name=Config.DOWNLOAD_DIR + "/", progress=dl_cb
        )
    except Exception as e:
        if status_msg:
            await status_msg.edit_text(f"❌ Download failed: <code>{e}</code>")
        return None

    if not file_path or not os.path.exists(file_path):
        if status_msg:
            await status_msg.edit_text("❌ File not found after download.")
        return None

    file_size = os.path.getsize(file_path)
    if file_size > Config.MAX_FILE_SIZE:
        os.remove(file_path)
        if status_msg:
            await status_msg.edit_text(f"❌ File too large: {humanbytes(file_size)}.")
        return None

    if status_msg:
        await status_msg.edit_text(
            f"📤 <b>Uploading…</b>\n📦 Size: {humanbytes(file_size)}"
        )

    thumb_path: Optional[str] = None
    if thumb_fid:
        try:
            thumb_path = await bot.download_media(
                thumb_fid, file_name=Config.DOWNLOAD_DIR + "/thumb_"
            )
        except Exception:
            thumb_path = None

    up_cb = make_progress_callback(status_msg, "Uploading", time.time()) if status_msg else None
    kw    = dict(chat_id=dest_chat_id, caption=caption, progress=up_cb)

    sent: Optional[Message] = None
    try:
        if media_type == "video":
            v    = src.video
            sent = await bot.send_video(
                file_name=os.path.basename(file_path), video=file_path,
                duration=v.duration if v else 0, width=v.width if v else 0,
                height=v.height if v else 0, thumb=thumb_path,
                supports_streaming=True, **kw,
            )
        elif media_type == "audio":
            a    = src.audio
            sent = await bot.send_audio(
                audio=file_path, duration=a.duration if a else 0,
                performer=a.performer if a else None,
                title=a.title if a else None, thumb=thumb_path, **kw,
            )
        elif media_type == "document":
            sent = await bot.send_document(document=file_path, thumb=thumb_path, **kw)
        elif media_type == "photo":
            sent = await bot.send_photo(photo=file_path, **kw)
        elif media_type == "animation":
            sent = await bot.send_animation(animation=file_path, thumb=thumb_path, **kw)
        elif media_type == "voice":
            vn   = src.voice
            sent = await bot.send_voice(
                voice=file_path, duration=vn.duration if vn else 0, **kw
            )
        elif media_type == "video_note":
            vn   = src.video_note
            sent = await bot.send_video_note(
                video_note=file_path, duration=vn.duration if vn else 0,
                length=vn.length if vn else 1, thumb=thumb_path,
                chat_id=dest_chat_id, progress=up_cb,
            )
        elif media_type == "sticker":
            sent = await bot.send_sticker(chat_id=dest_chat_id, sticker=file_path)
        else:
            sent = await bot.send_document(document=file_path, thumb=thumb_path, **kw)
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 2)
    except Exception as e:
        logger.error("Upload error: %s", e)
        if status_msg:
            await status_msg.edit_text(f"❌ Upload failed: <code>{e}</code>")
    finally:
        for p in filter(None, [file_path, thumb_path]):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass

    if sent:
        await redis_inc_downloads()
        await redis_inc_files()
    return sent


# ═══════════════════════════════════════════════════════════════════════════════
#  BATCH PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════

_batch_tasks: dict[int, asyncio.Task] = {}


async def _run_batch(
    uid: int,
    source_chat: Union[int, str],
    start_id: int,
    end_id: int,
    status_msg: Message,
) -> None:
    total   = end_id - start_id + 1
    sent_ok = 0
    failed  = 0

    await status_msg.edit_text(
        f"🚀 <b>Batch started</b>\n📋 Messages: <b>{total}</b>\n⏳ Processing…"
    )

    for idx, msg_id in enumerate(range(start_id, end_id + 1), start=1):
        state = await redis_get_batch(uid)
        if not state or state.get("cancelled"):
            await status_msg.edit_text(
                f"🛑 <b>Batch cancelled</b>\n✅ Sent: {sent_ok}   ❌ Failed: {failed}"
            )
            await redis_clear_batch(uid)
            return

        if idx % 5 == 0 or idx == total:
            try:
                await status_msg.edit_text(
                    f"⏳ <b>Batch in progress…</b>\n"
                    f"📋 {idx}/{total}  ✅ {sent_ok}  ❌ {failed}"
                )
            except Exception:
                pass

        result = await process_single_message(uid, source_chat, msg_id, status_msg=None)
        if result:
            sent_ok += 1
        else:
            failed += 1
        await asyncio.sleep(1.2)

    await redis_clear_batch(uid)
    summary = (
        f"✅ <b>Batch complete!</b>\n\n"
        f"📋 Total: {total}\n✅ Sent: {sent_ok}\n❌ Failed: {failed}"
    )
    try:
        await status_msg.edit_text(summary)
    except Exception:
        await bot.send_message(uid, summary)


# ═══════════════════════════════════════════════════════════════════════════════
#  yt-dlp HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

class _YtDlpProgress:
    def __init__(self):
        self.downloaded = 0; self.total = 0
        self.speed = 0.0;    self.eta   = 0.0
        self.status   = "starting"
        self.filename = ""
        self.error    = ""


def _ydl_hook(state: _YtDlpProgress):
    def _hook(d: dict) -> None:
        state.status = d.get("status", "unknown")
        if state.status == "downloading":
            state.downloaded = d.get("downloaded_bytes", 0)
            state.total      = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            state.speed      = d.get("speed") or 0.0
            state.eta        = d.get("eta") or 0.0
            state.filename   = d.get("filename", "")
        elif state.status == "finished":
            state.filename = d.get("filename", state.filename)
        elif state.status == "error":
            state.error = str(d.get("error", ""))
    return _hook


async def _poll_ytdlp(state: _YtDlpProgress, status_msg: Message, interval: float = 4.0):
    while state.status not in ("finished", "error"):
        await asyncio.sleep(interval)
        if state.total > 0:
            pct  = state.downloaded * 100 / state.total
            bar  = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
            text = (
                f"📥 <b>Downloading…</b>\n\n"
                f"<code>{bar}  {pct:.1f}%</code>\n\n"
                f"📦 {humanbytes(state.downloaded)} / {humanbytes(state.total)}\n"
                f"🚀 Speed: {humanbytes(int(state.speed))}/s\n"
                f"⏳ ETA: {time_formatter(int(state.eta))}"
            )
        else:
            text = f"📥 <b>Downloading…</b>\n{humanbytes(state.downloaded)} downloaded"
        try:
            await status_msg.edit_text(text)
        except Exception:
            pass


def _ydl_download_sync(url: str, out_dir: str, state: _YtDlpProgress, fmt: str) -> Optional[str]:
    opts: dict[str, Any] = {
        "format": fmt, "outtmpl": os.path.join(out_dir, "%(title).60s.%(ext)s"),
        "progress_hooks": [_ydl_hook(state)], "quiet": True, "no_warnings": True,
        "noplaylist": True, "merge_output_format": "mp4",
        "max_filesize": Config.MAX_FILE_SIZE,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info     = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            if not os.path.exists(filename):
                mp4 = os.path.splitext(filename)[0] + ".mp4"
                if os.path.exists(mp4):
                    filename = mp4
            state.filename = filename
            state.status   = "finished"
            return filename
    except Exception as e:
        state.status = "error"
        state.error  = str(e)
        return None


def _ydl_info_sync(url: str) -> Optional[dict]:
    try:
        with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
            return ydl.extract_info(url, download=False)
    except Exception:
        return None


async def _fetch_thumb_from_url(url: str, dest: str) -> Optional[str]:
    if not AIOHTTP_AVAILABLE:
        return None
    try:
        import aiohttp
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    with open(dest, "wb") as f:
                        f.write(await r.read())
                    return dest
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  HELP TEXT
# ═══════════════════════════════════════════════════════════════════════════════

HELP_TEXT = """
<b>🤖 Save Restricted Content Bot</b>

<b>How to use</b>
Send any Telegram message link and I'll re-deliver the file without restrictions.

<b>Supported link formats</b>
• <code>https://t.me/channelname/123</code>  — public channel
• <code>https://t.me/c/1234567890/123</code>  — private channel / group

<b>Batch download</b>
/batch — download a range of messages at once.

<b>Custom thumbnail</b>
/setthumb   — reply to a photo to set it as thumbnail
/delthumb   — remove your custom thumbnail
/showthumb  — preview current thumbnail

<b>Custom caption</b>
/setcaption [text] — add a caption to every file
/delcaption        — remove custom caption

<b>yt-dlp downloader</b>
Send a YouTube / Instagram / TikTok / Facebook or any supported URL.

<b>Commands</b>
/start   /help   /batch   /cancel
"""

_ALL_COMMANDS = [
    "start", "help", "batch", "cancel", "setthumb", "delthumb", "showthumb",
    "setcaption", "delcaption", "showcaption", "stats", "broadcast",
    "addpremium", "removepremium", "listpremium",
]


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — /start  /help  /setcaption  /delcaption
# ═══════════════════════════════════════════════════════════════════════════════

def _start_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 Help",  callback_data="cb_help"),
            InlineKeyboardButton("📊 Stats", callback_data="cb_stats"),
        ],
        [InlineKeyboardButton(
            "➕ Add to Group",
            url=f"https://t.me/{Config.BOT_USERNAME}?startgroup=true",
        )],
    ])


@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(_: Client, message: Message) -> None:
    uid    = message.from_user.id
    is_new = await redis_register_user(uid)
    if not await check_force_sub(bot, uid):
        return await send_force_sub_message(bot, message)
    await message.reply(
        f"👋 <b>Hello {message.from_user.mention}!</b>\n\n"
        "Send me a <b>Telegram message link</b> or any <b>video URL</b> to get started.",
        reply_markup=_start_kb(),
    )
    if is_new:
        await log_to_channel(
            bot, f"👤 New user: {message.from_user.mention} (<code>{uid}</code>)"
        )


@bot.on_message(filters.command("help") & filters.private)
async def cmd_help(_: Client, message: Message) -> None:
    if not await check_force_sub(bot, message.from_user.id):
        return await send_force_sub_message(bot, message)
    reply = await message.reply(HELP_TEXT, disable_web_page_preview=True)
    schedule_delete(reply)


@bot.on_message(filters.command("setcaption") & filters.private)
async def cmd_setcaption(_: Client, message: Message) -> None:
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply("Usage: <code>/setcaption Your caption here</code>")
    await redis_set_caption(message.from_user.id, parts[1].strip())
    reply = await message.reply(f"✅ Caption saved:\n<code>{parts[1].strip()}</code>")
    schedule_delete(reply)


@bot.on_message(filters.command("delcaption") & filters.private)
async def cmd_delcaption(_: Client, message: Message) -> None:
    await redis_del_caption(message.from_user.id)
    reply = await message.reply("🗑️ Custom caption removed.")
    schedule_delete(reply)


@bot.on_message(filters.command("showcaption") & filters.private)
async def cmd_showcaption(_: Client, message: Message) -> None:
    cap   = await redis_get_caption(message.from_user.id)
    reply = await message.reply(
        f"📝 Your caption:\n<code>{cap}</code>" if cap else "ℹ️ No custom caption set."
    )
    schedule_delete(reply)


@bot.on_callback_query(filters.regex(r"^cb_help$"))
async def cb_help(_: Client, query: CallbackQuery) -> None:
    await query.message.edit_text(
        HELP_TEXT,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Back", callback_data="cb_start")]]
        ),
        disable_web_page_preview=True,
    )


@bot.on_callback_query(filters.regex(r"^cb_start$"))
async def cb_start_back(_: Client, query: CallbackQuery) -> None:
    await query.message.edit_text(
        f"👋 <b>Hello {query.from_user.mention}!</b>\n\n"
        "Send me a Telegram link or video URL.",
        reply_markup=_start_kb(),
    )


@bot.on_callback_query(filters.regex(r"^cb_stats$"))
async def cb_stats_button(_: Client, query: CallbackQuery) -> None:
    s = await redis_get_stats()
    await query.answer(
        f"👥 Users: {s['users']}\n"
        f"📥 Downloads: {s['downloads']}\n"
        f"⭐ Premium: {s['premium']}",
        show_alert=True,
    )


@bot.on_callback_query(filters.regex(r"^check_sub$"))
async def cb_check_sub(_: Client, query: CallbackQuery) -> None:
    if await check_force_sub(bot, query.from_user.id):
        await query.message.edit_text(
            "✅ <b>Verified!</b> Send me a link or video URL."
        )
    else:
        await query.answer("❌ You haven't joined yet!", show_alert=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — THUMBNAIL
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(filters.command("setthumb") & filters.private)
async def cmd_setthumb(_: Client, message: Message) -> None:
    target = message.reply_to_message or message
    if not target.photo:
        reply = await message.reply(
            "📸 Reply to a <b>photo</b> with <code>/setthumb</code> to set your thumbnail."
        )
        schedule_delete(reply)
        return
    await redis_set_thumb(message.from_user.id, target.photo.file_id)
    reply = await message.reply("✅ Custom thumbnail saved!")
    schedule_delete(reply)


@bot.on_message(filters.command("delthumb") & filters.private)
async def cmd_delthumb(_: Client, message: Message) -> None:
    if not await redis_get_thumb(message.from_user.id):
        reply = await message.reply("ℹ️ You have no custom thumbnail set.")
        schedule_delete(reply)
        return
    await redis_del_thumb(message.from_user.id)
    reply = await message.reply("🗑️ Thumbnail removed.")
    schedule_delete(reply)


@bot.on_message(filters.command("showthumb") & filters.private)
async def cmd_showthumb(_: Client, message: Message) -> None:
    fid = await redis_get_thumb(message.from_user.id)
    if not fid:
        reply = await message.reply(
            "ℹ️ No thumbnail set. Use /setthumb (reply to a photo)."
        )
        schedule_delete(reply)
        return
    try:
        sent = await bot.send_photo(
            message.from_user.id, fid, caption="🖼️ <b>Your current thumbnail</b>"
        )
        schedule_delete(sent, delay=120)
    except Exception:
        await redis_del_thumb(message.from_user.id)
        reply = await message.reply("❌ Thumbnail stale — please set a new one with /setthumb.")
        schedule_delete(reply)


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — ADMIN
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_uid(message: Message) -> Optional[int]:
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user.id
    parts = message.text.split()
    if len(parts) >= 2:
        try:
            return int(parts[1])
        except ValueError:
            pass
    return None


@bot.on_message(filters.command("stats") & owner_filter)
async def cmd_stats(_: Client, message: Message) -> None:
    s = await redis_get_stats()
    reply = await message.reply(
        f"📊 <b>Bot Statistics</b>\n\n"
        f"👥 Total users:   <b>{s['users']:,}</b>\n"
        f"⭐ Premium users: <b>{s['premium']:,}</b>\n"
        f"📥 Downloads:     <b>{s['downloads']:,}</b>\n"
        f"📁 Files sent:    <b>{s['files']:,}</b>"
    )
    schedule_delete(reply, delay=120)


@bot.on_message(filters.command("broadcast") & owner_filter)
async def cmd_broadcast(_: Client, message: Message) -> None:
    target = message.reply_to_message
    parts  = message.text.split(None, 1)
    if not target and len(parts) < 2:
        reply = await message.reply(
            "Reply to a message with /broadcast, or: <code>/broadcast text</code>"
        )
        schedule_delete(reply)
        return
    status  = await message.reply("📡 <b>Broadcast started…</b>")
    users   = await redis_get_all_users()
    ok = fail = 0
    for idx, uid in enumerate(users, 1):
        try:
            if target:
                await target.copy(uid)
            else:
                await bot.send_message(uid, parts[1])
            ok += 1
        except (UserIsBlocked, InputUserDeactivated):
            fail += 1
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
            try:
                if target:
                    await target.copy(uid)
                else:
                    await bot.send_message(uid, parts[1])
                ok += 1
            except Exception:
                fail += 1
        except Exception:
            fail += 1
        if idx % 50 == 0 or idx == len(users):
            try:
                await status.edit_text(
                    f"📡 Broadcasting…\n{idx}/{len(users)} — ✅ {ok} | ❌ {fail}"
                )
            except Exception:
                pass
        await asyncio.sleep(0.05)
    summary = (
        f"📡 <b>Broadcast complete</b>\n\n"
        f"👥 Total: {len(users)}\n✅ Sent: {ok}\n❌ Failed: {fail}"
    )
    try:
        await status.edit_text(summary)
    except Exception:
        await message.reply(summary)


@bot.on_message(filters.command("addpremium") & owner_filter)
async def cmd_addpremium(_: Client, message: Message) -> None:
    uid = _extract_uid(message)
    if not uid:
        return await message.reply("Usage: <code>/addpremium USER_ID</code>")
    await redis_add_premium(uid)
    reply = await message.reply(f"⭐ Premium granted to <code>{uid}</code>.")
    schedule_delete(reply)
    try:
        await bot.send_message(uid, "🌟 <b>You've been granted Premium access!</b>")
    except Exception:
        pass


@bot.on_message(filters.command("removepremium") & owner_filter)
async def cmd_removepremium(_: Client, message: Message) -> None:
    uid = _extract_uid(message)
    if not uid:
        return await message.reply("Usage: <code>/removepremium USER_ID</code>")
    await redis_remove_premium(uid)
    reply = await message.reply(f"🗑️ Premium removed from <code>{uid}</code>.")
    schedule_delete(reply)


@bot.on_message(filters.command("listpremium") & owner_filter)
async def cmd_listpremium(_: Client, message: Message) -> None:
    members = await redis_get_all_premium()
    if not members:
        reply = await message.reply("ℹ️ No premium users yet.")
    else:
        lines = "\n".join(f"• <code>{m}</code>" for m in members)
        reply = await message.reply(f"⭐ <b>Premium ({len(members)})</b>\n\n{lines}")
    schedule_delete(reply, delay=120)


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — /batch  /cancel
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(filters.command("batch") & filters.private)
async def cmd_batch(_: Client, message: Message) -> None:
    uid = message.from_user.id
    if not await check_force_sub(bot, uid):
        return await send_force_sub_message(bot, message)
    state = await redis_get_batch(uid)
    if state and state.get("step") == "processing":
        reply = await message.reply("⚠️ A batch is already running. Use /cancel to stop it.")
        schedule_delete(reply)
        return
    await redis_set_batch(uid, {"step": "awaiting_start"})
    reply = await message.reply(
        "📋 <b>Batch Download</b>\n\n"
        "Send the <b>first message link</b> of the range.\n\n"
        "<i>Example:</i> <code>https://t.me/c/1234567890/100</code>\n\n"
        "Use /cancel to abort."
    )
    schedule_delete(reply, delay=120)


@bot.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(_: Client, message: Message) -> None:
    uid   = message.from_user.id
    state = await redis_get_batch(uid)
    if not state:
        reply = await message.reply("ℹ️ No active batch to cancel.")
        schedule_delete(reply)
        return
    state["cancelled"] = True
    await redis_set_batch(uid, state)
    task = _batch_tasks.pop(uid, None)
    if task and not task.done():
        task.cancel()
    reply = await message.reply("🛑 Batch cancelled.")
    schedule_delete(reply)


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER — Telegram link forwarder  (group=0)
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(
    filters.private & filters.text & ~filters.command(_ALL_COMMANDS),
    group=0,
)
async def handle_telegram_link(_: Client, message: Message) -> None:
    uid  = message.from_user.id
    text = message.text.strip()

    await redis_register_user(uid)

    if not await check_force_sub(bot, uid):
        return await send_force_sub_message(bot, message)

    if not is_telegram_link(text):
        return

    parsed = parse_telegram_link(text)
    if not parsed:
        reply = await message.reply(
            "❓ Couldn't parse this link.\n\n"
            "Supported formats:\n"
            "• <code>https://t.me/username/123</code>\n"
            "• <code>https://t.me/c/1234567890/123</code>"
        )
        schedule_delete(reply)
        return

    source_chat, msg_id = parsed
    status = await message.reply("⏳ <b>Processing…</b>")
    sent   = await process_single_message(message.chat.id, source_chat, msg_id, status)

    if sent:
        try:
            await status.delete()
        except Exception:
            pass
        await log_to_channel(
            bot,
            f"📥 <b>Forwarded</b>\n"
            f"User: {message.from_user.mention} (<code>{uid}</code>)\n"
            f"Link: <code>{text}</code>",
        )
    else:
        schedule_delete(status)


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER — yt-dlp URL downloader  (group=1)
# ═══════════════════════════════════════════════════════════════════════════════

if YTDLP_AVAILABLE:

    @bot.on_message(
        filters.private & filters.text & ~filters.command(_ALL_COMMANDS),
        group=1,
    )
    async def handle_ytdlp_url(_: Client, message: Message) -> None:
        uid  = message.from_user.id
        text = message.text.strip()

        if not is_ytdlp_url(text):
            return

        await redis_register_user(uid)
        if not await check_force_sub(bot, uid):
            return await send_force_sub_message(bot, message)

        status = await message.reply("🔍 <b>Fetching info…</b>")
        loop   = asyncio.get_running_loop()

        info = await loop.run_in_executor(None, _ydl_info_sync, text)
        if not info:
            await status.edit_text("❌ Could not fetch info for this URL.")
            schedule_delete(status)
            return

        title    = info.get("title", "video")[:80]
        duration = info.get("duration") or 0
        uploader = info.get("uploader", "")

        await status.edit_text(
            f"📹 <b>{title}</b>\n"
            f"👤 {uploader}\n"
            f"⏱ {time_formatter(int(duration))}\n\n"
            "⬇️ Downloading…"
        )

        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
        tmp_dir   = tempfile.mkdtemp(dir=Config.DOWNLOAD_DIR)
        prog      = _YtDlpProgress()
        dl_task   = loop.run_in_executor(
            None, _ydl_download_sync, text, tmp_dir, prog, Config.YTDLP_FORMAT
        )
        poll_task = asyncio.create_task(_poll_ytdlp(prog, status))

        file_path: Optional[str] = await dl_task
        poll_task.cancel()

        if not file_path or not os.path.exists(file_path):
            await status.edit_text(
                f"❌ Download failed:\n<code>{prog.error or 'Unknown error'}</code>"
            )
            schedule_delete(status)
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return

        thumb_fid  = await redis_get_thumb(uid)
        custom_cap = await redis_get_caption(uid)
        caption    = (custom_cap or f"<b>{title}</b>")[:1024]

        thumb_path: Optional[str] = None
        if thumb_fid:
            try:
                thumb_path = await bot.download_media(
                    thumb_fid, file_name=os.path.join(tmp_dir, "thumb_")
                )
            except Exception:
                thumb_path = None

        if not thumb_path and info.get("thumbnail"):
            thumb_path = await _fetch_thumb_from_url(
                info["thumbnail"], os.path.join(tmp_dir, "yt_thumb.jpg")
            )

        file_size = os.path.getsize(file_path)
        await status.edit_text(f"📤 <b>Uploading…</b>\n📦 {humanbytes(file_size)}")

        up_cb = make_progress_callback(status, "Uploading", time.time())
        ext   = os.path.splitext(file_path)[1].lower()

        try:
            if ext in (".mp4", ".mkv", ".webm", ".mov", ".avi"):
                await bot.send_video(
                    chat_id=uid, video=file_path, caption=caption,
                    duration=int(duration), width=info.get("width") or 0,
                    height=info.get("height") or 0, thumb=thumb_path,
                    supports_streaming=True, progress=up_cb,
                )
            elif ext in (".mp3", ".m4a", ".ogg", ".flac", ".wav", ".opus"):
                await bot.send_audio(
                    chat_id=uid, audio=file_path, caption=caption,
                    duration=int(duration), title=title, performer=uploader,
                    thumb=thumb_path, progress=up_cb,
                )
            else:
                await bot.send_document(
                    chat_id=uid, document=file_path, caption=caption,
                    thumb=thumb_path, progress=up_cb,
                )
            await redis_inc_downloads()
            try:
                await status.delete()
            except Exception:
                pass
            await log_to_channel(
                bot,
                f"📥 <b>yt-dlp</b>\nUser: <code>{uid}</code>\n"
                f"URL: <code>{text[:200]}</code>\nTitle: {title}",
            )
        except Exception as e:
            logger.error("yt-dlp upload error: %s", e)
            await status.edit_text(f"❌ Upload failed:\n<code>{e}</code>")
            schedule_delete(status)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER — Batch state-machine interceptor  (group=2)
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(
    filters.private & filters.text & ~filters.command(_ALL_COMMANDS),
    group=2,
)
async def batch_state_handler(_: Client, message: Message) -> None:
    uid   = message.from_user.id
    text  = message.text.strip()
    state = await redis_get_batch(uid)

    if not state:
        return

    step = state.get("step")

    if step == "awaiting_start":
        if not is_telegram_link(text):
            reply = await message.reply(
                "❌ Send a valid Telegram message link.\n"
                "<i>Example:</i> <code>https://t.me/c/1234567890/100</code>"
            )
            schedule_delete(reply)
            raise StopPropagation
        parsed = parse_telegram_link(text)
        if not parsed:
            await message.reply("❌ Could not parse the link. Try again.")
            raise StopPropagation
        source_chat, start_id = parsed
        state.update({
            "step": "awaiting_end",
            "source_chat": str(source_chat),
            "start_id": start_id,
        })
        await redis_set_batch(uid, state)
        reply = await message.reply(
            f"✅ Start: <code>#{start_id}</code>\n\nNow send the <b>last message link</b>."
        )
        schedule_delete(reply, delay=120)
        raise StopPropagation

    elif step == "awaiting_end":
        if not is_telegram_link(text):
            reply = await message.reply("❌ Send a valid Telegram message link (end of range).")
            schedule_delete(reply)
            raise StopPropagation
        parsed = parse_telegram_link(text)
        if not parsed:
            await message.reply("❌ Could not parse the link.")
            raise StopPropagation
        _, end_id = parsed
        start_id  = int(state["start_id"])
        if end_id < start_id:
            start_id, end_id = end_id, start_id
        total = end_id - start_id + 1
        if total > Config.BATCH_MAX:
            reply = await message.reply(
                f"❌ Range too large ({total} messages). Max: {Config.BATCH_MAX}."
            )
            schedule_delete(reply)
            await redis_clear_batch(uid)
            raise StopPropagation
        raw_chat    = state["source_chat"]
        source_chat = int(raw_chat) if raw_chat.lstrip("-").isdigit() else raw_chat
        state.update({"step": "processing", "end_id": end_id, "cancelled": False})
        await redis_set_batch(uid, state)
        status_msg = await message.reply(
            f"⏳ <b>Starting batch…</b>\n📋 {total} messages (#{start_id} → #{end_id})"
        )
        task = asyncio.create_task(
            _run_batch(uid, source_chat, start_id, end_id, status_msg)
        )
        _batch_tasks[uid] = task
        task.add_done_callback(lambda t: _batch_tasks.pop(uid, None))
        raise StopPropagation

    else:
        reply = await message.reply("⚙️ Batch is running. Use /cancel to stop it.")
        schedule_delete(reply)
        raise StopPropagation


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    global _redis

    # 1. Redis
    logger.info("Connecting to Redis…")
    _redis = aioredis.from_url(
        Config.REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        max_connections=20,
        socket_keepalive=True,
        retry_on_timeout=True,
    )
    await _redis.ping()
    logger.info("Redis connected ✓")

    # 2. Start both Pyrogram clients
    logger.info("Starting Pyrogram clients…")
    await bot.start()
    await user.start()

    bot_me  = await bot.get_me()
    user_me = await user.get_me()
    logger.info("Bot  client: @%s (%d) ✓", bot_me.username,  bot_me.id)
    logger.info("User client: @%s (%d) ✓", user_me.username, user_me.id)

    if Config.LOG_CHANNEL:
        try:
            await bot.send_message(
                Config.LOG_CHANNEL,
                f"🚀 <b>Bot started</b>\nBot: @{bot_me.username}\nUser: @{user_me.username}",
            )
        except Exception as e:
            logger.warning("Could not log startup: %s", e)

    logger.info("Bot is running. Press Ctrl-C to stop.")

    # 3. Keep running until SIGINT / SIGTERM
    # Using asyncio.Event + add_signal_handler (correct inside async context)
    stop_event = asyncio.Event()
    loop       = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, OSError):
            pass

    await stop_event.wait()

    # 4. Graceful shutdown
    logger.info("Shutting down…")
    try:
        await bot.stop()
    except Exception as e:
        logger.warning("bot.stop() error: %s", e)
    try:
        await user.stop()
    except Exception as e:
        logger.warning("user.stop() error: %s", e)
    try:
        await _redis.aclose()
    except Exception as e:
        logger.warning("redis.aclose() error: %s", e)
    logger.info("Goodbye 👋")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Start health-check server in a daemon thread (no asyncio, no loop conflict)
    start_health_server()

    # Pyrogram 2.0.106 internally calls asyncio.get_event_loop() in its dispatcher.
    # asyncio.run() in Python 3.11 uses Runner which does NOT register the loop
    # as the thread's current loop — causing Pyrogram's dispatcher tasks to land
    # on a "different loop". Fix: manually create + register the loop first.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        try:
            loop.close()
        except Exception:
            pass
