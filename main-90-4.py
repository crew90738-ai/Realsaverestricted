"""
Save Restricted Content Bot — single-file edition
===================================================
Deploy on Render.com as a Web Service:
  Build:  pip install -r requirements.txt
  Start:  python main.py

ROOT CAUSE FIX (Pyrogram 2.0.106 + Python 3.11)
─────────────────────────────────────────────────
Pyrogram binds asyncio objects to whatever event loop is "current"
at the time Client() is instantiated. If Client() is created before
asyncio.set_event_loop() is called, Pyrogram ends up on a temporary
default loop — not the one main() runs on — causing the famous
"attached to a different loop" dispatcher crash.

Fix: call asyncio.set_event_loop() BEFORE creating any Client().
The event loop setup block is clearly marked below.
"""

# ═══════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import io
import datetime
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

# ── yt-dlp ────────────────────────────────────────────────────────────────────
try:
    import yt_dlp  # type: ignore
    YTDLP_AVAILABLE = True
except ImportError:
    YTDLP_AVAILABLE = False

# ── aiohttp (only for thumbnail URL fetch, not for health server) ─────────────
try:
    import aiohttp as _aiohttp_mod
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

load_dotenv()

# ── CRITICAL PATCH: Fix "Peer id invalid" for newer Telegram channel IDs ─────
# Pyrogram 2.0.106 has MIN_CHANNEL_ID = -1002147483647 (old int32 limit).
# Telegram now issues channel IDs beyond this (e.g. -1003949721609).
# Without this patch, Pyrogram raises ValueError "Peer id invalid" for ALL
# channels created after Telegram expanded beyond the old int32 limit.
# Fix: https://github.com/pyrogram/pyrogram/pull/1430
import pyrogram.utils as _pyro_utils
_pyro_utils.MIN_CHANNEL_ID = -1009999999999999


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
#  HEALTH-CHECK SERVER  (pure threading — zero asyncio, zero loop conflict)
# ═══════════════════════════════════════════════════════════════════════════════

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
    def log_message(self, *args):
        pass   # suppress access logs


class _ReuseAddrHTTPServer(HTTPServer):
    """HTTPServer with SO_REUSEADDR so port is freed instantly on restart.
    Without this, when Render sends SIGTERM and immediately starts a new
    instance, the port may still be in TIME_WAIT state causing bind() to
    fail, which makes Render's health check fail and triggers a restart loop.
    """
    allow_reuse_address = True


def start_health_server() -> None:
    port = Config.PORT
    for attempt in range(3):
        try:
            server = _ReuseAddrHTTPServer(("0.0.0.0", port), _HealthHandler)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            logger.info("Health server on port %d ✓", port)
            return
        except OSError as e:
            logger.warning("Health server bind attempt %d failed: %s", attempt + 1, e)
            if attempt < 2:
                import time as _t
                _t.sleep(2)
    logger.error("Health server could not bind to port %d after 3 attempts", port)


# ═══════════════════════════════════════════════════════════════════════════════
#  ▼▼▼  EVENT LOOP — MUST BE SET BEFORE Client() IS CALLED  ▼▼▼
#
#  Pyrogram's Dispatcher creates asyncio Queue/Lock objects when Client()
#  is instantiated. Those objects bind to the "current" event loop at the
#  time of creation. If the loop isn't set yet, Python creates a temporary
#  one — different from the loop main() runs on → "attached to different loop".
#
#  Solution: create and register the loop HERE, before any Client().
# ═══════════════════════════════════════════════════════════════════════════════

_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

# NOTE: In Python 3.11, ThreadPoolExecutor threads are already daemon threads.
# The shutdown hang is prevented by os._exit(0) in the shutdown sequence below,
# which bypasses Python's atexit handlers (including the one that waits for
# thread pool futures to complete). No custom executor needed.

logger.info("Event loop created and registered ✓")


# ═══════════════════════════════════════════════════════════════════════════════
#  PYROGRAM CLIENTS  (created AFTER loop is set — this is the key)
# ═══════════════════════════════════════════════════════════════════════════════

from pyrogram import Client, StopPropagation, filters, idle  # noqa: E402
from pyrogram.enums import ParseMode  # noqa: E402
from pyrogram.errors import (  # noqa: E402
    ChannelInvalid, ChannelPrivate, ChatAdminRequired, FloodWait,
    InputUserDeactivated, MessageIdInvalid, UserIsBlocked,
    UsernameInvalid, UsernameNotOccupied, UserNotParticipant,
)
from pyrogram.types import (  # noqa: E402
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

bot = Client(
    name=":memory:",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    parse_mode=ParseMode.HTML,
    workers=8,
)

user = Client(
    name=":memory:",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    session_string=Config.SESSION_STRING,
    parse_mode=ParseMode.HTML,
    workers=4,
    no_updates=True,
)

logger.info("Pyrogram clients created ✓")


# ═══════════════════════════════════════════════════════════════════════════════
#  REDIS HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_redis: aioredis.Redis | None = None
_bot_id: int = 0   # cached after bot.start() — avoids repeated get_me() calls


def _r() -> aioredis.Redis:
    if _redis is None:
        raise RuntimeError("Redis not initialised.")
    return _redis


async def redis_add_premium(uid: int) -> None:    await _r().sadd("premium_users", str(uid))
async def redis_remove_premium(uid: int) -> None: await _r().srem("premium_users", str(uid))
async def redis_is_premium(uid: int) -> bool:     return await _r().sismember("premium_users", str(uid))
async def redis_get_all_premium() -> list[int]:   return [int(m) for m in await _r().smembers("premium_users")]

async def redis_register_user(uid: int) -> bool:
    added = await _r().sadd("bot_users", str(uid))
    if added:
        await _r().incr("stats:total_users")
    return bool(added)

async def redis_get_all_users() -> list[int]:  return [int(m) for m in await _r().smembers("bot_users")]
async def redis_inc_downloads() -> None:       await _r().incr("stats:total_downloads")
async def redis_inc_files() -> None:           await _r().incr("stats:total_files")

async def redis_get_stats() -> dict:
    r = _r()
    return {
        "users":     await r.scard("bot_users"),
        "downloads": int(await r.get("stats:total_downloads") or 0),
        "files":     int(await r.get("stats:total_files") or 0),
        "premium":   await r.scard("premium_users"),
    }

async def redis_set_thumb(uid: int, fid: str) -> None:   await _r().set(f"user_thumb:{uid}", fid)
async def redis_get_thumb(uid: int) -> Optional[str]:    return await _r().get(f"user_thumb:{uid}")
async def redis_del_thumb(uid: int) -> None:             await _r().delete(f"user_thumb:{uid}")
async def redis_set_caption(uid: int, cap: str) -> None: await _r().set(f"user_caption:{uid}", cap)
async def redis_get_caption(uid: int) -> Optional[str]:  return await _r().get(f"user_caption:{uid}")
async def redis_del_caption(uid: int) -> None:           await _r().delete(f"user_caption:{uid}")

async def redis_set_batch(uid: int, state: dict) -> None:
    await _r().set(f"batch_state:{uid}", json.dumps(state), ex=600)
async def redis_get_batch(uid: int) -> Optional[dict]:
    raw = await _r().get(f"batch_state:{uid}")
    return json.loads(raw) if raw else None
async def redis_clear_batch(uid: int) -> None:
    await _r().delete(f"batch_state:{uid}")


# ─── User session helpers (for /login + /dlpriv feature) ─────────────────────

async def redis_set_user_session(uid: int, session: str) -> None:
    try:
        await _r().set(f"user_session:{uid}", session)
    except Exception as e:
        logger.error("[REDIS] set_user_session(%d): %s", uid, e)

async def redis_get_user_session(uid: int) -> Optional[str]:
    try:
        return await _r().get(f"user_session:{uid}")
    except Exception as e:
        logger.error("[REDIS] get_user_session(%d): %s", uid, e)
        return None

async def redis_del_user_session(uid: int) -> None:
    try:
        await _r().delete(f"user_session:{uid}")
    except Exception as e:
        logger.error("[REDIS] del_user_session(%d): %s", uid, e)

async def redis_set_login_state(uid: int, state: dict) -> None:
    try:
        await _r().set(f"login_state:{uid}", json.dumps(state), ex=600)  # 10-min TTL
    except Exception as e:
        logger.error("[REDIS] set_login_state(%d): %s", uid, e)

async def redis_get_login_state(uid: int) -> Optional[dict]:
    try:
        raw = await _r().get(f"login_state:{uid}")
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.error("[REDIS] get_login_state(%d): %s", uid, e)
        return None

async def redis_clear_login_state(uid: int) -> None:
    try:
        await _r().delete(f"login_state:{uid}")
    except Exception as e:
        logger.error("[REDIS] clear_login_state(%d): %s", uid, e)

async def redis_inc_login_attempts(uid: int) -> int:
    try:
        key = f"login_attempts:{uid}"
        count = await _r().incr(key)
        await _r().expire(key, 3600)   # reset counter after 1 hour
        return int(count)
    except Exception as e:
        logger.error("[REDIS] inc_login_attempts(%d): %s", uid, e)
        return 0

async def redis_get_login_attempts(uid: int) -> int:
    try:
        raw = await _r().get(f"login_attempts:{uid}")
        return int(raw) if raw else 0
    except Exception as e:
        logger.error("[REDIS] get_login_attempts(%d): %s", uid, e)
        return 0


# ─── Per-user last bot message tracking (prevents orphan messages) ────────────
# Every time the bot sends a "menu" message, we store its ID.
# Next time a menu is shown, we delete/edit the previous one first.

async def set_last_bot_msg(uid: int, msg_id: int) -> None:
    await _r().set(f"last_bot_msg:{uid}", str(msg_id), ex=86400)  # 24h TTL

async def get_last_bot_msg(uid: int) -> Optional[int]:
    raw = await _r().get(f"last_bot_msg:{uid}")
    return int(raw) if raw else None

async def clear_last_bot_msg(uid: int) -> None:
    await _r().delete(f"last_bot_msg:{uid}")

async def delete_prev_bot_msg(uid: int) -> None:
    """Delete the previous bot menu message for this user, silently."""
    msg_id = await get_last_bot_msg(uid)
    if msg_id:
        try:
            await bot.delete_messages(uid, msg_id)
        except Exception:
            pass
        await clear_last_bot_msg(uid)

async def send_menu(uid: int, text: str, reply_markup=None, **kwargs) -> Optional[Message]:
    """
    Send a menu message, automatically deleting the previous one first.
    Stores the new message ID for next call. Use this for ALL menu messages.
    """
    await delete_prev_bot_msg(uid)
    try:
        msg = await bot.send_message(uid, text, reply_markup=reply_markup, **kwargs)
        await set_last_bot_msg(uid, msg.id)
        return msg
    except Exception as e:
        logger.error("send_menu error for %d: %s", uid, e)
        return None

async def edit_menu(message: Message, text: str, reply_markup=None, **kwargs) -> None:
    """Edit an existing menu message in-place (preferred over send_menu when possible)."""
    try:
        await message.edit_text(text, reply_markup=reply_markup, **kwargs)
        await set_last_bot_msg(message.chat.id, message.id)
    except Exception:
        # If edit fails (e.g. message too old), fall back to send
        await send_menu(message.chat.id, text, reply_markup=reply_markup, **kwargs)



async def gr_add_group(chat_id: int) -> None:
    await _r().sadd("gr_groups", str(chat_id))

async def gr_remove_group(chat_id: int) -> None:
    r = _r()
    await r.srem("gr_groups", str(chat_id))
    # Clean up all per-group repeat keys
    for key in [
        f"gr_last_sent:{chat_id}",
        f"gr_next_send:{chat_id}",
        f"gr_group_error:{chat_id}",
        f"gr_group_title:{chat_id}",
    ]:
        await r.delete(key)

async def gr_get_groups() -> list[int]:
    members = await _r().smembers("gr_groups")
    return [int(m) for m in members]

async def gr_group_count() -> int:
    return await _r().scard("gr_groups")

# ─── Global repeat config ─────────────────────────────────────────────────────

async def gr_set_enabled(val: bool) -> None:
    await _r().set("gr_repeat_enabled", "1" if val else "0")

async def gr_is_enabled() -> bool:
    return await _r().get("gr_repeat_enabled") == "1"

async def gr_set_text(text: str) -> None:
    await _r().set("gr_repeat_text", text)

async def gr_get_text() -> Optional[str]:
    return await _r().get("gr_repeat_text")

async def gr_set_interval(secs: int) -> None:
    await _r().set("gr_repeat_interval", str(secs))

async def gr_get_interval() -> int:
    raw = await _r().get("gr_repeat_interval")
    return int(raw) if raw else 3600

async def gr_set_autodelete(val: bool) -> None:
    await _r().set("gr_repeat_autodelete", "1" if val else "0")

async def gr_get_autodelete() -> bool:
    return await _r().get("gr_repeat_autodelete") == "1"

async def gr_set_selfdelete(secs: Optional[int]) -> None:
    if secs is None:
        await _r().delete("gr_repeat_selfdelete")
    else:
        await _r().set("gr_repeat_selfdelete", str(secs))

async def gr_get_selfdelete() -> Optional[int]:
    raw = await _r().get("gr_repeat_selfdelete")
    return int(raw) if raw else None

async def gr_get_last_sent(chat_id: int) -> Optional[int]:
    raw = await _r().get(f"gr_last_sent:{chat_id}")
    return int(raw) if raw else None

async def gr_set_last_sent(chat_id: int, msg_id: int) -> None:
    await _r().set(f"gr_last_sent:{chat_id}", str(msg_id))

async def gr_get_next_send(chat_id: int) -> float:
    raw = await _r().get(f"gr_next_send:{chat_id}")
    return float(raw) if raw else 0.0

async def gr_set_next_send(chat_id: int, ts: float, interval: int) -> None:
    await _r().set(f"gr_next_send:{chat_id}", str(ts), ex=interval * 3 + 60)

async def gr_reset_all_schedules() -> None:
    """Clear all next-send times so all groups fire immediately on next cycle."""
    groups = await gr_get_groups()
    for g in groups:
        await _r().delete(f"gr_next_send:{g}")


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

def make_progress_callback(message: Message, action: str = "Processing",
                           start_time: Optional[float] = None,
                           update_interval: int = Config.PROGRESS_UPDATE_INTERVAL):
    last_update: list[float] = [0.0]
    if start_time is None:
        start_time = time.time()
    async def _cb(current: int, total: int) -> None:
        now = time.time()
        if now - last_update[0] < update_interval and current != total:
            return
        last_update[0] = now
        elapsed = now - start_time
        speed = current / elapsed if elapsed > 0 else 0
        eta   = (total - current) / speed if speed > 0 else 0
        try:
            await message.edit_text(
                f"<b>{action}</b>\n\n"
                f"<code>{progress_bar(current, total)}</code>\n\n"
                f"📦 {humanbytes(current)} / {humanbytes(total)}\n"
                f"🚀 Speed: {humanbytes(speed)}/s\n"
                f"⏳ ETA: {time_formatter(eta)}"
            )
        except Exception:
            pass
    return _cb


# ═══════════════════════════════════════════════════════════════════════════════
#  GENERAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_PRIVATE_RE = re.compile(r"t\.me/c/(\d+)/(?:\d+/)?(\d+)")
_PUBLIC_RE  = re.compile(r"t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)")
_STORY_RE   = re.compile(r"t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/s/(\d+)")

def parse_telegram_link(url: str) -> Optional[Tuple[Union[int, str], int]]:
    url = url.strip().replace("https://", "").replace("http://", "")
    m = _PRIVATE_RE.search(url)
    if m:
        return int("-100" + m.group(1)), int(m.group(2))
    m = _PUBLIC_RE.search(url)
    if m:
        return m.group(1), int(m.group(2))
    return None

def parse_story_link(url: str) -> Optional[Tuple[str, int]]:
    """Returns (username, story_id) or None."""
    url = url.strip().replace("https://", "").replace("http://", "")
    m = _STORY_RE.search(url)
    if m:
        return m.group(1), int(m.group(2))
    return None

def is_telegram_link(text: str) -> bool:
    return bool(_PRIVATE_RE.search(text) or _PUBLIC_RE.search(text) or _STORY_RE.search(text))

def is_ytdlp_url(url: str) -> bool:
    patterns = [
        r"youtube\.com/watch", r"youtu\.be/", r"instagram\.com/",
        r"facebook\.com/", r"tiktok\.com/", r"twitter\.com/status",
        r"x\.com/", r"vimeo\.com/", r"dailymotion\.com/", r"twitch\.tv/",
        r"reddit\.com/r/.*/comments", r"pinterest\.", r"linkedin\.com/",
        r"soundcloud\.com/", r"mixcloud\.com/", r"bilibili\.com/",
    ]
    return any(re.search(p, url.lower()) for p in patterns)

async def _auto_delete(message: Message, delay: int) -> None:
    try:
        await asyncio.sleep(delay)
        await message.delete()
    except asyncio.CancelledError:
        pass   # task was cancelled on shutdown — totally fine, suppress warning
    except Exception:
        pass


def schedule_delete(message: Message, delay: int = Config.AUTO_DELETE_DELAY) -> None:
    if delay > 0:
        task = asyncio.create_task(_auto_delete(message, delay))
        # Attach a no-op callback so Python doesn't log "Task destroyed but pending"
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)


def get_media_type(message: Message) -> Optional[str]:
    for attr in ("document", "video", "audio", "photo", "animation",
                 "voice", "video_note", "sticker"):
        if getattr(message, attr, None):
            return attr
    return None

async def log_to_channel(b: Client, text: str) -> None:
    if not Config.LOG_CHANNEL:
        return
    try:
        await b.send_message(Config.LOG_CHANNEL, text)
    except Exception:
        pass

async def check_force_sub(b: Client, uid: int) -> bool:
    ch = Config.FORCE_SUB_CHANNEL
    if not ch:
        return True
    try:
        member = await b.get_chat_member(ch, uid)
        return member.status.value not in ("left", "banned", "kicked")
    except UserNotParticipant:
        return False
    except (ChatAdminRequired, Exception):
        return True

async def send_force_sub_msg(b: Client, message: Message) -> None:
    ch = Config.FORCE_SUB_CHANNEL
    if ch.lstrip("-").isdigit():
        try:
            invite = await b.export_chat_invite_link(int(ch))
        except Exception:
            invite = f"https://t.me/c/{ch.lstrip('-').removeprefix('100')}"
    else:
        invite = f"https://t.me/{ch.lstrip('@')}"
    await message.reply(
        "⚠️ <b>You must join our channel to use this bot.</b>\n\n"
        "Click below, join, then press <b>✅ I've Joined</b>.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📢 Join Channel", url=invite),
            InlineKeyboardButton("✅ I've Joined",  callback_data="check_sub"),
        ]]),
    )

def _is_owner(_, __, msg: Message) -> bool:
    return bool(msg.from_user and msg.from_user.id == Config.OWNER_ID)

owner_filter = filters.create(_is_owner)

_ALL_CMDS = [
    "start", "help", "batch", "cancel", "setthumb", "delthumb", "showthumb",
    "setcaption", "delcaption", "showcaption", "stats", "broadcast",
    "addpremium", "removepremium", "listpremium", "groups",
    "backup", "restore", "cancel_restore", "recover", "pfp",
    "dlbot", "login", "logout", "dlpriv",
]


# ═══════════════════════════════════════════════════════════════════════════════
#  BACKUP / RESTORE SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

# Module-level state
_last_backup_data: Optional[io.BytesIO] = None
_last_backup_time: float = 0.0
_last_backup_sent: float = 0.0

# Restore mode: {user_id: True}  — in-memory only, never Redis
_restore_mode: dict[int, bool] = {}

_BACKUP_HARD_MIN_BYTES  = 500    # below this = truly empty skeleton, Redis wiped, refuse to send
_BACKUP_WARN_MIN_BYTES  = 5120   # below this but above hard min = send but warn (could be new/low-activity bot)

# _backup_lock is created inside main() once the event loop is running — avoids
# DeprecationWarning from creating asyncio primitives before the loop starts.
_backup_lock: asyncio.Lock  # assigned in main()

# ── Keys this bot uses ────────────────────────────────────────────────────────
_BACKUP_SCALAR_KEYS = [
    "gr_repeat_enabled", "gr_repeat_text", "gr_repeat_interval",
    "gr_repeat_autodelete", "gr_repeat_selfdelete",
    "stats:total_downloads", "stats:total_files", "stats:total_users",
    "last_restore_time", "last_restore_backup_ts",
]
_BACKUP_SET_KEYS = ["bot_users", "premium_users", "gr_groups"]
_BACKUP_PATTERN_KEYS = [
    "user_thumb:*", "user_caption:*",
    "gr_last_sent:*", "gr_next_send:*",
    "gr_group_title:*", "gr_group_error:*",
    "last_bot_msg:*",
]
# Intentionally excluded (ephemeral):
#   batch_state:*, gr_awaiting:*


async def _backup_collect() -> tuple[dict, int]:
    """Collect all persistent Redis keys. Returns (data_dict, total_key_count)."""
    r = _r()
    data: dict = {
        "meta": {
            "bot": "SaveRestrictedBot",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "version": 1,
        },
        "scalars": {},
        "sets": {},
        "lists": {},
        "pattern_scalars": {},
    }

    # Scalars
    try:
        pipe = r.pipeline()
        for k in _BACKUP_SCALAR_KEYS:
            pipe.get(k)
        results = await pipe.execute()
        for k, v in zip(_BACKUP_SCALAR_KEYS, results):
            data["scalars"][k] = v
    except Exception as e:
        logger.error("[BACKUP] Scalar read error: %s", e)

    # Sets
    try:
        pipe = r.pipeline()
        for k in _BACKUP_SET_KEYS:
            pipe.smembers(k)
        results = await pipe.execute()
        for k, v in zip(_BACKUP_SET_KEYS, results):
            data["sets"][k] = list(v) if v else []
    except Exception as e:
        logger.error("[BACKUP] Set read error: %s", e)

    # Pattern keys
    try:
        all_keys: list[str] = []
        for pattern in _BACKUP_PATTERN_KEYS:
            cur = 0
            while True:
                cur, keys = await r.scan(cur, match=pattern, count=200)
                all_keys.extend(keys)
                if cur == 0:
                    break

        if all_keys:
            pipe = r.pipeline()
            for k in all_keys:
                pipe.type(k)
            types = await pipe.execute()

            pipe2 = r.pipeline()
            for k, t in zip(all_keys, types):
                if t == "string":   pipe2.get(k)
                elif t == "list":   pipe2.lrange(k, 0, -1)
                elif t == "set":    pipe2.smembers(k)
                elif t == "hash":   pipe2.hgetall(k)
                else:               pipe2.get(k)
            vals = await pipe2.execute()

            for k, t, v in zip(all_keys, types, vals):
                if t == "list":
                    data["lists"][k] = list(v) if v else []
                elif t == "set":
                    data["sets"][k] = list(v) if v else []
                else:
                    data["pattern_scalars"][k] = v
    except Exception as e:
        logger.error("[BACKUP] Pattern scan error: %s", e)

    total = (len(data["scalars"]) + len(data["sets"]) +
             len(data["lists"]) + len(data["pattern_scalars"]))
    return data, total


def _backup_build_bytes(data: dict) -> io.BytesIO:
    raw = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    buf = io.BytesIO(raw)
    buf.name = f"backup_{data['meta']['timestamp'][:10]}.json"
    return buf


async def _backup_send_to_owner(buf: io.BytesIO, caption: str | None = None) -> None:
    buf.seek(0)
    cap = caption or f"🗄 Auto-backup — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
    await bot.send_document(Config.OWNER_ID, buf, caption=cap, file_name=buf.name)


# ── Auto-backup background task ───────────────────────────────────────────────
async def _auto_backup_worker() -> None:
    global _last_backup_data, _last_backup_time, _last_backup_sent
    await asyncio.sleep(300)   # 5-min stagger after startup
    while True:
        try:
            data, total = await _backup_collect()
            buf      = _backup_build_bytes(data)
            buf_size = buf.getbuffer().nbytes
            now      = time.time()

            if buf_size < _BACKUP_HARD_MIN_BYTES:
                logger.warning("[BACKUP] Truly empty backup: %d bytes — Redis wiped. NOT sending.", buf_size)
                try:
                    warn_kb = InlineKeyboardMarkup([[
                        InlineKeyboardButton("📤 Backup Anyway", callback_data="backup_force_send")
                    ]])
                    await bot.send_message(
                        Config.OWNER_ID,
                        f"⚠️ <b>Backup Warning!</b>\n\n"
                        f"Redis appears completely empty or wiped!\n"
                        f"Backup is only <b>{buf_size} bytes</b> — this is just an empty skeleton.\n\n"
                        f"❌ File NOT sent to protect your last good backup.\n"
                        f"🔴 Check your Redis immediately.\n"
                        f"✅ Use /restore with a previous good backup file.\n\n"
                        f"<i>Tap below only if you're sure you want this file sent.</i>",
                        reply_markup=warn_kb,
                    )
                except Exception:
                    pass
                async with _backup_lock:
                    _last_backup_data = buf
                    _last_backup_time = now
                await asyncio.sleep(1800)
                continue

            async with _backup_lock:
                _last_backup_data = buf
                _last_backup_time = now
            logger.info("[BACKUP] Auto-backup collected %d keys (%d bytes).", total, buf_size)

            import time as _time
            async with _backup_lock:
                last_sent = _last_backup_sent
            if _time.time() - last_sent >= 3600:
                # Small but not empty — send with a note (new bot or low activity)
                small_note = (
                    f"\n\n⚠️ <i>Small backup ({buf_size} bytes). "
                    f"Could be a new bot or low activity — not necessarily a wipe.</i>"
                    if buf_size < _BACKUP_WARN_MIN_BYTES else ""
                )
                await _backup_send_to_owner(
                    buf,
                    caption=(
                        f"🗄 Auto-backup — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
                        f"{small_note}"
                    )
                )
                async with _backup_lock:
                    _last_backup_sent = _time.time()
                logger.info("[BACKUP] Auto-backup sent to owner.")

        except Exception as e:
            logger.error("[BACKUP] Auto-backup worker error: %s", e)
        await asyncio.sleep(1800)


# ── /backup command ───────────────────────────────────────────────────────────
@bot.on_message(filters.command("backup") & owner_filter)
async def cmd_backup(_: Client, message: Message) -> None:
    global _last_backup_data, _last_backup_time, _last_backup_sent
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass

    import time as _time
    async with _backup_lock:
        age    = _time.time() - _last_backup_time
        cached = _last_backup_data

    # Use cached if fresh (< 5 min) — but still apply size guard
    if cached and age < 300:
        cached.seek(0)
        buf_size = cached.getbuffer().nbytes
        if buf_size < _BACKUP_HARD_MIN_BYTES:
            warn_kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("📤 Backup Anyway", callback_data="backup_force_send")
            ]])
            status = await bot.send_message(
                uid,
                f"⚠️ <b>Backup Warning!</b>\n\n"
                f"Cached backup is only <b>{buf_size} bytes</b> — Redis appears wiped.\n\n"
                f"❌ File NOT sent to protect your last good backup.\n"
                f"<i>Tap below only if you're sure you want this file sent.</i>",
                reply_markup=warn_kb,
            )
            await set_last_bot_msg(uid, status.id)
            return
        small_note = (
            f"\n\n⚠️ <i>Small backup ({buf_size} bytes). "
            f"Could be a new bot or low activity.</i>"
            if buf_size < _BACKUP_WARN_MIN_BYTES else ""
        )
        status = await bot.send_message(uid, "📦 Using latest cached backup (< 5 min old)…")
        await _backup_send_to_owner(
            cached,
            caption=(
                f"📦 Manual /backup — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC (cached)"
                f"{small_note}"
            )
        )
        try:
            await status.edit_text("✅ Backup sent to your Saved Messages!")
        except Exception:
            pass
        schedule_delete(status, delay=30)
        return

    # Live backup with progress
    status = await bot.send_message(uid, "⏳ Starting backup…")

    try:
        await status.edit_text("🔍 Scanning Redis keys…\n<code>[██░░░░░░░░] 20%</code>")
        data, total = await _backup_collect()

        await status.edit_text(f"📦 Building JSON… ({total} keys)\n<code>[██████░░░░] 60%</code>")
        buf      = _backup_build_bytes(data)
        buf_size = buf.getbuffer().nbytes

        # Health check — tiered thresholds
        if buf_size < _BACKUP_HARD_MIN_BYTES:
            warn_kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("📤 Backup Anyway", callback_data="backup_force_send")
            ]])
            await status.edit_text(
                f"⚠️ <b>Backup Warning!</b>\n\n"
                f"Redis appears completely empty or wiped!\n"
                f"Backup is only <b>{buf_size} bytes</b> — just an empty skeleton.\n\n"
                f"❌ File NOT sent — use /restore with a previous good backup.\n\n"
                f"<i>Tap below only if you're sure you want this file sent.</i>",
                reply_markup=warn_kb,
            )
            await set_last_bot_msg(uid, status.id)
            return

        small_note = (
            f"\n\n⚠️ <i>Small backup ({buf_size} bytes). "
            f"Could be a new bot or low activity — not necessarily a wipe.</i>"
            if buf_size < _BACKUP_WARN_MIN_BYTES else ""
        )

        async with _backup_lock:
            _last_backup_data = buf
            _last_backup_time = _time.time()

        await status.edit_text("📤 Sending to Saved Messages…\n<code>[████████░░] 80%</code>")
        await _backup_send_to_owner(
            buf,
            caption=(
                f"📦 Manual /backup — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
                f"{small_note}"
            )
        )
        async with _backup_lock:
            _last_backup_sent = _time.time()

        await status.edit_text(
            f"✅ <b>Backup sent!</b>\n<code>[██████████] 100%</code>\n\n"
            f"📊 {total} keys backed up\n"
            f"📁 File sent to your Saved Messages"
        )
        logger.info("[BACKUP] Manual backup complete: %d keys.", total)
        schedule_delete(status, delay=60)

    except Exception as e:
        logger.error("[BACKUP] cmd_backup error: %s", e)
        try:
            await status.edit_text(f"❌ Backup failed:\n<code>{e}</code>")
        except Exception:
            pass
        schedule_delete(status, delay=30)


# ── /recover command ─────────────────────────────────────────────────────────
@bot.on_message(filters.command("recover") & owner_filter)
async def cmd_recover(_: Client, message: Message) -> None:
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    status = await bot.send_message(
        uid,
        "♻️ <b>Group Recovery Mode</b>\n\n"
        "Paste the group ID(s) you want to force-register into <code>gr_groups</code>.\n"
        "Separate multiple IDs with spaces or commas.\n\n"
        "Example:\n<code>-1001234567890 -1009876543210</code>\n\n"
        "These groups will be added back so the keep-alive broadcast reaches them again.\n\n"
        "Send the IDs now, or type <code>/cancel_restore</code> to abort."
    )
    await set_last_bot_msg(uid, status.id)
    _restore_mode[uid] = "recover"   # reuse restore_mode with a different value


@bot.on_message(filters.private & filters.text & ~filters.command(_ALL_CMDS) & owner_filter, group=5)
async def handle_recover_ids(_: Client, message: Message) -> None:
    uid = message.from_user.id
    if _restore_mode.get(uid) != "recover":
        return   # not in recover mode

    text = message.text.strip() if message.text else ""

    if text.lower() == "cancel":
        _restore_mode.pop(uid, None)
        reply = await message.reply("❌ Recovery cancelled.")
        schedule_delete(reply, delay=15)
        return

    parts = [x.strip() for x in re.split(r"[\s,]+", text) if x.strip()]
    if not parts:
        reply = await message.reply("⚠️ No IDs found. Send the group IDs or /cancel_restore.")
        schedule_delete(reply, delay=15)
        return

    _restore_mode.pop(uid, None)

    status = await message.reply(f"♻️ Processing {len(parts)} ID(s)…")

    r = _r()
    results   = []
    registered = skipped = failed = 0

    for part in parts:
        try:
            group_id = int(part)
        except ValueError:
            results.append(f"❌ <code>{part}</code> — not a valid integer ID")
            failed += 1
            continue
        try:
            already = await r.sismember("gr_groups", str(group_id))
            await r.sadd("gr_groups", str(group_id))

            # Try to fetch and cache the group title
            try:
                chat  = await bot.get_chat(group_id)
                title = chat.title or f"Group {group_id}"
                await r.set(f"gr_group_title:{group_id}", title)
            except Exception:
                title = await r.get(f"gr_group_title:{group_id}") or f"Group {group_id}"

            if already:
                results.append(f"🔄 <code>{group_id}</code> — already registered, refreshed\n   📌 {title}")
                skipped += 1
            else:
                results.append(f"✅ <code>{group_id}</code> — registered successfully\n   📌 {title}")
                registered += 1
        except Exception as e:
            results.append(f"❌ <code>{part}</code> — error: {str(e)[:80]}")
            failed += 1
            logger.error("[RECOVER] Failed to register %s: %s", part, e)

    summary = (
        f"♻️ <b>Recovery Complete</b>\n\n"
        f"✅ Newly registered: {registered}\n"
        f"🔄 Already existed (refreshed): {skipped}\n"
        f"❌ Failed: {failed}\n\n"
        + "\n".join(results)
    )
    try:
        await status.edit_text(summary)
    except Exception:
        await message.reply(summary)
    logger.info("[RECOVER] Done: %d new, %d refreshed, %d failed.", registered, skipped, failed)
    raise StopPropagation


# ── /restore command ──────────────────────────────────────────────────────────
@bot.on_message(filters.command("restore") & owner_filter)
async def cmd_restore(_: Client, message: Message) -> None:
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    _restore_mode[uid] = True
    status = await bot.send_message(
        uid,
        "🔄 <b>Restore mode activated.</b>\n\n"
        "Send the backup <code>.json</code> file now.\n"
        "Type /cancel_restore to abort.",
    )
    await set_last_bot_msg(uid, status.id)


@bot.on_message(filters.command("cancel_restore") & owner_filter)
async def cmd_cancel_restore(_: Client, message: Message) -> None:
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    was_active = _restore_mode.pop(uid, False)
    reply = await bot.send_message(
        uid,
        "❌ Restore cancelled. Bot running normally." if was_active else "ℹ️ Nothing to cancel."
    )
    schedule_delete(reply, delay=20)


@bot.on_callback_query(filters.regex(r"^backup_force_send$") & owner_filter)
async def cb_backup_force_send(_: Client, query: CallbackQuery) -> None:
    """Send the cached backup file regardless of size — owner explicitly requested it."""
    uid = query.from_user.id
    await query.answer("📤 Sending backup file…")
    try:
        async with _backup_lock:
            buf = _last_backup_data

        if not buf:
            await query.message.edit_text(
                "❌ No cached backup available.\nRun /backup to generate one."
            )
            return

        buf.seek(0)
        import time as _time
        # Remove the button immediately before sending — prevents double-tap
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await _backup_send_to_owner(
            buf,
            caption=(
                f"📤 Forced backup — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n"
                f"⚠️ Sent on your request despite small size."
            )
        )
        async with _backup_lock:
            _last_backup_sent = _time.time()

        await query.message.edit_text(
            "✅ Backup file sent to your Saved Messages.\n"
            "⚠️ Remember — this may be an empty/wiped backup."
        )
        logger.info("[BACKUP] Force-send by owner.")
    except Exception as e:
        logger.error("[BACKUP] Force-send failed: %s", e)
        try:
            await query.message.edit_text(f"❌ Force backup send failed:\n<code>{e}</code>")
        except Exception:
            pass


@bot.on_message(filters.private & filters.document & owner_filter)
async def handle_restore_file(_: Client, message: Message) -> None:
    uid = message.from_user.id
    if _restore_mode.pop(uid, None) is not True:
        return  # not in restore mode (could be in recover mode or nothing) — ignore

    status = await bot.send_message(uid, "📥 File received → parsing JSON…")

    try:
        file_bytes = await bot.download_media(message.document, in_memory=True)
        data       = json.loads(bytes(file_bytes.getvalue()))
    except Exception as e:
        await status.edit_text(f"❌ Invalid file:\n<code>{e}</code>\n\nPlease check the file and try again.")
        schedule_delete(status, delay=30)
        return

    if "meta" not in data or "scalars" not in data:
        await status.edit_text("❌ This doesn't look like a valid bot backup. Restore aborted.")
        schedule_delete(status, delay=20)
        return

    if data.get("meta", {}).get("version") != 1:
        await status.edit_text(
            "❌ Incompatible backup version.\n\n"
            "This backup was created with a different version of the bot.\n"
            "Restore aborted."
        )
        schedule_delete(status, delay=20)
        return

    await status.edit_text("🔧 Restoring keys…\n<code>[████░░░░░░] 40%</code>")

    r = _r()
    restored = errors = 0

    try:
        # Scalars
        scalars = data.get("scalars", {})
        if scalars:
            pipe = r.pipeline()
            for k, v in scalars.items():
                if v is not None:
                    pipe.set(k, v)
            await pipe.execute()
            restored += len([v for v in scalars.values() if v is not None])

        await status.edit_text(f"🔧 Restoring sets… ({restored} done)\n<code>[██████░░░░] 60%</code>")

        # Sets
        for k, members in data.get("sets", {}).items():
            try:
                pipe = r.pipeline()
                pipe.delete(k)
                if members:
                    pipe.sadd(k, *members)
                await pipe.execute()
                restored += 1
            except Exception as e:
                logger.error("[RESTORE] Set %s: %s", k, e)
                errors += 1

        await status.edit_text(f"🔧 Restoring lists & patterns… ({restored} done)\n<code>[████████░░] 80%</code>")

        # Lists
        for k, items in data.get("lists", {}).items():
            try:
                pipe = r.pipeline()
                pipe.delete(k)
                if items:
                    pipe.rpush(k, *items)
                await pipe.execute()
                restored += 1
            except Exception as e:
                logger.error("[RESTORE] List %s: %s", k, e)
                errors += 1

        # Pattern scalars
        ps = data.get("pattern_scalars", {})
        if ps:
            pipe = r.pipeline()
            for k, v in ps.items():
                if v is not None:
                    pipe.set(k, v)
            await pipe.execute()
            restored += len([v for v in ps.values() if v is not None])

    except Exception as e:
        await status.edit_text(f"❌ Restore failed during key write:\n<code>{e}</code>")
        logger.error("[RESTORE] Key write error: %s", e)
        return

    # Record restore metadata
    try:
        restore_ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        backup_ts  = data.get("meta", {}).get("timestamp", "unknown")[:16].replace("T", " ") + " UTC"
        await r.set("last_restore_time",      restore_ts)
        await r.set("last_restore_backup_ts", backup_ts)
    except Exception as e:
        logger.error("[RESTORE] Metadata write failed: %s", e)

    backup_ts_display = data.get("meta", {}).get("timestamp", "unknown")[:16].replace("T", " ") + " UTC"
    await status.edit_text(
        f"✅ <b>Restore complete!</b>\n<code>[██████████] 100%</code>\n\n"
        f"📦 Backup date: {backup_ts_display}\n"
        f"🔑 Keys restored: {restored}\n"
        f"{'⚠️ Errors: ' + str(errors) if errors else '✅ No errors'}\n\n"
        f"✅ Backup successfully uploaded to Redis.\n"
        f"🤖 Bot continuing and running normally.\n\n"
        f"<i>ℹ️ Note: Restore is additive — keys in the backup were written "
        f"but any extra keys already in Redis were not removed.</i>"
    )
    logger.info("[RESTORE] Complete: %d keys, %d errors.", restored, errors)
    schedule_delete(status, delay=120)



# ═══════════════════════════════════════════════════════════════════════════════
#  CORE: PROCESS A SINGLE RESTRICTED MESSAGE
#  Full error handling: deleted posts, paid media, timeouts, flood waits, etc.
# ═══════════════════════════════════════════════════════════════════════════════

async def process_single_message(
    dest_chat_id: int,
    source_chat: Union[int, str],
    msg_id: int,
    status_msg: Optional[Message] = None,
) -> Optional[Message]:
    """
    Fetch one message via the user client and re-send via the bot.
    Returns the sent Message on success, None on any failure.
    Always updates status_msg with a human-readable error — never hangs.
    """
    try:
        return await asyncio.wait_for(
            _process_inner(dest_chat_id, source_chat, msg_id, status_msg),
            timeout=180.0,   # 3-minute hard timeout — prevents infinite hangs
        )
    except asyncio.TimeoutError:
        logger.warning("process_single_message timed out for msg %d", msg_id)
        if status_msg:
            try:
                await status_msg.edit_text(
                    "❌ <b>Timed out.</b>\n"
                    "This can happen with very large files or slow connections.\n"
                    "Please try again."
                )
            except Exception:
                pass
        return None
    except Exception as e:
        logger.error("process_single_message unexpected error: %s", e)
        if status_msg:
            try:
                await status_msg.edit_text(f"❌ <b>Unexpected error:</b>\n<code>{e}</code>")
            except Exception:
                pass
        return None


async def _process_inner(
    dest_chat_id: int,
    source_chat: Union[int, str],
    msg_id: int,
    status_msg: Optional[Message],
    _fw_retries: int = 0,   # flood-wait retry depth — prevents infinite recursion
) -> Optional[Message]:
    """Inner implementation — all real logic here, wrapped by process_single_message."""

    # ── 1. Fetch the source message ──────────────────────────────────────────
    try:
        src: Message = await user.get_messages(source_chat, msg_id)
    except ChannelPrivate:
        if status_msg:
            await status_msg.edit_text(
                "❌ <b>Private channel — no access.</b>\n\n"
                "The saved session is not a member of this channel.\n\n"
                "If <b>you</b> are a member, you can download it using "
                "your own Telegram account.\n\n"
                "Tap below to connect your account, then resend the link "
                "using <code>/dlpriv [link]</code>.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔑 Login with My Account", callback_data="login_start"),
                ]]),
            )
        return None
    except (ChannelInvalid, UsernameInvalid, UsernameNotOccupied):
        if status_msg:
            # ChannelInvalid on a numeric (t.me/c/...) link almost always means
            # the session has no access/is not a member — peer not in cache.
            # Show the login prompt in that case rather than "deleted or renamed".
            if isinstance(source_chat, int):
                await status_msg.edit_text(
                    "❌ <b>Private channel — no access.</b>\n\n"
                    "The saved session is not a member of this channel.\n\n"
                    "If <b>you</b> are a member, you can download it using "
                    "your own Telegram account.\n\n"
                    "Tap below to connect your account, then resend the link "
                    "using <code>/dlpriv [link]</code>.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔑 Login with My Account", callback_data="login_start"),
                    ]]),
                )
            else:
                await status_msg.edit_text(
                    "❌ <b>Channel not found.</b>\n"
                    "Check the link — the channel may have been deleted or renamed."
                )
        return None
    except MessageIdInvalid:
        if status_msg:
            await status_msg.edit_text(
                f"❌ <b>Message not found.</b>\n"
                f"Message #{msg_id} may have been deleted."
            )
        return None
    except FloodWait as fw:
        if _fw_retries >= 3:
            logger.warning("FloodWait retry limit reached for msg %d", msg_id)
            if status_msg:
                try:
                    await status_msg.edit_text(
                        "❌ <b>Too many rate limits.</b>\nPlease try again in a few minutes."
                    )
                except Exception:
                    pass
            return None
        logger.warning("FloodWait %ds on get_messages", fw.value)
        if status_msg:
            try:
                await status_msg.edit_text(f"⏳ Rate limited by Telegram. Waiting {fw.value}s…")
            except Exception:
                pass
        await asyncio.sleep(fw.value + 1)
        return await _process_inner(dest_chat_id, source_chat, msg_id, status_msg, _fw_retries + 1)
    except Exception as e:
        err = str(e)
        if status_msg:
            await status_msg.edit_text(f"❌ <b>Failed to fetch message:</b>\n<code>{err}</code>")
        return None

    # ── 2. Check if message is empty / deleted ───────────────────────────────
    if not src or src.empty or (
        not src.text and not src.caption and not get_media_type(src)
    ):
        if status_msg:
            await status_msg.edit_text(
                "❌ <b>Message is empty or was deleted.</b>\n"
                "Nothing to save from this link."
            )
        return None

    # ── 3. Check for paid media (Telegram Stars) ─────────────────────────────
    if getattr(src, "paid_media", None):
        if status_msg:
            await status_msg.edit_text(
                "❌ <b>Paid media (Telegram Stars).</b>\n"
                "This content requires payment to access and cannot be saved."
            )
        return None

    # ── 4. Resolve caption / thumbnail ───────────────────────────────────────
    uid        = dest_chat_id
    custom_cap = await redis_get_caption(uid)
    thumb_fid  = await redis_get_thumb(uid)
    caption    = (custom_cap or src.caption or src.text or "")[:1024]
    media_type = get_media_type(src)

    # ── 5. Text-only message ─────────────────────────────────────────────────
    if not media_type:
        full_text = str(custom_cap or src.text or src.caption or "").strip()
        if not full_text:
            if status_msg:
                await status_msg.edit_text(
                    "ℹ️ <b>This message has no downloadable content.</b>\n"
                    "It may be a service message or empty post."
                )
            return None
        # Split into 4096-char chunks.
        # Escape &, <, > so URLs and special chars survive HTML parse mode intact.
        chunks = [full_text[i:i + 4096] for i in range(0, len(full_text), 4096)]
        sent = None
        for i, chunk in enumerate(chunks):
            safe_chunk = (
                chunk
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            try:
                sent = await bot.send_message(
                    dest_chat_id, safe_chunk, parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error("[TEXT] Chunk %d/%d failed: %s", i + 1, len(chunks), e)
        await redis_inc_downloads()
        return sent

    # ── 6. Download media ────────────────────────────────────────────────────
    if status_msg:
        try:
            await status_msg.edit_text("📥 <b>Downloading…</b>")
        except Exception:
            pass

    dl_start = time.time()
    try:
        dl_cb = make_progress_callback(status_msg, "Downloading", dl_start) if status_msg else None
        file_path = await user.download_media(
            src, file_name=Config.DOWNLOAD_DIR + "/", progress=dl_cb
        )
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 1)
        file_path = await user.download_media(
            src, file_name=Config.DOWNLOAD_DIR + "/"
        )
    except Exception as e:
        err = str(e).lower()
        if "paid" in err or "stars" in err:
            msg = "❌ <b>Paid media.</b> This content requires Telegram Stars to access."
        elif "private" in err or "forbidden" in err:
            msg = "❌ <b>Access denied.</b> The user account cannot access this content."
        else:
            msg = f"❌ <b>Download failed:</b>\n<code>{e}</code>"
        if status_msg:
            try:
                await status_msg.edit_text(msg)
            except Exception:
                pass
        return None

    if not file_path or not os.path.exists(file_path):
        if status_msg:
            await status_msg.edit_text(
                "❌ <b>Download failed.</b>\nFile not found after download."
            )
        return None

    # ── 7. File size check ───────────────────────────────────────────────────
    file_size = os.path.getsize(file_path)
    if file_size > Config.MAX_FILE_SIZE:
        os.remove(file_path)
        if status_msg:
            await status_msg.edit_text(
                f"❌ <b>File too large:</b> {humanbytes(file_size)}\n"
                f"Maximum size is 4 GB."
            )
        return None

    if status_msg:
        try:
            await status_msg.edit_text(
                f"📤 <b>Uploading…</b>\n📦 Size: {humanbytes(file_size)}"
            )
        except Exception:
            pass

    # ── 8. Resolve thumbnail ─────────────────────────────────────────────────
    thumb_path: Optional[str] = None
    if thumb_fid:
        try:
            thumb_path = await bot.download_media(
                thumb_fid, file_name=Config.DOWNLOAD_DIR + "/thumb_"
            )
        except Exception:
            thumb_path = None

    # ── 9. Upload ────────────────────────────────────────────────────────────
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
        logger.warning("FloodWait %ds on upload", fw.value)
        if status_msg:
            try:
                await status_msg.edit_text(f"⏳ Rate limited. Waiting {fw.value}s then retrying…")
            except Exception:
                pass
        await asyncio.sleep(fw.value + 2)
        # retry upload once after flood wait — dispatch on correct media type
        try:
            if media_type == "video":
                v = src.video
                sent = await bot.send_video(
                    file_name=os.path.basename(file_path), video=file_path,
                    duration=v.duration if v else 0, width=v.width if v else 0,
                    height=v.height if v else 0, thumb=thumb_path,
                    supports_streaming=True, **kw,
                )
            elif media_type == "audio":
                a = src.audio
                sent = await bot.send_audio(
                    audio=file_path, duration=a.duration if a else 0,
                    performer=a.performer if a else None,
                    title=a.title if a else None, thumb=thumb_path, **kw,
                )
            elif media_type == "photo":
                sent = await bot.send_photo(photo=file_path, **kw)
            elif media_type == "animation":
                sent = await bot.send_animation(animation=file_path, thumb=thumb_path, **kw)
            elif media_type == "voice":
                vn = src.voice
                sent = await bot.send_voice(
                    voice=file_path, duration=vn.duration if vn else 0, **kw
                )
            elif media_type == "video_note":
                vn = src.video_note
                sent = await bot.send_video_note(
                    video_note=file_path, duration=vn.duration if vn else 0,
                    length=vn.length if vn else 1, thumb=thumb_path,
                    chat_id=dest_chat_id, progress=up_cb,
                )
            elif media_type == "sticker":
                sent = await bot.send_sticker(chat_id=dest_chat_id, sticker=file_path)
            else:
                sent = await bot.send_document(document=file_path, thumb=thumb_path, **kw)
        except Exception as e2:
            logger.error("Upload retry failed: %s", e2)
            if status_msg:
                try:
                    await status_msg.edit_text(f"❌ <b>Upload failed after retry:</b>\n<code>{e2}</code>")
                except Exception:
                    pass
    except Exception as e:
        logger.error("Upload error: %s", e)
        if status_msg:
            try:
                await status_msg.edit_text(f"❌ <b>Upload failed:</b>\n<code>{e}</code>")
            except Exception:
                pass
    finally:
        # Always clean up temp files
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
#  STORY DOWNLOADER
# ═══════════════════════════════════════════════════════════════════════════════

async def handle_story_download(message: Message, url: str) -> None:
    """Fetch a Telegram story via the userbot raw API and deliver it unrestricted."""
    uid    = message.from_user.id
    parsed = parse_story_link(url)
    if not parsed:
        return

    username, story_id = parsed
    status = await message.reply(f"⏳ <b>Fetching story from @{username}…</b>")

    tmp_path:   Optional[str] = None
    thumb_path: Optional[str] = None

    try:
        # ── 1. Fetch story — tries all known raw API variants ────────────────
        media_type: Optional[str] = None
        raw_media = None
        try:
            from pyrogram.raw.types import (
                StoryItem, MessageMediaPhoto, MessageMediaDocument,
            )
            peer        = await user.resolve_peer(username)
            stories_list = None
            last_err     = None

            # Variant A: GetStoriesByID(user_id=, id=) — early API uses user_id
            try:
                from pyrogram.raw.functions.stories import GetStoriesByID
                r = await user.invoke(GetStoriesByID(user_id=peer, id=[story_id]))
                raw = r.stories if hasattr(r, 'stories') else r
                stories_list = raw.stories if hasattr(raw, 'stories') else list(raw)
            except Exception as e:
                last_err = e

            # Variant B: GetStoriesByID(peer=, id=) — newer API uses peer
            if stories_list is None:
                try:
                    from pyrogram.raw.functions.stories import GetStoriesByID
                    r = await user.invoke(GetStoriesByID(peer=peer, id=[story_id]))
                    raw = r.stories if hasattr(r, 'stories') else r
                    stories_list = raw.stories if hasattr(raw, 'stories') else list(raw)
                except Exception as e:
                    last_err = e

            # Variant C: GetPeerStories(peer=) — Layer 166+
            if stories_list is None:
                try:
                    from pyrogram.raw.functions.stories import GetPeerStories
                    r = await user.invoke(GetPeerStories(peer=peer))
                    raw = r.stories if hasattr(r, 'stories') else r
                    stories_list = raw.stories if hasattr(raw, 'stories') else list(raw)
                except Exception as e:
                    last_err = e

            # Variant D: GetUserStories(user_id=) — active stories only
            if stories_list is None:
                try:
                    from pyrogram.raw.functions.stories import GetUserStories
                    r = await user.invoke(GetUserStories(user_id=peer))
                    raw = r.stories if hasattr(r, 'stories') else r
                    stories_list = raw.stories if hasattr(raw, 'stories') else list(raw)
                except Exception as e:
                    last_err = e

            if stories_list is None:
                import pyrogram.raw.functions.stories as _sf
                avail = [x for x in dir(_sf) if x[0].isupper()]
                raise Exception(
                    f"No compatible story fetch API found.\n"
                    f"Available: {avail}\nLast error: {last_err}"
                )

            story = next(
                (s for s in stories_list
                 if isinstance(s, StoryItem) and s.id == story_id),
                None,
            )

            if story is None:
                # Show found IDs so we can diagnose the problem
                found_ids = []
                try:
                    found_ids = [s.id for s in stories_list if hasattr(s, 'id')]
                except Exception:
                    pass
                await status.edit_text(
                    f"❌ <b>Story #{story_id} not in fetched list.</b>\n\n"
                    f"Story IDs returned: <code>{found_ids}</code>\n"
                    f"Total: {len(found_ids)}\n\n"
                    f"<i>The story may be pinned/archived.</i>"
                )
                schedule_delete(status)
                return

            if not story.media:
                await status.edit_text("❌ <b>Story has no media.</b>")
                schedule_delete(status)
                return

            if isinstance(story.media, MessageMediaPhoto):
                media_type = "photo"
                raw_media  = story.media.photo
            elif isinstance(story.media, MessageMediaDocument):
                media_type = "video"
                raw_media  = story.media.document
            else:
                await status.edit_text("❌ <b>Unsupported story media type.</b>")
                schedule_delete(status)
                return

        except Exception as e:
            err = str(e).lower()
            if any(k in err for k in ("private", "not found", "stories_not_found",
                                      "peer_id_invalid", "username_invalid",
                                      "username_not_occupied")):
                await status.edit_text(
                    "❌ <b>Story not found or no access.</b>\n\n"
                    "Possible reasons:\n"
                    "• The story has expired (stories last 24 hours)\n"
                    "• The account is private — the session isn't following them\n"
                    "• The username is wrong\n\n"
                    "If <b>you</b> follow this account, connect your own Telegram "
                    "account and try again using <code>/dlpriv [link]</code>.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔑 Login with My Account", callback_data="login_start"),
                    ]]),
                )
            else:
                await status.edit_text(f"❌ <b>Failed to fetch story:</b>\n<code>{e}</code>")
            schedule_delete(status, delay=300)
            return

        # ── 2. Download ──────────────────────────────────────────────────────
        await status.edit_text(
            f"⬇️ <b>Downloading story {'📷' if media_type == 'photo' else '🎬'}…</b>"
        )
        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
        ext             = "jpg" if media_type == "photo" else "mp4"
        tmp_path_target = os.path.join(Config.DOWNLOAD_DIR, f"story_{uid}_{story_id}.{ext}")
        dl_cb           = make_progress_callback(status, "⬇️ Downloading story")
        tmp_path  = None
        dl_errors = []

        # Strategy 1: FileId encoding → download_media(file_id_string)
        # Explicit int()/bytes() casts fix "required argument is not an integer"
        # (raw MTProto ints are TL wrappers, not plain Python ints)
        try:
            from pyrogram.file_id import FileId, FileType
            if media_type == "photo":
                p_sizes = [s for s in raw_media.sizes if hasattr(s, 'w')]
                best    = max(p_sizes, key=lambda s: s.w * s.h) if p_sizes else raw_media.sizes[-1]
                fid = FileId(
                    file_type=FileType.PHOTO,
                    dc_id=int(raw_media.dc_id),
                    media_id=int(raw_media.id),
                    access_hash=int(raw_media.access_hash),
                    file_reference=bytes(raw_media.file_reference),
                    thumbnail_size=str(best.type),
                )
            else:
                fid = FileId(
                    file_type=FileType.VIDEO,
                    dc_id=int(raw_media.dc_id),
                    media_id=int(raw_media.id),
                    access_hash=int(raw_media.access_hash),
                    file_reference=bytes(raw_media.file_reference),
                )
            tmp_path = await user.download_media(
                fid.encode(), file_name=tmp_path_target, progress=dl_cb
            )
        except Exception as e:
            dl_errors.append(f"S1({type(e).__name__}): {e}")

        # Strategy 2: high-level type._parse → gives proper file_id attribute
        if not tmp_path or not os.path.exists(tmp_path):
            try:
                if media_type == "photo":
                    from pyrogram.types import Photo as _PyroPhoto
                    hl = _PyroPhoto._parse(user, raw_media)
                    tmp_path = await user.download_media(
                        hl, file_name=tmp_path_target, progress=dl_cb
                    )
                else:
                    from pyrogram.types import Video as _PyroVideo, Document as _PyroDoc
                    try:
                        hl = _PyroVideo._parse(user, raw_media, None)
                    except Exception:
                        hl = _PyroDoc._parse(user, raw_media, None)
                    tmp_path = await user.download_media(
                        hl, file_name=tmp_path_target, progress=dl_cb
                    )
            except Exception as e:
                dl_errors.append(f"S2({type(e).__name__}): {e}")

        if not tmp_path or not os.path.exists(tmp_path):
            err_detail = "\n".join(dl_errors) if dl_errors else "no detail"
            await status.edit_text(
                f"❌ <b>Download failed.</b>\n\n<code>{err_detail}</code>"
            )
            schedule_delete(status)
            return

        file_size = os.path.getsize(tmp_path)
        if file_size > Config.MAX_FILE_SIZE:
            await status.edit_text(
                f"❌ <b>File too large:</b> {humanbytes(file_size)}\n"
                f"Maximum size is 4 GB."
            )
            schedule_delete(status)
            return

        # ── 3. Caption & thumbnail ───────────────────────────────────────────
        custom_cap = await redis_get_caption(uid)
        caption    = (custom_cap or
                      f"📖 Story by <b>@{username}</b>\n"
                      f"🆔 Story ID: <code>{story_id}</code>")[:1024]

        thumb_fid = await redis_get_thumb(uid)
        if thumb_fid and media_type == "video":
            try:
                thumb_path = await bot.download_media(
                    thumb_fid, file_name=Config.DOWNLOAD_DIR + "/thumb_story_"
                )
            except Exception:
                thumb_path = None

        # ── 4. Upload ────────────────────────────────────────────────────────
        # Detect exact document subtype for correct send method
        is_animated  = False
        is_video_doc = False
        if media_type == "video" and hasattr(raw_media, 'attributes'):
            try:
                from pyrogram.raw.types import (
                    DocumentAttributeAnimated,
                    DocumentAttributeVideo,
                )
                for attr in raw_media.attributes:
                    if isinstance(attr, DocumentAttributeAnimated):
                        is_animated = True
                    elif isinstance(attr, DocumentAttributeVideo):
                        is_video_doc = True
            except Exception:
                is_video_doc = True  # assume video if detection fails

        await status.edit_text(f"⬆️ <b>Uploading…</b>\n📦 {humanbytes(file_size)}")
        up_cb = make_progress_callback(status, "⬆️ Uploading story")

        try:
            if media_type == "photo":
                await bot.send_photo(
                    chat_id=uid,
                    photo=tmp_path,
                    caption=caption,
                    progress=up_cb,
                )
            elif is_animated and not is_video_doc:
                # Pure GIF / animation (no video attributes)
                await bot.send_animation(
                    chat_id=uid,
                    animation=tmp_path,
                    caption=caption,
                    progress=up_cb,
                )
            else:
                # Real video (including live photos which are mp4)
                try:
                    await bot.send_video(
                        chat_id=uid,
                        video=tmp_path,
                        caption=caption,
                        thumb=thumb_path,
                        supports_streaming=True,
                        progress=up_cb,
                    )
                except Exception:
                    await bot.send_document(
                        chat_id=uid,
                        document=tmp_path,
                        caption=caption,
                        thumb=thumb_path,
                        progress=up_cb,
                    )
        except FloodWait as fw:
            await status.edit_text(f"⏳ Rate limited. Waiting {fw.value}s…")
            await asyncio.sleep(fw.value + 1)
            if media_type == "photo":
                await bot.send_photo(chat_id=uid, photo=tmp_path, caption=caption)
            elif is_animated and not is_video_doc:
                await bot.send_animation(chat_id=uid, animation=tmp_path, caption=caption)
            else:
                await bot.send_document(chat_id=uid, document=tmp_path, caption=caption)

        # ── 5. Cleanup & stats ───────────────────────────────────────────────
        await redis_inc_downloads()
        await redis_inc_files()
        await log_to_channel(
            bot,
            f"📖 <b>Story saved</b>\n"
            f"User: {message.from_user.mention} (<code>{uid}</code>)\n"
            f"Source: @{username}/s/{story_id}\n"
            f"Type: {media_type} | Size: {humanbytes(file_size)}",
        )
        try:
            await status.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error("[STORY] Unexpected error for %d / @%s/s/%d: %s", uid, username, story_id, e)
        try:
            await status.edit_text(f"❌ <b>Unexpected error:</b>\n<code>{e}</code>")
            schedule_delete(status)
        except Exception:
            pass

    finally:
        for p in filter(None, [tmp_path, thumb_path]):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass


# ═══════════════════════════════════════════════════════════════════════════════
#  BATCH PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════

_batch_tasks: dict[int, asyncio.Task] = {}


async def _run_batch(uid: int, source_chat: Union[int, str],
                     start_id: int, end_id: int, status_msg: Message) -> None:
    try:
        await _run_batch_inner(uid, source_chat, start_id, end_id, status_msg)
    except Exception as e:
        logger.error("Batch crashed for user %d: %s", uid, e)
        await redis_clear_batch(uid)
        try:
            await status_msg.edit_text(f"❌ <b>Batch failed:</b>\n<code>{e}</code>")
        except Exception:
            await bot.send_message(uid, f"❌ <b>Batch failed:</b>\n<code>{e}</code>")


async def _run_batch_inner(uid: int, source_chat: Union[int, str],
                           start_id: int, end_id: int, status_msg: Message) -> None:
    total    = end_id - start_id + 1
    sent_ok  = 0
    failed   = 0
    skipped  = 0

    # Telegram rate limits:
    # • 30 messages/second to a single chat (we send to one user, so this matters)
    # • 20 media uploads/minute recommended for large files
    # We use a conservative 2-second delay between messages to stay safe.
    # For large batches (>100 msgs), we add an extra pause every 50 messages.
    DELAY_BETWEEN    = 2.0    # seconds between each message
    PAUSE_EVERY      = 50     # pause every N messages
    LONG_PAUSE       = 10.0   # seconds for the longer pause

    await status_msg.edit_text(
        f"🚀 <b>Batch started</b>\n"
        f"📋 Messages: <b>{total}</b>\n"
        f"⏳ Processing… (this may take a while)"
    )

    for idx, msg_id in enumerate(range(start_id, end_id + 1), start=1):

        # Check for cancellation on every iteration
        state = await redis_get_batch(uid)
        if not state or state.get("cancelled"):
            await status_msg.edit_text(
                f"🛑 <b>Batch cancelled</b>\n"
                f"✅ Sent: {sent_ok}  ❌ Failed: {failed}  ⏭ Skipped: {skipped}"
            )
            await redis_clear_batch(uid)
            return

        # Update progress every 5 messages or on the last one
        if idx % 5 == 0 or idx == total:
            try:
                await status_msg.edit_text(
                    f"⏳ <b>Batch in progress…</b>\n"
                    f"📋 {idx}/{total}  ✅ {sent_ok}  ❌ {failed}  ⏭ {skipped}"
                )
            except Exception:
                pass

        # Process the message with individual error isolation —
        # one bad message must NEVER crash the whole batch
        try:
            result = await process_single_message(
                uid, source_chat, msg_id, status_msg=None
            )
            if result:
                sent_ok += 1
            else:
                # None = deleted/empty/paid/inaccessible — not a crash, just skip
                skipped += 1
        except Exception as e:
            # Absolute safety net — log and continue regardless
            logger.error("Batch msg %d error (continuing): %s", msg_id, e)
            failed += 1

        # Rate limiting: respect Telegram's limits
        if idx % PAUSE_EVERY == 0 and idx < total:
            # Longer pause every 50 messages to avoid hitting global rate limits
            try:
                await status_msg.edit_text(
                    f"⏸ <b>Pausing briefly</b> (Telegram rate limit protection)\n"
                    f"📋 {idx}/{total}  ✅ {sent_ok}  ❌ {failed}  ⏭ {skipped}\n"
                    f"Resuming in {int(LONG_PAUSE)}s…"
                )
            except Exception:
                pass
            await asyncio.sleep(LONG_PAUSE)
        else:
            await asyncio.sleep(DELAY_BETWEEN)

    # Batch complete
    await redis_clear_batch(uid)
    summary = (
        f"✅ <b>Batch complete!</b>\n\n"
        f"📋 Total:   {total}\n"
        f"✅ Sent:    {sent_ok}\n"
        f"⏭ Skipped: {skipped}  <i>(deleted/empty/paid)</i>\n"
        f"❌ Failed:  {failed}"
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
        self.downloaded = 0; self.total = 0; self.speed = 0.0
        self.eta = 0.0; self.status = "starting"; self.filename = ""; self.error = ""

def _ydl_hook(state: _YtDlpProgress):
    def _hook(d: dict) -> None:
        state.status = d.get("status", "unknown")
        if state.status == "downloading":
            state.downloaded = d.get("downloaded_bytes", 0)
            state.total      = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            state.speed      = d.get("speed") or 0.0
            state.eta        = d.get("eta") or 0.0
        elif state.status == "finished":
            state.filename = d.get("filename", state.filename)
        elif state.status == "error":
            state.error = str(d.get("error", ""))
    return _hook

async def _poll_ytdlp(state: _YtDlpProgress, status_msg: Message, interval: float = 4.0):
    while state.status not in ("finished", "error"):
        await asyncio.sleep(interval)
        if state.total > 0:
            pct = state.downloaded * 100 / state.total
            bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
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
                if os.path.exists(mp4): filename = mp4
            state.filename = filename; state.status = "finished"
            return filename
    except Exception as e:
        state.status = "error"; state.error = str(e)
        return None

def _ydl_info_sync(url: str) -> Optional[dict]:
    try:
        with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
            return ydl.extract_info(url, download=False)
    except Exception:
        return None

async def _fetch_thumb_url(url: str, dest: str) -> Optional[str]:
    if not AIOHTTP_AVAILABLE:
        return None
    try:
        import aiohttp
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    with open(dest, "wb") as f: f.write(await r.read())
                    return dest
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  HELP TEXT
# ═══════════════════════════════════════════════════════════════════════════════

HELP_TEXT = """
<b>🚀 What can I do?</b>

<b>📥 Save Restricted Content</b>
Send any Telegram link — private or public — and I'll re-deliver the file straight to you, no restrictions, no fuss.
• <code>https://t.me/channelname/123</code>  — public channel
• <code>https://t.me/c/1234567890/123</code>  — private channel / group

<b>🎬 Download from Anywhere</b>
Paste a link from YouTube, Instagram, TikTok, Facebook, Twitter/X, Vimeo, Reddit, SoundCloud and more — I'll download and send it directly.

<b>📦 Batch Download</b>
/batch — grab a whole range of messages at once (up to {batch_max})

<b>🖼 Custom Thumbnail</b>
/setthumb  — reply to any photo to set it as your thumbnail
/delthumb  — remove it
/showthumb — preview what's set

<b>✏️ Custom Caption</b>
/setcaption [text] — attach your own caption to every file
/delcaption        — remove it

<b>⚡ Commands</b>
/start  /help  /batch  /cancel  /stats
/pfp @username — download all profile photos of any channel or group
""".format(batch_max=Config.BATCH_MAX)


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — /start  /help  caption
# ═══════════════════════════════════════════════════════════════════════════════

def _start_kb(is_owner: bool = False, has_session: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📖 Help",  callback_data="cb_help"),
         InlineKeyboardButton("📊 Stats", callback_data="cb_stats")],
        [InlineKeyboardButton("➕ Add to Group",
                              url=f"https://t.me/{Config.BOT_USERNAME}?startgroup=true")],
        [InlineKeyboardButton(
            "🔓 My Session (Logout)" if has_session else "🔑 Login (Private Channels)",
            callback_data="login_logout_confirm" if has_session else "login_start",
        )],
    ]
    if is_owner:
        rows.append([InlineKeyboardButton(
            "🔁 Group Repeat (Keep-Alive)", callback_data="gr_menu"
        )])
    return InlineKeyboardMarkup(rows)


@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(_: Client, message: Message) -> None:
    uid    = message.from_user.id
    is_new = await redis_register_user(uid)
    if not await check_force_sub(bot, uid):
        return await send_force_sub_msg(bot, message)
    try:
        await message.delete()
    except Exception:
        pass
    has_session = bool(await redis_get_user_session(uid))
    # send_menu deletes any previous bot menu message before sending this one
    await send_menu(
        uid,
        f"👋 <b>Hey {message.from_user.mention}! Welcome!</b>\n\n"
        "I'm your all-in-one media bot. Here's what I do:\n\n"
        "📥 <b>Save restricted Telegram content</b> — send any t.me link\n"
        "🎬 <b>Download from YouTube, TikTok, Instagram, Twitter/X, Facebook</b> and more — just paste the URL\n"
        "📦 <b>Batch download</b> entire message ranges at once\n\n"
        "Just send me a link and I'll handle the rest. 🚀",
        reply_markup=_start_kb(is_owner=(uid == Config.OWNER_ID), has_session=has_session),
    )
    if is_new:
        await log_to_channel(bot, f"👤 New user: {message.from_user.mention} (<code>{uid}</code>)")


@bot.on_message(filters.command("help") & filters.private)
async def cmd_help(_: Client, message: Message) -> None:
    uid = message.from_user.id
    if not await check_force_sub(bot, uid):
        return await send_force_sub_msg(bot, message)
    try:
        await message.delete()
    except Exception:
        pass
    await send_menu(
        uid,
        HELP_TEXT,
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back", callback_data="cb_start")
        ]]),
    )


@bot.on_message(filters.command("setcaption") & filters.private)
async def cmd_setcaption(_: Client, message: Message) -> None:
    uid   = message.from_user.id
    parts = message.text.split(None, 1)
    try:
        await message.delete()
    except Exception:
        pass
    if len(parts) < 2:
        await send_menu(
            uid,
            "✏️ <b>Set Custom Caption</b>\n\nUsage: <code>/setcaption Your caption here</code>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="cb_start")
            ]]),
        )
        return
    await redis_set_caption(uid, parts[1].strip())
    await send_menu(
        uid,
        f"✅ <b>Caption saved!</b>\n<code>{parts[1].strip()}</code>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🗑 Remove Caption", callback_data="cb_delcaption"),
            InlineKeyboardButton("🔙 Back",           callback_data="cb_start"),
        ]]),
    )


@bot.on_message(filters.command("delcaption") & filters.private)
async def cmd_delcaption(_: Client, message: Message) -> None:
    await redis_del_caption(message.from_user.id)
    try:
        await message.delete()
    except Exception:
        pass
    await send_menu(
        message.from_user.id,
        "🗑️ <b>Custom caption removed.</b>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back", callback_data="cb_start")
        ]]),
    )


@bot.on_message(filters.command("showcaption") & filters.private)
async def cmd_showcaption(_: Client, message: Message) -> None:
    cap = await redis_get_caption(message.from_user.id)
    try:
        await message.delete()
    except Exception:
        pass
    await send_menu(
        message.from_user.id,
        f"📝 <b>Your caption:</b>\n<code>{cap}</code>" if cap else "ℹ️ No custom caption set.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🗑 Remove", callback_data="cb_delcaption"),
            InlineKeyboardButton("🔙 Back",   callback_data="cb_start"),
        ]]),
    )


@bot.on_callback_query(filters.regex(r"^cb_delcaption$"))
async def cb_delcaption(_: Client, query: CallbackQuery) -> None:
    await redis_del_caption(query.from_user.id)
    await query.answer("🗑️ Caption removed.")
    await edit_menu(
        query.message,
        "🗑️ <b>Custom caption removed.</b>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back", callback_data="cb_start")
        ]]),
    )


@bot.on_callback_query(filters.regex(r"^cb_help$"))
async def cb_help(_: Client, query: CallbackQuery) -> None:
    await edit_menu(
        query.message,
        HELP_TEXT,
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back", callback_data="cb_start")
        ]]),
    )


@bot.on_callback_query(filters.regex(r"^cb_start$"))
async def cb_start_back(_: Client, query: CallbackQuery) -> None:
    has_session = bool(await redis_get_user_session(query.from_user.id))
    await edit_menu(
        query.message,
        f"👋 <b>Hey {query.from_user.mention}!</b>\n\n"
        "📥 Send a <b>Telegram link</b> to save restricted content\n"
        "🎬 Or paste a <b>YouTube / TikTok / Instagram</b> URL to download it\n\n"
        "Just drop the link — I'll do the rest. 🚀",
        reply_markup=_start_kb(is_owner=(query.from_user.id == Config.OWNER_ID), has_session=has_session),
    )
    await set_last_bot_msg(query.from_user.id, query.message.id)


@bot.on_callback_query(filters.regex(r"^cb_stats$"))
async def cb_stats_btn(_: Client, query: CallbackQuery) -> None:
    s = await redis_get_stats()
    await edit_menu(
        query.message,
        f"📊 <b>Bot Statistics</b>\n\n"
        f"👥 Users: <b>{s['users']:,}</b>\n"
        f"📥 Downloads: <b>{s['downloads']:,}</b>\n"
        f"📁 Files: <b>{s['files']:,}</b>\n"
        f"⭐ Premium: <b>{s['premium']:,}</b>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back", callback_data="cb_start")
        ]]),
    )


@bot.on_callback_query(filters.regex(r"^check_sub$"))
async def cb_check_sub(_: Client, query: CallbackQuery) -> None:
    if await check_force_sub(bot, query.from_user.id):
        await edit_menu(
            query.message,
            "✅ <b>Verified!</b> Send me a link or video URL.",
            reply_markup=_start_kb(is_owner=(query.from_user.id == Config.OWNER_ID)),
        )
    else:
        await query.answer("❌ You haven't joined yet!", show_alert=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — THUMBNAIL
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(filters.command("setthumb") & filters.private)
async def cmd_setthumb(_: Client, message: Message) -> None:
    uid    = message.from_user.id
    target = message.reply_to_message or message
    try:
        await message.delete()
    except Exception:
        pass
    if not target.photo:
        await send_menu(
            uid,
            "📸 <b>Set Custom Thumbnail</b>\n\nReply to a <b>photo</b> with <code>/setthumb</code>.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="cb_start")
            ]]),
        )
        return
    await redis_set_thumb(uid, target.photo.file_id)
    await send_menu(
        uid,
        "✅ <b>Custom thumbnail saved!</b>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👁 Preview",  callback_data="cb_showthumb"),
            InlineKeyboardButton("🗑 Remove",   callback_data="cb_delthumb"),
            InlineKeyboardButton("🔙 Back",     callback_data="cb_start"),
        ]]),
    )


@bot.on_message(filters.command("delthumb") & filters.private)
async def cmd_delthumb(_: Client, message: Message) -> None:
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    if not await redis_get_thumb(uid):
        await send_menu(
            uid,
            "ℹ️ You have no custom thumbnail set.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="cb_start")
            ]]),
        )
        return
    await redis_del_thumb(uid)
    await send_menu(
        uid,
        "🗑️ <b>Thumbnail removed.</b>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back", callback_data="cb_start")
        ]]),
    )


@bot.on_message(filters.command("showthumb") & filters.private)
async def cmd_showthumb(_: Client, message: Message) -> None:
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    fid = await redis_get_thumb(uid)
    if not fid:
        await send_menu(
            uid,
            "ℹ️ No thumbnail set. Use /setthumb (reply to a photo).",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="cb_start")
            ]]),
        )
        return
    try:
        await delete_prev_bot_msg(uid)
        sent = await bot.send_photo(uid, fid, caption="🖼️ <b>Your current thumbnail</b>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑 Remove", callback_data="cb_delthumb"),
                InlineKeyboardButton("🔙 Back",   callback_data="cb_start"),
            ]]),
        )
        await set_last_bot_msg(uid, sent.id)
    except Exception:
        await redis_del_thumb(uid)
        await send_menu(
            uid,
            "❌ Thumbnail stale — please set a new one with /setthumb.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="cb_start")
            ]]),
        )


@bot.on_callback_query(filters.regex(r"^cb_showthumb$"))
async def cb_showthumb(_: Client, query: CallbackQuery) -> None:
    uid = query.from_user.id
    fid = await redis_get_thumb(uid)
    if not fid:
        await query.answer("ℹ️ No thumbnail set.", show_alert=True)
        return
    try:
        await query.message.delete()
        sent = await bot.send_photo(uid, fid, caption="🖼️ <b>Your current thumbnail</b>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑 Remove", callback_data="cb_delthumb"),
                InlineKeyboardButton("🔙 Back",   callback_data="cb_start"),
            ]]),
        )
        await set_last_bot_msg(uid, sent.id)
    except Exception:
        await redis_del_thumb(uid)
        await query.answer("❌ Thumbnail stale. Set a new one.", show_alert=True)


@bot.on_callback_query(filters.regex(r"^cb_delthumb$"))
async def cb_delthumb(_: Client, query: CallbackQuery) -> None:
    await redis_del_thumb(query.from_user.id)
    await query.answer("🗑️ Thumbnail removed.")
    await edit_menu(
        query.message,
        "🗑️ <b>Thumbnail removed.</b>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back", callback_data="cb_start")
        ]]),
    )



# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — /pfp  (Profile Photo Downloader)
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_pfp_target(text: str) -> Optional[str]:
    """Extract a username, numeric ID, or normalised invite link from a /pfp argument."""
    text = text.strip()
    # Preserve invite links (t.me/+HASH) — keep them intact for resolution
    if "/+" in text or text.startswith("+"):
        if "t.me/+" in text:
            idx = text.index("t.me/+")
            return "https://" + text[idx:]
        if text.startswith("+"):
            return "https://t.me/" + text
        return text
    # Regular username or public link
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    text = text.lstrip("@").split("/")[0]
    return text if text else None


@bot.on_message(filters.command("pfp") & filters.private)
async def cmd_pfp(_: Client, message: Message) -> None:
    uid = message.from_user.id
    if not await check_force_sub(bot, uid):
        return await send_force_sub_msg(bot, message)

    args = message.text.split(None, 1)
    if len(args) < 2:
        reply = await message.reply(
            "⚠️ <b>Usage:</b> <code>/pfp @username</code> or <code>/pfp t.me/+invitelink</code>\n"
            "Works for any public channel, group, or private group (via invite link).\n\n"
            "<i>Examples:</i>\n"
            "<code>/pfp @durov</code>\n"
            "<code>/pfp https://t.me/+AbCdEfGh</code>"
        )
        schedule_delete(reply, delay=30)
        return

    target = _parse_pfp_target(args[1])
    if not target:
        reply = await message.reply("⚠️ Invalid username or link.")
        schedule_delete(reply, delay=20)
        return

    is_invite = target.startswith("https://t.me/+")
    display   = target.split("t.me/")[1] if is_invite else target
    status    = await message.reply("🔍 <b>Fetching profile photos…</b>")
    tmp_paths: list[str] = []

    try:
        # ── Resolve invite link → numeric chat ID ────────────────────────────
        chat_id: Union[str, int] = target
        if is_invite:
            try:
                chat    = await user.get_chat(target)
                chat_id = chat.id
                display = chat.title or display
            except Exception as e:
                await status.edit_text(
                    f"❌ <b>Could not resolve invite link.</b>\n\n"
                    f"The session account is not a member of this group.\n\n"
                    f"If <b>you</b> are a member, connect your own account "
                    f"and retry with <code>/dlpriv [link]</code>.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔑 Login with My Account", callback_data="login_start"),
                    ]]),
                )
                schedule_delete(status, delay=300)
                return

        # ── Collect up to 20 photos via userbot ─────────────────────────────
        photos = []
        try:
            async for photo in user.get_chat_photos(chat_id, limit=20):
                photos.append(photo)
        except Exception as e:
            err = str(e).lower()
            if any(k in err for k in ("username", "peer", "not found",
                                      "invalid", "private")):
                await status.edit_text(
                    f"❌ <b>No access to:</b> <code>{display}</code>\n\n"
                    f"This is either a private group/channel the session isn't "
                    f"a member of, or the username doesn't exist.\n\n"
                    f"If <b>you</b> are a member, connect your own account "
                    f"and retry with <code>/dlpriv [link]</code>.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔑 Login with My Account", callback_data="login_start"),
                    ]]),
                )
            else:
                await status.edit_text(
                    f"❌ <b>Failed to fetch photos:</b>\n<code>{e}</code>"
                )
            schedule_delete(status, delay=300)
            return

        if not photos:
            await status.edit_text(f"ℹ️ <b>{display}</b> has no profile photos.")
            schedule_delete(status)
            return

        total = len(photos)
        await status.edit_text(f"⬇️ <b>Downloading {total} profile photo(s)…</b>")

        # ── Download each photo ──────────────────────────────────────────────
        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
        for i, photo in enumerate(photos):
            tmp = os.path.join(Config.DOWNLOAD_DIR, f"pfp_{uid}_{i}.jpg")
            try:
                dl = await user.download_media(photo, file_name=tmp)
                if dl and os.path.exists(dl):
                    tmp_paths.append(dl)
            except Exception as e:
                logger.error("[PFP] Photo %d download failed: %s", i, e)

        if not tmp_paths:
            await status.edit_text("❌ <b>Failed to download any photos.</b>")
            schedule_delete(status)
            return

        await status.edit_text(f"⬆️ <b>Sending {len(tmp_paths)} photo(s)…</b>")

        # ── Send — single photo or album (max 10 per group) ──────────────────
        from pyrogram.types import InputMediaPhoto as _IMP
        if len(tmp_paths) == 1:
            await bot.send_photo(
                uid,
                photo=tmp_paths[0],
                caption=f"📷 <b>{display}</b> — profile photo",
            )
        else:
            for batch_start in range(0, len(tmp_paths), 10):
                batch = tmp_paths[batch_start:batch_start + 10]
                media_group = []
                for j, p in enumerate(batch):
                    cap = (
                        f"📷 <b>{display}</b> — {batch_start + j + 1}/{len(tmp_paths)}"
                        if j == 0 else ""
                    )
                    media_group.append(_IMP(p, caption=cap))
                await bot.send_media_group(uid, media=media_group)

        await redis_inc_downloads()
        await log_to_channel(
            bot,
            f"📷 <b>Profile photos fetched</b>\n"
            f"User: {message.from_user.mention} (<code>{uid}</code>)\n"
            f"Target: {display} | Photos: {len(tmp_paths)}",
        )
        try:
            await status.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error("[PFP] Unexpected error for %s: %s", display, e)
        try:
            await status.edit_text(f"❌ <b>Unexpected error:</b>\n<code>{e}</code>")
            schedule_delete(status)
        except Exception:
            pass

    finally:
        for p in tmp_paths:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass


# ═══════════════════════════════════════════════════════════════════════════════
#  FEATURE 1: /dlbot — Download from your own private chat with any bot
#  Uses the existing bot owner's userbot session (no login required).
#  Works even when that bot has saving/forwarding fully disabled.
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(filters.command("dlbot") & filters.private)
async def cmd_dlbot(_: Client, message: Message) -> None:
    uid = message.from_user.id
    if not await check_force_sub(bot, uid):
        return await send_force_sub_msg(bot, message)

    args = message.text.split(None, 2)
    if len(args) < 2:
        reply = await message.reply(
            "📥 <b>Download from Bot Chat</b>\n\n"
            "<b>Usage:</b> <code>/dlbot @botusername [count]</code>\n\n"
            "<b>Examples:</b>\n"
            "• <code>/dlbot @somebot</code> — last 5 media files\n"
            "• <code>/dlbot @somebot 10</code> — last 10 files\n"
            "• <code>/dlbot @somebot 1</code> — just the latest file\n\n"
            "📌 <i>Downloads media from the saved session's private chat with "
            "any bot, even if that bot has saving and forwarding disabled.\n"
            "Max 50 files per command.</i>"
        )
        schedule_delete(reply, delay=45)
        return

    target_raw = args[1].lstrip("@").strip()
    try:
        count = int(args[2]) if len(args) > 2 else 5
        count = max(1, min(count, 50))
    except (ValueError, IndexError):
        count = 5

    status = await message.reply(
        f"🔍 <b>Searching your chat with @{target_raw}…</b>\n"
        f"Looking for up to {count} media file(s)."
    )

    try:
        # ── 1. Collect media messages ────────────────────────────────────────
        media_msgs: list[Message] = []
        scan_limit = min(count * 8, 200)  # overscan to find media-only messages

        try:
            async for msg in user.get_chat_history(target_raw, limit=scan_limit):
                if get_media_type(msg):
                    media_msgs.append(msg)
                if len(media_msgs) >= count:
                    break
        except FloodWait as fw:
            await status.edit_text(
                f"⏳ Telegram rate limit. Waiting {fw.value}s then continuing…"
            )
            await asyncio.sleep(fw.value + 1)
            async for msg in user.get_chat_history(target_raw, limit=scan_limit):
                if get_media_type(msg):
                    media_msgs.append(msg)
                if len(media_msgs) >= count:
                    break
        except Exception as e:
            err = str(e).lower()
            if any(k in err for k in ("peer", "not found", "username", "invalid")):
                await status.edit_text(
                    f"❌ <b>@{target_raw} not found.</b>\n\n"
                    "Make sure:\n"
                    "• The username is correct (no typos)\n"
                    "• The saved session has previously started a chat with that bot"
                )
            elif any(k in err for k in ("auth", "session", "unauthorized")):
                await status.edit_text(
                    "❌ <b>Session error.</b>\n"
                    "The bot's user session may have expired. Contact the bot owner."
                )
            else:
                await status.edit_text(
                    f"❌ <b>Could not access chat with @{target_raw}:</b>\n"
                    f"<code>{e}</code>"
                )
            schedule_delete(status)
            return

        if not media_msgs:
            await status.edit_text(
                f"ℹ️ <b>No media found</b> in chat with @{target_raw}.\n\n"
                "Possible reasons:\n"
                "• The bot hasn't sent any media yet\n"
                "• All recent messages are text-only\n"
                f"• Try increasing the count: <code>/dlbot @{target_raw} 50</code>"
            )
            schedule_delete(status)
            return

        total = len(media_msgs)
        await status.edit_text(
            f"✅ <b>Found {total} file(s).</b> Starting download…"
        )

        # ── 2. Download + upload each file ───────────────────────────────────
        sent_ok = 0
        failed  = 0

        for i, src_msg in enumerate(media_msgs, 1):
            file_path:  Optional[str] = None
            thumb_path: Optional[str] = None
            try:
                media_type = get_media_type(src_msg)
                try:
                    await status.edit_text(
                        f"📥 <b>Downloading {i}/{total}…</b>  (type: {media_type})"
                    )
                except Exception:
                    pass

                # Download
                try:
                    file_path = await user.download_media(
                        src_msg, file_name=Config.DOWNLOAD_DIR + "/"
                    )
                except FloodWait as fw:
                    await status.edit_text(
                        f"⏳ Download rate limited. Waiting {fw.value}s…"
                    )
                    await asyncio.sleep(fw.value + 1)
                    file_path = await user.download_media(
                        src_msg, file_name=Config.DOWNLOAD_DIR + "/"
                    )

                if not file_path or not os.path.exists(file_path):
                    failed += 1
                    continue

                file_size = os.path.getsize(file_path)
                if file_size > Config.MAX_FILE_SIZE:
                    os.remove(file_path)
                    file_path = None
                    logger.warning("[DLBOT] File %d too large (%s), skipping", i, humanbytes(file_size))
                    failed += 1
                    continue

                # Thumbnail (only for video/audio/document)
                thumb_fid = await redis_get_thumb(uid)
                if thumb_fid and media_type in ("video", "audio", "document", "animation"):
                    try:
                        thumb_path = await bot.download_media(
                            thumb_fid, file_name=Config.DOWNLOAD_DIR + "/thumb_dlbot_"
                        )
                    except Exception:
                        thumb_path = None

                # Caption
                custom_cap = await redis_get_caption(uid)
                caption    = (custom_cap or src_msg.caption or "")[:1024]

                try:
                    await status.edit_text(
                        f"⬆️ <b>Uploading {i}/{total}…</b>  📦 {humanbytes(file_size)}"
                    )
                except Exception:
                    pass

                kw = dict(chat_id=uid, caption=caption)
                try:
                    if media_type == "video":
                        v = src_msg.video
                        await bot.send_video(
                            video=file_path, thumb=thumb_path,
                            duration=v.duration if v else 0,
                            width=v.width if v else 0,
                            height=v.height if v else 0,
                            supports_streaming=True, **kw,
                        )
                    elif media_type == "audio":
                        a = src_msg.audio
                        await bot.send_audio(
                            audio=file_path, thumb=thumb_path,
                            duration=a.duration if a else 0,
                            title=a.title if a else None,
                            performer=a.performer if a else None, **kw,
                        )
                    elif media_type == "photo":
                        await bot.send_photo(photo=file_path, **kw)
                    elif media_type == "document":
                        await bot.send_document(document=file_path, thumb=thumb_path, **kw)
                    elif media_type == "animation":
                        await bot.send_animation(animation=file_path, thumb=thumb_path, **kw)
                    elif media_type == "voice":
                        vn = src_msg.voice
                        await bot.send_voice(
                            voice=file_path,
                            duration=vn.duration if vn else 0, **kw,
                        )
                    elif media_type == "video_note":
                        vn = src_msg.video_note
                        await bot.send_video_note(
                            chat_id=uid, video_note=file_path, thumb=thumb_path,
                            duration=vn.duration if vn else 0,
                            length=vn.length if vn else 1,
                        )
                    elif media_type == "sticker":
                        await bot.send_sticker(chat_id=uid, sticker=file_path)
                    else:
                        await bot.send_document(document=file_path, thumb=thumb_path, **kw)

                    sent_ok += 1
                    await redis_inc_downloads()
                    await redis_inc_files()

                except FloodWait as fw:
                    await status.edit_text(
                        f"⏳ Upload rate limited. Waiting {fw.value}s…"
                    )
                    await asyncio.sleep(fw.value + 2)
                    try:
                        await bot.send_document(chat_id=uid, document=file_path, caption=caption)
                        sent_ok += 1
                        await redis_inc_downloads()
                        await redis_inc_files()
                    except Exception as e2:
                        logger.error("[DLBOT] Fallback upload failed uid %d: %s", uid, e2)
                        failed += 1
                except Exception as e:
                    logger.error("[DLBOT] Upload error uid %d file %d: %s", uid, i, e)
                    failed += 1

            except Exception as e:
                logger.error("[DLBOT] Unexpected error file %d/%d uid %d: %s", i, total, uid, e)
                failed += 1
            finally:
                for p in filter(None, [file_path, thumb_path]):
                    try:
                        if p and os.path.exists(p):
                            os.remove(p)
                    except OSError:
                        pass

            # Modest delay between uploads to respect Telegram rate limits
            if i < total:
                await asyncio.sleep(1.5)

        # ── 3. Summary ───────────────────────────────────────────────────────
        summary = (
            f"{'✅' if sent_ok > 0 else 'ℹ️'} <b>Download complete!</b>\n\n"
            f"📥 Source: @{target_raw}\n"
            f"✅ Delivered: {sent_ok}/{total}"
            + (f"\n❌ Failed/skipped: {failed}" if failed else "")
        )
        try:
            await status.edit_text(summary)
        except Exception:
            await bot.send_message(uid, summary)
        schedule_delete(status, delay=60)

        await log_to_channel(
            bot,
            f"📥 <b>/dlbot</b>\n"
            f"User: {message.from_user.mention} (<code>{uid}</code>)\n"
            f"Source: @{target_raw} | Delivered: {sent_ok}/{total}",
        )

    except Exception as e:
        logger.error("[DLBOT] Top-level error uid %d: %s", uid, e)
        try:
            await status.edit_text(f"❌ <b>Unexpected error:</b>\n<code>{e}</code>")
        except Exception:
            pass
        schedule_delete(status)


# ═══════════════════════════════════════════════════════════════════════════════
#  FEATURE 2: /login, /logout, /dlpriv — User-session login & private download
#
#  Architecture:
#  • Per-user Pyrogram Client created on-demand from session string in Redis
#  • Login flow: phone → OTP → (2FA if enabled) → session saved to Redis
#  • _login_clients: in-memory dict of active login-flow clients (temp only)
#  • Owner receives activity alerts (NO credentials, NO session string)
# ═══════════════════════════════════════════════════════════════════════════════

# Temporary Pyrogram clients created during the login OTP flow only.
# Keyed by user_id. Cleared immediately after login succeeds or fails.
_login_clients: dict[int, Client] = {}


def _mask_phone(phone: str) -> str:
    """Mask phone for owner alerts: +234****789 — never logs full number."""
    phone = str(phone).strip()
    if len(phone) <= 6:
        return phone[:3] + "***"
    return phone[:4] + "****" + phone[-3:]


async def _alert_owner_login(text: str) -> None:
    """Send a login-event alert to owner only. Never raises."""
    try:
        await bot.send_message(Config.OWNER_ID, text, disable_web_page_preview=True)
    except Exception as e:
        logger.error("[ALERT] Owner alert failed: %s", e)


async def _get_fresh_user_client(uid: int) -> Optional[Client]:
    """
    Create and start a per-user Pyrogram Client from the session stored in Redis.
    Returns None if no session is saved, or if the session is invalid/expired
    (in which case the stored session is auto-deleted from Redis).
    Caller MUST call client.stop() when done.
    """
    try:
        session = await redis_get_user_session(uid)
    except Exception as e:
        logger.error("[USER_CLIENT] Redis error fetching session for %d: %s", uid, e)
        return None

    if not session:
        return None

    client = Client(
        name=f"dlpriv_{uid}",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        session_string=session,
        in_memory=True,
        no_updates=True,
    )
    try:
        await asyncio.wait_for(client.start(), timeout=30.0)
        return client
    except asyncio.TimeoutError:
        logger.error("[USER_CLIENT] Timeout starting client for uid %d", uid)
        try: await client.stop()
        except Exception: pass
        return None
    except Exception as e:
        err = str(e).lower()
        logger.error("[USER_CLIENT] Failed to start client for uid %d: %s", uid, e)
        # Auto-remove if session is clearly expired/revoked/banned
        if any(k in err for k in ("auth_key", "session", "unauthorized",
                                   "deactivated", "revoked", "banned",
                                   "user_deactivated")):
            try:
                await redis_del_user_session(uid)
            except Exception:
                pass
        try: await client.stop()
        except Exception: pass
        return None


async def _dlbot_cleanup_login_client(uid: int) -> None:
    """Disconnect and remove the login-flow client for a user; clear login state."""
    client = _login_clients.pop(uid, None)
    if client:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=5.0)
        except Exception:
            pass
    try:
        await redis_clear_login_state(uid)
    except Exception:
        pass


# ─── /login command ───────────────────────────────────────────────────────────

@bot.on_message(filters.command("login") & filters.private)
async def cmd_login(_: Client, message: Message) -> None:
    uid = message.from_user.id
    if not await check_force_sub(bot, uid):
        return await send_force_sub_msg(bot, message)
    try:
        await message.delete()
    except Exception:
        pass

    # If already logged in, show status
    try:
        existing = await redis_get_user_session(uid)
    except Exception:
        existing = None

    if existing:
        await send_menu(
            uid,
            "✅ <b>You're already logged in!</b>\n\n"
            "Your Telegram session is active and ready.\n\n"
            "<b>Available commands:</b>\n"
            "• <code>/dlpriv [link]</code> — download from your private channels\n"
            "• <code>/logout</code> — remove your session\n\n"
            "<i>If downloads aren't working, try /logout then /login to refresh.</i>",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚪 Logout", callback_data="login_logout_confirm")],
                [InlineKeyboardButton("🔙 Back",   callback_data="cb_start")],
            ]),
        )
        return

    # Clear any stale login state or orphaned client from a previous attempt
    try:
        await redis_clear_login_state(uid)
    except Exception:
        pass
    old_client = _login_clients.pop(uid, None)
    if old_client:
        try: await old_client.disconnect()
        except Exception: pass

    # Set initial state
    try:
        await redis_set_login_state(uid, {"step": "awaiting_phone"})
    except Exception as e:
        logger.error("[LOGIN] Redis error setting initial state for %d: %s", uid, e)
        await send_menu(uid, "❌ <b>Service error.</b> Please try again in a moment.")
        return

    from pyrogram.types import (
        ReplyKeyboardMarkup as _RKM,
        KeyboardButton as _KB,
    )

    kb_msg = await send_menu(
        uid,
        "🔐 <b>Login with Your Telegram Account</b>\n\n"
        "To download content from <b>private channels and groups you are a member "
        "of</b> — even when saving and forwarding are disabled — I need to access "
        "Telegram through your account for the duration of each download.\n\n"
        "<b>How it works:</b>\n"
        "① You provide your phone number\n"
        "② Telegram sends a verification code to your Telegram app\n"
        "③ You enter the code here\n"
        "④ Your session is saved — you only do this once\n\n"
        "<b>Privacy & Safety:</b>\n"
        "• Your session is used <i>only</i> when <i>you</i> request a download\n"
        "• We never read your private chats, contacts, or messages\n"
        "• You can remove your session at any time with /logout\n\n"
        "──────────────────────────────\n"
        "📱 <b>Step 1 — Enter your phone number</b>\n\n"
        "Tap <b>📱 Share My Number</b> for automatic entry, or type it manually "
        "in international format with your country code:\n\n"
        "<code>+447911123456</code>  (UK +44)\n"
        "<code>+12025551234</code>   (US +1)\n\n"
        "Use the same number your Telegram account is registered with.\n\n"
        "Send <code>cancel</code> at any time to abort.",
        reply_markup=_RKM(
            keyboard=[[_KB("📱 Share My Number", request_contact=True)]],
            one_time_keyboard=True,
            resize_keyboard=True,
        ),
    )
    # Update state with keyboard message ID so _login_handle_phone can delete it
    if kb_msg:
        try:
            await redis_set_login_state(uid, {
                "step": "awaiting_phone",
                "kb_msg_id": kb_msg.id,
            })
        except Exception:
            pass


# ─── /logout command ──────────────────────────────────────────────────────────

@bot.on_message(filters.command("logout") & filters.private)
async def cmd_logout(_: Client, message: Message) -> None:
    uid = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass

    try:
        existing = await redis_get_user_session(uid)
    except Exception:
        existing = None

    if not existing:
        await send_menu(
            uid,
            "ℹ️ <b>No active session.</b>\n\n"
            "You haven't logged in yet.\n"
            "Use /login to connect your account.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔑 Login Now", callback_data="login_start")],
                [InlineKeyboardButton("🔙 Back",       callback_data="cb_start")],
            ]),
        )
        return

    await send_menu(
        uid,
        "🚪 <b>Logout Confirmation</b>\n\n"
        "This will permanently remove your saved Telegram session from our system.\n\n"
        "You'll need to go through the login flow again to use <code>/dlpriv</code>.\n\n"
        "Are you sure?",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Logout",  callback_data="login_logout_confirm"),
                InlineKeyboardButton("❌ Cancel",        callback_data="cb_start"),
            ],
        ]),
    )


@bot.on_callback_query(filters.regex(r"^login_logout_confirm$"))
async def cb_logout_confirm(_: Client, query: CallbackQuery) -> None:
    uid = query.from_user.id
    await query.answer("🚪 Removing your session…")

    try:
        await redis_del_user_session(uid)
    except Exception as e:
        logger.error("[LOGOUT] Redis error for %d: %s", uid, e)

    try:
        await redis_clear_login_state(uid)
    except Exception:
        pass

    # Clean up any lingering login-flow client
    old_client = _login_clients.pop(uid, None)
    if old_client:
        try: await old_client.disconnect()
        except Exception: pass

    await edit_menu(
        query.message,
        "✅ <b>Session removed successfully.</b>\n\n"
        "Your Telegram session has been permanently deleted from our system.\n"
        "Use /login to connect again at any time.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Back", callback_data="cb_start")
        ]]),
    )

    # Owner alert — no credentials
    u   = query.from_user
    ts  = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    await _alert_owner_login(
        f"🚪 <b>Session Terminated</b>\n\n"
        f"User: {u.mention} (<code>{uid}</code>)\n"
        f"Username: {'@' + u.username if u.username else 'none'}\n"
        f"Name: {u.first_name}{' ' + u.last_name if u.last_name else ''}\n"
        f"Action: User requested logout\n"
        f"Time: {ts}"
    )


@bot.on_callback_query(filters.regex(r"^login_start$"))
async def cb_login_start(_: Client, query: CallbackQuery) -> None:
    """Redirect an inline button tap into the /login flow."""
    await query.answer()
    uid = query.from_user.id

    try:
        existing = await redis_get_user_session(uid)
    except Exception:
        existing = None

    if existing:
        await edit_menu(
            query.message,
            "✅ <b>Already logged in!</b>\n\n"
            "Use <code>/dlpriv [link]</code> to download from your private channels.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="cb_start")
            ]]),
        )
        return

    try:
        await redis_clear_login_state(uid)
        await redis_set_login_state(uid, {"step": "awaiting_phone"})
    except Exception:
        pass

    from pyrogram.types import (
        ReplyKeyboardMarkup as _RKM,
        KeyboardButton as _KB,
    )

    try:
        await query.answer()
    except Exception:
        pass

    # Use send_menu (fresh message) instead of edit_menu so no pending
    # schedule_delete timer from the error message carries over and kills this.
    await send_menu(
        uid,
        "📱 <b>Step 1 — Enter Your Phone Number</b>\n\n"
        "Tap the button below to share your number automatically,\n"
        "or type it manually in international format.\n\n"
        "<b>Examples:</b>\n"
        "<code>+447911123456</code>  (UK +44)\n"
        "<code>+12025551234</code>   (US +1)\n\n"
        "Use the same number your Telegram account is registered with.\n"
        "Send <code>cancel</code> to abort.",
    )
    kb_msg = await bot.send_message(
        uid,
        "👇 Tap the button or type your phone number:",
        reply_markup=_RKM(
            keyboard=[[_KB("📱 Share My Number", request_contact=True)]],
            one_time_keyboard=True,
            resize_keyboard=True,
        ),
    )
    # Store the keyboard message ID in Redis so _login_handle_phone can delete it
    try:
        await redis_set_login_state(uid, {
            "step": "awaiting_phone",
            "kb_msg_id": kb_msg.id,
        })
    except Exception:
        pass


# ─── Login flow interceptor (group=-2, highest priority) ─────────────────────
# Intercepts text and contact messages when a user is in the login flow.
# Raises StopPropagation so no other handler sees the message.

@bot.on_message(filters.private & (filters.text | filters.contact), group=-2)
async def login_flow_interceptor(_: Client, message: Message) -> None:
    if not message.from_user:
        return

    uid = message.from_user.id
    try:
        state = await redis_get_login_state(uid)
    except Exception:
        return  # Redis error — let other handlers proceed

    if not state:
        return  # Not in login flow — pass through to other handlers

    step = state.get("step")

    if step == "awaiting_phone":
        await _login_handle_phone(message, uid, state)
        raise StopPropagation

    elif step == "awaiting_otp" and message.text:
        await _login_handle_otp(message, uid, state)
        raise StopPropagation

    elif step == "awaiting_2fa" and message.text:
        await _login_handle_2fa(message, uid, state)
        raise StopPropagation


# ─── Login step: phone ────────────────────────────────────────────────────────

async def _login_handle_phone(message: Message, uid: int, state: dict) -> None:
    from pyrogram.types import ReplyKeyboardRemove as _RKR

    # Delete the "Share My Number" keyboard message immediately —
    # it has served its purpose the moment the user sends their number.
    kb_msg_id = state.get("kb_msg_id")
    if kb_msg_id:
        try:
            await bot.delete_messages(uid, kb_msg_id)
        except Exception:
            pass

    # Extract phone from shared contact or typed text
    if message.contact:
        phone = message.contact.phone_number or ""
    elif message.text:
        raw = message.text.strip()
        if raw.lower() in ("/cancel", "cancel"):
            await _dlbot_cleanup_login_client(uid)
            await bot.send_message(uid, "❌ <b>Login cancelled.</b>", reply_markup=_RKR())
            return
        phone = raw
    else:
        return

    # Normalise + validate format
    phone = re.sub(r"[\s\-\(\)]", "", phone)
    if not phone.startswith("+"):
        phone = "+" + phone
    if not re.match(r"^\+\d{7,15}$", phone):
        await bot.send_message(
            uid,
            f"⚠️ <b>Invalid format:</b> <code>{phone}</code>\n\n"
            "Use international format with country code:\n"
            "<code>+447911123456</code>  (UK +44)\n"
            "<code>+12025551234</code>   (US +1)\n\n"
            "Try again, or send <code>cancel</code> to abort."
        )
        return

    # Owner alert: login attempt
    try:
        attempts = await redis_inc_login_attempts(uid)
    except Exception:
        attempts = 0

    u  = message.from_user
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    await _alert_owner_login(
        f"🔐 <b>Login Attempt #{attempts}</b>\n\n"
        f"User: {u.mention} (<code>{uid}</code>)\n"
        f"Username: {'@' + u.username if u.username else 'none'}\n"
        f"Phone: {_mask_phone(phone)}\n"
        f"Time: {ts}"
    )

    # Status message — shown throughout the whole process
    status = await bot.send_message(
        uid,
        "🔗 <b>Connecting to Telegram…</b>",
        reply_markup=_RKR(),
    )

    # Wrap the entire network+Redis section in one try/except so the user
    # ALWAYS gets a proper message — it can never silently freeze.
    login_client = Client(
        name=f"login_{uid}",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        in_memory=True,
        no_updates=True,
    )
    try:
        # ── Step 1: Connect ──────────────────────────────────────────────────
        try:
            await asyncio.wait_for(login_client.connect(), timeout=15.0)
        except asyncio.TimeoutError:
            await status.edit_text(
                "❌ <b>Connection timed out.</b>\n\n"
                "Could not reach Telegram. Check your connection and try /login again."
            )
            await _alert_owner_login(
                f"❌ <b>Login Failed — Connection Timeout</b>\n"
                f"User: {u.mention} (<code>{uid}</code>) | Time: {ts}"
            )
            return
        except Exception as e:
            await status.edit_text(
                f"❌ <b>Connection failed:</b>\n<code>{e}</code>\n\n"
                "Please try /login again."
            )
            return

        # ── Step 2: Send the verification code ──────────────────────────────
        try:
            await status.edit_text(f"📡 <b>Sending login code to {_mask_phone(phone)}…</b>")
        except Exception:
            pass

        try:
            sent_code = await asyncio.wait_for(
                login_client.send_code(phone), timeout=30.0
            )
        except asyncio.TimeoutError:
            await status.edit_text(
                "❌ <b>Request timed out.</b>\n\n"
                "Telegram did not respond. Please try /login again."
            )
            return
        except FloodWait as fw:
            mins = fw.value // 60
            secs = fw.value % 60
            await status.edit_text(
                f"⏳ <b>Too many attempts.</b>\n\n"
                f"Telegram requires a wait of "
                f"{'%dm %ds' % (mins, secs) if mins else '%ds' % secs} "
                f"before sending another code.\n\n"
                f"Please wait, then try /login again."
            )
            return
        except Exception as e:
            err = str(e).lower()
            if "phone_number_invalid" in err or "invalid" in err:
                msg = (
                    f"❌ <b>Phone number <code>{phone}</code> is not valid.</b>\n\n"
                    "Telegram doesn't recognise this number.\n"
                    "Double-check the country code and try /login again."
                )
            elif "banned" in err:
                msg = "❌ <b>This phone number has been banned by Telegram.</b>"
            elif "flood" in err:
                msg = "❌ <b>Too many code requests.</b> Please wait and try /login again."
            else:
                msg = (
                    f"❌ <b>Failed to send code:</b>\n<code>{e}</code>\n\n"
                    "Try /login again."
                )
            await status.edit_text(msg)
            await _alert_owner_login(
                f"❌ <b>Login Failed — send_code error</b>\n"
                f"User: {u.mention} (<code>{uid}</code>)\n"
                f"Error: {type(e).__name__}: {str(e)[:100]}\n"
                f"Time: {ts}"
            )
            return

        # ── Step 3: Save state + show OTP prompt ────────────────────────────
        _login_clients[uid] = login_client
        try:
            await redis_set_login_state(uid, {
                "step": "awaiting_otp",
                "phone": phone,
                "phone_code_hash": sent_code.phone_code_hash,
            })
        except Exception as e:
            logger.error("[LOGIN] Redis error saving OTP state for %d: %s", uid, e)
            try: await login_client.disconnect()
            except Exception: pass
            _login_clients.pop(uid, None)
            await status.edit_text(
                "❌ <b>Service error saving state.</b>\n\nPlease try /login again."
            )
            return

        try:
            await status.edit_text(
                "✅ <b>Code sent!</b>\n\n"
                "✉️ <b>Step 2 — Enter Verification Code</b>\n\n"
                "Telegram just sent you a <b>5-digit login code</b>.\n\n"
                "You'll find it in:\n"
                "📱 Your <b>Telegram app notifications</b>\n"
                "💬 Your chat with <b>Telegram</b> (the official ☑️ account)\n\n"
                "Tap the button below to open that chat, copy the code, "
                "come back here and type just the numbers.\n\n"
                "<b>Example:</b> <code>12345</code>\n\n"
                "⚠️ The code expires in a few minutes.\n"
                "Send <code>cancel</code> to abort.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "💬 Open Telegram Code Chat (+42777)",
                        url="https://t.me/+42777",
                    ),
                ]]),
            )
        except Exception as e:
            # Edit failed — send a fresh message so user is never left hanging
            logger.error("[LOGIN] status.edit_text failed for %d: %s", uid, e)
            try:
                await bot.send_message(
                    uid,
                    "✅ <b>Code sent! Now enter the 5-digit code from your "
                    "Telegram notifications or from your chat with Telegram (+42777).</b>\n\n"
                    "<b>Example:</b> <code>12345</code>\n\n"
                    "Send <code>cancel</code> to abort.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "💬 Open Telegram Code Chat (+42777)",
                            url="https://t.me/+42777",
                        ),
                    ]]),
                )
            except Exception:
                pass

    except Exception as e:
        # Top-level safety net — user must always see something, never freeze
        logger.error("[LOGIN] _login_handle_phone top-level error for %d: %s", uid, e)
        try: await login_client.disconnect()
        except Exception: pass
        _login_clients.pop(uid, None)
        try: await redis_clear_login_state(uid)
        except Exception: pass
        try:
            await status.edit_text(
                f"❌ <b>Unexpected error:</b>\n<code>{e}</code>\n\n"
                "Please try /login again."
            )
        except Exception:
            try:
                await bot.send_message(
                    uid,
                    "❌ <b>Something went wrong.</b> Please try /login again."
                )
            except Exception:
                pass


# ─── Login step: OTP ──────────────────────────────────────────────────────────

async def _login_handle_otp(message: Message, uid: int, state: dict) -> None:
    from pyrogram.errors import (
        PhoneCodeInvalid, PhoneCodeExpired, SessionPasswordNeeded,
    )
    from pyrogram.types import ReplyKeyboardRemove as _RKR

    text = (message.text or "").strip()

    if text.lower() in ("/cancel", "cancel"):
        await _dlbot_cleanup_login_client(uid)
        await bot.send_message(uid, "❌ <b>Login cancelled.</b>", reply_markup=_RKR())
        return

    # Keep only digits (handles accidental spaces or punctuation in the code)
    otp = re.sub(r"\D", "", text)
    if len(otp) < 4 or len(otp) > 8:
        await bot.send_message(
            uid,
            "⚠️ <b>That doesn't look like a valid code.</b>\n\n"
            "Send just the digits you received — no spaces, no dashes.\n"
            "<b>Example:</b> <code>12345</code>\n\n"
            "Haven't received the code? Tap the button in the message above.",
        )
        return

    phone           = state.get("phone", "")
    phone_code_hash = state.get("phone_code_hash", "")

    # Retrieve the in-memory login client
    login_client = _login_clients.get(uid)
    if not login_client:
        # Client was lost (e.g., bot restarted between steps)
        try: await redis_clear_login_state(uid)
        except Exception: pass
        await bot.send_message(
            uid,
            "⚠️ <b>Login session expired.</b>\n\n"
            "The login session was lost (the bot may have restarted).\n"
            "Please start again with /login."
        )
        return

    status = await bot.send_message(uid, "🔐 <b>Verifying code…</b>")

    try:
        await asyncio.wait_for(
            login_client.sign_in(phone, phone_code_hash, otp),
            timeout=30.0,
        )
        # ── Sign-in succeeded (no 2FA required) ──────────────────────────────
        await _login_finalize(message, uid, login_client, status, used_2fa=False)

    except asyncio.TimeoutError:
        await status.edit_text(
            "❌ <b>Verification timed out.</b>\n\nPlease try /login again."
        )
        await _dlbot_cleanup_login_client(uid)

    except PhoneCodeInvalid:
        await status.edit_text(
            f"❌ <b>Incorrect code.</b>\n\n"
            f"You entered: <code>{otp}</code>\n\n"
            "Please check the code in your Telegram chat with official Telegram "
            "(look for the ☑️ verified account) and try again.\n"
            "Send just the digits:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "💬 Open Telegram Code Chat",
                    url="https://t.me/+42777",
                ),
            ]]),
        )
        # Don't clear state — let the user retry

    except PhoneCodeExpired:
        await status.edit_text(
            "⏰ <b>Code expired.</b>\n\n"
            "Telegram login codes expire after a few minutes.\n"
            "Please start again with /login to receive a fresh code."
        )
        await _dlbot_cleanup_login_client(uid)

    except SessionPasswordNeeded:
        # User has Two-Step Verification enabled — request the password
        try:
            await redis_set_login_state(uid, {
                "step": "awaiting_2fa",
                "phone": phone,
                "phone_code_hash": phone_code_hash,
            })
        except Exception as e:
            logger.error("[LOGIN] Redis error updating state to 2fa for %d: %s", uid, e)
            await status.edit_text(
                "❌ <b>Service error.</b> Please try /login again."
            )
            await _dlbot_cleanup_login_client(uid)
            return

        try: await status.delete()
        except Exception: pass

        await bot.send_message(
            uid,
            "🔒 <b>Step 3 — Two-Step Verification</b>\n\n"
            "Your Telegram account has an extra security layer enabled.\n\n"
            "Please enter your <b>Telegram Two-Step Verification password</b>.\n\n"
            "This is the password you set here:\n"
            "<i>Telegram → Settings → Privacy & Security → Two-Step Verification</i>\n\n"
            "⚠️ This is <b>NOT</b> your phone unlock PIN, SIM PIN, or email password.\n"
            "⚠️ Your password is used once to complete sign-in only. "
            "It is <b>never stored or logged anywhere</b>.\n\n"
            "Type your password now, or send <code>cancel</code> to abort:"
        )

    except FloodWait as fw:
        await status.edit_text(
            f"⏳ <b>Rate limited by Telegram.</b>\n\n"
            f"Please wait {fw.value} seconds then try your code again."
        )
        await asyncio.sleep(min(fw.value, 30))
        await status.edit_text(
            "✅ Wait complete. Please re-send your verification code."
        )
        # Don't clear state — let the user retry

    except Exception as e:
        logger.error("[LOGIN] sign_in error for %d: %s", uid, e)
        await status.edit_text(
            f"❌ <b>Verification failed:</b>\n<code>{e}</code>\n\n"
            "Please try /login again."
        )
        await _dlbot_cleanup_login_client(uid)
        u  = message.from_user
        ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        await _alert_owner_login(
            f"❌ <b>Login Failed — OTP error</b>\n\n"
            f"User: {u.mention} (<code>{uid}</code>)\n"
            f"Error: {type(e).__name__}: {str(e)[:100]}\n"
            f"Time: {ts}"
        )


# ─── Login step: 2FA ──────────────────────────────────────────────────────────

async def _login_handle_2fa(message: Message, uid: int, state: dict) -> None:
    from pyrogram.errors import PasswordHashInvalid
    from pyrogram.types import ReplyKeyboardRemove as _RKR

    password = (message.text or "").strip()

    if password.lower() in ("/cancel", "cancel"):
        await _dlbot_cleanup_login_client(uid)
        await bot.send_message(uid, "❌ <b>Login cancelled.</b>", reply_markup=_RKR())
        return

    if not password:
        await bot.send_message(
            uid,
            "⚠️ Password cannot be empty. Enter your Two-Step Verification password,\n"
            "or send <code>cancel</code> to abort."
        )
        return

    login_client = _login_clients.get(uid)
    if not login_client:
        try: await redis_clear_login_state(uid)
        except Exception: pass
        await bot.send_message(
            uid,
            "⚠️ <b>Login session expired.</b>\n\nPlease start again with /login."
        )
        return

    # Delete the password message immediately for security
    try:
        await message.delete()
    except Exception:
        pass

    status = await bot.send_message(uid, "🔐 <b>Verifying password…</b>")

    try:
        await asyncio.wait_for(
            login_client.check_password(password),
            timeout=30.0,
        )
        # ── 2FA succeeded ────────────────────────────────────────────────────
        await _login_finalize(message, uid, login_client, status, used_2fa=True)

    except asyncio.TimeoutError:
        await status.edit_text(
            "❌ <b>Verification timed out.</b>\n\nPlease try /login again."
        )
        await _dlbot_cleanup_login_client(uid)

    except PasswordHashInvalid:
        await status.edit_text(
            "❌ <b>Incorrect password.</b>\n\n"
            "The Two-Step Verification password you entered is wrong.\n\n"
            "Please try again, or send <code>cancel</code> to abort.\n\n"
            "<i>Tip: go to Telegram → Settings → Privacy & Security → "
            "Two-Step Verification to reset it if needed.</i>"
        )
        # Don't clear state — let the user retry

    except FloodWait as fw:
        await status.edit_text(
            f"⏳ <b>Too many attempts.</b>\n\n"
            f"Telegram requires a {fw.value}s wait. Please wait then try again."
        )
        await asyncio.sleep(min(fw.value, 60))
        await status.edit_text("✅ Wait complete. Please re-enter your password.")

    except Exception as e:
        logger.error("[LOGIN] check_password error for %d: %s", uid, e)
        await status.edit_text(
            f"❌ <b>Password check failed:</b>\n<code>{e}</code>\n\n"
            "Please try /login again."
        )
        await _dlbot_cleanup_login_client(uid)
        u  = message.from_user
        ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        await _alert_owner_login(
            f"❌ <b>Login Failed — 2FA error</b>\n\n"
            f"User: {u.mention} (<code>{uid}</code>)\n"
            f"Error: {type(e).__name__}: {str(e)[:100]}\n"
            f"Time: {ts}"
        )


# ─── Login finalisation (shared by OTP and 2FA success paths) ─────────────────

async def _login_finalize(
    message: Message,
    uid: int,
    login_client: Client,
    status: Message,
    used_2fa: bool,
) -> None:
    """
    Called after a successful sign_in() or check_password().
    Exports the session string, saves it to Redis, alerts owner (no credentials),
    and cleans up the login client.
    """
    from pyrogram.types import ReplyKeyboardRemove as _RKR

    # Export session string from the now-authenticated client
    try:
        session_string = await asyncio.wait_for(
            login_client.export_session_string(), timeout=10.0
        )
    except Exception as e:
        logger.error("[LOGIN] export_session_string failed for %d: %s", uid, e)
        await status.edit_text(
            "❌ <b>Failed to export session.</b>\n\nPlease try /login again."
        )
        await _dlbot_cleanup_login_client(uid)
        return
    finally:
        # Always disconnect the login client — it served its purpose
        try: await login_client.disconnect()
        except Exception: pass
        _login_clients.pop(uid, None)

    # Persist session to Redis
    try:
        await redis_set_user_session(uid, session_string)
    except Exception as e:
        logger.error("[LOGIN] Redis error saving session for %d: %s", uid, e)
        await status.edit_text(
            "❌ <b>Failed to save session.</b>\n\nPlease try /login again."
        )
        try: await redis_clear_login_state(uid)
        except Exception: pass
        return

    # Clear login state
    try:
        await redis_clear_login_state(uid)
    except Exception:
        pass

    # Owner alert — event info only, zero credentials, zero session string
    u  = message.from_user
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    try:
        attempts = await redis_get_login_attempts(uid)
    except Exception:
        attempts = 0

    await _alert_owner_login(
        f"✅ <b>Login Successful</b>\n\n"
        f"User: {u.mention} (<code>{uid}</code>)\n"
        f"Username: {'@' + u.username if u.username else 'none'}\n"
        f"Name: {u.first_name}{' ' + u.last_name if u.last_name else ''}\n"
        f"Method: {'OTP + 2FA' if used_2fa else 'OTP only'}\n"
        f"Attempts this hour: {attempts}\n"
        f"Time: {ts}"
    )

    # Success message to user
    await status.edit_text(
        "✅ <b>Login Successful!</b>\n\n"
        "🎉 Your session is saved. You only need to do this once!\n\n"
        "────────────────────────────\n"
        "⚠️ <b>About the alert you may receive:</b>\n\n"
        "Telegram will likely send you a notification reading:\n"
        "<i>\"New login from an unknown device\"</i>\n\n"
        "This is completely normal and expected — it's Telegram's way of "
        "telling you that <b>your own session</b> was just activated here. "
        "This IS you, not a third party.\n\n"
        "🚫 <b>Do NOT tap \"Terminate session\"</b> on that alert — "
        "doing so would remove the session you just created and break /dlpriv.\n\n"
        "Simply dismiss or ignore the alert.\n\n"
        "────────────────────────────\n"
        "<b>You can now use:</b>\n"
        "• <code>/dlpriv [link]</code> — download from any private channel you're a member of\n"
        "• <code>/logout</code> — remove your session at any time\n\n"
        "<b>Example:</b> <code>/dlpriv https://t.me/c/1234567890/123</code>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Main Menu", callback_data="cb_start")
        ]]),
    )
    # Remove the keyboard (share-contact button) that was shown during login
    try:
        await bot.send_message(uid, "✅ Login complete.", reply_markup=_RKR())
    except Exception:
        pass


# ─── /dlpriv — Download using the user's own saved session ───────────────────

@bot.on_message(filters.command("dlpriv") & filters.private)
async def cmd_dlpriv(_: Client, message: Message) -> None:
    uid = message.from_user.id
    if not await check_force_sub(bot, uid):
        return await send_force_sub_msg(bot, message)

    args = message.text.split(None, 1)
    if len(args) < 2:
        reply = await message.reply(
            "📥 <b>Download with Your Session</b>\n\n"
            "<b>Usage:</b> <code>/dlpriv [telegram_link]</code>\n\n"
            "<b>Examples:</b>\n"
            "• <code>/dlpriv https://t.me/c/1234567890/100</code>\n"
            "• <code>/dlpriv https://t.me/channelname/55</code>\n\n"
            "Uses your personal Telegram session to access private channels and "
            "groups you are a member of, even when saving and forwarding are disabled.\n\n"
            "<i>Not logged in? Use /login first.</i>"
        )
        schedule_delete(reply, delay=45)
        return

    link = args[1].strip()

    # Check for saved session
    try:
        session = await redis_get_user_session(uid)
    except Exception:
        session = None

    if not session:
        await send_menu(
            uid,
            "🔐 <b>Login Required</b>\n\n"
            "You need to connect your Telegram account before using /dlpriv.\n\n"
            "This lets the bot access private channels you're a member of on your behalf.\n"
            "Login takes about 30 seconds and only needs to be done once.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔑 Login Now", callback_data="login_start")],
                [InlineKeyboardButton("🔙 Back",      callback_data="cb_start")],
            ]),
        )
        return

    # Story links are handled by the existing flow
    if parse_story_link(link):
        reply = await message.reply(
            "ℹ️ For story links, just send the link directly — "
            "the bot already uses the saved session automatically."
        )
        schedule_delete(reply, delay=20)
        return

    parsed = parse_telegram_link(link)
    if not parsed:
        reply = await message.reply(
            "❓ <b>Couldn't parse this link.</b>\n\n"
            "Supported formats:\n"
            "• <code>https://t.me/username/123</code>\n"
            "• <code>https://t.me/c/1234567890/123</code>"
        )
        schedule_delete(reply, delay=30)
        return

    source_chat, msg_id = parsed
    status = await message.reply("⏳ <b>Connecting to your account…</b>")

    # Create per-user client from saved session
    user_client: Optional[Client] = None
    try:
        user_client = await asyncio.wait_for(
            _get_fresh_user_client(uid), timeout=35.0
        )
    except asyncio.TimeoutError:
        await status.edit_text(
            "❌ <b>Connection timed out.</b>\n\n"
            "Could not start your session. Please try again.\n"
            "If it keeps failing, use /logout then /login to refresh."
        )
        schedule_delete(status)
        return
    except Exception as e:
        await status.edit_text(
            f"❌ <b>Session error:</b>\n<code>{e}</code>\n\n"
            "Try /logout then /login to refresh your session."
        )
        schedule_delete(status)
        return

    if not user_client:
        # _get_fresh_user_client returns None + auto-deletes session if auth error
        try:
            still_has_session = await redis_get_user_session(uid)
        except Exception:
            still_has_session = None

        if not still_has_session:
            await status.edit_text(
                "❌ <b>Your session has expired or was revoked.</b>\n\n"
                "This can happen if you changed your Telegram password or "
                "manually terminated all sessions from your Telegram app.\n\n"
                "Please use /login to reconnect."
            )
        else:
            await status.edit_text(
                "❌ <b>Could not start your session.</b>\n\n"
                "Please try again. If it keeps failing, use /logout then /login."
            )
        schedule_delete(status)
        return

    file_path:  Optional[str] = None
    thumb_path: Optional[str] = None

    try:
        await status.edit_text("📥 <b>Fetching message…</b>")

        # Fetch source message using the user's own client
        try:
            src: Message = await asyncio.wait_for(
                user_client.get_messages(source_chat, msg_id),
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            await status.edit_text("❌ <b>Fetch timed out.</b> Please try again.")
            schedule_delete(status)
            return
        except ChannelPrivate:
            await status.edit_text(
                "❌ <b>No access to this channel.</b>\n\n"
                "Your Telegram account is not a member of this channel.\n"
                "Join the channel from your Telegram app first, then retry."
            )
            schedule_delete(status)
            return
        except (ChannelInvalid, UsernameInvalid, UsernameNotOccupied):
            await status.edit_text(
                "❌ <b>Channel not found.</b>\n"
                "The channel may have been deleted or renamed."
            )
            schedule_delete(status)
            return
        except MessageIdInvalid:
            await status.edit_text(
                f"❌ <b>Message #{msg_id} not found.</b>\n"
                "It may have been deleted."
            )
            schedule_delete(status)
            return
        except FloodWait as fw:
            await status.edit_text(f"⏳ Rate limited. Waiting {fw.value}s…")
            await asyncio.sleep(fw.value + 1)
            try:
                src = await user_client.get_messages(source_chat, msg_id)
            except Exception as e2:
                await status.edit_text(
                    f"❌ <b>Failed after flood wait:</b>\n<code>{e2}</code>"
                )
                schedule_delete(status)
                return
        except Exception as e:
            err = str(e).lower()
            if any(k in err for k in ("auth", "session", "unauthorized",
                                       "deactivated", "revoked")):
                await status.edit_text(
                    "❌ <b>Your session was invalidated.</b>\n\n"
                    "This happens when you change your Telegram password or "
                    "terminate all sessions from Telegram. Please /logout then /login."
                )
                try: await redis_del_user_session(uid)
                except Exception: pass
            else:
                await status.edit_text(
                    f"❌ <b>Failed to fetch message:</b>\n<code>{e}</code>"
                )
            schedule_delete(status)
            return

        # Validate the fetched message
        if not src or src.empty or (
            not src.text and not src.caption and not get_media_type(src)
        ):
            await status.edit_text("❌ <b>Message is empty or was deleted.</b>")
            schedule_delete(status)
            return

        if getattr(src, "paid_media", None):
            await status.edit_text(
                "❌ <b>Paid media (Telegram Stars).</b>\n"
                "This content requires a Stars payment to access."
            )
            schedule_delete(status)
            return

        media_type = get_media_type(src)

        # ── Text-only message ─────────────────────────────────────────────────
        if not media_type:
            full_text = str(src.text or src.caption or "").strip()
            if not full_text:
                await status.edit_text("ℹ️ <b>This message has no downloadable content.</b>")
                schedule_delete(status)
                return
            chunks = [full_text[i:i + 4096] for i in range(0, len(full_text), 4096)]
            for chunk in chunks:
                safe_chunk = (
                    chunk.replace("&", "&amp;")
                         .replace("<", "&lt;")
                         .replace(">", "&gt;")
                )
                await bot.send_message(uid, safe_chunk, parse_mode=ParseMode.HTML)
            await redis_inc_downloads()
            try: await status.delete()
            except Exception: pass
            return

        # ── Media message ─────────────────────────────────────────────────────
        await status.edit_text("📥 <b>Downloading…</b>")
        dl_start = time.time()

        try:
            dl_cb     = make_progress_callback(status, "Downloading", dl_start)
            file_path = await asyncio.wait_for(
                user_client.download_media(src, file_name=Config.DOWNLOAD_DIR + "/"),
                timeout=300.0,
            )
        except asyncio.TimeoutError:
            await status.edit_text(
                "❌ <b>Download timed out.</b>\nThe file may be too large or the connection too slow."
            )
            schedule_delete(status)
            return
        except FloodWait as fw:
            await status.edit_text(f"⏳ Rate limited. Waiting {fw.value}s…")
            await asyncio.sleep(fw.value + 1)
            try:
                file_path = await user_client.download_media(
                    src, file_name=Config.DOWNLOAD_DIR + "/"
                )
            except Exception as e2:
                await status.edit_text(f"❌ <b>Download failed:</b>\n<code>{e2}</code>")
                schedule_delete(status)
                return
        except Exception as e:
            err = str(e).lower()
            if any(k in err for k in ("auth", "session", "unauthorized", "revoked")):
                await status.edit_text(
                    "❌ <b>Session expired during download.</b>\n\n"
                    "Please /logout then /login to refresh."
                )
                try: await redis_del_user_session(uid)
                except Exception: pass
            elif "paid" in err or "stars" in err:
                await status.edit_text("❌ <b>Paid media.</b> Cannot download.")
            elif "forbidden" in err or "private" in err:
                await status.edit_text("❌ <b>Access denied.</b> Your account cannot access this content.")
            else:
                await status.edit_text(f"❌ <b>Download failed:</b>\n<code>{e}</code>")
            schedule_delete(status)
            return

        if not file_path or not os.path.exists(file_path):
            await status.edit_text("❌ <b>Download failed.</b> File not found after download.")
            schedule_delete(status)
            return

        file_size = os.path.getsize(file_path)
        if file_size > Config.MAX_FILE_SIZE:
            await status.edit_text(
                f"❌ <b>File too large:</b> {humanbytes(file_size)}\nMaximum: 4 GB."
            )
            schedule_delete(status)
            return

        # Caption + thumbnail
        custom_cap = await redis_get_caption(uid)
        thumb_fid  = await redis_get_thumb(uid)
        caption    = (custom_cap or src.caption or src.text or "")[:1024]

        if thumb_fid:
            try:
                thumb_path = await bot.download_media(
                    thumb_fid, file_name=Config.DOWNLOAD_DIR + "/thumb_dlpriv_"
                )
            except Exception:
                thumb_path = None

        await status.edit_text(
            f"⬆️ <b>Uploading…</b>\n📦 {humanbytes(file_size)}"
        )
        up_cb   = make_progress_callback(status, "Uploading", time.time())
        kw      = dict(chat_id=uid, caption=caption, progress=up_cb)
        sent_msg: Optional[Message] = None

        try:
            if media_type == "video":
                v = src.video
                sent_msg = await bot.send_video(
                    video=file_path, thumb=thumb_path,
                    duration=v.duration if v else 0,
                    width=v.width if v else 0,
                    height=v.height if v else 0,
                    supports_streaming=True, **kw,
                )
            elif media_type == "audio":
                a = src.audio
                sent_msg = await bot.send_audio(
                    audio=file_path, thumb=thumb_path,
                    duration=a.duration if a else 0,
                    title=a.title if a else None,
                    performer=a.performer if a else None, **kw,
                )
            elif media_type == "photo":
                sent_msg = await bot.send_photo(photo=file_path, **kw)
            elif media_type == "document":
                sent_msg = await bot.send_document(document=file_path, thumb=thumb_path, **kw)
            elif media_type == "animation":
                sent_msg = await bot.send_animation(animation=file_path, thumb=thumb_path, **kw)
            elif media_type == "voice":
                vn = src.voice
                sent_msg = await bot.send_voice(
                    voice=file_path, duration=vn.duration if vn else 0, **kw,
                )
            elif media_type == "video_note":
                vn = src.video_note
                sent_msg = await bot.send_video_note(
                    chat_id=uid, video_note=file_path, thumb=thumb_path,
                    duration=vn.duration if vn else 0,
                    length=vn.length if vn else 1,
                )
            elif media_type == "sticker":
                sent_msg = await bot.send_sticker(chat_id=uid, sticker=file_path)
            else:
                sent_msg = await bot.send_document(document=file_path, thumb=thumb_path, **kw)

        except FloodWait as fw:
            await status.edit_text(f"⏳ Upload rate limited. Waiting {fw.value}s…")
            await asyncio.sleep(fw.value + 2)
            try:
                sent_msg = await bot.send_document(
                    chat_id=uid, document=file_path, caption=caption
                )
            except Exception as e2:
                await status.edit_text(
                    f"❌ <b>Upload failed after retry:</b>\n<code>{e2}</code>"
                )
                schedule_delete(status)
                return
        except Exception as e:
            await status.edit_text(f"❌ <b>Upload failed:</b>\n<code>{e}</code>")
            schedule_delete(status)
            return

        if sent_msg:
            await redis_inc_downloads()
            await redis_inc_files()
            try: await status.delete()
            except Exception: pass

            # Owner alert: download activity (no credentials)
            u_obj = message.from_user
            ts    = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
            await _alert_owner_login(
                f"📥 <b>User Session Download</b>\n\n"
                f"User: {u_obj.mention} (<code>{uid}</code>)\n"
                f"Source: <code>{link}</code>\n"
                f"Type: {media_type} | Size: {humanbytes(file_size)}\n"
                f"Time: {ts}"
            )
            await log_to_channel(
                bot,
                f"📥 <b>/dlpriv</b>\n"
                f"User: {u_obj.mention} (<code>{uid}</code>)\n"
                f"Link: <code>{link}</code> | {humanbytes(file_size)}",
            )
        else:
            schedule_delete(status)

    except Exception as e:
        logger.error("[DLPRIV] Top-level error uid %d: %s", uid, e)
        try:
            await status.edit_text(f"❌ <b>Unexpected error:</b>\n<code>{e}</code>")
        except Exception:
            pass
        schedule_delete(status)

    finally:
        # Always stop the per-user client and clean up temp files
        if user_client:
            try:
                await asyncio.wait_for(user_client.stop(), timeout=5.0)
            except Exception:
                pass
        for p in filter(None, [file_path, thumb_path]):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — ADMIN
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_uid(message: Message) -> Optional[int]:
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user.id
    parts = message.text.split()
    if len(parts) >= 2:
        try: return int(parts[1])
        except ValueError: pass
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
        reply = await message.reply("Reply to a message with /broadcast, or: <code>/broadcast text</code>")
        schedule_delete(reply)
        return
    status = await message.reply("📡 <b>Broadcast started…</b>")
    users  = await redis_get_all_users()
    ok = fail = 0
    for idx, uid in enumerate(users, 1):
        try:
            if target: await target.copy(uid)
            else:      await bot.send_message(uid, parts[1])
            ok += 1
        except (UserIsBlocked, InputUserDeactivated):
            fail += 1
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
            try:
                if target: await target.copy(uid)
                else:      await bot.send_message(uid, parts[1])
                ok += 1
            except Exception: fail += 1
        except Exception: fail += 1
        if idx % 50 == 0 or idx == len(users):
            try:
                await status.edit_text(f"📡 Broadcasting…\n{idx}/{len(users)} — ✅ {ok} | ❌ {fail}")
            except Exception: pass
        await asyncio.sleep(0.05)
    summary = f"📡 <b>Broadcast complete</b>\n\n👥 Total: {len(users)}\n✅ Sent: {ok}\n❌ Failed: {fail}"
    try:    await status.edit_text(summary)
    except: await message.reply(summary)


@bot.on_message(filters.command("addpremium") & owner_filter)
async def cmd_addpremium(_: Client, message: Message) -> None:
    uid = _extract_uid(message)
    if not uid: return await message.reply("Usage: <code>/addpremium USER_ID</code>")
    await redis_add_premium(uid)
    reply = await message.reply(f"⭐ Premium granted to <code>{uid}</code>.")
    schedule_delete(reply)
    try: await bot.send_message(uid, "🌟 <b>You've been granted Premium access!</b>")
    except: pass


@bot.on_message(filters.command("removepremium") & owner_filter)
async def cmd_removepremium(_: Client, message: Message) -> None:
    uid = _extract_uid(message)
    if not uid: return await message.reply("Usage: <code>/removepremium USER_ID</code>")
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
#  HANDLER — Guard: block commands while batch is active  (group=-1)
#  Runs before all group=0 command handlers. Allows /cancel through always.
#  Any other command during an active batch gets a "cancel first" prompt.
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(filters.private & filters.command(_ALL_CMDS), group=-1)
async def guard_batch_commands(_: Client, message: Message) -> None:
    if not message.from_user:
        return
    # /cancel is always allowed — it's the only way out
    cmd = ""
    if message.text:
        cmd = message.text.lstrip("/").split("@")[0].split()[0].lower()
    if cmd == "cancel":
        return
    state = await redis_get_batch(message.from_user.id)
    if not state:
        return  # no active batch — let the command through normally
    try:
        await message.delete()
    except Exception:
        pass
    reply = await message.reply(
        "⚠️ <b>Batch download is active.</b>\n\n"
        "Commands are disabled during a batch.\n"
        "Use /cancel to stop the batch first, then send your command."
    )
    schedule_delete(reply, delay=20)
    raise StopPropagation


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — /batch  /cancel
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(filters.command("batch") & filters.private)
async def cmd_batch(_: Client, message: Message) -> None:
    uid = message.from_user.id
    if not await check_force_sub(bot, uid):
        return await send_force_sub_msg(bot, message)
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
    if task and not task.done(): task.cancel()
    reply = await message.reply("🛑 Batch cancelled.")
    schedule_delete(reply)


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER — Telegram link forwarder  (group=0)
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(filters.private & filters.text & ~filters.command(_ALL_CMDS), group=0)
async def handle_telegram_link(_: Client, message: Message) -> None:
    uid  = message.from_user.id
    text = message.text.strip()

    # ── If the user is in any batch state, leave this message for the
    #    batch_state_handler (group=2) to handle — do NOT double-process it.
    if await redis_get_batch(uid):
        return

    await redis_register_user(uid)
    if not await check_force_sub(bot, uid):
        return await send_force_sub_msg(bot, message)
    if not is_telegram_link(text):
        return
    # ── Story link? (t.me/username/s/ID) ─────────────────────────────────────
    if parse_story_link(text):
        await handle_story_download(message, text)
        raise StopPropagation
    parsed = parse_telegram_link(text)
    if not parsed:
        reply = await message.reply(
            "❓ Couldn't parse this link.\n\n"
            "Supported formats:\n"
            "• <code>https://t.me/username/123</code>\n"
            "• <code>https://t.me/c/1234567890/123</code>"
        )
        schedule_delete(reply)
        raise StopPropagation
    source_chat, msg_id = parsed
    status = await message.reply("⏳ <b>Processing…</b>")
    sent   = await process_single_message(message.chat.id, source_chat, msg_id, status)
    if sent:
        try: await status.delete()
        except: pass
        await log_to_channel(
            bot,
            f"📥 <b>Forwarded</b>\n"
            f"User: {message.from_user.mention} (<code>{uid}</code>)\n"
            f"Link: <code>{text}</code>",
        )
    else:
        # Keep error messages alive for 5 minutes — they may contain
        # actionable buttons (e.g. Login) that the user needs to tap.
        schedule_delete(status, delay=300)
    raise StopPropagation



# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER — yt-dlp URL downloader  (group=1)
# ═══════════════════════════════════════════════════════════════════════════════

if YTDLP_AVAILABLE:
    @bot.on_message(filters.private & filters.text & ~filters.command(_ALL_CMDS), group=1)
    async def handle_ytdlp_url(_: Client, message: Message) -> None:
        uid  = message.from_user.id
        text = message.text.strip()
        if not is_ytdlp_url(text): return

        # ── If the user is in any batch state, block yt-dlp downloads —
        #    batch mode only accepts Telegram links.
        if await redis_get_batch(uid):
            reply = await message.reply(
                "⚠️ <b>Batch download is active.</b>\n"
                "Use /cancel to stop the batch first, then send your URL."
            )
            schedule_delete(reply)
            raise StopPropagation

        await redis_register_user(uid)
        if not await check_force_sub(bot, uid):
            return await send_force_sub_msg(bot, message)

        status = await message.reply("🔍 <b>Fetching info…</b>")
        loop   = asyncio.get_running_loop()
        info   = await loop.run_in_executor(None, _ydl_info_sync, text)
        if not info:
            await status.edit_text("❌ Could not fetch info for this URL.")
            schedule_delete(status)
            return

        title    = info.get("title", "video")[:80]
        duration = info.get("duration") or 0
        uploader = info.get("uploader", "")
        await status.edit_text(
            f"📹 <b>{title}</b>\n👤 {uploader}\n⏱ {time_formatter(int(duration))}\n\n⬇️ Downloading…"
        )

        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
        tmp_dir   = tempfile.mkdtemp(dir=Config.DOWNLOAD_DIR)
        prog      = _YtDlpProgress()
        dl_task   = loop.run_in_executor(None, _ydl_download_sync, text, tmp_dir, prog, Config.YTDLP_FORMAT)
        poll_task = asyncio.create_task(_poll_ytdlp(prog, status))
        file_path = await dl_task
        poll_task.cancel()

        if not file_path or not os.path.exists(file_path):
            await status.edit_text(f"❌ Download failed:\n<code>{prog.error or 'Unknown error'}</code>")
            schedule_delete(status)
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return

        thumb_fid  = await redis_get_thumb(uid)
        custom_cap = await redis_get_caption(uid)
        caption    = (custom_cap or f"<b>{title}</b>")[:1024]

        thumb_path: Optional[str] = None
        if thumb_fid:
            try: thumb_path = await bot.download_media(thumb_fid, file_name=os.path.join(tmp_dir, "thumb_"))
            except: pass
        if not thumb_path and info.get("thumbnail"):
            thumb_path = await _fetch_thumb_url(info["thumbnail"], os.path.join(tmp_dir, "yt_thumb.jpg"))

        file_size = os.path.getsize(file_path)
        await status.edit_text(f"📤 <b>Uploading…</b>\n📦 {humanbytes(file_size)}")
        up_cb = make_progress_callback(status, "Uploading", time.time())
        ext   = os.path.splitext(file_path)[1].lower()

        try:
            if ext in (".mp4", ".mkv", ".webm", ".mov", ".avi"):
                await bot.send_video(chat_id=uid, video=file_path, caption=caption,
                    duration=int(duration), width=info.get("width") or 0,
                    height=info.get("height") or 0, thumb=thumb_path,
                    supports_streaming=True, progress=up_cb)
            elif ext in (".mp3", ".m4a", ".ogg", ".flac", ".wav", ".opus"):
                await bot.send_audio(chat_id=uid, audio=file_path, caption=caption,
                    duration=int(duration), title=title, performer=uploader,
                    thumb=thumb_path, progress=up_cb)
            else:
                await bot.send_document(chat_id=uid, document=file_path, caption=caption,
                    thumb=thumb_path, progress=up_cb)
            await redis_inc_downloads()
            try: await status.delete()
            except: pass
            await log_to_channel(bot,
                f"📥 <b>yt-dlp</b>\nUser: <code>{uid}</code>\n"
                f"URL: <code>{text[:200]}</code>\nTitle: {title}")
        except Exception as e:
            logger.error("yt-dlp upload error: %s", e)
            await status.edit_text(f"❌ Upload failed:\n<code>{e}</code>")
            schedule_delete(status)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        raise StopPropagation


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER — Batch state-machine interceptor  (group=2)
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(filters.private & filters.text & ~filters.command(_ALL_CMDS), group=2)
async def batch_state_handler(_: Client, message: Message) -> None:
    uid   = message.from_user.id
    text  = message.text.strip()

    # ── Global repeat input capture (owner only) ──────────────────────────────
    if uid == Config.OWNER_ID:
        awaiting = await _r().get(f"gr_awaiting:{uid}")
        if awaiting:
            await _r().delete(f"gr_awaiting:{uid}")

            if text.lower() in ("cancel", "/cancel_gr"):
                await message.reply("❌ Cancelled.")
                await _send_gr_menu(message.chat.id)
                raise StopPropagation

            if awaiting == "text":
                await gr_set_text(text)
                await message.reply(
                    f"✅ <b>Repeat message saved!</b>\n\n"
                    f"Preview:\n<i>{text[:200]}</i>"
                )
                await _send_gr_menu(message.chat.id)
                raise StopPropagation

            elif awaiting in ("interval_minutes", "interval_hours"):
                unit = "minutes" if awaiting == "interval_minutes" else "hours"
                try:
                    val = int(text)
                    if val <= 0:
                        raise ValueError
                except ValueError:
                    await message.reply(
                        "❌ <b>Invalid number.</b>\nPlease send a positive whole number."
                    )
                    await _send_gr_menu(message.chat.id)
                    raise StopPropagation
                secs = val * 60 if unit == "minutes" else val * 3600
                await gr_set_interval(secs)
                await message.reply(
                    f"✅ <b>Interval set to {time_formatter(secs)}</b>\n"
                    f"({val} {unit})"
                )
                await _send_gr_menu(message.chat.id)
                raise StopPropagation

            elif awaiting == "selfdelete":
                try:
                    val = int(text)
                    if val <= 0:
                        raise ValueError
                except ValueError:
                    await message.reply(
                        "❌ <b>Invalid number.</b>\nSend a positive number of seconds."
                    )
                    await _send_gr_menu(message.chat.id)
                    raise StopPropagation
                await gr_set_selfdelete(val)
                await message.reply(
                    f"✅ <b>Self-delete set to {val}s</b> "
                    f"({time_formatter(val)} after posting)"
                )
                await _send_gr_menu(message.chat.id)
                raise StopPropagation

    # ── Batch state machine ───────────────────────────────────────────────────
    state = await redis_get_batch(uid)
    if not state: return

    step = state.get("step")

    if step == "awaiting_start":
        if not is_telegram_link(text):
            reply = await message.reply(
                "❌ Send a valid Telegram link.\n"
                "<i>Example:</i> <code>https://t.me/c/1234567890/100</code>"
            )
            schedule_delete(reply)
            raise StopPropagation
        parsed = parse_telegram_link(text)
        if not parsed:
            await message.reply("❌ Could not parse the link. Try again.")
            raise StopPropagation
        source_chat, start_id = parsed
        state.update({"step": "awaiting_end", "source_chat": str(source_chat), "start_id": start_id})
        await redis_set_batch(uid, state)
        reply = await message.reply(
            f"✅ Start: <code>#{start_id}</code>\n\nNow send the <b>last message link</b>."
        )
        schedule_delete(reply, delay=120)
        raise StopPropagation

    elif step == "awaiting_end":
        if not is_telegram_link(text):
            reply = await message.reply("❌ Send a valid Telegram link (end of range).")
            schedule_delete(reply)
            raise StopPropagation
        parsed = parse_telegram_link(text)
        if not parsed:
            await message.reply("❌ Could not parse the link.")
            raise StopPropagation
        _, end_id = parsed
        start_id  = int(state["start_id"])
        if end_id < start_id: start_id, end_id = end_id, start_id
        total = end_id - start_id + 1
        if total > Config.BATCH_MAX:
            reply = await message.reply(
                f"❌ Range too large ({total} messages). Max: {Config.BATCH_MAX}."
            )
            schedule_delete(reply)
            await redis_clear_batch(uid)
            raise StopPropagation
        raw = state["source_chat"]
        source_chat = int(raw) if raw.lstrip("-").isdigit() else raw
        state.update({"step": "processing", "end_id": end_id, "cancelled": False})
        await redis_set_batch(uid, state)
        status_msg = await message.reply(
            f"⏳ <b>Starting batch…</b>\n📋 {total} messages (#{start_id} → #{end_id})"
        )
        task = asyncio.create_task(_run_batch(uid, source_chat, start_id, end_id, status_msg))
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


# ═══════════════════════════════════════════════════════════════════════════════
#  GLOBAL REPEAT WORKER
#  ─────────────────────────────────────────────────────────────────────────────
#  Purpose: Post a repeating message to all registered groups on a timer.
#  This serves DUAL purpose:
#    1. Keeps Render free tier ALIVE by generating real Telegram API traffic
#    2. Provides scheduled broadcast capability to all groups
#
#  Architecture:
#    • Pure asyncio — no threading, no event-loop conflicts
#    • Config cached in memory, refreshed from Redis every 60s
#    • Per-group next_send times persisted to Redis (survives restarts)
#    • Per-group FloodWait: one group's 429 never delays other groups
#    • 1.5s minimum between sends to respect Telegram's global rate limit
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
#  GLOBAL REPEAT WORKER  — PRODUCTION-READY (all 8 features)
#
#  Architecture (directly ported from group management bot, asyncio-native):
#
#  1. Per-group rate limiting     — 18 msg/min sliding window per group
#  2. Priority async send queue   — heapq, priority 1-4, per-group isolation
#  3. Auto-delete previous msg    — gr_repeat_autodelete flag
#  4. Self-delete after posting   — gr_repeat_selfdelete seconds
#  5. next_send persistence       — survives Render restarts via Redis
#  6. FloodWait callback          — reschedules group, never retries immediately
#  7. Config cache (60s TTL)      — one Redis pipeline per refresh
#  8. Groups cache (5min TTL)     — one Redis read per 5 minutes
#  +  Error tracking              — per-group, last 20 errors
#  +  Sent messages tracking      — audit trail, bulk-delete support
#  +  Group info cache (1hr TTL)  — title + status, pipeline read
#  +  Heartbeat logger            — every 60s, Redis ping, flood count
# ═══════════════════════════════════════════════════════════════════════════════

import heapq as _heapq
from collections import deque as _deque

# ── In-memory runtime state ────────────────────────────────────────────────────

_gr_task: Optional[asyncio.Task] = None
_gr_queue_task: Optional[asyncio.Task] = None
_gr_hb_task: Optional[asyncio.Task] = None   # heartbeat — tracked for clean shutdown

# Per-group rate limiting
_gr_msg_timestamps: dict[int, _deque] = {}   # chat_id → deque of send timestamps
_gr_cooldown_until: dict[int, float]  = {}   # chat_id → cooldown-until timestamp
_GR_MAX_PER_MIN   = 18       # stay under Telegram's ~20/min per-chat limit
_GR_INTER_DELAY   = 0.4      # seconds between any two sends (≤2.5/sec global)

# Priority async send queue
# Priority levels: 1=urgent, 2=normal broadcasts, 3=manual, 4=global repeat
_gr_send_queue: list = []    # heapq of (priority, seq, chat_id, text, future)
_gr_send_seq   = 0           # monotonic tie-breaker

# Config cache
_gr_cfg_cache:      dict  = {}
_gr_cfg_fetched_at: float = 0.0
_GR_CFG_TTL = 60.0

# Groups cache
_gr_groups_cache:      list  = []
_gr_groups_fetched_at: float = 0.0
_GR_GROUPS_TTL = 300.0

# next_send schedule (in-memory + persisted to Redis)
_gr_next_send: dict[int, float] = {}

# Error tracking (last 20 per group)
_gr_runtime_errors: dict[int, list] = {}

# Flood wait counter (for heartbeat logging)
_gr_flood_count = 0


# ══════════════════════════════════════════════════════════════════════════════
#  PER-GROUP RATE LIMITING
# ══════════════════════════════════════════════════════════════════════════════

def _gr_is_allowed(chat_id: int) -> bool:
    """True = safe to send to this group right now (rate limit + cooldown check)."""
    now = time.time()
    if now < _gr_cooldown_until.get(chat_id, 0):
        return False
    ts = _gr_msg_timestamps.setdefault(chat_id, _deque())
    cutoff = now - 60
    while ts and ts[0] < cutoff:
        ts.popleft()
    return len(ts) < _GR_MAX_PER_MIN


def _gr_record_send(chat_id: int) -> None:
    """Record a successful send in the sliding window."""
    _gr_msg_timestamps.setdefault(chat_id, _deque()).append(time.time())


def _gr_set_cooldown(chat_id: int, retry_after: int) -> None:
    """Set per-group 429 cooldown. Never blocks other groups."""
    global _gr_flood_count
    _gr_cooldown_until[chat_id] = time.time() + retry_after + 2
    _gr_flood_count += 1
    logger.warning("GR FloodWait %ds for group %d (total floods: %d)",
                   retry_after, chat_id, _gr_flood_count)


# ══════════════════════════════════════════════════════════════════════════════
#  PRIORITY ASYNC SEND QUEUE
#  Lower priority number = higher urgency (heapq is min-heap).
#  Each item: (priority, seq, chat_id, text, asyncio.Future | None)
#  The queue worker consumes items in priority order, respecting per-group
#  rate limits. Items for rate-limited groups are re-queued with +0.001.
# ══════════════════════════════════════════════════════════════════════════════

async def _gr_send_queue_worker() -> None:
    """Async priority queue worker — drains queue respecting per-group limits."""
    global _gr_send_seq
    logger.info("GR send queue worker started ✓")
    while True:
        try:
            if not _gr_send_queue:
                await asyncio.sleep(0.05)
                continue

            # Pop highest-priority item
            priority, seq, chat_id, text, fut = _heapq.heappop(_gr_send_queue)

            # Per-group rate limit check
            if not _gr_is_allowed(chat_id):
                # Re-queue with slightly lower priority to avoid starvation
                _heapq.heappush(_gr_send_queue, (priority + 0.001, seq, chat_id, text, fut))
                await asyncio.sleep(0.05)
                continue

            # Send
            result = await _gr_do_send(chat_id, text)

            # Resolve future if caller is waiting
            if fut is not None and not fut.done():
                fut.set_result(result)

            await asyncio.sleep(_GR_INTER_DELAY)

        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error("GR queue worker error: %s", e)
            await asyncio.sleep(1)


async def _gr_enqueue(
    chat_id: int,
    text: str,
    priority: int = 4,
    wait: bool = False,
) -> Optional[int]:
    """
    Enqueue a message for sending.
    priority: 1=urgent 2=normal 3=manual 4=global-repeat
    wait=True: block until sent and return message_id.
    wait=False: fire-and-forget, returns None immediately.
    """
    global _gr_send_seq
    _gr_send_seq += 1
    fut: Optional[asyncio.Future] = None
    if wait:
        fut = asyncio.get_running_loop().create_future()
    _heapq.heappush(_gr_send_queue, (priority, _gr_send_seq, chat_id, text, fut))
    if wait and fut is not None:
        try:
            return await asyncio.wait_for(fut, timeout=180.0)
        except asyncio.TimeoutError:
            return None
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  CORE SEND / DELETE
# ══════════════════════════════════════════════════════════════════════════════

async def _gr_do_send(
    chat_id: int,
    text: str,
    on_flood=None,    # async callable(chat_id, retry_after) — for reschedule
) -> Optional[int]:
    """
    Execute a send with full error handling.
    Returns message_id on success, None on any failure.
    Never raises.
    """
    for attempt in range(3):
        try:
            sent = await bot.send_message(chat_id, text, disable_web_page_preview=True)
            _gr_record_send(chat_id)
            await _gr_clear_error(chat_id)
            return sent.id

        except FloodWait as fw:
            _gr_set_cooldown(chat_id, fw.value)
            if on_flood:
                await on_flood(chat_id, fw.value)
            return None   # skip; cooldown prevents immediate retry

        except Exception as e:
            err = str(e).lower()
            if any(k in err for k in ("forbidden", "kicked", "not a member",
                                      "chat not found", "deactivated", "bot was blocked")):
                logger.info("GR: bot removed from group %d — unregistering", chat_id)
                await gr_remove_group(chat_id)
                _gr_invalidate_groups()
                return None
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                await _gr_track_error(chat_id, "send", str(e))
                return None
    return None


async def _gr_safe_delete(chat_id: int, msg_id: int) -> None:
    """Delete a message, ignoring all errors."""
    try:
        await bot.delete_messages(chat_id, msg_id)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG + GROUPS CACHING  (one Redis pipeline per TTL window)
# ══════════════════════════════════════════════════════════════════════════════

def _gr_invalidate_config() -> None:
    global _gr_cfg_fetched_at
    _gr_cfg_fetched_at = 0.0


def _gr_invalidate_groups() -> None:
    global _gr_groups_fetched_at
    _gr_groups_fetched_at = 0.0


async def _gr_refresh_config() -> bool:
    """Pipeline-read all 5 config keys. Returns enabled bool. Caches 60s."""
    global _gr_cfg_cache, _gr_cfg_fetched_at
    now = time.time()
    if now - _gr_cfg_fetched_at < _GR_CFG_TTL and _gr_cfg_cache:
        return _gr_cfg_cache.get("enabled", False)
    try:
        pipe = _r().pipeline()
        pipe.get("gr_repeat_enabled")
        pipe.get("gr_repeat_text")
        pipe.get("gr_repeat_interval")
        pipe.get("gr_repeat_autodelete")
        pipe.get("gr_repeat_selfdelete")
        r0, r1, r2, r3, r4 = await pipe.execute()
        _gr_cfg_cache = {
            "enabled":    r0 == "1",
            "text":       r1,
            "interval":   int(r2) if r2 else 3600,
            "autodelete": r3 == "1",
            "selfdelete": int(r4) if r4 else None,
        }
        _gr_cfg_fetched_at = time.time()
    except Exception as e:
        logger.error("GR config refresh failed: %s", e)
    return _gr_cfg_cache.get("enabled", False)


async def _gr_get_groups_cached() -> list:
    """Groups list from cache (refreshed every 5 min)."""
    global _gr_groups_cache, _gr_groups_fetched_at
    now = time.time()
    if now - _gr_groups_fetched_at < _GR_GROUPS_TTL and _gr_groups_cache:
        return list(_gr_groups_cache)
    try:
        members = await _r().smembers("gr_groups")
        _gr_groups_cache = [int(m) for m in members]
        _gr_groups_fetched_at = time.time()
    except Exception as e:
        logger.error("GR get_groups failed: %s", e)
    return list(_gr_groups_cache)


# ══════════════════════════════════════════════════════════════════════════════
#  ERROR TRACKING
# ══════════════════════════════════════════════════════════════════════════════

async def _gr_track_error(chat_id: int, context: str, error: str) -> None:
    msg = f"[{context}] {error[:200]}"
    errors = _gr_runtime_errors.setdefault(chat_id, [])
    errors.append(msg)
    if len(errors) > 20:
        _gr_runtime_errors[chat_id] = errors[-20:]
    try:
        await _r().sadd("gr_groups_with_errors", str(chat_id))
        await _r().set(f"gr_group_error:{chat_id}", error[:200])
    except Exception:
        pass
    logger.error("GR group %d [%s]: %s", chat_id, context, error[:100])


async def _gr_clear_error(chat_id: int) -> None:
    _gr_runtime_errors.pop(chat_id, None)
    try:
        await _r().srem("gr_groups_with_errors", str(chat_id))
        await _r().delete(f"gr_group_error:{chat_id}")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
#  SENT MESSAGES TRACKING
# ══════════════════════════════════════════════════════════════════════════════

async def gr_save_sent(chat_id: int, msg_id: int) -> None:
    """Track sent msg_id per group. Max 100, 7-day TTL. Pipeline write."""
    key = f"gr_sent:{chat_id}"
    try:
        pipe = _r().pipeline()
        pipe.lpush(key, str(msg_id))
        pipe.ltrim(key, 0, 99)
        pipe.expire(key, 604800)
        await pipe.execute()
    except Exception:
        pass


async def gr_get_sent(chat_id: int) -> list:
    try:
        msgs = await _r().lrange(f"gr_sent:{chat_id}", 0, -1)
        return [int(m) for m in msgs if m.isdigit()]
    except Exception:
        return []


async def gr_clear_sent(chat_id: int) -> None:
    try:
        await _r().delete(f"gr_sent:{chat_id}")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
#  GROUP INFO CACHE  (title + status, 1-hour TTL, pipeline read)
# ══════════════════════════════════════════════════════════════════════════════

async def gr_get_group_info(chat_id: int) -> tuple:
    """(title, status) — cached 1 hour, falls back to Telegram API on miss."""
    try:
        pipe = _r().pipeline()
        pipe.get(f"gr_group_title:{chat_id}")
        pipe.get(f"gr_group_status:{chat_id}")
        ct, cs = await pipe.execute()
        if ct and cs:
            return ct, cs
    except Exception:
        pass
    title = f"Group {chat_id}"
    status = "Unknown"
    try:
        chat   = await bot.get_chat(chat_id)
        title  = chat.title or title
        member = await bot.get_chat_member(chat_id, _bot_id or (await bot.get_me()).id)
        status = "Admin" if member.status.value in ("administrator", "creator") else "Member"
        pipe   = _r().pipeline()
        pipe.set(f"gr_group_title:{chat_id}",  title,  ex=3600)
        pipe.set(f"gr_group_status:{chat_id}", status, ex=3600)
        await pipe.execute()
    except Exception:
        pass
    return title, status


# ══════════════════════════════════════════════════════════════════════════════
#  FLOODWAIT RESCHEDULE CALLBACK
#  When group hits 429: push its next_send forward by retry_after + 5s.
#  Persisted to Redis so restart doesn't cause immediate retry.
# ══════════════════════════════════════════════════════════════════════════════

async def _gr_on_flood(chat_id: int, retry_after: int) -> None:
    reschedule_to = time.time() + retry_after + 5
    _gr_next_send[chat_id] = reschedule_to
    try:
        interval = _gr_cfg_cache.get("interval", 3600)
        await _r().set(
            f"gr_next_send:{chat_id}",
            str(reschedule_to),
            ex=interval * 3 + 60,
        )
    except Exception:
        pass
    logger.info("GR group %d rescheduled +%ds after FloodWait", chat_id, retry_after + 5)


# ══════════════════════════════════════════════════════════════════════════════
#  HEARTBEAT  (logs key metrics every 60s)
# ══════════════════════════════════════════════════════════════════════════════

async def _gr_heartbeat_worker() -> None:
    """Logs system health every 60 seconds."""
    while True:
        try:
            await asyncio.sleep(60)
            worker_alive = _gr_task is not None and not _gr_task.done()
            group_count  = await gr_group_count()
            try:
                await _r().ping()
                redis_ok = "OK"
            except Exception:
                redis_ok = "ERROR"
            logger.info(
                "HEARTBEAT | repeat_worker=%s | groups=%d | redis=%s | floods=%d",
                "RUNNING" if worker_alive else "STOPPED",
                group_count,
                redis_ok,
                _gr_flood_count,
            )
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error("Heartbeat error: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN WORKER
#  Direct port of main-6.py _global_repeat_worker() to asyncio.
#  All logic identical — threading replaced with await/asyncio.sleep.
# ══════════════════════════════════════════════════════════════════════════════

async def _global_repeat_worker() -> None:
    logger.info("Global repeat worker started ✓")

    while True:
        try:
            # 1. Refresh config (pipeline, cached 60s)
            enabled = await _gr_refresh_config()
            if not enabled:
                logger.info("Global repeat: disabled flag — stopping.")
                return

            cfg        = _gr_cfg_cache
            text       = cfg.get("text")
            interval   = cfg.get("interval", 3600)
            autodelete = cfg.get("autodelete", False)   # delete prev before sending
            selfdelete = cfg.get("selfdelete")           # delete sent msg after N secs

            if not text:
                logger.warning("Global repeat: no text set — sleeping 60s.")
                await asyncio.sleep(60)
                continue

            # 2. Get groups (cached 5 min)
            groups = await _gr_get_groups_cached()
            if not groups:
                await asyncio.sleep(30)
                continue

            sent_this_tick = False

            # 3. Iterate groups
            for chat_id in groups:

                # In-memory enabled check — no Redis read
                if not _gr_cfg_cache.get("enabled", False):
                    logger.info("Global repeat: stopped mid-loop.")
                    return

                # Load persisted next_send on first encounter
                if chat_id not in _gr_next_send:
                    try:
                        raw = await _r().get(f"gr_next_send:{chat_id}")
                        _gr_next_send[chat_id] = float(raw) if raw else 0.0
                    except Exception:
                        _gr_next_send[chat_id] = 0.0

                now = time.time()

                # Not due yet — skip
                if now < _gr_next_send.get(chat_id, 0):
                    continue

                # Per-group rate limit check (exactly as main-6.py line 650)
                if not _gr_is_allowed(chat_id):
                    _gr_next_send[chat_id] = now + 5   # bump to avoid tight loop
                    continue

                # Schedule NEXT send BEFORE sending — prevents double-send on restart
                prev_next = _gr_next_send.get(chat_id, 0)
                new_next  = (prev_next + interval) if prev_next > 0 else (now + interval)
                _gr_next_send[chat_id] = new_next
                try:
                    await _r().set(
                        f"gr_next_send:{chat_id}",
                        str(new_next),
                        ex=interval * 3 + 60,
                    )
                except Exception:
                    pass

                # AUTO-DELETE previous message (main-6.py line 664)
                if autodelete:
                    prev_id = await gr_get_last_sent(chat_id)
                    if prev_id:
                        await _gr_safe_delete(chat_id, prev_id)

                # FloodWait callback — reschedules group on 429, never blocks others
                on_flood = _gr_on_flood

                # Send via do_send with flood callback
                msg_id = await _gr_do_send(chat_id, text, on_flood=on_flood)

                if msg_id:
                    # Persist last sent (for auto-delete next cycle)
                    await gr_set_last_sent(chat_id, msg_id)
                    # Audit trail
                    await gr_save_sent(chat_id, msg_id)
                    sent_this_tick = True

                    # SELF-DELETE after N seconds (main-6.py line 680)
                    if selfdelete:
                        async def _self_del(cid=chat_id, mid=msg_id, d=selfdelete):
                            await asyncio.sleep(d)
                            await _gr_safe_delete(cid, mid)
                        t = asyncio.create_task(_self_del())
                        t.add_done_callback(
                            lambda _t: _t.exception() if not _t.cancelled() else None
                        )

                await asyncio.sleep(_GR_INTER_DELAY)

            # 4. Nothing due this cycle — wait 10s (matches main-6.py line 690)
            if not sent_this_tick:
                await asyncio.sleep(10)

        except asyncio.CancelledError:
            logger.info("Global repeat worker cancelled.")
            return
        except Exception as e:
            # Top-level safety net — worker must NEVER crash permanently
            logger.error("Global repeat worker error (will retry in 30s): %s", e)
            await asyncio.sleep(30)


async def gr_start_worker() -> None:
    """Start worker + queue worker + heartbeat. Idempotent."""
    global _gr_task, _gr_queue_task, _gr_hb_task
    _gr_invalidate_config()

    # Start priority send queue worker
    if _gr_queue_task is None or _gr_queue_task.done():
        _gr_queue_task = asyncio.create_task(_gr_send_queue_worker())
        _gr_queue_task.add_done_callback(
            lambda t: t.exception() if not t.cancelled() else None
        )

    # Start heartbeat (one instance — idempotent)
    if _gr_hb_task is None or _gr_hb_task.done():
        _gr_hb_task = asyncio.create_task(_gr_heartbeat_worker())
        _gr_hb_task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)

    # Start main repeat worker (only if enabled)
    if not await gr_is_enabled():
        return
    if _gr_task and not _gr_task.done():
        return
    _gr_task = asyncio.create_task(_global_repeat_worker())
    _gr_task.add_done_callback(
        lambda t: t.exception() if not t.cancelled() else None
    )
    logger.info("Global repeat task started ✓")


async def gr_stop_worker() -> None:
    """Stop all worker tasks cleanly — prevents 'Task destroyed but pending' warnings."""
    global _gr_task, _gr_queue_task, _gr_hb_task
    _gr_invalidate_config()

    for task, name in [
        (_gr_task,       "repeat worker"),
        (_gr_queue_task, "send queue worker"),
        (_gr_hb_task,    "heartbeat"),
    ]:
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    _gr_task = _gr_queue_task = _gr_hb_task = None
    logger.info("Global repeat worker stopped.")


# ═══════════════════════════════════════════════════════════════════════════════
#  GROUP EVENT HANDLERS
#  Bot added to / removed from groups — registration and owner notification
# ═══════════════════════════════════════════════════════════════════════════════

@bot.on_message(
    (filters.new_chat_members) & (filters.group | filters.channel)
)
async def handle_new_chat_members(_: Client, message: Message) -> None:
    """Fires when any member joins a group the bot is in."""
    bot_me = await bot.get_me()

    # Check if the bot itself just joined
    bot_joined = any(
        m.id == bot_me.id
        for m in (message.new_chat_members or [])
    )

    if not bot_joined:
        return

    chat_id = message.chat.id

    # Register the group
    await gr_add_group(chat_id)

    # Build detailed notification for the owner
    title        = message.chat.title or f"Group {chat_id}"
    chat_type    = str(message.chat.type).split(".")[-1].title()
    member_count = "N/A"
    admins_text  = "Could not fetch"
    invite_link  = None
    bot_is_admin = False

    # Cache the group title immediately
    try:
        pipe = _r().pipeline()
        pipe.set(f"gr_group_title:{chat_id}", title, ex=3600)
        await pipe.execute()
    except Exception:
        pass

    try:
        member_count = await bot.get_chat_members_count(chat_id)
    except Exception:
        pass

    try:
        bot_member   = await bot.get_chat_member(chat_id, bot_me.id)
        bot_is_admin = bot_member.status.value in ("administrator", "creator")
    except Exception:
        pass

    try:
        chat_full = await bot.get_chat(chat_id)
        invite_link = chat_full.invite_link
    except Exception:
        pass

    try:
        from pyrogram.enums import ChatMembersFilter
        admin_list = []
        async for a in bot.get_chat_members(chat_id, filter=ChatMembersFilter.ADMINISTRATORS):
            u     = a.user
            role  = "👑 Owner" if a.status.value == "creator" else "🛡 Admin"
            name  = f"{u.first_name or ''} {u.last_name or ''}".strip() or f"User {u.id}"
            uname = f" @{u.username}" if u.username else ""
            admin_list.append(f"{role}: {name}{uname}")
        admins_text = "\n".join(admin_list) if admin_list else "None visible"
    except Exception as e:
        admins_text = f"Could not fetch ({e})"

    adder = message.from_user
    adder_info = (
        f"{((adder.first_name or '') + ' ' + (adder.last_name or '')).strip() or 'Unknown'} (<code>{adder.id}</code>)"
        if adder else "Unknown"
    )

    notification = (
        f"✅ <b>Bot added to a new group!</b>\n\n"
        f"📌 <b>Title:</b> {title}\n"
        f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n"
        f"📂 <b>Type:</b> {chat_type}\n"
        f"👥 <b>Members:</b> {member_count}\n"
        f"🤖 <b>Bot status:</b> {'Admin ✅' if bot_is_admin else 'Member (no admin rights)'}\n"
        f"👤 <b>Added by:</b> {adder_info}\n"
    )

    if invite_link and bot_is_admin:
        notification += f"🔗 <b>Link:</b> {invite_link}\n"

    notification += f"\n<b>Admins:</b>\n{admins_text}"

    kb_rows = []
    if not bot_is_admin:
        kb_rows.append([InlineKeyboardButton(
            "⚠️ Bot has no admin rights",
            callback_data="gr_no_admin_info"
        )])
    kb_rows.append([InlineKeyboardButton("🔁 Group Repeat Menu", callback_data="gr_menu")])

    try:
        await bot.send_message(
            Config.OWNER_ID,
            notification,
            reply_markup=InlineKeyboardMarkup(kb_rows) if kb_rows else None,
        )
    except Exception as e:
        logger.error("Could not notify owner about new group %d: %s", chat_id, e)

    await log_to_channel(
        bot,
        f"➕ <b>Added to group</b>\n{title} (<code>{chat_id}</code>)\nAdded by: {adder_info}"
    )


@bot.on_message(
    (filters.left_chat_member) & (filters.group | filters.channel)
)
async def handle_left_chat_member(_: Client, message: Message) -> None:
    """Fires when the bot is removed from a group."""
    if not message.left_chat_member:
        return
    bot_me = await bot.get_me()
    if message.left_chat_member.id != bot_me.id:
        return

    chat_id = message.chat.id
    title   = message.chat.title or f"Group {chat_id}"
    await gr_remove_group(chat_id)
    logger.info("Bot removed from group %d (%s) — unregistered.", chat_id, title)

    try:
        await bot.send_message(
            Config.OWNER_ID,
            f"🚫 <b>Bot was removed from a group</b>\n\n"
            f"📌 <b>Title:</b> {title}\n"
            f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n\n"
            f"This group has been removed from the repeat list.",
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
#  GLOBAL REPEAT MENU  (owner-only, via /groups command or inline button)
# ═══════════════════════════════════════════════════════════════════════════════

# ── /groups command ───────────────────────────────────────────────────────────

@bot.on_message(filters.command("groups") & owner_filter & filters.private)
async def cmd_groups(_: Client, message: Message) -> None:
    """Open the group repeat management panel."""
    await _send_gr_menu(message.chat.id)


async def _send_gr_menu(chat_id: int, edit_msg_id: Optional[int] = None) -> None:
    """Build and send (or edit) the global repeat menu. Always has a Back button."""
    enabled    = await gr_is_enabled()
    text_val   = await gr_get_text()
    interval   = await gr_get_interval()
    autodel    = await gr_get_autodelete()
    selfdel    = await gr_get_selfdelete()
    grp_count  = await gr_group_count()
    worker_run = _gr_task is not None and not _gr_task.done()

    interval_str = time_formatter(interval)
    text_preview = (text_val[:60] + "…") if text_val and len(text_val) > 60 else (text_val or "❌ Not set")

    status_icon = "✅ Running" if (enabled and worker_run) else ("⏸ Starting…" if enabled else "❌ Stopped")
    menu_text = (
        f"🔁 <b>Global Repeat Broadcast</b>\n"
        f"<i>Keeps Render alive + broadcasts to all groups</i>\n\n"
        f"<b>Status:</b>   {status_icon}\n"
        f"<b>Groups:</b>   {grp_count}\n"
        f"<b>Interval:</b> {interval_str}\n"
        f"<b>Message:</b>  {text_preview}\n"
        f"<b>Auto-del:</b> {'✅ ON' if autodel else '❌ OFF'}\n"
        f"<b>Self-del:</b> {f'{selfdel}s ({time_formatter(selfdel)})' if selfdel else '❌ OFF'}\n\n"
        f"<i>Add the bot to groups as admin — it will post at the set interval.</i>"
    )

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Turn ON" if not enabled else "⏹ Turn OFF",
                                 callback_data="gr_on" if not enabled else "gr_off"),
        ],
        [
            InlineKeyboardButton("✏️ Set Message",   callback_data="gr_settext"),
            InlineKeyboardButton("⏱ Set Interval",   callback_data="gr_setinterval"),
        ],
        [
            InlineKeyboardButton(
                f"🗑 Auto-delete: {'ON ✅' if autodel else 'OFF ❌'}",
                callback_data="gr_toggle_autodelete"
            ),
        ],
        [
            InlineKeyboardButton("💣 Self-Delete Timer", callback_data="gr_set_selfdelete"),
            InlineKeyboardButton("❌ Remove Self-Del",    callback_data="gr_remove_selfdelete"),
        ],
        [
            InlineKeyboardButton("👥 View Groups",    callback_data="gr_viewgroups"),
            InlineKeyboardButton("🔄 Reset Schedule", callback_data="gr_reset_schedule"),
        ],
        [
            InlineKeyboardButton("🔙 Back to Main",   callback_data="cb_start"),   # ← BACK BUTTON
        ],
    ])

    if edit_msg_id:
        try:
            await bot.edit_message_text(
                chat_id, edit_msg_id, menu_text, reply_markup=kb
            )
            await set_last_bot_msg(chat_id, edit_msg_id)
            return
        except Exception:
            pass

    # send_menu deletes any previous bot message before sending this one
    await send_menu(chat_id, menu_text, reply_markup=kb)


# ── Repeat menu callbacks ─────────────────────────────────────────────────────

@bot.on_callback_query(filters.regex(r"^gr_") & filters.create(
    lambda _, __, q: q.from_user.id == Config.OWNER_ID
))
async def gr_callback(_: Client, query: CallbackQuery) -> None:
    data     = query.data
    chat_id  = query.message.chat.id
    msg_id   = query.message.id

    async def answer(text: str = "", alert: bool = False) -> None:
        try:
            await query.answer(text, show_alert=alert)
        except Exception:
            pass

    async def reload_menu() -> None:
        await answer()
        await _send_gr_menu(chat_id, edit_msg_id=msg_id)

    # ── ON ────────────────────────────────────────────────────────────────────
    if data == "gr_on":
        text_val = await gr_get_text()
        if not text_val:
            await answer("⚠️ Set a repeat message first!", alert=True)
            return
        await gr_set_enabled(True)
        await gr_reset_all_schedules()
        await gr_start_worker()
        await answer("✅ Global repeat started!")
        await reload_menu()

    # ── OFF ───────────────────────────────────────────────────────────────────
    elif data == "gr_off":
        await gr_set_enabled(False)   # persist OFF state — survives restarts
        await gr_stop_worker()
        await answer("⏹ Global repeat stopped.")
        await reload_menu()

    # ── SET MESSAGE ───────────────────────────────────────────────────────────
    elif data == "gr_settext":
        await _r().set(f"gr_awaiting:{Config.OWNER_ID}", "text", ex=300)
        await answer()
        await query.message.edit_text(
            "✏️ <b>Set Repeat Message</b>\n\n"
            "Send the message you want posted in all groups.\n"
            "Supports text, emoji, and links.\n\n"
            "⬅️ Send <code>cancel</code> or tap ❌ Cancel to go back.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="gr_cancel_input")
            ]]),
        )

    # ── SET INTERVAL ─────────────────────────────────────────────────────────
    elif data == "gr_setinterval":
        await answer()
        await query.message.edit_text(
            "⏱ <b>Set Repeat Interval</b>\n\nChoose the time unit:",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🕐 Minutes", callback_data="gr_interval_min"),
                    InlineKeyboardButton("🕑 Hours",   callback_data="gr_interval_hr"),
                ],
                [InlineKeyboardButton("🔙 Back", callback_data="gr_menu")],
            ]),
        )

    elif data in ("gr_interval_min", "gr_interval_hr"):
        unit     = "minutes" if data == "gr_interval_min" else "hours"
        # Store full key name to match the interceptor check
        await _r().set(f"gr_awaiting:{Config.OWNER_ID}", f"interval_{unit}", ex=300)
        await answer()
        min_val = "1" if unit == "hours" else "10"
        example = f"<code>1</code> = 1 {unit[:-1]}" if unit == "hours" else f"<code>30</code> = 30 min"
        await query.message.edit_text(
            f"⏱ <b>Set Interval ({unit.title()})</b>\n\n"
            f"Send a number. Example: {example}\n\n"
            f"Minimum: {min_val} {unit}\n\n"
            f"⬅️ Send <code>cancel</code> or tap ❌ Cancel to go back.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="gr_cancel_input")
            ]]),
        )

    # ── TOGGLE AUTO-DELETE ────────────────────────────────────────────────────
    elif data == "gr_toggle_autodelete":
        current = await gr_get_autodelete()
        await gr_set_autodelete(not current)
        await answer(f"🗑 Auto-delete prev: {'✅ ON' if not current else '❌ OFF'}")
        await reload_menu()

    # ── SET SELF-DELETE ───────────────────────────────────────────────────────
    elif data == "gr_set_selfdelete":
        await _r().set(f"gr_awaiting:{Config.OWNER_ID}", "selfdelete", ex=300)
        await answer()
        await query.message.edit_text(
            "💣 <b>Set Self-Delete Delay</b>\n\n"
            "Send the delay in <b>seconds</b> after which each posted\n"
            "message will auto-delete from the group.\n\n"
            "Examples:\n"
            "• <code>300</code>  → 5 minutes\n"
            "• <code>3600</code> → 1 hour\n"
            "• <code>86400</code> → 24 hours\n\n"
            "⬅️ Send <code>cancel</code> or tap ❌ Cancel to go back.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="gr_cancel_input")
            ]]),
        )

    # ── REMOVE SELF-DELETE ────────────────────────────────────────────────────
    elif data == "gr_remove_selfdelete":
        await gr_set_selfdelete(None)
        await answer("✅ Self-delete removed.")
        await reload_menu()

    # ── CANCEL INPUT ─────────────────────────────────────────────────────────
    elif data == "gr_cancel_input":
        await _r().delete(f"gr_awaiting:{Config.OWNER_ID}")
        await answer("❌ Cancelled.")
        await reload_menu()

    # ── VIEW GROUPS ───────────────────────────────────────────────────────────
    elif data == "gr_viewgroups":
        await answer()
        groups = await gr_get_groups()
        if not groups:
            await query.message.edit_text(
                "👥 <b>No groups registered yet.</b>\n\n"
                "Add this bot to your Telegram groups as an admin and they'll\n"
                "appear here automatically.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data="gr_menu")
                ]]),
            )
            return

        # Live validation pass — show progress indicator first
        try:
            await query.message.edit_text(
                f"🔍 <b>Checking {len(groups)} group(s)…</b>\n"
                f"<i>Validating membership and removing dead groups…</i>"
            )
        except Exception:
            pass

        bot_uid = _bot_id or (await bot.get_me()).id
        removed: list[int] = []
        live:    list[int] = []

        for g in groups:
            try:
                member = await bot.get_chat_member(g, bot_uid)
                # Reachable — refresh status cache immediately so display is current
                status = "Admin" if member.status.value in ("administrator", "creator") else "Member"
                try:
                    pipe = _r().pipeline()
                    pipe.set(f"gr_group_status:{g}", status, ex=3600)
                    await pipe.execute()
                except Exception:
                    pass
                live.append(g)
            except Exception as e:
                err = str(e).lower()
                # Known dead-group signals: bot removed, group deleted, deactivated, etc.
                if any(k in err for k in (
                    "forbidden", "kicked", "not a member", "chat not found",
                    "deactivated", "bot was blocked", "channel invalid",
                    "peer id invalid", "user not participant",
                )):
                    await gr_remove_group(g)
                    _gr_invalidate_groups()
                    removed.append(g)
                    logger.info("GR viewgroups: auto-removed dead group %d — %s", g, err[:80])
                else:
                    # Unknown/transient error — keep in list, don't auto-remove
                    live.append(g)

        removed_note = (
            f"\n\n🗑 <i>Auto-removed {len(removed)} dead group(s).</i>"
            if removed else ""
        )

        if not live:
            await query.message.edit_text(
                f"👥 <b>No active groups remaining.</b>{removed_note}\n\n"
                "Add this bot to your Telegram groups as an admin.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data="gr_menu")
                ]]),
            )
            return

        lines = []
        now   = time.time()
        for g in live:
            title, status = await gr_get_group_info(g)
            cooldown  = _gr_cooldown_until.get(g, 0)
            next_ts   = _gr_next_send.get(g) or await gr_get_next_send(g)
            err       = (_gr_runtime_errors.get(g) or [None])[-1]

            if cooldown > now:
                send_status = f"⏳ FloodWait {int(cooldown - now)}s"
            elif next_ts > now:
                send_status = f"⏰ in {time_formatter(next_ts - now)}"
            else:
                send_status = "🟢 ready"

            role_icon = "🛡" if status == "Admin" else "👁"
            err_line  = f"\n  ⚠️ <i>{err[-80:]}</i>" if err else ""
            lines.append(
                f"• {role_icon} <b>{title}</b>\n"
                f"  <code>{g}</code> — {send_status}{err_line}"
            )

        text = (
            f"👥 <b>Registered Groups ({len(live)})</b>{removed_note}\n\n"
            + "\n\n".join(lines)
        )
        if len(text) > 4000:
            text = text[:3990] + "\n…(truncated)"

        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑 Delete All Sent (All Groups)", callback_data="gr_purge_all")],
                [InlineKeyboardButton("🔙 Back", callback_data="gr_menu")],
            ]),
        )

    # ── PURGE ALL SENT MESSAGES ───────────────────────────────────────────────
    elif data == "gr_purge_all":
        await answer("🗑 Deleting all tracked messages…")
        groups = await gr_get_groups()
        deleted = failed = 0
        for g in groups:
            for mid in await gr_get_sent(g):
                try:
                    await bot.delete_messages(g, mid)
                    deleted += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)
            await gr_clear_sent(g)
        await query.message.edit_text(
            f"🗑 <b>Purge complete!</b>\n\n"
            f"✅ Deleted: {deleted}\n"
            f"❌ Failed (already gone): {failed}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="gr_viewgroups")
            ]]),
        )

    # ── RESET SCHEDULE ────────────────────────────────────────────────────────
    elif data == "gr_reset_schedule":
        await gr_reset_all_schedules()
        _gr_next_send.clear()   # also clear in-memory cache
        await answer("✅ All group schedules reset — next cycle will post immediately.", alert=True)
        await reload_menu()

    # ── BACK TO MENU ──────────────────────────────────────────────────────────
    elif data == "gr_menu":
        await reload_menu()

    # ── INFO BUTTON ───────────────────────────────────────────────────────────
    elif data == "gr_no_admin_info":
        await answer(
            "Make the bot an admin with 'Delete Messages' permission\n"
            "for auto-delete to work. Posting still works without it.",
            alert=True
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    global _redis, _backup_lock, _bot_id

    # Create asyncio.Lock here — event loop is now running, no DeprecationWarning.
    _backup_lock = asyncio.Lock()

    # ── Global asyncio exception handler ────────────────────────────────────
    # Catches ALL unhandled task exceptions. Prevents any single task from
    # crashing the entire bot. Just logs the error and continues.
    def _handle_exception(loop, context):
        exc = context.get("exception")
        msg = context.get("message", "Unknown asyncio error")
        # Suppress expected CancelledError and "Task destroyed but pending"
        # These occur during clean shutdown and are harmless
        if isinstance(exc, asyncio.CancelledError):
            return
        if "Task was destroyed but it is pending" in msg:
            return
        if "Task destroyed" in msg:
            return
        future_str = str(context.get("future", ""))
        if any(k in future_str for k in ("auto_delete", "_self_del", "heartbeat", "queue")):
            return
        if exc:
            logger.error("Unhandled async exception: %s — %s", type(exc).__name__, exc)
        else:
            logger.error("Asyncio error: %s", msg)

    _loop.set_exception_handler(_handle_exception)

    # 1. Redis
    logger.info("Connecting to Redis…")
    _redis = aioredis.from_url(
        Config.REDIS_URL, encoding="utf-8", decode_responses=True,
        max_connections=20, socket_keepalive=True, retry_on_timeout=True,
    )
    await _redis.ping()
    logger.info("Redis connected ✓")

    # 2. Start both clients
    logger.info("Starting Pyrogram clients…")
    await bot.start()
    await user.start()
    bot_me  = await bot.get_me()
    user_me = await user.get_me()
    _bot_id = bot_me.id   # cache — used by gr_get_group_info() to avoid repeated get_me() calls
    logger.info("Bot  client: @%s (%d) ✓", bot_me.username,  bot_me.id)
    logger.info("User client: @%s (%d) ✓", user_me.username, user_me.id)

    # 3. Register bot commands — makes them visible in Telegram's
    #    command menu (bottom-left keyboard icon) immediately after deploy.
    try:
        from pyrogram.types import BotCommand
        await bot.set_bot_commands([
            BotCommand("start",       "👋 Start the bot"),
            BotCommand("help",        "📖 Help & features"),
            BotCommand("batch",       "📦 Batch download links"),
            BotCommand("cancel",      "🛑 Cancel active batch"),
            BotCommand("setthumb",    "🖼 Set custom thumbnail"),
            BotCommand("delthumb",    "🗑 Remove thumbnail"),
            BotCommand("showthumb",   "👁 Preview thumbnail"),
            BotCommand("setcaption",  "✏️ Set custom caption"),
            BotCommand("delcaption",  "❌ Remove caption"),
            BotCommand("groups",      "🔁 Group repeat panel"),
            BotCommand("stats",       "📊 Bot statistics"),
            BotCommand("backup",      "🗄 Backup Redis data now"),
            BotCommand("restore",     "♻️ Restore Redis from backup file"),
            BotCommand("recover",     "🔁 Re-register groups after data wipe"),
            BotCommand("cancel_restore", "❌ Cancel restore/recover mode"),
            BotCommand("pfp",           "📷 Download profile photos of any channel/group"),
            BotCommand("dlbot",         "📥 Download from a bot's private chat"),
            BotCommand("login",         "🔑 Connect your Telegram account"),
            BotCommand("logout",        "🚪 Remove your saved session"),
            BotCommand("dlpriv",        "🔐 Download with your own session"),
        ])
        logger.info("Bot commands registered ✓")
    except Exception as e:
        logger.warning("Could not register bot commands: %s", e)

    # Warm up the peer cache in the BACKGROUND — this is non-blocking so the bot
    # starts accepting user messages immediately after connecting, not after waiting
    # potentially 30-120 seconds for dialogs to load.
    # The user client needs dialogs to resolve private channel access_hashes,
    # but this can happen concurrently while the bot is already serving requests.
    async def _load_dialogs_bg() -> None:
        try:
            logger.info("Loading user dialogs in background (peer cache warmup)…")
            count = 0
            async for _ in user.get_dialogs():
                count += 1
            logger.info("Peer cache ready: %d dialogs loaded ✓", count)
        except Exception as e:
            logger.warning("Could not load dialogs (non-fatal): %s", e)

    bg_dialogs = asyncio.create_task(_load_dialogs_bg())
    bg_dialogs.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)

    # 4. Start global repeat worker (if it was previously enabled before restart)
    await gr_start_worker()
    if _gr_task and not _gr_task.done():
        grp_count = await gr_group_count()
        logger.info("Global repeat worker running ✓ (%d groups registered)", grp_count)
    else:
        logger.info("Global repeat: not enabled (use /groups or tap the button in /start)")

    # 4b. Start auto-backup task
    asyncio.create_task(_auto_backup_worker())
    logger.info("Auto-backup task started ✓")

    # 5. Self-ping worker — THE CRITICAL KEEP-ALIVE for Render free tier
    #
    #  Render spins down web services after 15 min of no INBOUND HTTP requests.
    #  Our bot uses Telegram long-polling — Telegram NEVER sends HTTP to us.
    #  Outbound traffic (group posts) does NOT count. Only inbound counts.
    #  This worker makes an HTTP GET to our OWN health endpoint every 10 min,
    #  generating inbound traffic that keeps Render from spinning down.
    #
    #  RENDER_EXTERNAL_URL is automatically set by Render (no manual config needed).
    #  It's the public URL of your service, e.g. https://your-bot.onrender.com

    async def _self_ping_worker() -> None:
        render_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
        if not render_url:
            logger.warning(
                "RENDER_EXTERNAL_URL not set — self-ping disabled.\n"
                "Render will spin down this service after 15 min of inactivity.\n"
                "Render sets this automatically — check your service's environment variables."
            )
            return
        logger.info("Self-ping active → %s (every 10 min)", render_url)
        while True:
            try:
                await asyncio.sleep(600)   # every 10 minutes
                if AIOHTTP_AVAILABLE:
                    import aiohttp
                    async with aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=15)
                    ) as session:
                        async with session.get(render_url) as resp:
                            logger.debug("Self-ping OK (HTTP %d)", resp.status)
                else:
                    # Fallback using urllib (no aiohttp)
                    import urllib.request
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None,
                        lambda: urllib.request.urlopen(render_url, timeout=10)
                    )
                    logger.debug("Self-ping OK (urllib)")
            except asyncio.CancelledError:
                return
            except Exception as e:
                # Non-fatal — just log at debug level
                logger.debug("Self-ping error (non-fatal): %s", e)

    ping_task = asyncio.create_task(_self_ping_worker())
    ping_task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)

    logger.info("=" * 60)
    logger.info("BOT ONLINE — accepting messages NOW (health server: port %d)", Config.PORT)
    logger.info("=" * 60)

    if Config.LOG_CHANNEL:
        try:
            await bot.send_message(
                Config.LOG_CHANNEL,
                f"🟢 <b>Bot back online</b>\n"
                f"@{bot_me.username} is ready.\n"
                f"Peer cache loading in background.",
            )
        except Exception as e:
            logger.warning("Could not log startup: %s", e)

    # 6. Keep alive — Pyrogram's idle() handles SIGINT/SIGTERM correctly
    await idle()

    # 7. Graceful shutdown with HARD TIMEOUT
    #
    # CRITICAL: Without this timeout, the process hangs for minutes after SIGTERM.
    # Root cause: yt-dlp runs in the default ThreadPoolExecutor (non-daemon threads).
    # Python waits for ALL thread pool threads to finish before exiting. If a large
    # yt-dlp download is in progress, this blocks exit for the full download duration.
    # During this hang, Render cannot start the new instance cleanly → long downtime.
    #
    # Fix: wrap shutdown in a hard 8-second deadline. If exceeded, os._exit(0) kills
    # the process immediately, bypassing Python's thread pool join entirely.
    # Render gives 30 seconds before SIGKILL anyway — we voluntarily exit in ≤8s.

    logger.info("Shutting down…")

    async def _do_shutdown() -> None:
        await gr_stop_worker()
        try:
            await asyncio.wait_for(bot.stop(),  timeout=3.0)
        except Exception:
            pass
        try:
            await asyncio.wait_for(user.stop(), timeout=3.0)
        except Exception:
            pass
        try:
            await _redis.aclose()
        except Exception:
            pass

    try:
        await asyncio.wait_for(_do_shutdown(), timeout=8.0)
        logger.info("Goodbye 👋")
    except asyncio.TimeoutError:
        logger.warning("Shutdown exceeded 8s — forcing exit now")
    except Exception as e:
        logger.error("Shutdown error: %s", e)
    finally:
        # os._exit bypasses Python's atexit hooks and thread pool join,
        # ensuring the process exits immediately regardless of pending threads.
        import os as _os
        _os._exit(0)


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Health server in a daemon thread — zero asyncio, zero loop conflict
    start_health_server()

    # _loop was created and set via asyncio.set_event_loop() ABOVE,
    # before any Client() was instantiated. This is what prevents the
    # "attached to a different loop" error permanently.
    try:
        _loop.run_until_complete(main())
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as e:
        logger.error("Fatal error in main(): %s", e)
    finally:
        # HARD EXIT at the outermost level.
        # os._exit(0) skips ALL Python cleanup: atexit hooks, thread pool joins,
        # garbage collection. This guarantees the process exits within seconds
        # of main() returning, regardless of any pending yt-dlp threads.
        # Without this, Python silently waits for ThreadPoolExecutor threads
        # (yt-dlp downloads) to finish, causing 3-7 minute restart delays.
        import os as _os
        _os._exit(0)
