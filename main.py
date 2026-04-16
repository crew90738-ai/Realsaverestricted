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

# Use a daemon-thread executor for ALL run_in_executor calls (primarily yt-dlp).
# Daemon threads are killed instantly when the process exits — they do NOT block
# Python's shutdown sequence. Without this, a running yt-dlp download would hold
# the process alive for minutes after SIGTERM, causing long restart downtime.
import concurrent.futures as _cf

class _DaemonThreadPoolExecutor(_cf.ThreadPoolExecutor):
    """ThreadPoolExecutor whose threads are daemon threads — they die with the process."""
    def _adjust_thread_count(self):
        super()._adjust_thread_count()
        for t in self._threads:
            t.daemon = True

_daemon_executor = _DaemonThreadPoolExecutor(max_workers=4, thread_name_prefix="yt_worker")
_loop.set_default_executor(_daemon_executor)

logger.info("Event loop + daemon executor created ✓")


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
    invite = (
        f"https://t.me/c/{ch.lstrip('-').removeprefix('100')}"
        if ch.lstrip("-").isdigit()
        else f"https://t.me/{ch.lstrip('@')}"
    )
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
]


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
) -> Optional[Message]:
    """Inner implementation — all real logic here, wrapped by process_single_message."""

    # ── 1. Fetch the source message ──────────────────────────────────────────
    try:
        src: Message = await user.get_messages(source_chat, msg_id)
    except ChannelPrivate:
        if status_msg:
            await status_msg.edit_text(
                "❌ <b>Private channel — no access.</b>\n"
                "The user account is not a member of this channel."
            )
        return None
    except (ChannelInvalid, UsernameInvalid, UsernameNotOccupied):
        if status_msg:
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
        logger.warning("FloodWait %ds on get_messages", fw.value)
        if status_msg:
            try:
                await status_msg.edit_text(f"⏳ Rate limited by Telegram. Waiting {fw.value}s…")
            except Exception:
                pass
        await asyncio.sleep(fw.value + 1)
        return await _process_inner(dest_chat_id, source_chat, msg_id, status_msg)
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
        if not caption.strip():
            if status_msg:
                await status_msg.edit_text(
                    "ℹ️ <b>This message has no downloadable content.</b>\n"
                    "It may be a service message or empty post."
                )
            return None
        sent = await bot.send_message(dest_chat_id, caption)
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
        # retry upload once after flood wait
        try:
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
<b>🤖 Save Restricted Content Bot</b>

<b>How to use</b>
Send any Telegram message link and I'll re-deliver the file without restrictions.

<b>Supported link formats</b>
• <code>https://t.me/channelname/123</code>  — public channel
• <code>https://t.me/c/1234567890/123</code>  — private channel / group

<b>Batch download</b>
/batch — download a range of messages at once

<b>Custom thumbnail</b>
/setthumb  — reply to a photo to set as thumbnail
/delthumb  — remove thumbnail
/showthumb — preview current thumbnail

<b>Custom caption</b>
/setcaption [text] — add caption to every file
/delcaption        — remove caption

<b>yt-dlp</b>
Send a YouTube / Instagram / TikTok / Facebook URL

<b>Commands</b>  /start  /help  /batch  /cancel
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS — /start  /help  caption
# ═══════════════════════════════════════════════════════════════════════════════

def _start_kb(is_owner: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📖 Help",  callback_data="cb_help"),
         InlineKeyboardButton("📊 Stats", callback_data="cb_stats")],
        [InlineKeyboardButton("➕ Add to Group",
                              url=f"https://t.me/{Config.BOT_USERNAME}?startgroup=true")],
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
    # send_menu deletes any previous bot menu message before sending this one
    await send_menu(
        uid,
        f"👋 <b>Hello {message.from_user.mention}!</b>\n\n"
        "Send me a <b>Telegram message link</b> or any <b>video URL</b> to get started.",
        reply_markup=_start_kb(is_owner=(uid == Config.OWNER_ID)),
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
    await edit_menu(
        query.message,
        f"👋 <b>Hello {query.from_user.mention}!</b>\n\nSend me a link or URL.",
        reply_markup=_start_kb(is_owner=(query.from_user.id == Config.OWNER_ID)),
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
    await redis_register_user(uid)
    if not await check_force_sub(bot, uid):
        return await send_force_sub_msg(bot, message)
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
        try: await status.delete()
        except: pass
        await log_to_channel(
            bot,
            f"📥 <b>Forwarded</b>\n"
            f"User: {message.from_user.mention} (<code>{uid}</code>)\n"
            f"Link: <code>{text}</code>",
        )
    else:
        # If process_single_message returned None but didn't update the status
        # (e.g. unexpected edge case), make sure we never leave "Processing..." forever
        try:
            current_text = status.text or ""
            if "Processing" in current_text:
                await status.edit_text(
                    "❌ <b>Could not retrieve this content.</b>\n"
                    "The message may have been deleted, is paid media, or is inaccessible."
                )
        except Exception:
            pass
        schedule_delete(status)



# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER — yt-dlp URL downloader  (group=1)
# ═══════════════════════════════════════════════════════════════════════════════

if YTDLP_AVAILABLE:
    @bot.on_message(filters.private & filters.text & ~filters.command(_ALL_CMDS), group=1)
    async def handle_ytdlp_url(_: Client, message: Message) -> None:
        uid  = message.from_user.id
        text = message.text.strip()
        if not is_ytdlp_url(text): return
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

            if text == "/cancel":
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
_gr_queue_task: Optional[asyncio.Task] = None

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
    _heapq.heappush(_gr_send_queue, (_gr_send_seq + priority, _gr_send_seq, chat_id, text, fut))
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
        me     = await bot.get_me()
        member = await bot.get_chat_member(chat_id, me.id)
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

                # FloodWait callback (main-6.py line 669) — reschedules on 429
                async def _on_flood_cb(cid=chat_id, itvl=interval):
                    async def _inner(c: int, retry_after: int) -> None:
                        await _gr_on_flood(c, retry_after)
                    return _inner

                on_flood = await _on_flood_cb()

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
    await gr_set_enabled(False)
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
            "⬅️ Send /cancel to go back.",
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
            f"⬅️ Send /cancel to go back.",
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
            "⬅️ Send /cancel to go back.",
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

        lines = []
        now   = time.time()
        for g in groups:
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

        text = f"👥 <b>Registered Groups ({len(groups)})</b>\n\n" + "\n\n".join(lines)
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
    global _redis

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

    # 5. Keep alive — Pyrogram's idle() handles SIGINT/SIGTERM correctly
    await idle()

    # 6. Graceful shutdown with HARD TIMEOUT
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
    except KeyboardInterrupt:
        pass
    finally:
        try:
            _loop.close()
        except Exception:
            pass
