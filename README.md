# 🔐 Save Restricted Content Bot

A production-ready Telegram bot that bypasses content restrictions using a
**dual-client architecture** (bot + user session).  Built with **Pyrogram v2**,
**Redis**, and deployable on **Render.com** in minutes.

---

## ✨ Features

| Feature | Details |
|---|---|
| Restricted content | Photos, videos, audio, documents, animations, voice, video notes |
| Public channels | `t.me/username/123` |
| Private channels | `t.me/c/1234567890/123` |
| Batch download | `/batch` — range of messages with `/cancel` support |
| Custom thumbnail | `/setthumb` / `/delthumb` / `/showthumb` |
| Custom caption | `/setcaption` / `/delcaption` |
| yt-dlp | YouTube, Instagram, TikTok, Facebook, Twitter + 1 000+ sites |
| Progress bars | Real-time download & upload speed + ETA |
| Premium system | `/addpremium` / `/removepremium` via Redis |
| Force subscribe | Optional channel gate before bot use |
| Admin broadcast | `/broadcast` to all users |
| Auto-delete | Command replies auto-delete after configurable delay |
| 4 GB uploads | Supported when user session has Telegram Premium |

---

## 📁 Project Structure

```
save-restricted-bot/
├── main.py                   # Entry point: clients, Redis, health server
├── config.py                 # All settings from environment variables
├── requirements.txt
├── Procfile                  # Render deployment command
├── .env.example              # Template — copy to .env
├── handlers/
│   ├── __init__.py           # Registers all handlers
│   ├── start.py              # /start, /help, /setcaption, /delcaption
│   ├── forward.py            # Core restricted-content forwarding
│   ├── batch.py              # /batch, /cancel
│   ├── yt_dlp_handler.py     # YouTube / Instagram / TikTok / etc.
│   ├── thumbnail.py          # /setthumb, /delthumb, /showthumb
│   └── admin.py              # /stats, /broadcast, /addpremium, /removepremium
└── utils/
    ├── __init__.py
    ├── helpers.py            # Link parsing, force-sub, auto-delete, …
    ├── progress.py           # Progress bar formatting
    └── redis_helper.py       # All Redis CRUD operations
```

---

## 🔑 Environment Variables

| Variable | Required | Description |
|---|---|---|
| `API_ID` | ✅ | From https://my.telegram.org/apps |
| `API_HASH` | ✅ | From https://my.telegram.org/apps |
| `BOT_TOKEN` | ✅ | From @BotFather |
| `SESSION_STRING` | ✅ | Pyrogram user session (see below) |
| `OWNER_ID` | ✅ | Your numeric Telegram user ID |
| `REDIS_URL` | ✅ | Render Key Value URL |
| `LOG_CHANNEL` | ➖ | Channel ID for action logs |
| `FORCE_SUB_CHANNEL` | ➖ | Channel username/ID users must join |
| `BOT_USERNAME` | ➖ | Bot username without @ |
| `BATCH_MAX` | ➖ | Max messages per /batch (default 50) |
| `AUTO_DELETE_DELAY` | ➖ | Seconds before reply auto-delete (default 60) |

---

## 🔐 Generating SESSION_STRING

The user session string lets the bot read messages from restricted channels
**using your Telegram account**.

### Option 1 — Termux (Android)

```bash
# Install Termux from F-Droid (NOT Play Store)
pkg update && pkg upgrade -y
pkg install python -y
pip install pyrogram TgCrypto

python3 - <<'EOF'
import asyncio
from pyrogram import Client

async def main():
    async with Client(
        "my_session",
        api_id=YOUR_API_ID,        # <-- replace
        api_hash="YOUR_API_HASH",  # <-- replace
    ) as app:
        print("\n\n=== YOUR SESSION STRING ===")
        print(await app.export_session_string())
        print("===========================\n")

asyncio.run(main())
EOF
```

### Option 2 — Local Python (PC / Mac / Linux)

```bash
pip install pyrogram TgCrypto
python3 - <<'EOF'
import asyncio
from pyrogram import Client

async def main():
    async with Client(
        "my_session",
        api_id=YOUR_API_ID,
        api_hash="YOUR_API_HASH",
    ) as app:
        print(await app.export_session_string())

asyncio.run(main())
EOF
```

> ⚠️ **Security**: This string gives full access to your Telegram account.
> Treat it like a password.  Never share it or commit it to git.

---

## 🚀 Deploying on Render.com

### Step 1 — Push to GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/save-restricted-bot.git
git push -u origin main
```

### Step 2 — Create Redis (Key Value)

1. Go to **Render Dashboard → New → Key Value**
2. Name it `save-bot-redis`, choose the free plan
3. Copy the **Internal Redis URL** (starts with `redis://`)

### Step 3 — Create Web Service

1. Go to **Render Dashboard → New → Web Service**
2. Connect your GitHub repo
3. Fill in:
   - **Name**: `save-restricted-bot`
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python main.py`
   - **Instance Type**: Free (or Starter for better performance)

### Step 4 — Add Environment Variables

In the **Environment** tab of your Render Web Service, add:

```
API_ID          = 12345678
API_HASH        = abcdef...
BOT_TOKEN       = 1234567890:ABC...
SESSION_STRING  = BQA...
OWNER_ID        = 987654321
REDIS_URL       = redis://red-xxxx:6379   ← from Step 2
BOT_USERNAME    = YourBotUsername
LOG_CHANNEL     = -1001234567890          ← optional
FORCE_SUB_CHANNEL = yourchannel          ← optional
```

### Step 5 — Deploy

Click **Deploy**.  Render will:
1. Install dependencies
2. Start `python main.py`
3. Run the health-check on `GET /health`

First deploy takes ~2 minutes.  After that, pushes to `main` auto-deploy.

---

## 💻 Local Development

```bash
git clone https://github.com/YOUR_USERNAME/save-restricted-bot.git
cd save-restricted-bot

python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env with your credentials

python main.py
```

---

## 📋 Commands Reference

| Command | Who | Description |
|---|---|---|
| `/start` | Everyone | Welcome message |
| `/help` | Everyone | Full feature list |
| `/batch` | Everyone | Batch message download |
| `/cancel` | Everyone | Cancel active batch |
| `/setthumb` | Everyone | Set custom thumbnail |
| `/delthumb` | Everyone | Delete custom thumbnail |
| `/showthumb` | Everyone | Preview current thumbnail |
| `/setcaption [text]` | Everyone | Set custom caption |
| `/delcaption` | Everyone | Remove custom caption |
| `/stats` | Owner only | Bot statistics |
| `/broadcast` | Owner only | Message all users |
| `/addpremium [id]` | Owner only | Grant premium |
| `/removepremium [id]` | Owner only | Revoke premium |
| `/listpremium` | Owner only | List premium users |

---

## 🔒 How It Works

```
User  ──sends link──►  Bot
                        │
                        ├──► User Client (SESSION_STRING)
                        │         │
                        │    get_messages(channel, msg_id)
                        │         │
                        │    download_media() → /tmp/dl/file
                        │
                        └──► Bot Client
                                  │
                             send_video/audio/document
                                  │
                             ◄────── delivered to User (unrestricted)
```

The **user client** (your account) has membership in the private channels,
so it can read the restricted messages.  The **bot client** re-uploads the
downloaded file to the requesting user — no forwarding, no restrictions.

---

## ⚠️ Important Notes

- The account used for `SESSION_STRING` **must be a member** of any private
  channel you want to save from.
- Saving content from channels where you are not a member will fail.
- This bot is for **personal archival use**.  Respect copyright and Telegram's
  Terms of Service.
- Using a secondary/burner Telegram account for the session string is
  recommended.

---

## 🐛 Troubleshooting

| Error | Fix |
|---|---|
| `SessionExpired` | Re-generate `SESSION_STRING` |
| `ChannelPrivate` | The session account is not in the channel |
| `FloodWait` | Bot automatically sleeps and retries |
| Redis connection refused | Check `REDIS_URL` in environment variables |
| Health check failing | Ensure `PORT` env var matches Render's expected port |
| yt-dlp `DownloadError` | URL may be geo-blocked or require login |
