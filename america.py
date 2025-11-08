import os
import asyncio
import logging
from typing import Optional, Tuple

import aiohttp
import discord
from urllib.parse import quote_plus

# ---------- Config ----------
TOKEN = os.environ.get("TOKEN")
GUILD_ID_RAW = os.environ.get("GUILD_ID")  # optional; if unset, updates all guilds
INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "60"))  # update cadence (sec)

if not TOKEN:
    raise SystemExit("Missing env var TOKEN")

GUILD_ID: Optional[int] = None
if GUILD_ID_RAW:
    cleaned = GUILD_ID_RAW.strip()
    if cleaned.isdigit():
        GUILD_ID = int(cleaned)
    else:
        print("Warning: GUILD_ID is not a pure integer; ignoring and updating all guilds.")

# NASDAQ Composite on Stooq
STOOQ_SYMBOL = "^ndq"

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nasdaq-price-bot")

# ---------- Discord client ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True  # enable "Server Members Intent" in the Dev Portal
client = discord.Client(intents=intents)

_http_session: Optional[aiohttp.ClientSession] = None
update_task: Optional[asyncio.Task] = None


# ---------- Data fetch ----------
async def fetch_nasdaq_close_and_change(session: aiohttp.ClientSession) -> Tuple[float, float]:
    """
    Fetch daily CSV from Stooq and compute 1D % change from the last two closes.
    Returns (last_close_price, change_pct).
    """
    # Example: https://stooq.com/q/d/l/?s=%5Endq&i=d
    url = f"https://stooq.com/q/d/l/?s={quote_plus(STOOQ_SYMBOL)}&i=d"
    timeout = aiohttp.ClientTimeout(total=10)

    for attempt in range(1, 5):
        try:
            async with session.get(
                url,
                timeout=timeout,
                headers={"User-Agent": "Mozilla/5.0"}
            ) as resp:
                if resp.status != 200:
                    txt = ""
                    try:
                        txt = (await resp.text())[:200]
                    except Exception:
                        pass
                    log.warning(f"Stooq HTTP {resp.status} (attempt {attempt}) body={txt!r}")
                    await asyncio.sleep(1.5 * attempt)
                    continue

                text = await resp.text()
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                if len(lines) < 3:
                    # header + at least 2 data rows
                    log.warning(f"Stooq CSV too short (attempt {attempt}): {text[:200]!r}")
                    await asyncio.sleep(1.5 * attempt)
                    continue

                # CSV format: Date,Open,High,Low,Close,Volume
                last = lines[-1].split(",")
                prev = lines[-2].split(",")
                if len(last) < 5 or len(prev) < 5:
                    log.warning(f"Stooq CSV missing fields (attempt {attempt}) last={last!r} prev={prev!r}")
                    await asyncio.sleep(1.5 * attempt)
                    continue

                last_close = float(last[4])
                prev_close = float(prev[4])
                change_pct = ((last_close - prev_close) / prev_close) * 100.0
                return last_close, change_pct

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"Stooq fetch error (attempt {attempt}): {e}")
            await asyncio.sleep(1.5 * attempt)

    raise RuntimeError("Failed to fetch NASDAQ data after retries")


async def get_self_member(guild: discord.Guild) -> Optional[discord.Member]:
    """Robustly get the bot's Member object in a guild."""
    me = getattr(guild, "me", None)
    if isinstance(me, discord.Member):
        return me
    m = guild.get_member(client.user.id)
    if isinstance(m, discord.Member):
        return m
    try:
        return await guild.fetch_member(client.user.id)
    except discord.HTTPException as e:
        log.warning(f"[{guild.name}] fetch_member failed: {e}")
        return None


async def update_guild(guild: discord.Guild):
    """Update nickname/presence for a single guild to show NASDAQ."""
    me = await get_self_member(guild)
    if not me:
        log.info(f"[{guild.name}] Could not obtain bot Member; skipping.")
        return

    perms = me.guild_permissions
    can_edit_nick = perms.change_nickname or perms.manage_nicknames

    price, change_pct = await fetch_nasdaq_close_and_change(_http_session)
    emoji = "🟢" if change_pct >= 0 else "🔴"

    # Nickname: "$price 🟢/🔴" (no % in name), with thousand separators
    nickname = f"${price:,.2f} {emoji}"
    if len(nickname) > 32:
        nickname = nickname[:32]

    if can_edit_nick:
        try:
            await me.edit(nick=nickname, reason="Auto NASDAQ price update (Stooq)")
        except discord.Forbidden:
            log.info(f"[{guild.name}] Forbidden by role hierarchy; cannot change nickname.")
        except discord.HTTPException as e:
            log.warning(f"[{guild.name}] HTTP error updating nick: {e}")
    else:
        log.info(f"[{guild.name}] Missing permission: Change Nickname/Manage Nicknames.")

    # Presence underneath name: "NASDAQ 1D +X.XX%"
    try:
        await client.change_presence(activity=discord.Game(name=f"NASDAQ 1D {change_pct:+.2f}%"))
    except Exception as e:
        log.debug(f"[{guild.name}] Could not set presence: {e}")

    log.info(f"[{guild.name}] NASDAQ → Nick: {nickname if can_edit_nick else '(unchanged)'} | 1D {change_pct:+.2f}%")


# ---------- Loop ----------
async def updater_loop():
    await client.wait_until_ready()
    log.info(f"Updater loop started. Target: {'all guilds' if not GUILD_ID else GUILD_ID}")

    # one shared HTTP session
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()

    while not client.is_closed():
        try:
            if GUILD_ID:
                g = client.get_guild(GUILD_ID)
                targets = [g] if g else []
                if not g:
                    log.info("Configured GUILD_ID not found yet. Is the bot in that server?")
            else:
                targets = list(client.guilds)

            if not targets:
                log.info("No guilds to update yet.")
            else:
                await asyncio.gather(*(update_guild(g) for g in targets))

        except Exception as e:
            log.error(f"Updater loop error: {e}")

        await asyncio.sleep(INTERVAL_SECONDS)


@client.event
async def on_ready():
    global update_task
    log.info(f"Logged in as {client.user} in {len(client.guilds)} guild(s).")
    if update_task is None or update_task.done():
        update_task = asyncio.create_task(updater_loop())


if __name__ == "__main__":
    client.run(TOKEN)
