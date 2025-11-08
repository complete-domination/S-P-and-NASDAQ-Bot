import os
import asyncio
import logging
from typing import Optional, Tuple, List

import aiohttp
import discord
from urllib.parse import quote_plus

# ---------- Config ----------
TOKEN = os.environ.get("TOKEN")
GUILD_ID_RAW = os.environ.get("GUILD_ID")  # optional; if unset, update all guilds
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

# Yahoo Finance symbol for E-mini NASDAQ-100 futures
Y_SYMBOL = "NQ=F"

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nasdaq-futures-bot")

# ---------- Discord client ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True  # enable "Server Members Intent" in the Dev Portal
client = discord.Client(intents=intents)

_http_session: Optional[aiohttp.ClientSession] = None
update_task: Optional[asyncio.Task] = None

# ---------- Yahoo helpers ----------
Y_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Connection": "keep-alive",
}

async def _yahoo_quote(session: aiohttp.ClientSession) -> Optional[Tuple[float, float]]:
    """
    Fast path: Yahoo 'quote' for NQ=F.
    Returns (price, change_pct) or None if unavailable.
    """
    sym = quote_plus(Y_SYMBOL)
    urls = [
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={sym}",
        f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={sym}",
    ]
    for url in urls:
        try:
            async with session.get(url, headers=Y_HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    body = ""
                    try:
                        body = (await resp.text())[:200]
                    except Exception:
                        pass
                    log.warning(f"[quote] HTTP {resp.status} url={url} body={body!r}")
                    continue
                payload = await resp.json()
                results = payload.get("quoteResponse", {}).get("result", [])
                if not results:
                    log.warning("[quote] No results payload head=%r", str(payload)[:200])
                    continue
                row = results[0]
                # Use regularMarketPrice / regularMarketChangePercent if present
                price = row.get("regularMarketPrice")
                change_pct = row.get("regularMarketChangePercent")
                if price is not None and change_pct is not None:
                    log.info("[quote] OK from %s", url.split("//", 1)[-1].split("/", 1)[0])
                    return float(price), float(change_pct)
                else:
                    log.warning("[quote] Missing fields: price=%r change_pct=%r", price, change_pct)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"[quote] error: {e}")
    return None

def _last_non_null(vals: List[Optional[float]]) -> Optional[float]:
    for v in reversed(vals):
        if v is not None:
            return float(v)
    return None

async def _yahoo_chart(session: aiohttp.ClientSession) -> Optional[Tuple[float, float]]:
    """
    Fallback: Yahoo 'chart' for NQ=F. Compute price + 1D % from last close vs previous close.
    Returns (price, change_pct) or None.
    """
    sym = quote_plus(Y_SYMBOL)
    urls = [
        f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=1d&interval=1m",
        f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}?range=1d&interval=1m",
    ]
    for url in urls:
        try:
            async with session.get(url, headers=Y_HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    body = ""
                    try:
                        body = (await resp.text())[:200]
                    except Exception:
                        pass
                    log.warning(f"[chart] HTTP {resp.status} url={url} body={body!r}")
                    continue
                payload = await resp.json()
                result = (payload.get("chart", {}) or {}).get("result", [])
                if not result:
                    log.warning("[chart] No result payload head=%r", str(payload)[:200])
                    continue
                meta = result[0].get("meta", {}) or {}
                indicators = result[0].get("indicators", {}) or {}
                closes = (indicators.get("quote", [{}])[0].get("close") or [])
                last = _last_non_null(closes)
                prev_close = meta.get("previousClose")
                if last is None or prev_close is None:
                    log.warning("[chart] Missing last/previousClose last=%r prev=%r", last, prev_close)
                    continue
                change_pct = ((last - float(prev_close)) / float(prev_close)) * 100.0
                log.info("[chart] OK from %s", url.split("//", 1)[-1].split("/", 1)[0])
                return float(last), float(change_pct)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"[chart] error: {e}")
    return None

async def fetch_nq_price_change(session: aiohttp.ClientSession) -> Tuple[float, float]:
    """
    Unified fetcher: try quote first, then chart fallback.
    Raises RuntimeError if both fail.
    """
    q = await _yahoo_quote(session)
    if q is not None:
        return q
    c = await _yahoo_chart(session)
    if c is not None:
        return c
    raise RuntimeError("Both Yahoo quote and chart failed")

# ---------- Discord helpers ----------
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
    """Update nickname/presence for a single guild to show NASDAQ futures."""
    me = await get_self_member(guild)
    if not me:
        log.info(f"[{guild.name}] Could not obtain bot Member; skipping.")
        return

    perms = me.guild_permissions
    can_edit_nick = perms.change_nickname or perms.manage_nicknames

    try:
        assert _http_session is not None, "HTTP session not initialized"
        price, change_pct = await fetch_nq_price_change(_http_session)
    except Exception as e:
        log.error(f"[{guild.name}] Quote fetch failed: {e}")
        try:
            await client.change_presence(activity=discord.Game(name="NASDAQ Futures: error"))
        except Exception:
            pass
        return

    emoji = "🟢" if change_pct >= 0 else "🔴"
    nickname = f"${price:,.2f} {emoji}"
    if len(nickname) > 32:
        nickname = nickname[:32]

    if can_edit_nick:
        try:
            await me.edit(nick=nickname, reason="Auto NASDAQ futures price update (Yahoo)")
        except discord.Forbidden:
            log.info(f"[{guild.name}] Forbidden by role hierarchy; cannot change nickname.")
        except discord.HTTPException as e:
            log.warning(f"[{guild.name}] HTTP error updating nick: {e}")
    else:
        log.info(f"[{guild.name}] Missing permission: Change Nickname/Manage Nicknames.")

    try:
        await client.change_presence(activity=discord.Game(name=f"NASDAQ Futures 1D {change_pct:+.2f}%"))
    except Exception as e:
        log.debug(f"[{guild.name}] Could not set presence: {e}")

    log.info(f"[{guild.name}] NASDAQ Futures → Nick: {nickname if can_edit_nick else '(unchanged)'} "
             f"| 1D {change_pct:+.2f}%")

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
