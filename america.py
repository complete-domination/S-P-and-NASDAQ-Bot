import os
import asyncio
import logging
import random
from typing import Optional, Tuple, List, Dict, Any

import aiohttp
import discord
from urllib.parse import quote_plus

# ========= Config =========
TOKEN = os.environ.get("TOKEN")
GUILD_ID_RAW = os.environ.get("GUILD_ID")  # optional; if unset, bot updates all guilds it's in
INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "120"))  # update cadence
# Optional: Prefer a key-based provider to avoid throttling:
FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN")  # get one free at finnhub.io
FINNHUB_SYMBOL = os.environ.get("FINNHUB_SYMBOL", "CME_MINI:NQ1!")  # continuous NQ front contract (common format)

if not TOKEN:
    raise SystemExit("Missing env var TOKEN")

GUILD_ID: Optional[int] = None
if GUILD_ID_RAW and GUILD_ID_RAW.strip().isdigit():
    GUILD_ID = int(GUILD_ID_RAW.strip())

# Yahoo & Stooq symbols for fallback paths
Y_SYMBOL = "NQ=F"    # Yahoo: E-mini NASDAQ-100 futures
STOOQ_FUT = "nq.f"   # Stooq: NASDAQ-100 futures daily CSV

# ========= Logging =========
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nasdaq-futures-bot")

# ========= Discord client =========
intents = discord.Intents.default()
intents.guilds = True
intents.members = True  # enable "Server Members Intent" in the Dev Portal
client = discord.Client(intents=intents)

_http_session: Optional[aiohttp.ClientSession] = None
update_task: Optional[asyncio.Task] = None

# ========= HTTP helpers =========
Y_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Connection": "keep-alive",
}

def _last_non_null(vals: List[Optional[float]]) -> Optional[float]:
    for v in reversed(vals):
        if v is not None:
            return float(v)
    return None

# -------- Provider A: Finnhub (recommended) --------
async def finnhub_quote(session: aiohttp.ClientSession) -> Optional[Tuple[float, float]]:
    """
    Finnhub: /quote returns:
      c = current price, pc = previous close
    change_pct = (c - pc) / pc * 100
    """
    if not FINNHUB_TOKEN:
        return None
    params = {"symbol": FINNHUB_SYMBOL, "token": FINNHUB_TOKEN}
    url = "https://finnhub.io/api/v1/quote"
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                body = ""
                try: body = (await resp.text())[:200]
                except Exception: pass
                log.warning(f"[finnhub] HTTP {resp.status} body={body!r}")
                return None
            data: Dict[str, Any] = await resp.json()
            c = data.get("c")  # current price
            pc = data.get("pc")  # previous close
            if c is None or pc is None or pc == 0:
                log.warning(f"[finnhub] Missing fields c/pc: {data}")
                return None
            change_pct = ((float(c) - float(pc)) / float(pc)) * 100.0
            log.info("[finnhub] OK")
            return float(c), float(change_pct)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning(f"[finnhub] error: {e}")
        return None

# -------- Provider B: Yahoo (quote → chart) --------
async def yahoo_quote(session: aiohttp.ClientSession) -> Optional[Tuple[float, float]]:
    sym = quote_plus(Y_SYMBOL)
    urls = [
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={sym}",
        f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={sym}",
    ]
    for url in urls:
        try:
            async with session.get(url, headers=Y_HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 429:
                    log.warning("[yahoo quote] 429 Too Many Requests")
                    return None
                if resp.status != 200:
                    body = ""
                    try: body = (await resp.text())[:200]
                    except Exception: pass
                    log.warning(f"[yahoo quote] HTTP {resp.status} body={body!r}")
                    continue
                payload = await resp.json()
                results = payload.get("quoteResponse", {}).get("result", [])
                if not results:
                    log.warning("[yahoo quote] No results")
                    continue
                row = results[0]
                p = row.get("regularMarketPrice")
                chg = row.get("regularMarketChangePercent")
                if p is None or chg is None:
                    log.warning("[yahoo quote] Missing fields")
                    continue
                log.info("[yahoo quote] OK")
                return float(p), float(chg)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"[yahoo quote] error: {e}")
    return None

async def yahoo_chart(session: aiohttp.ClientSession) -> Optional[Tuple[float, float]]:
    sym = quote_plus(Y_SYMBOL)
    urls = [
        f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=1d&interval=1m",
        f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}?range=1d&interval=1m",
    ]
    for url in urls:
        try:
            async with session.get(url, headers=Y_HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 429:
                    log.warning("[yahoo chart] 429 Too Many Requests")
                    return None
                if resp.status != 200:
                    body = ""
                    try: body = (await resp.text())[:200]
                    except Exception: pass
                    log.warning(f"[yahoo chart] HTTP {resp.status} body={body!r}")
                    continue
                payload = await resp.json()
                result = (payload.get("chart", {}) or {}).get("result", [])
                if not result:
                    log.warning("[yahoo chart] No result")
                    continue
                meta = result[0].get("meta", {}) or {}
                closes = (result[0].get("indicators", {}) or {}).get("quote", [{}])[0].get("close") or []
                last = _last_non_null(closes)
                prev_close = meta.get("previousClose")
                if last is None or prev_close is None:
                    log.warning("[yahoo chart] Missing last/previousClose")
                    continue
                chg = ((float(last) - float(prev_close)) / float(prev_close)) * 100.0
                log.info("[yahoo chart] OK")
                return float(last), float(chg)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"[yahoo chart] error: {e}")
    return None

# -------- Provider C: Stooq (daily CSV fallback) --------
async def stooq_last_and_change(session: aiohttp.ClientSession) -> Optional[Tuple[float, float]]:
    url = f"https://stooq.com/q/d/l/?s={quote_plus(STOOQ_FUT)}&i=d"
    try:
        async with session.get(url, headers={"User-Agent": Y_HEADERS["User-Agent"]},
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                body = ""
                try: body = (await resp.text())[:200]
                except Exception: pass
                log.warning(f"[stooq] HTTP {resp.status} body={body!r}")
                return None
            text = await resp.text()
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            if len(lines) < 3:
                log.warning("[stooq] CSV too short")
                return None
            last = lines[-1].split(",")
            prev = lines[-2].split(",")
            if len(last) < 5 or len(prev) < 5:
                log.warning(f"[stooq] CSV missing fields last={last!r} prev={prev!r}")
                return None
            last_close = float(last[4]); prev_close = float(prev[4])
            chg = ((last_close - prev_close) / prev_close) * 100.0
            log.info("[stooq] OK")
            return last_close, chg
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning(f"[stooq] error: {e}")
        return None

# Unified fetcher
async def fetch_price_change(session: aiohttp.ClientSession) -> Tuple[float, float, str]:
    """Try Finnhub -> Yahoo quote -> Yahoo chart -> Stooq. Return (price, change_pct, source)."""
    # small jitter so multiple deployments don't sync-hammer providers
    await asyncio.sleep(random.uniform(0.0, 0.8))

    if FINNHUB_TOKEN:
        q = await finnhub_quote(session)
        if q is not None:
            return q[0], q[1], "finnhub"

    q = await yahoo_quote(session)
    if q is not None:
        return q[0], q[1], "yahoo-quote"

    c = await yahoo_chart(session)
    if c is not None:
        return c[0], c[1], "yahoo-chart"

    s = await stooq_last_and_change(session)
    if s is not None:
        return s[0], s[1], "stooq"

    raise RuntimeError("All sources failed")

# ========= Discord helpers =========
async def get_self_member(guild: discord.Guild) -> Optional[discord.Member]:
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
    me = await get_self_member(guild)
    if not me:
        log.info(f"[{guild.name}] Could not obtain bot Member; skipping.")
        return

    perms = me.guild_permissions
    can_edit_nick = perms.change_nickname or perms.manage_nicknames

    try:
        assert _http_session is not None, "HTTP session not initialized"
        price, change_pct, source = await fetch_price_change(_http_session)
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
            await me.edit(nick=nickname, reason=f"Auto NASDAQ futures update ({source})")
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

    log.info(f"[{guild.name}] NASDAQ Futures [{source}] → Nick: {nickname if can_edit_nick else '(unchanged)'} "
             f"| 1D {change_pct:+.2f}%")

# ========= Loop =========
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

        await asyncio.sleep(INTERVAL_SECONDS + random.uniform(0.0, 2.0))

@client.event
async def on_ready():
    global update_task
    log.info(f"Logged in as {client.user} in {len(client.guilds)} guild(s).")
    if update_task is None or update_task.done():
        update_task = asyncio.create_task(updater_loop())

if __name__ == "__main__":
    client.run(TOKEN)
