import os
import asyncio
import logging
from typing import Optional, Dict, Tuple

import aiohttp
import discord
from urllib.parse import quote_plus

# ---------- Config ----------
TOKEN = os.environ.get("TOKEN")
GUILD_ID_RAW = os.environ.get("GUILD_ID")  # optional; if unset, update all guilds
INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "30"))  # alternate every 30s

if not TOKEN:
    raise SystemExit("Missing env var TOKEN")

GUILD_ID: Optional[int] = None
if GUILD_ID_RAW:
    cleaned = GUILD_ID_RAW.strip()
    if cleaned.isdigit():
        GUILD_ID = int(cleaned)
    else:
        print("Warning: GUILD_ID is not a pure integer; ignoring and updating all guilds.")

# Index symbols
YAHOO_SYMBOLS = {
    "NASDAQ": "^IXIC",
    "S&P 500": "^GSPC",
}
# Stooq symbols for fallback (no percent change available via this CSV)
STOOQ_SYMBOLS = {
    "NASDAQ": "^ndq",   # Nasdaq Composite on Stooq
    "S&P 500": "^spx",  # S&P 500 on Stooq
}

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("index-price-bot")

# ---------- Discord client ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True  # ensure "Server Members Intent" is enabled in the Dev Portal
client = discord.Client(intents=intents)

_http_session: Optional[aiohttp.ClientSession] = None
update_task: Optional[asyncio.Task] = None


# ---------- Data sources ----------
YAHOO_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

async def _fetch_yahoo_quotes(session: aiohttp.ClientSession) -> Dict[str, Dict]:
    """Try Yahoo (query1 then query2) to get price and 1D % change."""
    symbols_csv = ",".join(quote_plus(sym) for sym in YAHOO_SYMBOLS.values())
    urls = [
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_csv}",
        f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={symbols_csv}",
    ]
    backoffs = [0, 1.5, 3.0, 5.0]

    last_error = None
    for url in urls:
        for attempt, delay in enumerate(backoffs, start=1):
            if delay:
                await asyncio.sleep(delay)
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(url, headers=YAHOO_HEADERS, timeout=timeout) as resp:
                    if resp.status != 200:
                        body = ""
                        try:
                            body = (await resp.text())[:200]
                        except Exception:
                            pass
                        log.warning(f"Yahoo {url} HTTP {resp.status} (attempt {attempt}) body={body!r}")
                        last_error = f"HTTP {resp.status}"
                        continue

                    payload = await resp.json()
                    results = payload.get("quoteResponse", {}).get("result", [])
                    if not results:
                        log.warning(f"Yahoo returned no results (attempt {attempt}); head={str(payload)[:200]}")
                        last_error = "no results"
                        continue

                    out: Dict[str, Dict] = {}
                    for r in results:
                        sym = r.get("symbol")
                        price = r.get("regularMarketPrice")
                        change_pct = r.get("regularMarketChangePercent")
                        if sym is None or price is None or change_pct is None:
                            continue
                        out[sym] = {"price": float(price), "change_pct": float(change_pct)}

                    if not out:
                        last_error = "missing fields"
                        continue

                    log.info("Fetched quotes from Yahoo successfully.")
                    return out
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = f"{type(e).__name__}: {e}"
                log.warning(f"Yahoo error (attempt {attempt}): {e}")

    raise RuntimeError(f"Yahoo failed: {last_error or 'unknown error'}")


async def _fetch_stooq_price(session: aiohttp.ClientSession, stooq_symbol: str) -> Optional[float]:
    """
    Fallback: fetch latest daily CSV for one symbol from Stooq.
    Returns price (close) or None. No 1D % change available from this endpoint.
    """
    # Example: https://stooq.com/q/l/?s=%5Espx&i=d  (CSV with columns: Symbol,Date,Time,Open,High,Low,Close,Volume)
    url = f"https://stooq.com/q/l/?s={quote_plus(stooq_symbol)}&i=d"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(url, headers={"User-Agent": YAHOO_HEADERS["User-Agent"]}, timeout=timeout) as resp:
            if resp.status != 200:
                log.warning(f"Stooq HTTP {resp.status} for {stooq_symbol}")
                return None
            text = await resp.text()
            # Expect a header line then a single data line
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            if len(lines) < 2:
                log.warning(f"Stooq unexpected CSV for {stooq_symbol}: {text[:200]!r}")
                return None
            # Parse Close from second line
            parts = [p.strip() for p in lines[1].split(",")]
            if len(parts) < 8:
                log.warning(f"Stooq CSV missing fields for {stooq_symbol}: {lines[1]!r}")
                return None
            close_str = parts[6]
            try:
                return float(close_str)
            except ValueError:
                log.warning(f"Stooq Close parse failed for {stooq_symbol}: {close_str!r}")
                return None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning(f"Stooq error {stooq_symbol}: {e}")
        return None


async def fetch_index_quotes(session: aiohttp.ClientSession) -> Dict[str, Tuple[float, Optional[float], str]]:
    """
    Unified fetcher. Returns:
      { "NASDAQ": (price, change_pct or None, "yahoo"/"stooq"),
        "S&P 500": (price, change_pct or None, "yahoo"/"stooq") }
    """
    # 1) Try Yahoo for both
    try:
        yahoo = await _fetch_yahoo_quotes(session)
        out: Dict[str, Tuple[float, Optional[float], str]] = {}
        for idx_name, ysym in YAHOO_SYMBOLS.items():
            row = yahoo.get(ysym)
            if row:
                out[idx_name] = (row["price"], row["change_pct"], "yahoo")
        if len(out) == 2:
            return out
        else:
            log.warning("Yahoo missing one or more symbols; falling back where needed.")
    except Exception as e:
        log.error(f"Yahoo fetch failed entirely: {e}")

    # 2) Fallback to Stooq per index as needed (price only)
    out: Dict[str, Tuple[float, Optional[float], str]] = {}
    for idx_name, stoq in STOOQ_SYMBOLS.items():
        price = await _fetch_stooq_price(session, stoq)
        if price is not None:
            out[idx_name] = (price, None, "stooq")
    if out:
        return out

    # If all failed:
    raise RuntimeError("Failed to fetch quotes from Yahoo and Stooq")


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


async def update_guild(guild: discord.Guild, which: str, quotes: Dict[str, Tuple[float, Optional[float], str]]):
    """Update nickname/presence for a single guild to show one index."""
    me = await get_self_member(guild)
    if not me:
        log.info(f"[{guild.name}] Could not obtain bot Member; skipping.")
        return

    perms = me.guild_permissions
    can_edit_nick = perms.change_nickname or perms.manage_nicknames

    idx_name = which  # "NASDAQ" or "S&P 500"
    tup = quotes.get(idx_name)
    if not tup:
        log.warning(f"[{guild.name}] Missing quote for {idx_name}")
        try:
            await client.change_presence(activity=discord.Game(name=f"{idx_name} quote: n/a"))
        except Exception:
            pass
        return

    price, change_pct, source = tup
    emoji = "🟢" if (change_pct is not None and change_pct >= 0) else ("🔴" if change_pct is not None else "⚪")

    # Nickname: "$price 🟢/🔴" (no % in name), with thousand separators
    nickname = f"${price:,.2f} {emoji}"
    if len(nickname) > 32:
        nickname = nickname[:32]

    if can_edit_nick:
        try:
            await me.edit(nick=nickname, reason=f"Auto {idx_name} price update ({source})")
        except discord.Forbidden:
            log.info(f"[{guild.name}] Forbidden by role hierarchy; cannot change nickname.")
        except discord.HTTPException as e:
            log.warning(f"[{guild.name}] HTTP error updating nick: {e}")
    else:
        log.info(f"[{guild.name}] Missing permission: Change Nickname/Manage Nicknames.")

    # Presence underneath name
    suffix = "" if source == "yahoo" else " (fallback)"
    pct_text = f"{change_pct:+.2f}%" if change_pct is not None else "n/a%"
    try:
        await client.change_presence(activity=discord.Game(name=f"{idx_name} 1D {pct_text}{suffix}"))
    except Exception as e:
        log.debug(f"[{guild.name}] Could not set presence: {e}")

    log.info(f"[{guild.name}] {idx_name} [{source}] → Nick: {nickname if can_edit_nick else '(unchanged)'} "
             f"| 1D {pct_text}")


# ---------- Updater loop ----------
async def updater_loop():
    await client.wait_until_ready()
    log.info(f"Updater loop started. Target guilds: "
             f"{'all joined guilds' if not GUILD_ID else GUILD_ID}")

    show_nasdaq = True  # toggle state

    # single shared session
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()

    while not client.is_closed():
        which = "NASDAQ" if show_nasdaq else "S&P 500"
        show_nasdaq = not show_nasdaq

        quotes = None
        try:
            quotes = await fetch_index_quotes(_http_session)
        except Exception as e:
            log.error(f"Quote fetch failed: {e}")

        if quotes is None:
            # show error presence so you see the loop is alive
            try:
                await client.change_presence(activity=discord.Game(name="quotes: error"))
            except Exception:
                pass
            log.info("Quotes unavailable; presence set to 'quotes: error'.")
        else:
            # Resolve guilds and update
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
                await asyncio.gather(*(update_guild(g, which, quotes) for g in targets))

        await asyncio.sleep(INTERVAL_SECONDS)


@client.event
async def on_ready():
    global update_task
    log.info(f"Logged in as {client.user} in {len(client.guilds)} guild(s).")
    if update_task is None or update_task.done():
        update_task = asyncio.create_task(updater_loop())


if __name__ == "__main__":
    client.run(TOKEN)
