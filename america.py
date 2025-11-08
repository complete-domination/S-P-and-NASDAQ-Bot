import os
import asyncio
import logging
from typing import Optional, Dict

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
    try:
        GUILD_ID = int(GUILD_ID_RAW)
    except ValueError:
        raise SystemExit("GUILD_ID must be an integer if provided")

# Symbols (Yahoo Finance)
# S&P 500 = ^GSPC, NASDAQ Composite = ^IXIC
INDEX_SYMBOLS = {
    "S&P 500": "^GSPC",
    "NASDAQ": "^IXIC",
}

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("index-price-bot")

# ---------- Discord client ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True  # enable Server Members Intent in the Dev Portal
client = discord.Client(intents=intents)

_http_session: Optional[aiohttp.ClientSession] = None
update_task: Optional[asyncio.Task] = None


async def fetch_index_quotes(session: aiohttp.ClientSession) -> Dict[str, Dict]:
    """
    Fetch quotes for both indices in one Yahoo Finance call.
    Returns mapping: {symbol: {"price": float, "change_pct": float}}
    """
    symbols_csv = ",".join(quote_plus(sym) for sym in INDEX_SYMBOLS.values())
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_csv}"

    backoffs = [0, 1.5, 3.0, 5.0]
    for attempt, delay in enumerate(backoffs, start=1):
        if delay:
            await asyncio.sleep(delay)
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(url, timeout=timeout) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    raise RuntimeError(f"Yahoo HTTP {resp.status}: {txt[:200]}")
                payload = await resp.json()
                results = payload.get("quoteResponse", {}).get("result", [])
                if not results:
                    raise RuntimeError("Yahoo returned no results")
                out: Dict[str, Dict] = {}
                for r in results:
                    sym = r.get("symbol")
                    price = r.get("regularMarketPrice")
                    change_pct = r.get("regularMarketChangePercent")
                    if sym is None or price is None or change_pct is None:
                        continue
                    out[sym] = {"price": float(price), "change_pct": float(change_pct)}
                if not out:
                    raise RuntimeError("Yahoo results missing price/change fields")
                return out
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"HTTP error (attempt {attempt}): {e}")
        except Exception as e:
            log.warning(f"Quote fetch error (attempt {attempt}): {e}")
    raise RuntimeError("Failed to fetch index quotes after retries")


async def update_guild(guild: discord.Guild, which: str, quotes: Dict[str, Dict]):
    """Update nickname/presence for a single guild to show one index."""
    try:
        me = guild.me or await guild.fetch_member(client.user.id)
    except discord.HTTPException as e:
        log.warning(f"[{guild.name}] Could not fetch bot member: {e}")
        return

    perms = me.guild_permissions
    if not (perms.change_nickname or perms.manage_nicknames):
        log.info(f"[{guild.name}] Missing permission: Change Nickname (or Manage Nicknames).")
        return

    idx_name = which  # "NASDAQ" or "S&P 500"
    symbol = INDEX_SYMBOLS[idx_name]
    q = quotes.get(symbol)
    if not q:
        log.warning(f"[{guild.name}] Missing quote for {idx_name} ({symbol})")
        return

    price = q["price"]
    change_pct = q["change_pct"]
    emoji = "🟢" if change_pct >= 0 else "🔴"

    # Nickname: "$price 🟢/🔴" (no % in name), with thousand separators
    nickname = f"${price:,.2f} {emoji}"
    if len(nickname) > 32:
        nickname = nickname[:32]

    try:
        await me.edit(nick=nickname, reason=f"Auto {idx_name} price update")
    except discord.Forbidden:
        log.info(f"[{guild.name}] Forbidden: role hierarchy/permissions block nickname change.")
    except discord.HTTPException as e:
        log.warning(f"[{guild.name}] HTTP error updating nick: {e}")

    # Presence underneath name: "NASDAQ 1D +X.XX%" or "S&P 500 1D -X.XX%"
    try:
        await client.change_presence(
            activity=discord.Game(name=f"{idx_name} 1D {change_pct:+.2f}%")
        )
    except Exception as e:
        log.debug(f"[{guild.name}] Could not set presence: {e}")

    log.info(f"[{guild.name}] {idx_name} → Nick: {nickname} | 1D {change_pct:+.2f}%")


async def updater_loop():
    await client.wait_until_ready()
    log.info("Updater loop started.")

    show_nasdaq = True  # toggle state

    while not client.is_closed():
        try:
            # Choose which index to display this cycle
            which = "NASDAQ" if show_nasdaq else "S&P 500"
            show_nasdaq = not show_nasdaq  # flip for next time

            # Fetch both quotes once
            assert _http_session is not None, "HTTP session not initialized"
            quotes = await fetch_index_quotes(_http_session)

            # Resolve guilds
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

        except Exception as e:
            log.error(f"Updater loop error: {e}")

        await asyncio.sleep(INTERVAL_SECONDS)


@client.event
async def on_ready():
    global update_task, _http_session
    log.info(f"Logged in as {client.user} in {len(client.guilds)} guild(s).")

    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()

    if update_task is None or update_task.done():
        update_task = asyncio.create_task(updater_loop())


if __name__ == "__main__":
    client.run(TOKEN)
