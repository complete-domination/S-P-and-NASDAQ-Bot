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
    cleaned = GUILD_ID_RAW.strip()
    if cleaned.isdigit():
        GUILD_ID = int(cleaned)
    else:
        print("Warning: GUILD_ID is not a pure integer; ignoring and updating all guilds.")

# Symbols (Yahoo Finance)
# S&P 500 = ^GSPC, NASDAQ Composite = ^IXIC
INDEX_SYMBOLS = {
    "NASDAQ": "^IXIC",
    "S&P 500": "^GSPC",
}

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("index-price-bot")

# ---------- Discord client ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True  # make sure "Server Members Intent" is enabled in the Dev Portal
client = discord.Client(intents=intents)

_http_session: Optional[aiohttp.ClientSession] = None
update_task: Optional[asyncio.Task] = None


async def fetch_index_quotes(session: aiohttp.ClientSession) -> Dict[str, Dict]:
    """
    Fetch quotes for both indices (price + 1D change %) from Yahoo in one call.
    Returns mapping: {symbol: {"price": float, "change_pct": float}}
    """
    symbols_csv = ",".join(quote_plus(sym) for sym in INDEX_SYMBOLS.values())
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_csv}"

    headers = {
        # Yahoo often rejects requests without a UA
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    backoffs = [0, 1.5, 3.0, 5.0]
    last_text_preview = ""
    for attempt, delay in enumerate(backoffs, start=1):
        if delay:
            await asyncio.sleep(delay)
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status != 200:
                    try:
                        txt = await resp.text()
                        last_text_preview = txt[:200]
                    except Exception:
                        last_text_preview = "<no body>"
                    log.warning(f"Yahoo HTTP {resp.status} (attempt {attempt}) body={last_text_preview!r}")
                    continue

                payload = await resp.json()
                results = payload.get("quoteResponse", {}).get("result", [])
                if not results:
                    log.warning(f"Yahoo returned no results (attempt {attempt}); payload head={str(payload)[:200]}")
                    continue

                out: Dict[str, Dict] = {}
                for r in results:
                    sym = r.get("symbol")
                    price = r.get("regularMarketPrice")
                    change_pct = r.get("regularMarketChangePercent")
                    if sym is None or price is None or change_pct is None:
                        # log once per attempt if a field is missing
                        log.debug(f"Missing fields in result: {r}")
                        continue
                    out[sym] = {"price": float(price), "change_pct": float(change_pct)}

                if not out:
                    log.warning(f"Yahoo results missing price/change fields; results={results}")
                    continue

                return out

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"HTTP error (attempt {attempt}): {e}")

    # If we get here, all attempts failed
    raise RuntimeError("Failed to fetch index quotes after retries")


async def get_self_member(guild: discord.Guild) -> Optional[discord.Member]:
    """Robustly get the bot's Member object in a guild."""
    me = getattr(guild, "me", None)
    if isinstance(me, discord.Member):
        return me
    m = guild.get_member(client.user.id)
    if isinstance(m, discord.Member):
        return m
    try:
        # requires Server Members Intent enabled in Dev Portal (we set intents.members=True)
        return await guild.fetch_member(client.user.id)
    except discord.HTTPException as e:
        log.warning(f"[{guild.name}] fetch_member failed: {e}")
        return None


async def update_guild(guild: discord.Guild, which: str, quotes: Optional[Dict[str, Dict]]):
    """
    Update nickname/presence for a single guild.
    If quotes is None (fetch error), show presence with an error note.
    """
    me = await get_self_member(guild)
    if not me:
        log.info(f"[{guild.name}] Could not obtain bot Member; skipping.")
        return

    perms = me.guild_permissions
    can_edit_nick = perms.change_nickname or perms.manage_nicknames

    if quotes is None:
        # Show *something* so it's obvious the loop is alive
        try:
            await client.change_presence(activity=discord.Game(name="quotes: error"))
        except Exception:
            pass
        log.info(f"[{guild.name}] Quote fetch failed; presence set to 'quotes: error'.")
        return

    idx_name = which  # "NASDAQ" or "S&P 500"
    symbol = INDEX_SYMBOLS[idx_name]
    q = quotes.get(symbol)
    if not q:
        log.warning(f"[{guild.name}] Missing quote for {idx_name} ({symbol})")
        try:
            await client.change_presence(activity=discord.Game(name=f"{idx_name} quote: n/a"))
        except Exception:
            pass
        return

    price = q["price"]
    change_pct = q["change_pct"]
    emoji = "🟢" if change_pct >= 0 else "🔴"

    # Nickname: "$price 🟢/🔴" (no % in name), with thousand separators
    nickname = f"${price:,.2f} {emoji}"
    if len(nickname) > 32:
        nickname = nickname[:32]

    if can_edit_nick:
        try:
            await me.edit(nick=nickname, reason=f"Auto {idx_name} price update")
        except discord.Forbidden:
            log.info(f"[{guild.name}] Forbidden by role hierarchy; cannot change nickname.")
        except discord.HTTPException as e:
            log.warning(f"[{guild.name}] HTTP error updating nick: {e}")
    else:
        log.info(f"[{guild.name}] Missing permission: Change Nickname/Manage Nicknames.")

    # Presence underneath name: e.g., "NASDAQ 1D +1.15%"
    try:
        await client.change_presence(activity=discord.Game(name=f"{idx_name} 1D {change_pct:+.2f}%"))
    except Exception as e:
        log.debug(f"[{guild.name}] Could not set presence: {e}")

    log.info(f"[{guild.name}] {idx_name} → Nick: {nickname if can_edit_nick else '(unchanged)'} "
             f"| 1D {change_pct:+.2f}%")


async def updater_loop():
    await client.wait_until_ready()
    log.info(f"Updater loop started. Target guilds: "
             f"{'all joined guilds' if not GUILD_ID else GUILD_ID}")

    show_nasdaq = True  # toggle state

    # single shared session with connection pooling
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()

    while not client.is_closed():
        # choose which index to display this cycle
        which = "NASDAQ" if show_nasdaq else "S&P 500"
        show_nasdaq = not show_nasdaq

        # fetch quotes with error handling
        quotes = None
        try:
            quotes = await fetch_index_quotes(_http_session)
        except Exception as e:
            log.error(f"Quote fetch failed: {e}")

        # resolve guilds
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
