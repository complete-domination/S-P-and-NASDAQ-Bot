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


# ---------- Data fetch (Yahoo Finance) ----------
YAHOO_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

async def fetch_nq_futures(session: aiohttp.ClientSession) -> Tuple[float, float]:
    """
    Fetch price and 1D % change for NQ=F (E-mini NASDAQ-100 futures) from Yahoo Finance.
    Returns (price, change_pct). Raises on persistent failure.
    """
    symbols_csv = quote_plus(Y_SYMBOL)
    urls = [
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_csv}",
        f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={symbols_csv}",
    ]
    backoffs = [0, 1.5, 3.0, 5.0]

    last_err = "unknown error"
    for url in urls:
        for attempt, delay in enumerate(backoffs, start=1):
            if delay:
                await asyncio.sleep(delay)
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(url, headers=YAHOO_HEADERS, timeout=timeout) as resp:
                    if resp.status != 200:
                        # log short body to help debug if needed
                        body = ""
                        try:
                            body = (await resp.text())[:200]
                        except Exception:
                            pass
                        log.warning(f"Yahoo {url} HTTP {resp.status} (attempt {attempt}) body={body!r}")
                        last_err = f"HTTP {resp.status}"
                        continue

                    payload = await resp.json()
                    results = payload.get("quoteResponse", {}).get("result", [])
                    if not results:
                        last_err = "no results"
                        log.warning(f"Yahoo returned no results (attempt {attempt}); head={str(payload)[:200]}")
                        continue

                    row = results[0]
                    price = row.get("regularMarketPrice")
                    change_pct = row.get("regularMarketChangePercent")
                    if price is None or change_pct is None:
                        last_err = "missing fields"
                        log.warning(f"Missing fields in Yahoo row: {row}")
                        continue

                    log.info("Fetched NQ=F from Yahoo successfully.")
                    return float(price), float(change_pct)

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = f"{type(e).__name__}: {e}"
                log.warning(f"Yahoo error (attempt {attempt}): {e}")

    raise RuntimeError(f"Failed to fetch NQ=F: {last_err}")


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
        price, change_pct = await fetch_nq_futures(_http_session)
    except Exception as e:
        log.error(f"[{guild.name}] Quote fetch failed: {e}")
        # Show something so you know the loop is alive
        try:
            await client.change_presence(activity=discord.Game(name="NASDAQ Futures: error"))
        except Exception:
            pass
        return

    emoji = "🟢" if change_pct >= 0 else "🔴"

    # Nickname: "$price 🟢/🔴" (no % in name), with thousand separators
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

    # Presence underneath name: "NASDAQ Futures 1D +X.XX%"
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
