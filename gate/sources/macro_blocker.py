"""RSS-based macro event blocker.

Polls CoinDesk + Reuters every 60s. If critical keyword detected → 60-min block.
No LLM. Covers ~70% of critical macro events.
"""
from __future__ import annotations
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

import httpx
from loguru import logger

RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://cryptopanic.com/news/rss/",
    "https://decrypt.co/feed",
]

BLOCK_PATTERNS = re.compile(
    # Fed / macro
    r"fed emergency|emergency rate (cut|hike|change)|market-wide halt"
    r"|trading (halted|suspended) (globally|across|on all)"
    # Exchange-level crisis
    r"|exchange (hacked|hack|breached|breach|insolvent|bankrupt|halts withdrawals)"
    r"|bybit (hacked|hack|halts|suspended|bankrupt|insolvent)"
    r"|binance (hacked|hack|halts|suspended|bankrupt|insolvent)"
    # Regulatory ban
    r"|bitcoin (banned|illegal|prohibited) in"
    r"|crypto (banned|illegal|prohibited) in"
    r"|sec (bans|banning|shuts down) (crypto|bitcoin|ethereum)"
    # Systemic events
    r"|flash crash (in|on|across) (crypto|bitcoin|ethereum|markets)"
    r"|market-wide circuit breaker"
    r"|war (declared|declaration) (against|between) (us|china|russia|europe)"
    r"|nuclear (attack|strike|threat) (against|on) (us|china|russia|europe)",
    re.IGNORECASE,
)

BLOCK_DURATION_MIN = 60

_block_until: datetime | None = None
_last_trigger: str = ""


def _parse_rss_titles(xml_text: str) -> list[str]:
    try:
        root = ET.fromstring(xml_text)
        titles = []
        for item in root.iter("item"):
            title = item.findtext("title") or ""
            desc = item.findtext("description") or ""
            titles.append(f"{title} {desc}")
        return titles
    except Exception:
        return []


async def check_feeds() -> bool:
    """Polls all RSS feeds. Returns True if macro block activated."""
    global _block_until, _last_trigger

    triggered = False
    async with httpx.AsyncClient(timeout=8.0) as client:
        for url in RSS_FEEDS:
            try:
                r = await client.get(url, follow_redirects=True)
                r.raise_for_status()
                for text in _parse_rss_titles(r.text):
                    m = BLOCK_PATTERNS.search(text)
                    if m:
                        _block_until = datetime.now(timezone.utc) + timedelta(minutes=BLOCK_DURATION_MIN)
                        _last_trigger = text[:120]
                        logger.warning(f"MACRO BLOCK activated: '{m.group()}' in '{_last_trigger}'")
                        triggered = True
                        break
            except Exception as e:
                logger.warning(f"RSS fetch failed ({url}): {e}")

    return triggered


def is_blocked() -> tuple[bool, str]:
    """Returns (blocked, reason). Thread-safe read."""
    global _block_until
    if _block_until and datetime.now(timezone.utc) < _block_until:
        remaining = int((_block_until - datetime.now(timezone.utc)).total_seconds() / 60)
        return True, f"Macro block active ({remaining}min remaining): {_last_trigger}"
    return False, ""
