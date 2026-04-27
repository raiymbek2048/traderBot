from __future__ import annotations
import httpx
from loguru import logger


class Notifier:
    def __init__(self, token: str, chat_id: str):
        self._token = token
        self._chat_id = chat_id
        self._enabled = bool(token and chat_id)

    def send(self, text: str) -> None:
        if not self._enabled:
            logger.info(f"[TELEGRAM disabled] {text}")
            return
        try:
            url = f"https://api.telegram.org/bot{self._token}/sendMessage"
            httpx.post(url, json={"chat_id": self._chat_id, "text": text}, timeout=5)
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")
