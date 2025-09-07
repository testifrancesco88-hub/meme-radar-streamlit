# pump_feed.py — Feed live Pump.fun (subscribeNewToken) con reconnect & snapshot
# Dipende da: websockets, asyncio

from __future__ import annotations
import asyncio, json, time, threading, logging
from collections import deque
from typing import Callable, Deque, Dict, Optional, List

import websockets

LOG = logging.getLogger("PumpFeed")
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    LOG.addHandler(h)
    LOG.setLevel(logging.INFO)

WS_URL = "wss://pumpportal.fun/api/data"   # docs ufficiali
PING_SEC = 20
RETRY_BASE = 1.5

def _first(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def _normalize_new_token(msg: dict) -> Dict:
    """
    Il payload reale può variare: normalizziamo i campi più utili.
    """
    data = msg.get("message") or msg.get("data") or msg  # compat
    mint    = _first(data, "mint", "token", "mintAddress", "ca", default="")
    name    = _first(data, "name", "token_name", "tokenName", default="")
    symbol  = _first(data, "symbol", "ticker", "token_symbol", "tokenSymbol", default="")
    ts      = _first(data, "timestamp", "createdAt", "ts", default=int(time.time()*1000))
    creator = _first(data, "creator", "user", "owner", default="")
    pump    = f"https://pump.fun/coin/{mint}" if mint else ""
    dexs    = f"https://dexscreener.com/solana?query={mint}" if mint else "https://dexscreener.com/solana/f%3Apumpfun"
    return {
        "mint": mint,
        "name": name,
        "symbol": symbol,
        "creator": creator,
        "timestamp": ts,
        "pumpUrl": pump,
        "dexsUrl": dexs,
        "_raw": data,
    }

class PumpFeed:
    """
    Mantiene UNA sola connessione WS a PumpPortal e pubblica gli eventi 'new token' in una coda.
    - start()/stop() gestiscono il thread con event-loop asyncio.
    - snapshot() ritorna gli ultimi N record già normalizzati.
    - on_new(record) callback opzionale su ogni nuovo token.
    """
    def __init__(self, api_key: Optional[str] = None, max_items: int = 300,
                 on_new: Optional[Callable[[Dict], None]] = None) -> None:
        self.url = WS_URL if not api_key else f"{WS_URL}?api-key={api_key}"
        self._q: Deque[Dict] = deque(maxlen=max_items)
        self._seen: set = set()
        self._on_new = on_new
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.connected: bool = False
        self._last_msg_ts: float = 0.0

    # ---------- public ----------
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._runner, name="PumpFeedLoop", daemon=True)
        self._thread.start()

    def stop(self, join: bool = False):
        self._stop.set()
        if join and self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def snapshot(self, limit: int = 50) -> List[Dict]:
        items = list(self._q)
        return items[:limit]

    def last_heartbeat(self) -> float:
        return self._last_msg_ts

    # ---------- internal ----------
    def _runner(self):
        asyncio.run(self._main())

    async def _main(self):
        backoff = RETRY_BASE
        while not self._stop.is_set():
            try:
                async with websockets.connect(self.url, ping_interval=PING_SEC, ping_timeout=PING_SEC) as ws:
                    self.connected = True
                    backoff = RETRY_BASE
                    # iscriviti agli eventi nuovi token (puoi aggiungere subscribeMigration se vuoi)
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            LOG.debug("Non-JSON message: %s", raw)
                            continue
                        rec = _normalize_new_token(msg)
                        if not rec["mint"] or rec["mint"] in self._seen:
                            # evita duplicati/rumore
                            continue
                        self._seen.add(rec["mint"])
                        self._q.appendleft(rec)
                        self._last_msg_ts = time.time()
                        if self._on_new:
                            try:
                                self._on_new(rec)
                            except Exception as e:
                                LOG.warning("on_new error: %s", e)
            except Exception as e:
                self.connected = False
                LOG.warning("WS disconnected: %s", e)
                # backoff progressivo con tetto
                await asyncio.sleep(min(15.0, backoff))
                backoff *= 1.75
        self.connected = False
