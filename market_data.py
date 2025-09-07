# market_data.py — Provider DexScreener v3 (nested priceChange safe)
# - search multipla su DexScreener
# - normalizza record mantenendo priceChange (h1,h4,h6,h24)
# - filtri: only_raydium, min_liq, exclude_quotes
# - thread di auto-refresh opzionale
# - snapshot in DataFrame con campi usati dalla UI

from __future__ import annotations
import time
import threading
import random
import math
from typing import Iterable, Optional, Tuple, Dict, Any, List

import requests
import pandas as pd

DEX_SEARCH = "https://api.dexscreener.com/latest/dex/search"
UA_HEADERS = {
    "User-Agent": "MemeRadar/1.0 (+https://github.com/) Python/Requests",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

def _to_float(x, default=None):
    if x is None:
        return default
    try:
        s = str(x).replace(",", "").strip()
        if s.endswith("%"):
            s = s[:-1]
        return float(s)
    except Exception:
        return default

def _sum_tx1h(txns: Dict[str, Any] | None) -> int:
    """DexScreener txns: {'h1': {'buys': int, 'sells': int}, ...}"""
    try:
        h1 = (txns or {}).get("h1") or {}
        b, s = int(h1.get("buys", 0) or 0), int(h1.get("sells", 0) or 0)
        return max(0, b + s)
    except Exception:
        return 0

def _liq_usd(liq: Dict[str, Any] | None) -> float | None:
    try:
        v = (liq or {}).get("usd")
        return _to_float(v, None)
    except Exception:
        return None

def _vol24_usd(vol: Dict[str, Any] | None) -> float | None:
    try:
        # DexScreener: 'volume': {'h24': <usd>}
        v = (vol or {}).get("h24")
        return _to_float(v, None)
    except Exception:
        return None

def _safe_get_price_change(obj: Dict[str, Any] | None, key: str) -> float | None:
    """obj può essere priceChange dict; key es. 'h1', 'h4', 'h6', 'h24'"""
    try:
        if not isinstance(obj, dict):
            return None
        v = obj.get(key, None)
        return _to_float(v, None)
    except Exception:
        return None

def _norm_pair(p: Dict[str, Any]) -> Dict[str, Any]:
    """Normalizza un singolo pair DexScreener in un record piatto + nested 'priceChange'."""
    base = (p.get("baseToken") or {})
    quote = (p.get("quoteToken") or {})
    rec: Dict[str, Any] = {}

    rec["baseSymbol"]   = base.get("symbol") or ""
    rec["quoteSymbol"]  = quote.get("symbol") or ""
    rec["baseAddress"]  = base.get("address") or ""
    rec["quoteAddress"] = quote.get("address") or ""
    rec["pairAddress"]  = p.get("pairAddress") or p.get("pairAddress") or ""

    rec["dexId"]        = p.get("dexId") or ""
    rec["url"]          = p.get("url") or ""

    # prezzi e metriche
    rec["priceUsd"]       = _to_float(p.get("priceUsd"), None)
    rec["liquidityUsd"]   = _liq_usd(p.get("liquidity"))
    rec["volume24hUsd"]   = _vol24_usd(p.get("volume"))
    rec["txns1h"]         = _sum_tx1h(p.get("txns"))

    # timestamps
    # Dexscreener usa ms epoch in 'pairCreatedAt' se disponibile
    rec["pairCreatedAt"]  = p.get("pairCreatedAt") or p.get("createdAt") or None

    # Manteniamo priceChange annidato (se esiste)
    price_change = p.get("priceChange") if isinstance(p.get("priceChange"), dict) else None
    rec["priceChange"] = price_change

    # Valori "flat" (se presenti) — NON indispensabili per la UI (che legge anche nested)
    rec["priceChange1hPct"]  = (
        _to_float(p.get("priceChange1hPct"), None)
        or _safe_get_price_change(price_change, "h1")
        or _to_float(p.get("pc1h"), None)
    )
    # Dexscreener non sempre espone h4; la UI ha fallback a h6
    rec["priceChange4hPct"]  = (
        _to_float(p.get("priceChange4hPct"), None)
        or _safe_get_price_change(price_change, "h4")
        or _to_float(p.get("pc4h"), None)
    )
    rec["priceChange6hPct"]  = (
        _to_float(p.get("priceChange6hPct"), None)
        or _safe_get_price_change(price_change, "h6")
        or _to_float(p.get("pc6h"), None)
    )
    rec["priceChange24hPct"] = (
        _to_float(p.get("priceChange24hPct"), None)
        or _safe_get_price_change(price_change, "h24")
        or _to_float(p.get("pc24h"), None)
    )

    return rec

class MarketDataProvider:
    """
    Recupera e normalizza i pairs da DexScreener su Solana (tramite /search?q=).
    Conserva un DataFrame unificato (dedup per pairAddress).
    """

    def __init__(self, refresh_sec: int = 60, preserve_on_empty: bool = True):
        self.refresh_sec = int(refresh_sec)
        self.preserve_on_empty = bool(preserve_on_empty)

        self._queries: List[str] = ["chain:solana"]
        self.only_raydium: bool = False
        self.min_liq: float = 0.0
        self.exclude_quotes: set[str] = set()

        self._last_df: pd.DataFrame = pd.DataFrame()
        self._last_ts: float = 0.0
        self._http_codes: List[int | str] = []

        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

        self._session = requests.Session()
        self._session.headers.update(UA_HEADERS)

    # ---------------- Public API ----------------

    def set_queries(self, queries: Iterable[str]) -> None:
        q = list(queries) if queries else []
        if not q:
            q = ["chain:solana"]
        with self._lock:
            self._queries = q[:]

    def set_filters(self, only_raydium: bool = False, min_liq: float = 0, exclude_quotes: Iterable[str] | None = None) -> None:
        """exclude_quotes: lista/set di symbol (case-insensitive) da escludere come quote (es. USDC, USDT, SOL)"""
        self.only_raydium = bool(only_raydium)
        try:
            self.min_liq = float(min_liq or 0.0)
        except Exception:
            self.min_liq = 0.0
        if exclude_quotes is not None:
            try:
                self.exclude_quotes = {str(x).upper() for x in exclude_quotes if str(x).strip()}
            except Exception:
                self.exclude_quotes = set()
        else:
            self.exclude_quotes = set()

    def start_auto_refresh(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name="MarketDataProviderLoop", daemon=True)
        self._thread.start()

    def stop_auto_refresh(self) -> None:
        self._stop.set()

    def force_refresh(self) -> Tuple[pd.DataFrame, float]:
        df, ts = self._collect_all()
        with self._lock:
            if not df.empty or not self.preserve_on_empty:
                self._last_df = df
                self._last_ts = ts
        return self._last_df.copy(), self._last_ts

    def get_snapshot(self) -> Tuple[pd.DataFrame, float]:
        with self._lock:
            return self._last_df.copy(), self._last_ts

    def get_last_http_codes(self) -> List[int | str]:
        with self._lock:
            return list(self._http_codes)

    # ---------------- Internal ----------------

    def _run_loop(self):
        jitter = 0.15
        while not self._stop.is_set():
            try:
                self.force_refresh()
            except Exception:
                # non blocchiamo il loop su eccezioni momentanee
                pass
            wait = max(3, int(self.refresh_sec * (1.0 + random.uniform(-jitter, jitter))))
            self._stop.wait(wait)

    def _fetch_query(self, q: str) -> Tuple[List[Dict[str, Any]], int | str]:
        try:
            r = self._session.get(DEX_SEARCH, params={"q": q}, timeout=20)
            code = r.status_code
            if not r.ok:
                return ([], code)
            data = r.json()
            pairs = data.get("pairs") or []
            if not isinstance(pairs, list):
                pairs = []
            return (pairs, code)
        except Exception:
            return ([], "ERR")

    def _apply_filters(self, recs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in recs:
            # only_raydium
            if self.only_raydium:
                if str(r.get("dexId", "")).lower() != "raydium":
                    continue
            # min_liq
            try:
                liq = _to_float(r.get("liquidityUsd"), 0.0) or 0.0
                if liq < float(self.min_liq):
                    continue
            except Exception:
                pass
            # exclude_quotes
            try:
                qsym = str(r.get("quoteSymbol") or "").upper()
                if qsym and qsym in self.exclude_quotes:
                    continue
            except Exception:
                pass
            out.append(r)
        return out

    def _dedup_by_pair(self, recs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        out = []
        for r in recs:
            pid = r.get("pairAddress") or r.get("url") or (r.get("baseAddress"), r.get("quoteAddress"), r.get("dexId"))
            if pid in seen:
                continue
            seen.add(pid)
            out.append(r)
        return out

    def _collect_all(self) -> Tuple[pd.DataFrame, float]:
        all_recs: List[Dict[str, Any]] = []
        codes: List[int | str] = []

        # fetch per query
        with self._lock:
            queries = self._queries[:]
        if not queries:
            queries = ["chain:solana"]

        for q in queries:
            pairs, code = self._fetch_query(q)
            codes.append(code)
            if pairs:
                for p in pairs:
                    all_recs.append(_norm_pair(p))

        # filtri provider
        all_recs = self._apply_filters(all_recs)
        # dedup
        all_recs = self._dedup_by_pair(all_recs)

        df = pd.DataFrame(all_recs) if all_recs else pd.DataFrame(columns=[
            "baseSymbol","quoteSymbol","baseAddress","quoteAddress","pairAddress",
            "dexId","url","priceUsd","liquidityUsd","volume24hUsd","txns1h",
            "pairCreatedAt","priceChange","priceChange1hPct","priceChange4hPct","priceChange6hPct","priceChange24hPct",
        ])

        ts = time.time()
        with self._lock:
            self._http_codes = codes[-10:]  # mantieni ultimi 10
        return df, ts
