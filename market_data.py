# market_data.py — Provider dati live per token/pairs (Solana) con aggiornamento continuo
# v8.2: schema stabile anche a DF vuoto + filtri robusti su exclude_quotes

from __future__ import annotations

import math, random, threading, time, logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

DEFAULT_QUERIES = [
    "chain:solana raydium",
    "chain:solana orca",
    "chain:solana meteora",
    "chain:solana lifinity",
    "chain:solana usdc",
    "chain:solana usdt",
    "chain:solana sol",
    "chain:solana bonk",
    "chain:solana wif",
    "chain:solana pepe",
]

DEXSEARCH_BASE = "https://api.dexscreener.com/latest/dex/search?q="
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MemeRadar/1.0 Chrome/120 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

LOG = logging.getLogger("MarketDataProvider")
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    LOG.addHandler(h)
    LOG.setLevel(logging.INFO)

EXPECTED_COLS = [
    "pairAddress","url","dexId","baseSymbol","quoteSymbol",
    "priceUsd","liquidityUsd","volume24hUsd","priceChange24hPct",
    "pairCreatedAt","txns1h",
]

def _first_num(*vals) -> Optional[float]:
    for v in vals:
        try:
            if v is None: 
                continue
            n = float(v)
            if math.isnan(n):
                continue
            return n
        except Exception:
            continue
    return None

def _is_solana_pair(p: Dict[str, Any]) -> bool:
    return str((p or {}).get("chainId", "solana")).lower() == "solana"

def _txns1h(p: Dict[str, Any]) -> int:
    h1 = ((p or {}).get("txns") or {}).get("h1") or {}
    return int((h1.get("buys") or 0) + (h1.get("sells") or 0))

def _normalize_exclude_quotes(exclude_quotes: Optional[Iterable[Any]]) -> set[str]:
    out: set[str] = set()
    if not exclude_quotes:
        return out
    try:
        for item in exclude_quotes:
            if item is None:
                continue
            if isinstance(item, (list, tuple, set)):
                for sub in item:
                    if sub is None:
                        continue
                    out.add(str(sub).upper())
            else:
                out.add(str(item).upper())
    except TypeError:
        # Non iterabile: converto direttamente
        out.add(str(exclude_quotes).upper())
    return out

class MarketDataProvider:
    def __init__(
        self,
        queries: Iterable[str] = DEFAULT_QUERIES,
        refresh_sec: int = 60,
        session: Optional[requests.Session] = None,
        on_update: Optional[Callable[[pd.DataFrame, float], None]] = None,
        max_retries: int = 3,
        backoff_base: float = 0.7,
        timeout_sec: int = 15,
        only_raydium: bool = False,
        min_liq: float = 0.0,
        exclude_quotes: Optional[Iterable[str]] = None,
        preserve_on_empty: bool = False,
    ) -> None:
        self.queries = list(queries)
        self.refresh_sec = max(5, int(refresh_sec))
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.timeout_sec = timeout_sec

        self._session = session or requests.Session()
        self._session.headers.update(UA_HEADERS)

        self._df: pd.DataFrame = pd.DataFrame(columns=EXPECTED_COLS)
        self._last_updated: float = 0.0
        self._last_http_codes: List[int] = []
        self._lock = threading.Lock()

        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._on_update = on_update

        self.only_raydium = bool(only_raydium)
        self.min_liq = float(min_liq) if min_liq else 0.0
        self.exclude_quotes = _normalize_exclude_quotes(exclude_quotes)
        self.preserve_on_empty = bool(preserve_on_empty)

    # -------- Public API --------
    def update(self) -> Tuple[pd.DataFrame, float]:
        pairs, codes = self._multi_search(self.queries)
        self._last_http_codes = codes
        df_raw = self._pairs_to_df(pairs)
        df = self._apply_filters(df_raw)

        if self.preserve_on_empty and df.empty and not df_raw.empty:
            with self._lock:
                df_prev = self._df.copy()
                ts_prev = self._last_updated
            if not df_prev.empty:
                LOG.warning("Fetch filtrato vuoto — preservo snapshot precedente (%d righe).", len(df_prev))
                return df_prev, ts_prev

        ts = time.time()
        with self._lock:
            self._df = df
            self._last_updated = ts

        if self._on_update:
            try: self._on_update(df, ts)
            except Exception as e: LOG.warning("on_update callback error: %s", e)
        return df.copy(), ts

    def get_snapshot(self) -> Tuple[pd.DataFrame, float]:
        with self._lock:
            df = self._df.copy()
            for col in EXPECTED_COLS:
                if col not in df.columns:
                    df[col] = None
            return df[EXPECTED_COLS].copy(), self._last_updated

    def get_last_http_codes(self) -> List[int]:
        with self._lock:
            return list(self._last_http_codes)

    def start_auto_refresh(self) -> None:
        if self._thread and self._thread.is_alive(): return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._loop, name="MarketDataProviderLoop", daemon=True)
        self._thread.start()

    def stop(self, join: bool = False) -> None:
        self._stop_evt.set()
        if join and self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def set_queries(self, queries: Iterable[str]) -> None:
        self.queries = list(queries)

    def set_refresh_interval(self, seconds: int) -> None:
        self.refresh_sec = max(5, int(seconds))

    def set_filters(
        self,
        *,
        only_raydium: Optional[bool] = None,
        min_liq: Optional[float] = None,
        exclude_quotes: Optional[Iterable[Any]] = None,
    ) -> None:
        if only_raydium is not None: self.only_raydium = bool(only_raydium)
        if min_liq is not None: self.min_liq = float(min_liq) if min_liq else 0.0
        if exclude_quotes is not None: self.exclude_quotes = _normalize_exclude_quotes(exclude_quotes)

    def get_top_by(self, field: str, n: int = 10) -> pd.DataFrame:
        df, _ = self.get_snapshot()
        if df.empty or field not in df.columns: return pd.DataFrame(columns=EXPECTED_COLS)
        return df.sort_values(by=[field], ascending=False).head(n).reset_index(drop=True)

    def find_by_symbol(self, symbol: str) -> pd.DataFrame:
        df, _ = self.get_snapshot()
        if df.empty: return df
        s = (symbol or "").upper()
        return df[df["baseSymbol"].str.upper() == s].reset_index(drop=True)

    def export_csv(self, path: str) -> None:
        df, _ = self.get_snapshot()
        df.to_csv(path, index=False)

    # -------- Internal --------
    def _loop(self) -> None:
        try: self.update()
        except Exception as e: LOG.warning("Initial update() failed: %s", e)
        while not self._stop_evt.is_set():
            jitter = random.uniform(0.0, 0.35)
            wait_s = self.refresh_sec * (1.0 + jitter)
            end = time.time() + wait_s
            while not self._stop_evt.is_set() and time.time() < end:
                time.sleep(0.25)
            if self._stop_evt.is_set(): break
            try: self.update()
            except Exception as e: LOG.warning("update() failed: %s", e)

    def _multi_search(self, queries: Iterable[str]) -> Tuple[List[dict], List[int]]:
        all_pairs: List[dict] = []
        seen = set()
        codes: List[int] = []
        for q in queries:
            url = DEXSEARCH_BASE + requests.utils.quote(q, safe="")
            data, code = self._fetch_with_retry(url)
            codes.append(code)
            pairs = (data or {}).get("pairs", []) if data else []
            for p in pairs:
                if not _is_solana_pair(p): continue
                addr = p.get("pairAddress") or p.get("url") or (p.get("baseToken", {}).get("address"))
                key = str(addr)
                if key and key not in seen:
                    seen.add(key)
                    all_pairs.append(p)
        return all_pairs, codes

    def _fetch_with_retry(self, url: str) -> Tuple[Optional[dict], int]:
        last: Tuple[Optional[dict], int] = (None, 0)
        for i in range(self.max_retries):
            try:
                r = self._session.get(url, timeout=self.timeout_sec)
                code = r.status_code
                if r.ok: return r.json(), code
                last = (None, code)
                if code in (429, 500, 502, 503, 504):
                    time.sleep(self.backoff_base * (i + 1) + random.uniform(0, 0.4))
                    continue
                break
            except Exception:
                last = (None, -1)
                time.sleep(self.backoff_base * (i + 1) + random.uniform(0, 0.4))
        return last

    def _pairs_to_df(self, pairs: List[dict]) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for p in pairs:
            base = ((p.get("baseToken") or {}).get("symbol")) or ""
            quote = ((p.get("quoteToken") or {}).get("symbol")) or ""
            rows.append({
                "pairAddress": p.get("pairAddress") or "",
                "url": p.get("url") or "",
                "dexId": (p.get("dexId") or ""),
                "baseSymbol": base,
                "quoteSymbol": quote,
                "priceUsd": _first_num(p.get("priceUsd")),
                "liquidityUsd": _first_num(((p.get("liquidity") or {}).get("usd"))),
                "volume24hUsd": _first_num(((p.get("volume") or {}).get("h24"))),
                "priceChange24hPct": _first_num(((p.get("priceChange") or {}).get("h24"))),
                "pairCreatedAt": p.get("pairCreatedAt") or 0,
                "txns1h": _txns1h(p),
            })
        if not rows:
            return pd.DataFrame(columns=EXPECTED_COLS)
        df = pd.DataFrame(rows)
        for c in EXPECTED_COLS:
            if c not in df.columns: df[c] = None
        df = df.sort_values(by=["volume24hUsd","txns1h","liquidityUsd"], ascending=[False, False, False]).reset_index(drop=True)
        return df

    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df[EXPECTED_COLS]
        out = df.copy()
        if self.only_raydium:
            out = out[out["dexId"].str.lower() == "raydium"]
        if self.min_liq:
            out = out[(out["liquidityUsd"].fillna(0) >= float(self.min_liq))]
        if self.exclude_quotes:
            out = out[~out["quoteSymbol"].str.upper().isin(self.exclude_quotes)]
        for c in EXPECTED_COLS:
            if c not in out.columns: out[c] = None
        return out.reset_index(drop=True)[EXPECTED_COLS]
