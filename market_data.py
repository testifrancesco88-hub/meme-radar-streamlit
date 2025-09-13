# market_data.py
# Provider per Meme Radar — Solana
# Requisiti: requests, pandas

import time
import threading
from typing import List, Tuple, Dict, Any, Optional

import requests
import pandas as pd


DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 MemeRadar/1.0",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


class MarketDataProvider:
    """
    Aggrega risultati da DexScreener /search per una lista di query.
    Mantiene in memoria l'ultimo snapshot come DataFrame normalizzato e timestamp UNIX.
    Applica filtri provider-level (dex, min_liq, exclude_quotes).
    Thread di auto-refresh opzionale.
    """

    def __init__(self, refresh_sec: int = 60, preserve_on_empty: bool = True, timeout: int = 15):
        self.refresh_sec = max(5, int(refresh_sec))
        self.preserve_on_empty = bool(preserve_on_empty)
        self.timeout = int(timeout)

        self._queries: List[str] = []
        self._filters = {
            "only_raydium": False,
            "min_liq": 0.0,
            "exclude_quotes": [],  # ["USDC", "USDT", ...]
        }

        self._snapshot_df: pd.DataFrame = pd.DataFrame()
        self._snapshot_ts: float = 0.0
        self._last_http_codes: Dict[str, Any] = {}  # query -> code/ERR
        self._lock = threading.Lock()
        self._running = False
        self._th: Optional[threading.Thread] = None

    # ---------------- Public API ----------------

    def set_queries(self, queries: List[str]) -> None:
        with self._lock:
            self._queries = list(queries or [])

    def set_filters(self, *, only_raydium: bool, min_liq: float, exclude_quotes: List[str]) -> None:
        with self._lock:
            self._filters["only_raydium"] = bool(only_raydium)
            try:
                self._filters["min_liq"] = float(min_liq or 0.0)
            except Exception:
                self._filters["min_liq"] = 0.0
            self._filters["exclude_quotes"] = [str(x).upper() for x in (exclude_quotes or [])]

    def start_auto_refresh(self) -> None:
        if self._running:
            return
        self._running = True
        self._th = threading.Thread(target=self._auto_loop, daemon=True)
        self._th.start()

    def stop(self) -> None:
        self._running = False

    def get_snapshot(self) -> Tuple[pd.DataFrame, float]:
        with self._lock:
            return self._snapshot_df.copy(), float(self._snapshot_ts)

    def get_last_http_codes(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._last_http_codes)

    # ---------------- Internal helpers ----------------

    def _auto_loop(self):
        # Primo fetch immediato
        try:
            self._refresh_once()
        except Exception:
            pass

        while self._running:
            t0 = time.time()
            try:
                self._refresh_once()
            except Exception:
                # non rompiamo il loop
                pass
            # sleep rimanente
            elapsed = time.time() - t0
            to_sleep = max(1.0, self.refresh_sec - elapsed)
            time.sleep(to_sleep)

    def _refresh_once(self):
        queries = []
        with self._lock:
            queries = list(self._queries)

        all_rows = []
        http_codes = {}
        for q in queries:
            try:
                params = {"q": q}
                r = requests.get(DEX_SEARCH_URL, params=params, headers=UA_HEADERS, timeout=self.timeout)
                http_codes[q] = r.status_code
                if not r.ok:
                    continue
                data = r.json()
                pairs = data.get("pairs") or []
                for p in pairs:
                    row = self._normalize_pair(p)
                    if row:
                        all_rows.append(row)
            except Exception:
                http_codes[q] = "ERR"

        if not all_rows:
            # Se vuoto e vogliamo preservare, non tocchiamo lo snapshot
            with self._lock:
                self._last_http_codes = http_codes
            return

        df = pd.DataFrame(all_rows)
        df = self._apply_filters(df)

        with self._lock:
            self._snapshot_df = df
            self._snapshot_ts = time.time()
            self._last_http_codes = http_codes

    # ---- mapping ----
    def _normalize_pair(self, p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            if (p.get("chainId") or "").lower() != "solana":
                return None

            base = p.get("baseToken", {}) or {}
            quote = p.get("quoteToken", {}) or {}
            liq = p.get("liquidity", {}) or {}
            vol = p.get("volume", {}) or {}
            tx  = p.get("txns", {}) or {}
            price_change = p.get("priceChange", {}) or {}

            tx_buys = 0
            tx_sells = 0
            h1 = tx.get("h1", {})
            try:
                tx_buys = int(h1.get("buys", 0) or 0)
            except Exception:
                tx_buys = 0
            try:
                tx_sells = int(h1.get("sells", 0) or 0)
            except Exception:
                tx_sells = 0
            txns1h = tx_buys + tx_sells

            row = {
                "baseSymbol": base.get("symbol") or "",
                "quoteSymbol": quote.get("symbol") or "",
                "dexId": p.get("dexId") or "",
                "liquidityUsd": float(liq.get("usd") or 0),
                "txns1h": int(txns1h),
                "volume24hUsd": float(vol.get("h24") or 0),
                "priceUsd": (float(p.get("priceUsd")) if p.get("priceUsd") not in (None, "") else None),
                "pairCreatedAt": int(p.get("pairCreatedAt") or 0),  # seconds
                "url": p.get("url") or "",
                "baseAddress": base.get("address") or "",
                "pairAddress": p.get("pairAddress") or "",
                # porta dentro il blocco priceChange così com'è (h1/h4/h6/h24…)
                "priceChange": price_change if isinstance(price_change, dict) else {},
            }
            return row
        except Exception:
            return None

    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        f = self._filters
        out = df.copy()

        # only raydium
        if f.get("only_raydium", False):
            try:
                out = out[out["dexId"].str.lower() == "raydium"]
            except Exception:
                pass

        # min liquidity
        try:
            out = out[pd.to_numeric(out["liquidityUsd"], errors="coerce").fillna(0) >= float(f.get("min_liq", 0.0))]
        except Exception:
            pass

        # exclude quote symbols
        excl = [s.upper() for s in (f.get("exclude_quotes") or [])]
        if excl:
            try:
                mask = ~out["quoteSymbol"].astype(str).str.upper().isin(excl)
                out = out[mask]
            except Exception:
                pass

        # dedup: preferisci vol24hUsd maggiore per la stessa pairAddress
        try:
            out = (out.sort_values(by=["volume24hUsd"], ascending=False)
                     .drop_duplicates(subset=["pairAddress"], keep="first"))
        except Exception:
            out = out.drop_duplicates()

        out.reset_index(drop=True, inplace=True)
        return out
