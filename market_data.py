# market_data.py — Provider dati live per token/pairs (Solana) con aggiornamento continuo
# Recupera: prezzo USD, liquidità USD, volume 24h USD, variazione 24h (%), txns 1h, timestamp creazione pair.
# Fonte: DexScreener /latest/dex/search?q=...
# Autore: Meme Radar — Francesco edition ⚡

from __future__ import annotations

import math
import random
import threading
import time
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

# ---------------- Config di base ----------------
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
    # logging minimal se non già configurato a livello app
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    LOG.addHandler(handler)
    LOG.setLevel(logging.INFO)


# ---------------- Helpers ----------------
def _first_num(*vals) -> Optional[float]:
    """Ritorna il primo valore numerico valido (float) tra quelli passati; altrimenti None."""
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


# ---------------- Provider ----------------
class MarketDataProvider:
    """
    Provider di dati live basato su DexScreener /search con supporto:
      - multi-search (più query mirate) e dedup per pairAddress
      - retry con backoff e User-Agent realistico
      - filtri opzionali (solo Raydium, min liquidity, exclude quote)
      - snapshot in pandas.DataFrame
      - aggiornamento continuo in thread separato (start/stop sicuri)

    Colonne dello snapshot:
      ['pairAddress','url','dexId','baseSymbol','quoteSymbol',
       'priceUsd','liquidityUsd','volume24hUsd','priceChange24hPct',
       'pairCreatedAt','txns1h']

    Uso base:
        provider = MarketDataProvider(refresh_sec=60)
        df, ts = provider.update()               # fetch singolo
        df, ts = provider.get_snapshot()

        provider.start_auto_refresh()            # aggiornamento continuo
        ...
        provider.stop(join=True)

    Filtri:
        provider.set_filters(only_raydium=True, min_liq=20000, exclude_quotes={'USDC','USDT'})

    Utilities:
        provider.get_top_by('volume24hUsd', n=10)
        provider.find_by_symbol('WIF')  # case-insensitive sul simbolo base
        provider.export_csv('snapshot.csv')
    """

    # ---------- Init ----------
    def __init__(
        self,
        queries: Iterable[str] = DEFAULT_QUERIES,
        refresh_sec: int = 60,
        session: Optional[requests.Session] = None,
        on_update: Optional[Callable[[pd.DataFrame, float], None]] = None,
        max_retries: int = 3,
        backoff_base: float = 0.7,
        timeout_sec: int = 15,
        # Filtri opzionali all'init
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

        # Snapshot
        self._df: pd.DataFrame = pd.DataFrame()
        self._last_updated: float = 0.0
        self._last_http_codes: List[int] = []
        self._lock = threading.Lock()

        # Loop
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._on_update = on_update

        # Filtri
        self.only_raydium = bool(only_raydium)
        self.min_liq = float(min_liq) if min_liq else 0.0
        self.exclude_quotes = set(map(str.upper, exclude_quotes or []))

        # Se True, non sovrascrive snapshot con DF vuoto (mantiene ultimi dati validi)
        self.preserve_on_empty = bool(preserve_on_empty)

    # ---------- Public API ----------
    def update(self) -> Tuple[pd.DataFrame, float]:
        """Esegue un ciclo di fetch, applica filtri e aggiorna lo snapshot interno."""
        pairs, codes = self._multi_search(self.queries)
        self._last_http_codes = codes

        df_raw = self._pairs_to_df(pairs)
        df = self._apply_filters(df_raw)

        if self.preserve_on_empty and df.empty and not df_raw.empty:
            # Mantieni l'ultimo snapshot non-vuoto per evitare "drop" momentanei
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
            try:
                self._on_update(df, ts)
            except Exception as e:
                LOG.warning("on_update callback error: %s", e)

        return df.copy(), ts

    def get_snapshot(self) -> Tuple[pd.DataFrame, float]:
        """Ritorna una copia dello snapshot corrente (DataFrame, epoch timestamp)."""
        with self._lock:
            return self._df.copy(), self._last_updated

    def get_last_http_codes(self) -> List[int]:
        """Codici HTTP dell'ultimo ciclo di multi-search (uno per query)."""
        with self._lock:
            return list(self._last_http_codes)

    def start_auto_refresh(self) -> None:
        """Avvia un thread daemon che esegue update() a intervalli regolari (con jitter)."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._loop, name="MarketDataProviderLoop", daemon=True)
        self._thread.start()

    def stop(self, join: bool = False) -> None:
        """Ferma il loop di aggiornamento."""
        self._stop_evt.set()
        if join and self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def set_queries(self, queries: Iterable[str]) -> None:
        """Aggiorna l'insieme di query DexScreener da usare (multi-search)."""
        self.queries = list(queries)

    def set_refresh_interval(self, seconds: int) -> None:
        """Aggiorna l'intervallo di refresh (min 5s)."""
        self.refresh_sec = max(5, int(seconds))

    def set_filters(
        self,
        *,
        only_raydium: Optional[bool] = None,
        min_liq: Optional[float] = None,
        exclude_quotes: Optional[Iterable[str]] = None,
    ) -> None:
        """Imposta/aggiorna i filtri da applicare allo snapshot."""
        if only_raydium is not None:
            self.only_raydium = bool(only_raydium)
        if min_liq is not None:
            self.min_liq = float(min_liq) if min_liq else 0.0
        if exclude_quotes is not None:
            self.exclude_quotes = set(map(str.upper, exclude_quotes))

    # ---------- Utility ----------
    def get_top_by(self, field: str, n: int = 10) -> pd.DataFrame:
        """Ritorna le top-n righe ordinate desc per 'field' (se esiste)."""
        df, _ = self.get_snapshot()
        if df.empty or field not in df.columns:
            return pd.DataFrame()
        return df.sort_values(by=[field], ascending=False).head(n).reset_index(drop=True)

    def find_by_symbol(self, symbol: str) -> pd.DataFrame:
        """Trova righe con baseSymbol == symbol (case-insensitive)."""
        df, _ = self.get_snapshot()
        if df.empty:
            return df
        s = (symbol or "").upper()
        return df[df["baseSymbol"].str.upper() == s].reset_index(drop=True)

    def export_csv(self, path: str) -> None:
        """Esporta lo snapshot corrente in CSV."""
        df, _ = self.get_snapshot()
        df.to_csv(path, index=False)

    # ---------- Internal ----------
    def _loop(self) -> None:
        # primo update immediato
        try:
            self.update()
        except Exception as e:
            LOG.warning("Initial update() failed: %s", e)

        # ciclo continuo con jitter
        while not self._stop_evt.is_set():
            jitter = random.uniform(0.0, 0.35)
            wait_s = self.refresh_sec * (1.0 + jitter)
            end = time.time() + wait_s
            while not self._stop_evt.is_set() and time.time() < end:
                time.sleep(0.25)
            if self._stop_evt.is_set():
                break
            try:
                self.update()
            except Exception as e:
                LOG.warning("update() failed: %s", e)

    def _multi_search(self, queries: Iterable[str]) -> Tuple[List[dict], List[int]]:
        """Esegue più chiamate /search e unisce i risultati (no duplicati)."""
        all_pairs: List[dict] = []
        seen = set()
        codes: List[int] = []
        for q in queries:
            url = DEXSEARCH_BASE + requests.utils.quote(q, safe="")
            data, code = self._fetch_with_retry(url)
            codes.append(code)
            pairs = (data or {}).get("pairs", []) if data else []
            for p in pairs:
                if not _is_solana_pair(p):
                    continue
                addr = p.get("pairAddress") or p.get("url") or (p.get("baseToken", {}).get("address"))
                key = str(addr)
                if key and key not in seen:
                    seen.add(key)
                    all_pairs.append(p)
        return all_pairs, codes

    def _fetch_with_retry(self, url: str) -> Tuple[Optional[dict], int]:
        """GET con retry/backoff su 429/5xx; ritorna (json|None, http_code)."""
        last: Tuple[Optional[dict], int] = (None, 0)
        for i in range(self.max_retries):
            try:
                r = self._session.get(url, timeout=self.timeout_sec)
                code = r.status_code
                if r.ok:
                    return r.json(), code
                last = (None, code)
                # retry solo su codici “morbidi”
                if code in (429, 500, 502, 503, 504):
                    time.sleep(self.backoff_base * (i + 1) + random.uniform(0, 0.4))
                    continue
                break
            except Exception:
                last = (None, -1)
                time.sleep(self.backoff_base * (i + 1) + random.uniform(0, 0.4))
        return last

    def _pairs_to_df(self, pairs: List[dict]) -> pd.DataFrame:
        """Normalizza i campi in un DataFrame comodo e coerente."""
        rows: List[Dict[str, Any]] = []
        for p in pairs:
            base = ((p.get("baseToken") or {}).get("symbol")) or ""
            quote = ((p.get("quoteToken") or {}).get("symbol")) or ""
            rows.append(
                {
                    "pairAddress": p.get("pairAddress") or "",
                    "url": p.get("url") or "",
                    "dexId": (p.get("dexId") or ""),
                    "baseSymbol": base,
                    "quoteSymbol": quote,
                    "priceUsd": _first_num(p.get("priceUsd")),
                    "liquidityUsd": _first_num(((p.get("liquidity") or {}).get("usd"))),
                    "volume24hUsd": _first_num(((p.get("volume") or {}).get("h24"))),
                    "priceChange24hPct": _first_num(((p.get("priceChange") or {}).get("h24"))),  # es. 12.3 = +12.3%
                    "pairCreatedAt": p.get("pairCreatedAt") or 0,
                    "txns1h": _txns1h(p),
                }
            )
        df = pd.DataFrame(rows)

        # Ordinamento di default: volume 24h desc (se disponibile), poi txns1h
        if not df.empty:
            by = [c for c in ["volume24hUsd", "txns1h", "liquidityUsd"] if c in df.columns]
            df = df.sort_values(by=by, ascending=[False] * len(by)).reset_index(drop=True)
        return df

    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applica i filtri impostati (solo Raydium / min_liq / exclude_quotes)."""
        if df.empty:
            return df
        out = df.copy()

        if self.only_raydium and "dexId" in out.columns:
            out = out[out["dexId"].str.lower() == "raydium"]

        if self.min_liq and "liquidityUsd" in out.columns:
            out = out[(out["liquidityUsd"].fillna(0) >= float(self.min_liq))]

        if self.exclude_quotes and "quoteSymbol" in out.columns:
            out = out[~out["quoteSymbol"].str.upper().isin(self.exclude_quotes)]

        return out.reset_index(drop=True)


# ---------------- Esempio d’uso CLI ----------------
if __name__ == "__main__":
    LOG.setLevel(logging.DEBUG)
    prov = MarketDataProvider(refresh_sec=30, only_raydium=False, min_liq=0, exclude_quotes={"USDC", "USDT"})
    df, ts = prov.update()
    print(f"Aggiornato @ {time.strftime('%H:%M:%S', time.localtime(ts))} — {len(df)} righe")
    print(df.head(10)[["baseSymbol", "quoteSymbol", "priceUsd", "liquidityUsd", "volume24hUsd", "priceChange24hPct", "txns1h", "url"]])

    # Avvio loop continuo per 90 secondi
    prov.start_auto_refresh()
    time.sleep(90)
    prov.stop(join=True)
    df2, ts2 = prov.get_snapshot()
    print(f"\nDopo loop — {len(df2)} righe @ {time.strftime('%H:%M:%S', time.localtime(ts2))}")
