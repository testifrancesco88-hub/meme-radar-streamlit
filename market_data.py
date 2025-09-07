# market_data.py — Provider DexScreener v3.1 (priceChange enrichment)
# - Search multipla su DexScreener
# - Enrichment: /latest/dex/pairs/{chainId}/{pairIds} per riempire priceChange (h1/h4/h6/h24)
# - Normalizza record mantenendo priceChange nested + flat fallback
# - Filtri: only_raydium, min_liq, exclude_quotes
# - Thread di auto-refresh
# - Snapshot in DataFrame con campi consumati dalla UI

from __future__ import annotations
import time
import threading
import random
from typing import Iterable, Optional, Tuple, Dict, Any, List

import requests
import pandas as pd

DEX_SEARCH = "https://api.dexscreener.com/latest/dex/search"
DEX_PAIRS  = "https://api.dexscreener.com/latest/dex/pairs/{chainId}/{pairIds}"  # comma-separated pairIds
CHAIN_ID   = "solana"  # questa app è focalizzata su Solana

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

    rec["chainId"]      = p.get("chainId") or CHAIN_ID
    rec["baseSymbol"]   = base.get("symbol") or ""
    rec["quoteSymbol"]  = quote.get("symbol") or ""
    rec["baseAddress"]  = base.get("address") or ""
    rec["quoteAddress"] = quote.get("address") or ""
    rec["pairAddress"]  = p.get("pairAddress") or ""

    rec["dexId"]        = p.get("dexId") or ""
    rec["url"]          = p.get("url") or ""

    # prezzi e metriche
    rec["priceUsd"]       = _to_float(p.get("priceUsd"), None)
    rec["liquidityUsd"]   = _liq_usd(p.get("liquidity"))
    rec["volume24hUsd"]   = _vol24_usd(p.get("volume"))
    rec["txns1h"]         = _sum_tx1h(p.get("txns"))

    # timestamps
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
    Recupera e normalizza i pairs da DexScreener (Solana).
    - /latest/dex/search?q=...
    - Enrichment: /latest/dex/pairs/{chainId}/{pairIds} (blocchi) per riempire priceChange
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

    # -------- Enrichment: pairs endpoint (riempie priceChange h1/h4/h6/h24) --------
    def _enrich_price_change(self, recs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[int | str]]:
        """
        Per i pair senza priceChange.h1/h4, chiama /latest/dex/pairs/solana/{comma-separated}
        a blocchi (max 30 id per chiamata, prudenziale) e aggiorna i record in-place.
        """
        # Se pochi record o già completi, si esce subito
        if not recs:
            return recs, []
        need_ids: List[str] = []
        for r in recs:
            pc = r.get("priceChange")
            has_h1 = isinstance(pc, dict) and (pc.get("h1") is not None)
            has_h4 = isinstance(pc, dict) and (pc.get("h4") is not None)
            # Se mancano h1 o h4, proviamo ad arricchire (NB: la UI farà fallback su h6)
            if (not has_h1) or (not has_h4):
                pid = r.get("pairAddress")
                if pid:
                    need_ids.append(pid)

        # Niente da arricchire
        if not need_ids:
            return recs, []

        # Raggruppa in blocchi (prudenziale 30 id)
        CHUNK = 30
        codes: List[int | str] = []
        idx_map = {r.get("pairAddress"): r for r in recs if r.get("pairAddress")}
        session = self._session

        for i in range(0, len(need_ids), CHUNK):
            chunk = need_ids[i:i+CHUNK]
            pair_ids = ",".join(chunk)
            url = DEX_PAIRS.format(chainId=CHAIN_ID, pairIds=pair_ids)
            try:
                resp = session.get(url, timeout=20)
                codes.append(resp.status_code)
                if not resp.ok:
                    continue
                data = resp.json() or {}
                pairs = data.get("pairs") or []
                for p in pairs:
                    pid = p.get("pairAddress")
                    tgt = idx_map.get(pid)
                    if not tgt:
                        continue
                    pc = p.get("priceChange") if isinstance(p.get("priceChange"), dict) else None
                    if not pc:
                        continue
                    # aggiorna nested + flat
                    tgt["priceChange"] = pc
                    # flat
                    tgt["priceChange1hPct"] = tgt.get("priceChange1hPct") or _safe_get_price_change(pc, "h1")
                    # h4 può mancare, ma UI farà fallback su h6
                    tgt["priceChange4hPct"] = tgt.get("priceChange4hPct") or _safe_get_price_change(pc, "h4")
                    tgt["priceChange6hPct"] = tgt.get("priceChange6hPct") or _safe_get_price_change(pc, "h6")
                    tgt["priceChange24hPct"]= tgt.get("priceChange24hPct") or _safe_get_price_change(pc, "h24")
            except Exception:
                codes.append("ERR")
        return recs, codes

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

        # Enrichment: per contenere le chiamate, priorità ai pair con volume24h più alto
        if all_recs:
            try:
                # ordina per volume desc e limita a 180 rec da arricchire (rate-limit safe)
                tmp_df = pd.DataFrame(all_recs)
                tmp_df["__v24__"] = pd.to_numeric(tmp_df.get("volume24hUsd", 0), errors="coerce").fillna(0)
                top_ids = tmp_df.sort_values("__v24__", ascending=False)["pairAddress"].dropna().astype(str).tolist()[:180]
                # mantieni ordine e filtra i record in base a top_ids
                id_set = set(top_ids)
                top_recs = [r for r in all_recs if r.get("pairAddress") in id_set]
                enr_recs, enr_codes = self._enrich_price_change(top_recs)
                codes.extend(enr_codes)
                # rimpiazza nei record totali quelli arricchiti
                by_id = {r.get("pairAddress"): r for r in all_recs if r.get("pairAddress")}
                for r in enr_recs:
                    pid = r.get("pairAddress")
                    if pid in by_id:
                        by_id[pid].update(r)
                all_recs = list(by_id.values()) + [r for r in all_recs if not r.get("pairAddress")]
            except Exception:
                # se l'arricchimento fallisce, proseguiamo con i dati disponibili
                pass

        df = pd.DataFrame(all_recs) if all_recs else pd.DataFrame(columns=[
            "chainId","baseSymbol","quoteSymbol","baseAddress","quoteAddress","pairAddress",
            "dexId","url","priceUsd","liquidityUsd","volume24hUsd","txns1h",
            "pairCreatedAt","priceChange","priceChange1hPct","priceChange4hPct","priceChange6hPct","priceChange24hPct",
        ])

        ts = time.time()
        with self._lock:
            # tieni ultimi 20 codici HTTP per diagnostica
            self._http_codes = (self._http_codes + codes)[-20:]
        return df, ts
