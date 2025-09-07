# getmoni_client.py — integrazione GetMoni (social sentiment)
from __future__ import annotations
import time
from typing import Dict, Optional, Tuple
import requests

MONI_BASE = "https://api.discover.getmoni.io/api/v3"

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "MemeRadar/1.0 (+streamlit)",
}

class MoniError(Exception):
    pass

def _mk_headers(api_key: str, auth_header: str = "X-API-Key") -> Dict[str, str]:
    """
    Alcune integrazioni usano X-API-Key, altre Authorization: Bearer.
    Di default usiamo X-API-Key, ma supportiamo 'Authorization' con Bearer.
    """
    h = {**DEFAULT_HEADERS}
    if not api_key:
        return h
    if auth_header.lower() == "authorization":
        h["Authorization"] = f"Bearer {api_key}"
    else:
        # copriamo sia 'X-API-Key' che una variante minuscola
        h["X-API-Key"] = api_key
        h["x-api-key"] = api_key
    return h

def _get_json(path: str, api_key: str, params: Optional[dict] = None, auth_header: str = "X-API-Key", timeout: int = 20) -> Tuple[Optional[dict], int]:
    url = f"{MONI_BASE}/{path.lstrip('/')}"
    try:
        r = requests.get(url, headers=_mk_headers(api_key, auth_header), params=params or {}, timeout=timeout)
        if r.status_code == 429:
            return None, 429
        if not r.ok:
            return None, r.status_code
        return r.json(), r.status_code
    except Exception:
        return None, -1

def _num(*candidates, default=None):
    for c in candidates:
        try:
            if c is None: 
                continue
            x = float(c)
            return x
        except Exception:
            continue
    return default

def _last_ts_value(series) -> Optional[float]:
    """
    Accetta formati comuni: [{"ts": "...", "value": n}, ...] oppure coppie [ts, value] ecc.
    Ritorna l'ultimo value numerico se disponibile.
    """
    if not isinstance(series, (list, tuple)) or not series:
        return None
    v = series[-1]
    if isinstance(v, dict):
        return _num(v.get("value"), default=None)
    if isinstance(v, (list, tuple)) and len(v) >= 2:
        return _num(v[1], default=None)
    return None

class SocialSentimentAnalyzer:
    """
    Calcola menzioni H24, engagement 'smarts' e un sentiment_score (proxy: Moni Score normalizzato).
    Richiede una mappatura SYMBOL -> @username per interrogare l'account X/Twitter del progetto.
    Endpoints usati:
      - /accounts/{username}/info/full/  (Moni score, mentions, smart mentions, ecc.)
      - /accounts/{username}/info/smart_engagement/  (smarts_count, ecc.)
      - /accounts/{username}/history/mentions_count/?timeframe=H24  (lista punti -> ultimo valore ~ 24h)
    """
    def __init__(
        self,
        api_key: str,
        *,
        auth_header: str = "X-API-Key",
        symbol_to_user: Optional[Dict[str, str]] = None,
        cache_ttl_sec: int = 600,
        min_mentions_24h: int = 50,
        min_smart_engagement: int = 10,
        min_sentiment_score: float = 30.0,  # 0-100 (Moni score come proxy)
    ):
        self.api_key = api_key or ""
        self.auth_header = auth_header
        self.map = { (k or "").upper(): (v or "").lstrip("@") for k, v in (symbol_to_user or {}).items() }
        self.cache: Dict[str, Dict] = {}
        self.cache_ttl = int(cache_ttl_sec)
        self.th_mentions = int(min_mentions_24h)
        self.th_smarts = int(min_smart_engagement)
        self.th_sent = float(min_sentiment_score)

    def update_mapping(self, symbol_to_user: Dict[str, str]):
        self.map.update({ (k or "").upper(): (v or "").lstrip("@") for k, v in (symbol_to_user or {}).items() })

    def _cached(self, key: str) -> Optional[Dict]:
        hit = self.cache.get(key)
        if hit and (time.time() - hit["ts"] <= self.cache_ttl):
            return hit["data"]
        return None

    def _set_cache(self, key: str, data: Dict):
        self.cache[key] = {"ts": time.time(), "data": data}

    def _fetch_account_full(self, username: str) -> Tuple[Optional[dict], int]:
        return _get_json(f"accounts/{username}/info/full/", self.api_key, auth_header=self.auth_header)

    def _fetch_engagement(self, username: str) -> Tuple[Optional[dict], int]:
        return _get_json(f"accounts/{username}/info/smart_engagement/", self.api_key, auth_header=self.auth_header)

    def _fetch_mentions_h24(self, username: str) -> Tuple[Optional[dict], int]:
        return _get_json(f"accounts/{username}/history/mentions_count/", self.api_key, params={"timeframe":"H24"}, auth_header=self.auth_header)

    def username_for_symbol(self, symbol: str) -> Optional[str]:
        if not symbol: return None
        return self.map.get((symbol or "").upper())

    def analyze(self, symbol: str) -> Dict:
        """
        Ritorna:
          {
            "username": str|None,
            "mentions_24h": int|None,
            "smarts_count": int|None,
            "moni_score": float|None,
            "sentiment_score": float|None,  # alias di moni_score per ora
            "passes": bool,
            "_http": [codes...]
          }
        Se non esiste mappatura -> 'passes=True' (N/A), così non blocchiamo per mancanza di dato.
        """
        user = self.username_for_symbol(symbol)
        if not user:
            return {"username": None, "mentions_24h": None, "smarts_count": None, "moni_score": None, "sentiment_score": None, "passes": True, "_http": []}

        cache_key = f"an:{user}"
        cached = self._cached(cache_key)
        if cached: 
            return cached

        http_codes = []
        # Full info
        full, c1 = self._fetch_account_full(user); http_codes.append(c1)
        # Engagement
        eng,  c2 = self._fetch_engagement(user); http_codes.append(c2)
        # Mentions H24
        mh,   c3 = self._fetch_mentions_h24(user); http_codes.append(c3)

        moni_score = None
        smarts_count = None
        mentions_24h = None

        # Parsers conservativi (chiavi possono variare leggermente fra piani/response)
        if isinstance(full, dict):
            # possibili chiavi: "moni_score", "moniScore", "score"
            moni_score = _num(full.get("moni_score"), full.get("moniScore"), full.get("score"), default=None)
            # a volte è presente "smart_mentions" / "smartMentions"
            if smarts_count is None:
                smarts_count = _num(full.get("smarts_count"), full.get("smartsCount"), default=None)

        if isinstance(eng, dict):
            sc = _num(eng.get("smarts_count"), eng.get("smartsCount"), default=None)
            if sc is not None: smarts_count = sc
            if moni_score is None:
                moni_score = _num(eng.get("moni_score"), eng.get("moniScore"), default=None)

        if isinstance(mh, dict):
            # possibili forme: {"series":[...]} / {"data":[...]}
            series = mh.get("series") or mh.get("data") or mh.get("points")
            last = _last_ts_value(series)
            if last is not None:
                mentions_24h = int(round(last))

        # Sentiment score: usiamo Moni score come proxy [0..100]
        sentiment = moni_score if moni_score is not None else None

        passes = True
        # Applica soglie solo se abbiamo valori
        if mentions_24h is not None and mentions_24h < self.th_mentions:
            passes = False
        if smarts_count is not None and smarts_count < self.th_smarts:
            passes = False
        if sentiment is not None and sentiment < self.th_sent:
            passes = False

        result = {
            "username": f"@{user}",
            "mentions_24h": mentions_24h,
            "smarts_count": int(smarts_count) if smarts_count is not None else None,
            "moni_score": float(moni_score) if moni_score is not None else None,
            "sentiment_score": float(sentiment) if sentiment is not None else None,
            "passes": bool(passes),
            "_http": http_codes,
        }
        self._set_cache(cache_key, result)
        return result
