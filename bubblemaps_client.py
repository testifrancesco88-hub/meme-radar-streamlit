# bubblemaps_client.py — integrazione Bubblemaps (cluster/concentrazione)
from __future__ import annotations
import time
from typing import Dict, Optional, Tuple
import requests

BM_BASE = "https://api.bubblemaps.io"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "MemeRadar/1.0 (+streamlit)",
}

class BubbleMapsError(Exception):
    pass

def fetch_map(chain: str, token_address: str, api_key: str, session: Optional[requests.Session] = None) -> dict:
    """
    Chiama Bubblemaps 'Get Map Data' e ritorna il JSON.
    Restituisce clusters, decentralization_score, ecc.
    Doc: https://docs.bubblemaps.io/data/api/endpoints/get-map-data
    """
    if not api_key:
        raise BubbleMapsError("Missing Bubblemaps API key")
    url = f"{BM_BASE}/maps/{chain}/{token_address}"
    params = {
        "use_magic_nodes": "true",
        "return_nodes": "false",
        "return_relationships": "false",
        "return_clusters": "true",
        "return_decentralization_score": "true",
    }
    s = session or requests.Session()
    headers = {**DEFAULT_HEADERS, "X-ApiKey": api_key}
    r = s.get(url, params=params, headers=headers, timeout=25)
    if r.status_code == 404:
        # Nessun holder trovato → non possiamo stimare; NON alziamo eccezione
        return {"clusters": [], "decentralization_score": None, "_status": 404}
    if r.status_code == 422:
        raise BubbleMapsError("Unprocessable (controlla chain/address)")
    if r.status_code == 429:
        raise BubbleMapsError("Rate limited (429) - riprova più tardi")
    if not r.ok:
        raise BubbleMapsError(f"HTTP {r.status_code}")
    return r.json()

def check_wallet_clusters(
    token_address: str,
    *,
    chain: str = "solana",
    api_key: Optional[str] = None,
    session: Optional[requests.Session] = None,
    # soglie “buon senso” per meme coin: 1 cluster >35% o top-3 >60% = HIGH
    th_top1: float = 0.35,
    th_top3: float = 0.60,
) -> Dict:
    """
    Analizza la concentrazione per cluster. Se un piccolo numero di cluster
    detiene la maggioranza, marca 'HIGH_RISK' e restituisce dettagli.
    Ritorna: { is_high_risk: bool, reason: str, top1: float, top3: float, score: float|None }
    """
    if not token_address:
        return {"is_high_risk": False, "reason": "no-address", "top1": None, "top3": None, "score": None}

    data = fetch_map(chain, token_address, api_key or "")
    clusters = data.get("clusters") or []
    score = data.get("decentralization_score")  # può essere None

    # shares in [0,1] (42% => 0.42) — vedi doc
    shares = sorted([float(c.get("share", 0) or 0) for c in clusters], reverse=True)
    top1 = shares[0] if shares else 0.0
    top3 = sum(shares[:3]) if shares else 0.0

    high = False
    reason = []
    if top1 >= th_top1:
        high = True; reason.append(f"Top1 cluster {top1:.0%} ≥ {th_top1:.0%}")
    if top3 >= th_top3:
        high = True; reason.append(f"Top3 cluster {top3:.0%} ≥ {th_top3:.0%}")
    # opzionale: se esiste uno score di decentralizzazione molto basso (es. <30 su 100) lo consideriamo rischio
    try:
        if score is not None and float(score) < 30:
            high = True; reason.append(f"Decentralization score {float(score):.0f} < 30")
    except Exception:
        pass

    return {
        "is_high_risk": bool(high),
        "reason": " ; ".join(reason) if reason else "OK",
        "top1": top1,
        "top3": top3,
        "score": score,
        "_raw_status": data.get("_status"),
    }
