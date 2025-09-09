# streamlit_app.py — Meme Radar (no-trading) v16.0
# - NIENTE trading: rimossi engine/segnali/posizioni/performance/Jupiter live
# - Restano: KPI mercato, filtri provider, watchlist, PAIRS (tabella) con filtri (Meme/PairAge/Liq log/Vol log),
#   applicazione opzionale dei filtri PAIRS anche alla "diagnostica", Top10 per Volume, alert Telegram, diagnostica mercato
# - Compatibile con Streamlit >= 1.33: usa st.query_params

import os, time, math, random, datetime, json, threading
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from market_data import MarketDataProvider

# =============================== App Config/UI =================================
st.set_page_config(page_title="Meme Radar — Solana", layout="wide")
st.title("Solana Meme Coin Radar")

REFRESH_SEC   = int(os.getenv("REFRESH_SEC", "60"))
PROXY_TICKET  = float(os.getenv("PROXY_TICKET_USD", "150"))
BIRDEYE_URL   = "https://public-api.birdeye.so/defi/tokenlist?chain=solana&sort=createdBlock&order=desc&limit=50"

SEARCH_QUERIES = [
    "chain:solana raydium","chain:solana orca","chain:solana meteora","chain:solana lifinity",
    "chain:solana usdc","chain:solana usdt","chain:solana sol","chain:solana bonk","chain:solana wif","chain:solana pepe",
    "chain:solana pump",
]

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MemeRadar/1.0 Chrome/120 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

# Stato esecuzione
if "app_running" not in st.session_state:
    st.session_state["app_running"] = True
if "last_refresh_ts" not in st.session_state:
    st.session_state["last_refresh_ts"] = time.time()

# ---------------- Sidebar ----------------
with st.sidebar:
    st.header("Impostazioni")

    # --- Start/Stop fetch/refresh ---
    st.subheader("Esecuzione")
    col_run1, col_run2 = st.columns(2)
    if col_run1.button("▶️ Start", disabled=st.session_state["app_running"]):
        st.session_state["app_running"] = True
        st.toast("Esecuzione avviata", icon="✅")
        st.rerun()
    if col_run2.button("⏹ Stop", type="primary", disabled=not st.session_state["app_running"]):
        st.session_state["app_running"] = False
        st.toast("Esecuzione in pausa", icon="⏸️")
        st.rerun()

    st.divider()
    auto_refresh = st.toggle("Auto-refresh", value=True)
    disable_all_filters = st.toggle("Disattiva filtri provider (mostra tutto)", value=False)
    only_raydium = st.toggle("Solo Raydium (dexId=raydium)", value=False, disabled=disable_all_filters)
    min_liq = st.number_input("Min liquidity (USD)", min_value=0, value=0, step=1000, disabled=disable_all_filters)
    exclude_quotes = st.multiselect("Escludi quote (stable/major)",
        options=["USDC","USDT","USDH","SOL","wSOL","stSOL"],
        default=["USDC","USDT"] if not disable_all_filters else [],
        disabled=disable_all_filters
    )
    st.caption(f"Proxy ticket (USD): {PROXY_TICKET:.0f} • Refresh: {REFRESH_SEC}s")

    # Watchlist
    st.divider()
    st.subheader("Watchlist")
    wl_default = os.getenv("WATCHLIST", "")
    watchlist_input = st.text_input("Simboli o address (comma-separated)", value=wl_default,
                                    help="Es: WIF,BONK,So111...,<pairAddress>", key="watchlist_input")
    watchlist_only = st.toggle("Mostra solo watchlist", value=False, key="watchlist_only")

    # Volume 24h filtro “strategico” (solo dataset generale, NO trading)
    st.divider()
    st.subheader("Filtro Volume 24h (USD) — dataset")
    vol24_min = st.number_input("Volume 24h MIN", min_value=0, value=st.session_state.get("vol24_min", 0), step=10000, key="vol24_min")
    vol24_max = st.number_input("Volume 24h MAX (0 = illimitato)", min_value=0, value=st.session_state.get("vol24_max", 0), step=100000, key="vol24_max")

    # Meme Score (per ranking tabella)
    st.divider()
    st.subheader("Meme Score")
    sort_by_meme = st.toggle("Ordina per Meme Score (desc)", value=True)
    liq_min_sweet = st.number_input("Sweet spot liquidity MIN", min_value=0, value=10000, step=1000)
    liq_max_sweet = st.number_input("Sweet spot liquidity MAX", min_value=0, value=200000, step=5000)
    with st.expander("Pesi avanzati (0–100)"):
        w_symbol = st.slider("Peso: Nome 'meme'", 0, 100, 20)
        w_age    = st.slider("Peso: Freschezza (pairCreatedAt)", 0, 100, 20)
        w_txns   = st.slider("Peso: Txns 1h", 0, 100, 25)
        w_liq    = st.slider("Peso: Sweet spot di Liquidity", 0, 100, 20)
        w_dex    = st.slider("Peso: DEX (Raydium > altri)", 0, 100, 15)

    # DEX consentiti (per diagnostica)
    allowed_dex = st.multiselect(
        "DEX consentiti",
        ["raydium", "orca", "meteora", "lifinity"],
        default=st.session_state.get("allowed_dex", ["raydium", "orca", "meteora", "lifinity"])
    )
    st.session_state["allowed_dex"] = allowed_dex

    # Tabella
    st.divider()
    st.subheader("Tabella")
    show_pair_age = st.toggle("Mostra 'Pair Age' (min/ore)", value=True)
    show_top10_table = st.toggle("Tabella: mostra solo Top 10 per Volume 24h", value=True)
    show_h6_fallback = st.toggle("Fallback: mostra Change H6 se H4 mancante", value=True)

    # Filtri PAIRS (tabella)
    st.subheader("Filtri PAIRS (tabella)")
    pairs_meme_min = st.slider(
        "Meme Score min (PAIRS)", 0, 100, 0,
        help="Applica un minimo di Meme Score alla tabella."
    )
    pairs_age_min_h, pairs_age_max_h = st.slider(
        "Pair Age (ore) — range",
        min_value=0.0, max_value=10000.0, value=(0.0, 10000.0), step=0.5,
        help="Filtra per età della pair (in ore) solo nella tabella."
    )
    st.markdown("**Liquidity (USD) — range (log)**")
    pairs_liq_enable = st.toggle(
        "Abilita filtro Liquidity (tabella, log scale)",
        value=False,
        help="Filtra SOLO la tabella PAIRS in scala log."
    )
    pairs_liq_exp_min, pairs_liq_exp_max = st.slider(
        "10^x (min, max) [Liquidity]",
        min_value=0.0, max_value=12.0, value=(0.0, 12.0), step=0.25,
        disabled=not pairs_liq_enable
    )
    pairs_liq_min = 10 ** pairs_liq_exp_min
    pairs_liq_max = 10 ** pairs_liq_exp_max
    if pairs_liq_enable:
        st.caption(f"Range Liquidity tabella: ${int(pairs_liq_min):,} → ${int(pairs_liq_max):,}".replace(",", "."))
    else:
        st.caption("Filtro liquidity disattivato (tabella mostra tutte le liquidità).")

    st.markdown("**Volume 24h (USD) — range (log)**")
    pairs_vol_enable = st.toggle(
        "Abilita filtro Volume 24h (tabella, log scale)",
        value=False,
        help="Filtra SOLO la tabella PAIRS in scala log."
    )
    pairs_vol_exp_min, pairs_vol_exp_max = st.slider(
        "10^x (min, max) [Volume 24h]",
        min_value=0.0, max_value=12.0, value=(0.0, 12.0), step=0.25,
        disabled=not pairs_vol_enable
    )
    pairs_vol_min = 10 ** pairs_vol_exp_min
    pairs_vol_max = 10 ** pairs_vol_exp_max
    if pairs_vol_enable:
        st.caption(f"Range Volume 24h tabella: ${int(pairs_vol_min):,} → ${int(pairs_vol_max):,}".replace(",", "."))
    else:
        st.caption("Filtro volume disattivato (tabella mostra tutti i volumi).")

    # Applicazione facoltativa filtri PAIRS anche alla diagnostica
    st.divider()
    st.subheader("Applica filtri PAIRS → Diagnostica")
    pairs_filters_to_strategy = st.toggle(
        "Applica i filtri PAIRS anche alla diagnostica",
        value=False,
        help="Se attivo, scegli quali filtri PAIRS impattano anche la diagnostica (NO trading)."
    )
    col_apply1, col_apply2 = st.columns(2)
    with col_apply1:
        apply_meme_to_strat = st.checkbox("Meme Score min → diagnostica", value=True, disabled=not pairs_filters_to_strategy)
        apply_age_to_strat  = st.checkbox("Pair Age range → diagnostica", value=True, disabled=not pairs_filters_to_strategy)
    with col_apply2:
        apply_liq_to_strat  = st.checkbox("Liquidity (log) → diagnostica", value=False, disabled=not pairs_filters_to_strategy)
        apply_vol_to_strat  = st.checkbox("Volume 24h (log) → diagnostica", value=False, disabled=not pairs_filters_to_strategy)

    # Parametri diagnostici (ex strategia) — NO trading
    st.divider()
    st.subheader("Parametri diagnostici (NO trading)")
    strat_meme     = st.slider("Soglia Meme Score", 0, 100, st.session_state.get("strat_meme", 70), key="strat_meme")
    strat_txns     = st.number_input("Soglia Txns 1h", min_value=0, value=st.session_state.get("strat_txns", 250), step=25, key="strat_txns")
    strat_turnover = st.number_input("Turnover minimo (Vol24h / Liq)", min_value=0.0, value=float(st.session_state.get("strat_turnover", 1.2)), step=0.1, key="strat_turnover")
    colh1, colh2 = st.columns(2)
    with colh1:
        heat_topN = st.number_input("Heat TopN (per volume)", min_value=3, max_value=20, value=int(st.session_state.get("heat_topN", 10)), step=1, key="heat_topN")
    with colh2:
        heat_avg  = st.number_input("Heat: media Txns1h minima", min_value=0, value=int(st.session_state.get("strat_heat_avg", 120)), step=10, key="strat_heat_avg")
    colchg1, colchg2 = st.columns(2)
    with colchg1:
        chg_min = st.number_input("Change 24h minimo (%)", value=-8, step=1, key="chg_min")
    with colchg2:
        chg_max = st.number_input("Change 24h massimo (%)", value=180, step=10, key="chg_max")

    # Alert Telegram (su tabella)
    st.divider()
    st.subheader("Alert Telegram (tabella)")
    TELEGRAM_BOT_TOKEN = st.text_input("Bot Token", value=os.getenv("TELEGRAM_BOT_TOKEN",""), type="password")
    TELEGRAM_CHAT_ID   = st.text_input("Chat ID", value=os.getenv("TELEGRAM_CHAT_ID",""))
    alert_tx1h_min     = st.number_input("Soglia txns 1h", min_value=0, value=200, step=10)
    alert_liq_min      = st.number_input("Soglia liquidity USD", min_value=0, value=20000, step=1000)
    alert_meme_min     = st.number_input("Soglia Meme Score (0=disattiva)", min_value=0, max_value=100, value=70, step=5)
    alert_cooldown_min = st.number_input("Cooldown alert (min)", min_value=1, value=30, step=5)
    alert_max_per_run  = st.number_input("Max alert per refresh", min_value=1, value=3, step=1)
    enable_alerts      = st.toggle("Abilita alert Telegram", value=False)
    def _tg_send_test():
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            st.warning("Inserisci BOT_TOKEN e CHAT_ID prima del test."); return
        try:
            rq = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                              params={"chat_id": TELEGRAM_CHAT_ID, "text": "✅ Test dal Meme Radar — Telegram OK", "disable_web_page_preview": True}, timeout=15)
            st.success("Messaggio di test inviato ✅" if rq.ok else f"Telegram {rq.status_code}.")
        except Exception as e:
            st.error(f"Errore Telegram: {e}")
    if st.button("Test Telegram"): _tg_send_test()

# ---------- Stato: mostra DOPO i bottoni ----------
running = bool(st.session_state.get("app_running", True))
st.markdown(f"**Stato:** {'🟢 Running' if running else '⏸️ Pausa'}")

# 🔁 Auto-refresh
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

def _tick_query_param():
    try:
        st.query_params.update({"_": str(int(time.time() // max(1, REFRESH_SEC)))})
    except Exception:
        pass

if "last_refresh_ts" not in st.session_state:
    st.session_state["last_refresh_ts"] = time.time()
_prev_ts = float(st.session_state.get("last_refresh_ts", time.time()))
_next_ts = _prev_ts + max(1, REFRESH_SEC)
secs_left = max(0, int(round(_next_ts - time.time())))

effective_auto_refresh = auto_refresh and running
if effective_auto_refresh:
    if st_autorefresh:
        st_autorefresh(interval=int(REFRESH_SEC * 1000), key="auto_refresh_tick")
    else:
        def _delayed_rerun():
            time.sleep(max(1, REFRESH_SEC))
            try: st.rerun()
            except Exception: pass
        if "fallback_rerun_thread" not in st.session_state:
            t = threading.Thread(target=_delayed_rerun, daemon=True)
            t.start()
            st.session_state["fallback_rerun_thread"] = True
    _tick_query_param()

util_col1, util_col2 = st.columns([5, 1])
with util_col1: st.caption(f"Prossimo refresh ~{secs_left}s")
with util_col2:
    if st.button("Aggiorna ora", use_container_width=True): st.rerun()

# ---------------- Helpers ----------------
def fetch_with_retry(url, tries=3, base_backoff=0.7, headers=None):
    last = (None, None)
    for i in range(tries):
        try:
            r = requests.get(url, headers=headers or UA_HEADERS, timeout=15)
            code = r.status_code
            if r.ok: return r.json(), code
            last = (None, code)
            if code in (429,500,502,503,504):
                time.sleep(base_backoff*(i+1) + random.uniform(0,0.3)); continue
            break
        except Exception:
            last = (None, "ERR"); time.sleep(base_backoff*(i+1) + random.uniform(0,0.3))
    return last

def fmt_int(n): return f"{int(round(n)):,}".replace(",", ".") if n is not None else "N/D"

def hours_since_ms(ms_or_s):
    if ms_or_s is None: return None
    try:
        v = int(ms_or_s)
        if v > 10_000_000_000: v = v/1000.0
        return max(0.0, (time.time() - v) / 3600.0)
    except Exception: return None

def ms_to_dt(ms_or_s):
    if not ms_or_s: return ""
    try:
        v = int(ms_or_s)
        if v > 10_000_000_000: v = v//1000
        return datetime.datetime.utcfromtimestamp(v).strftime("%Y-%m-%d %H:%M")
    except Exception: return str(ms_or_s)

def fmt_age(hours):
    if hours is None: return ""
    if hours < 1: return f"{int(round(hours*60))}m"
    if hours < 48:
        h = int(hours); m = int(round((hours - h) * 60))
        return f"{h}h {m}m"
    d = int(hours // 24); h = int(hours % 24)
    return f"{d}d {h}h"

def safe_series_mean(s):
    vals = []
    for x in s:
        try:
            if pd.notna(x): vals.append(float(x))
        except Exception: pass
    return (sum(vals)/len(vals)) if vals else None

def safe_sort(df, col, ascending=False):
    if df is None or df.empty or col not in df.columns: return df
    return df.sort_values(by=[col], ascending=ascending)

def to_float0(x, default=0.0):
    if x is None:
        return default
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        try:
            s = str(x).replace(",", "").strip().replace("%", "")
            v = float(s) if s else default
            return v if math.isfinite(v) else default
        except Exception:
            return default

def to_int0(x, default=0):
    v = to_float0(x, float(default))
    return int(round(v))

# Meme Score helpers
STRONG_MEMES = {"WIF","BONK","PEPE","DOGE","DOG","SHIB","WOJAK","MOG","TRUMP","ELON","CAT","KITTY","MOON","PUMP","FLOKI","BABYDOGE"}
WEAK_MEMES   = {"FROG","COIN","INU","APE","GIGA","PONZI","LUNA","RUG","RICK","MORTY","ROCKET","HAMSTER"}
DEX_WEIGHTS  = {"raydium":1.0, "orca":0.9, "meteora":0.85, "lifinity":0.8}

def s_sigmoid(x, k=0.02):
    try: return 1.0 / (1.0 + math.exp(-k * (float(x) - 200)))
    except Exception: return 0.0
def score_symbol(s):
    S=(s or "").upper()
    return 1.0 if any(t in S for t in STRONG_MEMES) else (0.6 if any(t in S for t in WEAK_MEMES) else 0.3)
def score_age(hours):
    if hours is None: return 0.5
    return max(0.0, min(1.0, 1.0 - (hours / 72.0)))
def score_liq(liq, mn, mx):
    if liq is None or liq <= 0: return 0.0
    try: mn = float(mn) if mn is not None else 0.0
    except Exception: mn = 0.0
    try: mx = float(mx) if mx not in (None, 0) else float("inf")
    except Exception: mx = float("inf")
    if mn <= liq <= mx: return 1.0
    if liq < mn: return max(0.0, liq / (mn if mn > 0 else 1.0))
    return max(0.0, (mx if mx < float("inf") else 0.0) / liq) if mx < float("inf") else 0.6
def score_dex(d): return DEX_WEIGHTS.get((d or "").lower(), 0.6)

def compute_meme_score_row(r, weights=None, sweet_min=None, sweet_max=None):
    base = r.get("baseSymbol","") if hasattr(r, "get") else r["baseSymbol"]
    dex  = r.get("dexId","") if hasattr(r, "get") else r["dexId"]
    liq  = r.get("liquidityUsd", None) if hasattr(r, "get") else r["liquidityUsd"]
    tx1  = r.get("txns1h", 0) if hasattr(r, "get") else r["txns1h"]
    ageh = hours_since_ms(r.get("pairCreatedAt", 0) if hasattr(r, "get") else r["pairCreatedAt"])
    local_weights = tuple(weights or (w_symbol, w_age, w_txns, w_liq, w_dex))
    f = (local_weights[0]*score_symbol(base) + local_weights[1]*score_age(ageh) +
         local_weights[2]*s_sigmoid(tx1) + local_weights[3]*score_liq((liq or 0.0), liq_min_sweet, liq_max_sweet) +
         local_weights[4]*score_dex(dex))
    return round(100.0 * f / max(1e-6, sum(local_weights)))

# ---------------- Provider init ----------------
if "provider" not in st.session_state:
    prov = MarketDataProvider(refresh_sec=REFRESH_SEC, preserve_on_empty=True)
    prov.set_queries(SEARCH_QUERIES)
    st.session_state["provider"] = prov
    prov.start_auto_refresh()
provider: MarketDataProvider = st.session_state["provider"]

# Filtri provider
try:
    if disable_all_filters:
        provider.set_filters(only_raydium=False, min_liq=0, exclude_quotes=[])
    else:
        provider.set_filters(only_raydium=only_raydium, min_liq=min_liq, exclude_quotes=[str(x) for x in (exclude_quotes or [])])
except Exception:
    provider.set_filters(only_raydium=only_raydium if not disable_all_filters else False,
                         min_liq=min_liq if not disable_all_filters else 0,
                         exclude_quotes=[])

# Snapshot provider
df_provider, ts = provider.get_snapshot()
codes = provider.get_last_http_codes()
st.caption(f"Aggiornato: {time.strftime('%H:%M:%S', time.localtime(ts))}" if ts else "Aggiornamento in corso…")

# Watchlist & Volume filtro dataset
df_view = df_provider.copy()
pre_count = len(df_view)

def norm_list(s):
    out = []
    for part in (s or "").replace(" ", "").split(","):
        if not part: continue
        out.append(part.upper() if len(part) <= 8 else part)
    return out

watchlist_input_val = st.session_state.get("watchlist_input", "")
watchlist = norm_list(watchlist_input_val)

def is_watch_hit_row(r):
    base = str(r.get("baseSymbol","")).upper() if hasattr(r,"get") else str(r["baseSymbol"]).upper()
    quote= str(r.get("quoteSymbol","")).upper() if hasattr(r,"get") else str(r["quoteSymbol"]).upper()
    addr = r.get("pairAddress","") if hasattr(r,"get") else r["pairAddress"]
    return (watchlist and (base in watchlist or quote in watchlist or addr in watchlist))

if st.session_state.get("watchlist_only", False) and not df_view.empty:
    mask = df_view.apply(is_watch_hit_row, axis=1)
    df_view = df_view[mask].reset_index(drop=True)
post_watch_count = len(df_view)

# Filtro Volume 24h (dataset, non tabella)
vmin = int(st.session_state.get("vol24_min", 0))
vmax = int(st.session_state.get("vol24_max", 0))
if not df_view.empty:
    vol_series = pd.to_numeric(df_view["volume24hUsd"], errors="coerce").fillna(0)
    mask_vol = (vol_series >= vmin) & ((vmax == 0) | (vol_series <= vmax))
    df_view = df_view[mask_vol].reset_index(drop=True)
post_vol_count = len(df_view)

# ---------------- KPI base ----------------
if df_provider.empty:
    vol24_avg = None; tx1h_avg = None
else:
    top10_raw = df_provider.sort_values(by=["volume24hUsd"], ascending=False).head(10)
    vol24_avg = safe_series_mean(top10_raw["volume24hUsd"])
    tx1h_avg  = safe_series_mean(top10_raw["txns1h"])
if (not vol24_avg or vol24_avg == 0) and (tx1h_avg and tx1h_avg > 0):
    vol24_avg = tx1h_avg * 24 * PROXY_TICKET

# ---------------- Nuove coin — Birdeye + Fallback ----------------
be_headers = {"accept": "application/json"}
be_key = os.getenv("BE_API_KEY","")
if be_key: be_headers["x-api-key"] = be_key
bird_data, bird_code = fetch_with_retry(BIRDEYE_URL, headers={**UA_HEADERS, **be_headers})
bird_tokens, bird_ok = [], False
if bird_data and "data" in bird_data:
    if isinstance(bird_data["data"], dict) and isinstance(bird_data["data"].get("tokens"), list):
        bird_tokens = bird_data["data"]["tokens"]; bird_ok = True
    elif isinstance(bird_data["data"], list):
        bird_tokens = bird_data["data"]; bird_ok = True

def liquidity_from_birdeye_token(t):
    for k in ("liquidity","liquidityUsd","liquidityUSD"):
        try:
            v = t.get(k)
            if v is None: continue
            x = float(v)
            if not math.isnan(x): return x
        except Exception:
            pass
    return None
if bird_ok and bird_tokens:
    new_liq_values = [liquidity_from_birdeye_token(t) for t in bird_tokens[:20]]
    new_liq_values = [v for v in new_liq_values if v is not None]
else:
    recents = safe_sort(df_provider, "pairCreatedAt", ascending=False)
    recents = recents.head(20) if recents is not None and not recents.empty else pd.DataFrame(columns=["liquidityUsd","baseSymbol"])
    liq_series = recents.get("liquidityUsd", pd.Series(dtype=float))
    new_liq_values = [float(x) for x in liq_series.tolist() if pd.notna(x)]
new_liq_avg = (sum(new_liq_values)/len(new_liq_values)) if new_liq_values else None

# Score mercato
score = "N/D"
if vol24_avg is not None and vol24_avg > 0:
    if vol24_avg > 1_000_000: score = "ON FIRE"
    elif vol24_avg > 200_000: score = "MEDIO"
    else: score = "FIACCO"

# ---------------- KPI UI ----------------
c1, c2, c3, c4 = st.columns(4)
with c1:
    tone = {"ON FIRE":"🟢","MEDIO":"🟡","FIACCO":"🔴","N/D":"⚪️"}.get(score,"")
    st.metric("Score mercato", f"{tone} {score}")
with c2: st.metric("Volume 24h medio Top 10", fmt_int(vol24_avg))
with c3: st.metric("Txns 1h medie Top 10", fmt_int(tx1h_avg))
with c4: st.metric("Nuove coin – Liquidity media", fmt_int(new_liq_avg))

# ---------------- Tabella PAIRS ----------------
def _first_or_none(d, keys):
    for k in keys:
        try:
            v = d.get(k) if hasattr(d, "get") else d[k]
            if v is not None:
                return v
        except Exception:
            pass
    return None

def _to_float_pct(x):
    if x is None:
        return None
    try:
        s = str(x).replace("%", "").strip()
        return float(s) if s != "" else None
    except Exception:
        return None

def _get_change_pct_from_nested(r, nested_key, candidates):
    try:
        obj = r.get(nested_key) if hasattr(r, "get") else r[nested_key]
    except Exception:
        obj = None
    if isinstance(obj, dict):
        for name in candidates:
            if name in obj and obj[name] is not None:
                val = _to_float_pct(obj[name])
                if val is not None:
                    return val
    return None

def _get_change_pct(r, flat_keys, nested_key, nested_candidates):
    v = _first_or_none(r, flat_keys)
    v = _to_float_pct(v)
    if v is not None:
        return v
    return _get_change_pct_from_nested(r, nested_key, nested_candidates)

def build_table(df):
    rows = []
    for r in df.to_dict(orient="records"):
        mscore = compute_meme_score_row(r, (w_symbol, w_age, w_txns, w_liq, w_dex), liq_min_sweet, liq_max_sweet)
        ageh = hours_since_ms(r.get("pairCreatedAt", 0))

        chg_1h = _get_change_pct(
            r,
            flat_keys=["priceChange1hPct","priceChangeH1Pct","pc1h","priceChange1h"],
            nested_key="priceChange",
            nested_candidates=("h1","1h","m60","60m")
        )
        chg_4h = _get_change_pct(
            r,
            flat_keys=["priceChange4hPct","priceChangeH4Pct","pc4h","priceChange4h"],
            nested_key="priceChange",
            nested_candidates=("h4","4h","m240","240m")
        )
        if chg_4h is None and show_h6_fallback:
            chg_4h = _get_change_pct(
                r,
                flat_keys=["priceChange6hPct","priceChangeH6Pct","pc6h","priceChange6h"],
                nested_key="priceChange",
                nested_candidates=("h6","6h","m360","360m")
            )
        chg_24h = _get_change_pct(
            r,
            flat_keys=["priceChange24hPct","priceChangeH24Pct","pc24h","priceChange24h"],
            nested_key="priceChange",
            nested_candidates=("h24","24h","m1440","1440m")
        )

        rows.append({
            "Meme Score": mscore,
            "Pair": f"{r.get('baseSymbol','')}/{r.get('quoteSymbol','')}",
            "DEX": r.get("dexId",""),
            "Liquidity (USD)": to_int0(r.get("liquidityUsd"), 0),
            "Txns 1h": to_int0(r.get("txns1h"), 0),
            "Volume 24h (USD)": to_int0(r.get("volume24hUsd"), 0),
            "Price (USD)": (None if r.get("priceUsd") in (None, "") else to_float0(r.get("priceUsd"), None)),
            "Change 1h (%)": chg_1h,
            "Change 4h/6h (%)": chg_4h,
            "Change 24h (%)": chg_24h,
            "Created (UTC)": ms_to_dt(r.get("pairCreatedAt", 0)),
            "Pair Age": fmt_age(ageh),
            "PairAgeHours": (float(ageh) if ageh is not None else None),  # per filtri tabella/diagnostica opzionali
            "Link": r.get("url",""),
            "Base Address": r.get("baseAddress",""),
            "baseSymbol": r.get("baseSymbol",""),
            "quoteSymbol": r.get("quoteSymbol",""),
        })
    out = pd.DataFrame(rows)
    if not out.empty and sort_by_meme:
        out = out.sort_values(by=["Meme Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False])
    return out

df_pairs = build_table(df_view)

# === Filtri SOLO tabella ===
df_pairs_table = df_pairs.copy()

if not df_pairs_table.empty and pairs_meme_min > 0:
    df_pairs_table = df_pairs_table[
        pd.to_numeric(df_pairs_table["Meme Score"], errors="coerce").fillna(0) >= int(pairs_meme_min)
    ]

if not df_pairs_table.empty and "PairAgeHours" in df_pairs_table.columns:
    age_series = pd.to_numeric(df_pairs_table["PairAgeHours"], errors="coerce")
    df_pairs_table = df_pairs_table[age_series.between(float(pairs_age_min_h), float(pairs_age_max_h), inclusive="both")]

if pairs_liq_enable and not df_pairs_table.empty and "Liquidity (USD)" in df_pairs_table.columns:
    liq_series = pd.to_numeric(df_pairs_table["Liquidity (USD)"], errors="coerce").fillna(0)
    df_pairs_table = df_pairs_table[(liq_series >= pairs_liq_min) & (liq_series <= pairs_liq_max)]

if pairs_vol_enable and not df_pairs_table.empty and "Volume 24h (USD)" in df_pairs_table.columns:
    vol_series_tbl = pd.to_numeric(df_pairs_table["Volume 24h (USD)"], errors="coerce").fillna(0)
    df_pairs_table = df_pairs_table[(vol_series_tbl >= pairs_vol_min) & (vol_series_tbl <= pairs_vol_max)]

# === Filtri opzionali anche sulla diagnostica ===
df_pairs_diag = df_pairs.copy()
if pairs_filters_to_strategy and not df_pairs_diag.empty:
    if apply_meme_to_strat and pairs_meme_min > 0:
        df_pairs_diag = df_pairs_diag[
            pd.to_numeric(df_pairs_diag["Meme Score"], errors="coerce").fillna(0) >= int(pairs_meme_min)
        ]
    if apply_age_to_strat and "PairAgeHours" in df_pairs_diag.columns:
        age_series_str = pd.to_numeric(df_pairs_diag["PairAgeHours"], errors="coerce")
        df_pairs_diag = df_pairs_diag[age_series_str.between(float(pairs_age_min_h), float(pairs_age_max_h), inclusive="both")]
    if apply_liq_to_strat and pairs_liq_enable and "Liquidity (USD)" in df_pairs_diag.columns:
        liq_series_str = pd.to_numeric(df_pairs_diag["Liquidity (USD)"], errors="coerce").fillna(0)
        df_pairs_diag = df_pairs_diag[(liq_series_str >= pairs_liq_min) & (liq_series_str <= pairs_liq_max)]
    if apply_vol_to_strat and pairs_vol_enable and "Volume 24h (USD)" in df_pairs_diag.columns:
        vol_series_str = pd.to_numeric(df_pairs_diag["Volume 24h (USD)"], errors="coerce").fillna(0)
        df_pairs_diag = df_pairs_diag[(vol_series_str >= pairs_vol_min) & (vol_series_str <= pairs_vol_max)]

# --- Pillola riassuntiva: Filtri PAIRS applicati alla diagnostica ---
df_pairs_used = df_pairs_diag if (pairs_filters_to_strategy) else df_pairs
def _fmt_exp_range(lo_exp, hi_exp):
    try:
        return f"10^{lo_exp:g}–10^{hi_exp:g}"
    except Exception:
        return f"10^{lo_exp}–10^{hi_exp}"

applied = []
if pairs_filters_to_strategy:
    if apply_meme_to_strat and int(pairs_meme_min) > 0:
        applied.append(f"Meme ≥ {int(pairs_meme_min)}")
    if apply_age_to_strat and (float(pairs_age_min_h) > 0.0 or float(pairs_age_max_h) < 10000.0):
        applied.append(f"Age {pairs_age_min_h:g}–{pairs_age_max_h:g}h")
    if apply_liq_to_strat and pairs_liq_enable and (pairs_liq_exp_min > 0.0 or pairs_liq_exp_max < 12.0):
        applied.append(f"Liq {_fmt_exp_range(pairs_liq_exp_min, pairs_liq_exp_max)}")
    if apply_vol_to_strat and pairs_vol_enable and (pairs_vol_exp_min > 0.0 or pairs_vol_exp_max < 12.0):
        applied.append(f"Vol24 {_fmt_exp_range(pairs_vol_exp_min, pairs_vol_exp_max)}")

base_n = 0 if df_pairs is None or df_pairs.empty else len(df_pairs)
used_n = 0 if df_pairs_used is None or df_pairs_used.empty else len(df_pairs_used)
pct = (used_n / base_n * 100.0) if base_n > 0 else 0.0

if pairs_filters_to_strategy:
    if applied:
        st.success(
            f"**PAIRS→Diagnostica: ON** • Filtri attivi: " + ", ".join(applied) +
            f" • Universo: **{used_n}/{base_n}** (≈ {pct:.1f}%)",
            icon="🧠"
        )
    else:
        st.info(
            f"**PAIRS→Diagnostica: ON** • Nessun filtro effettivo (range completi/valori neutri). "
            f"Universo: **{used_n}/{base_n}** (≈ {pct:.1f}%)",
            icon="ℹ️"
        )
else:
    st.caption("PAIRS→Diagnostica: **OFF** — i filtri PAIRS impattano solo **Tabella/Top 10**, non la diagnostica.")

# ---------------- Charts (usano la tabella filtrata) ----------------
left, right = st.columns(2)
with left:
    if not df_pairs_table.empty:
        df_top = df_pairs_table.sort_values(by=["Volume 24h (USD)"], ascending=False).head(10)
        df_chart = pd.DataFrame({"Token": df_top["Pair"], "Volume 24h": df_top["Volume 24h (USD)"].fillna(0)})
        fig = px.bar(df_chart, x="Token", y="Volume 24h", title="Top 10 Volume 24h (tabella filtrata)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Nessuna coppia disponibile con i filtri tabella attuali.")
with right:
    if bird_ok and bird_tokens:
        names, liqs = [], []
        for t in bird_tokens[:20]:
            names.append(t.get("name") or t.get("symbol") or (t.get("mint") or "")[:6])
            liqs.append(liquidity_from_birdeye_token(t) or 0)
        df_liq = pd.DataFrame({"Token": names, "Liquidity": liqs})
        fig2 = px.bar(df_liq, x="Token", y="Liquidity", title="Ultime 20 Nuove Coin – Liquidity (Birdeye)")
        st.plotly_chart(fig2, use_container_width=True)

# ---------------- Tabella PAIRS ----------------
st.markdown("### Pairs (post-filtri)")
if not df_pairs_table.empty:
    base_for_view = df_pairs_table
    df_pairs_for_view = base_for_view.sort_values(by=["Volume 24h (USD)"], ascending=False).head(10) if show_top10_table else base_for_view

    display_cols = [c for c in df_pairs_for_view.columns if c not in ("baseSymbol","quoteSymbol","PairAgeHours")]
    if not show_pair_age and "Pair Age" in display_cols:
        display_cols.remove("Pair Age")

    st.dataframe(
        df_pairs_for_view[display_cols],
        use_container_width=True,
        column_config={
            "Link": st.column_config.LinkColumn("Link", help="Apri su DexScreener"),
            "Liquidity (USD)": st.column_config.NumberColumn(format="%,d"),
            "Volume 24h (USD)": st.column_config.NumberColumn(format="%,d"),
            "Meme Score": st.column_config.NumberColumn(help="0–100: più alto = più 'meme' + momentum"),
            "Price (USD)": st.column_config.NumberColumn(format="%.8f"),
            "Change 1h (%)": st.column_config.NumberColumn(format="%.2f"),
            "Change 4h/6h (%)": st.column_config.NumberColumn(format="%.2f"),
            "Change 24h (%)": st.column_config.NumberColumn(format="%.2f"),
        }
    )
    try:
        n1 = pd.to_numeric(df_pairs_for_view["Change 1h (%)"], errors="coerce").notna().sum()
        n4 = pd.to_numeric(df_pairs_for_view["Change 4h/6h (%)"], errors="coerce").notna().sum()
        st.caption(f"Diagnostica Change: 1h valorizzati {n1}/{len(df_pairs_for_view)} • 4h/6h valorizzati {n4}/{len(df_pairs_for_view)}")
    except Exception:
        pass
    cap = "Top 10 per Volume 24h (tabella filtrata)." if show_top10_table else "Tutte le coppie (tabella filtrata)."
    cap += "  (Se H4 mancante, mostrata H6)" if show_h6_fallback else ""
    st.caption(cap)
else:
    st.caption("Nessuna coppia disponibile con i filtri attuali.")

# ---------------- Diagnostica mercato (NO trading) ----------------
def market_heat_value(df: pd.DataFrame, topN: int) -> float:
    if df is None or df.empty:
        return 0.0
    if "Volume 24h (USD)" not in df.columns or "Txns 1h" not in df.columns:
        return 0.0
    top = df.sort_values(by=["Volume 24h (USD)"], ascending=False).head(max(1, int(topN)))
    return float(pd.to_numeric(top["Txns 1h"], errors="coerce").fillna(0).mean())

st.markdown("### Diagnostica mercato")
if df_pairs_used is None or df_pairs_used.empty:
    st.caption("Nessuna coppia post-filtri (provider/watchlist/volume e, se attivo, PAIRS→Diagnostica).")
else:
    heat_val = market_heat_value(df_pairs_used, int(st.session_state.get("heat_topN", 10)))
    heat_thr = float(st.session_state.get("strat_heat_avg", 120))
    st.caption(f"Market heat (media **Txns1h** top {int(st.session_state.get('heat_topN', 10))} per **Volume 24h**): "
               f"**{int(heat_val)}** vs soglia **{int(heat_thr)}** → "
               f"{'OK ✅' if heat_val >= heat_thr else 'BASSO 🔻'}")

    s = df_pairs_used.copy()
    total = len(s)
    try:
        chg_series = pd.to_numeric(s["Change 24h (%)"], errors="coerce").fillna(0)
    except Exception:
        chg_series = pd.Series([0]*len(s))

    def cnt(mask):
        try: return int(mask.sum())
        except Exception: return 0

    c_liq = cnt((s["Liquidity (USD)"] >= float(liq_min_sweet)) & (s["Liquidity (USD)"] <= float(liq_max_sweet)))
    c_dex = cnt(s["DEX"].str.lower().isin(list(st.session_state.get("allowed_dex", ["raydium","orca","meteora","lifinity"]))))
    c_meme = cnt(s["Meme Score"] >= int(st.session_state.get("strat_meme", 70)))
    c_tx   = cnt(s["Txns 1h"] >= int(st.session_state.get("strat_txns", 250)))
    if vmin is None: vmin = 0
    if vmax > 0:
        c_vol = cnt((s["Volume 24h (USD)"] >= vmin) & (s["Volume 24h (USD)"] <= vmax))
    else:
        c_vol = cnt((s["Volume 24h (USD)"] >= vmin))
    c_turn = cnt((s["Volume 24h (USD)"] / s["Liquidity (USD)"].replace(0,1)) >= float(st.session_state.get("strat_turnover", 1.2)))
    c_chg  = cnt((chg_series >= float(st.session_state.get("chg_min", -8))) & (chg_series <= float(st.session_state.get("chg_max", 180))))

    cols = st.columns(8)
    cols[0].metric("Totale", total)
    cols[1].metric("Liq OK", c_liq)
    cols[2].metric("DEX OK", c_dex)
    cols[3].metric("Meme OK", c_meme)
    cols[4].metric("Tx1h OK", c_tx)
    cols[5].metric("Vol24 OK", c_vol)
    cols[6].metric("Turnover OK", c_turn)
    cols[7].metric("Change24 OK", c_chg)

# ---------------- ALERT TELEGRAM ----------------
def tg_send(text: str) -> tuple[bool, str | None]:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID): return False, "missing-credentials"
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                         params={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}, timeout=15)
        return (True, None) if r.ok else (False, f"status={r.status_code}")
    except Exception as e:
        return False, str(e)

if "tg_sent" not in st.session_state: st.session_state["tg_sent"] = {}

tg_sent_now = 0
if running and enable_alerts and (df_pairs_table is not None) and not df_pairs_table.empty and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    try:
        df_alert = df_pairs_table.copy()
        mask = (df_alert["Txns 1h"] >= int(alert_tx1h_min)) & (df_alert["Liquidity (USD)"] >= int(alert_liq_min))
        if int(alert_meme_min) > 0: mask &= (df_alert["Meme Score"] >= int(alert_meme_min))
        df_alert = df_alert[mask]
        if not df_alert.empty:
            df_alert = df_alert.sort_values(by=["Meme Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False])
            now = time.time(); cooldown = int(alert_cooldown_min) * 60; max_send = int(alert_max_per_run)
            for _, row in df_alert.head(max_send*2).iterrows():
                addr = str(row.get("Base Address","")) or row.get("Pair")
                last_ts = st.session_state["tg_sent"].get(addr, 0)
                if now - last_ts < cooldown: continue
                pair = row.get("Pair",""); dex = row.get("DEX",""); ms = int(row.get("Meme Score",0) or 0)
                tx1 = int(row.get("Txns 1h",0) or 0); liq = int(row.get("Liquidity (USD)",0) or 0)
                vol = int(row.get("Volume 24h (USD)",0) or 0)
                px  = row.get("Price (USD)", None); chg = row.get("Change 24h (%)", None)
                link= row.get("Link","")
                txt = f"⚡️ Radar Hit — {pair}\nDEX: {dex}  |  MemeScore: {ms}\nTxns 1h: {tx1:,}  |  Liq: ${liq:,}  |  Vol24h: ${vol:,}"
                if isinstance(px,(int,float)) and px: txt += f"\nPrice: {px:.8f}"
                if chg is not None:
                    try: txt += f"  |  24h: {float(chg):.2f}%"
                    except Exception: pass
                if link: txt += f"\n{link}"
                ok, err = tg_send(txt)
                if ok:
                    st.session_state["tg_sent"][addr] = now
                    tg_sent_now += 1
                    if tg_sent_now >= max_send: break
    except Exception as e:
        st.caption(f"Alert Telegram: errore — {e}")

# ---------------- Diagnostica finale ----------------
st.subheader("Diagnostica")
d1, d2, d3, d4, d5 = st.columns(5)
with d1: st.text(f"Query provider: {len(SEARCH_QUERIES)}  •  HTTP: {codes if codes else '—'}")
with d2: st.text(f"Righe provider (post-filtri provider): {pre_count}")
with d3: st.text(f"Righe dopo watchlist: {post_watch_count}")
with d4: st.text(f"Righe dopo filtro volume: {post_vol_count}")
with d5:
    src = 'Birdeye' if (bird_ok and bird_tokens) else 'DexScreener (fallback)'
    st.text(f"Nuove coin source: {src}")
st.caption(
    f"Stato esecuzione: {'🟢 Running' if running else '⏸️ Pausa'} • Refresh: {REFRESH_SEC}s • "
    f"TG alerts (run): {tg_sent_now} • Ticket proxy: ${PROXY_TICKET:.0f} • "
    f"PAIRS→Diagnostica: {'ON' if pairs_filters_to_strategy else 'OFF'}"
)

st.session_state["last_refresh_ts"] = time.time()
