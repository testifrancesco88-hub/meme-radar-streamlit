# streamlit_app.py — Meme Radar (no trading) + Drill-down + ROI/ATH/DD + Survivors + Winners
# + Equity + Entry Finder (smart+presets+vol filter) + Paper Trading (vanilla) + Telegram alerts
# Requisiti: streamlit, plotly, pandas, requests; file market_data.py con MarketDataProvider

import os, time, math, random, datetime, threading
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from urllib.parse import urlparse

from market_data import MarketDataProvider

# ==================== Config ====================
st.set_page_config(page_title="Meme Radar — Solana", layout="wide")
st.title("Solana Meme Coin Radar")

REFRESH_SEC   = int(os.getenv("REFRESH_SEC", "60"))
PROXY_TICKET  = float(os.getenv("PROXY_TICKET_USD", "150"))
BIRDEYE_URL   = "https://public-api.birdeye.so/defi/tokenlist?chain=solana&sort=createdBlock&order=desc&limit=50"
AGE_LIMIT_HOURS = 10000.0

SEARCH_QUERIES = [
    "chain:solana raydium","chain:solana orca","chain:solana meteora","chain:solana lifinity",
    "chain:solana usdc","chain:solana usdt","chain:solana sol","chain:solana bonk","chain:solana wif","chain:solana pepe",
    "chain:solana pump",
]
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 MemeRadar/1.0",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

# =============== Session State ===============
if "app_running" not in st.session_state: st.session_state["app_running"] = True
if "last_refresh_ts" not in st.session_state: st.session_state["last_refresh_ts"] = time.time()

# Profit metrics baseline/ath per token
for k in ("baseline_px","ath_px"):
    if k not in st.session_state: st.session_state[k] = {}

# Equity curve state
if "eq_enabled" not in st.session_state: st.session_state["eq_enabled"] = True
if "eq_init_capital" not in st.session_state: st.session_state["eq_init_capital"] = 1000.0
if "eq_equity" not in st.session_state: st.session_state["eq_equity"] = float(st.session_state["eq_init_capital"])
if "eq_history" not in st.session_state: st.session_state["eq_history"] = []   # [{ts,equity,ret,n}]
if "eq_last_prices" not in st.session_state: st.session_state["eq_last_prices"] = {}

# ================= Sidebar =================
with st.sidebar:
    st.header("Impostazioni")

    st.subheader("Esecuzione")
    col_run1, col_run2 = st.columns(2)
    if col_run1.button("▶️ Start", disabled=st.session_state["app_running"]):
        st.session_state["app_running"] = True
        st.toast("Esecuzione avviata", icon="✅"); st.rerun()
    if col_run2.button("⏹ Stop", type="primary", disabled=not st.session_state["app_running"]):
        st.session_state["app_running"] = False
        st.toast("Esecuzione in pausa", icon="⏸️"); st.rerun()

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
    st.divider(); st.subheader("Watchlist")
    wl_default = os.getenv("WATCHLIST", "")
    watchlist_input = st.text_input("Simboli o address (comma-separated)", value=wl_default,
                                    help="Es: WIF,BONK,So111...,<pairAddress>", key="watchlist_input")
    watchlist_only = st.toggle("Mostra solo watchlist", value=False, key="watchlist_only")

    # Filtro Volume 24h (dataset base)
    st.divider(); st.subheader("Filtro Volume 24h (USD) — dataset")
    vol24_min = st.number_input("Volume 24h MIN", min_value=0, value=st.session_state.get("vol24_min", 0), step=10000, key="vol24_min")
    vol24_max = st.number_input("Volume 24h MAX (0 = illimitato)", min_value=0, value=st.session_state.get("vol24_max", 0), step=100000, key="vol24_max")

    # Meme Score (ranking tabella)
    st.divider(); st.subheader("Meme Score")
    sort_by_meme = st.toggle("Ordina per Meme Score (desc)", value=True)
    liq_min_sweet = st.number_input("Sweet spot liquidity MIN", min_value=0, value=10000, step=1000)
    liq_max_sweet = st.number_input("Sweet spot liquidity MAX", min_value=0, value=200000, step=5000)
    with st.expander("Pesi avanzati (0–100)"):
        w_symbol = st.slider("Peso: Nome 'meme'", 0, 100, 20)
        w_age    = st.slider("Peso: Freschezza", 0, 100, 20)
        w_txns   = st.slider("Peso: Txns 1h", 0, 100, 25)
        w_liq    = st.slider("Peso: Sweet spot Liquidity", 0, 100, 20)
        w_dex    = st.slider("Peso: DEX (Raydium > altri)", 0, 100, 15)

    # DEX consentiti
    allowed_dex = st.multiselect(
        "DEX consentiti",
        ["raydium","orca","meteora","lifinity"],
        default=st.session_state.get("allowed_dex", ["raydium","orca","meteora","lifinity"])
    )
    st.session_state["allowed_dex"] = allowed_dex

    # Tabella
    st.divider(); st.subheader("Tabella")
    show_pair_age = st.toggle("Mostra 'Pair Age' (min/ore)", value=True)
    show_top10_table = st.toggle("Tabella: mostra solo Top 10 per Volume 24h", value=True)
    show_h6_fallback = st.toggle("Fallback: mostra Change H6 se H4 mancante", value=True)

    # Filtri PAIRS (tabella)
    st.subheader("Filtri PAIRS (tabella)")
    pairs_meme_min = st.slider("Meme Score min (PAIRS)", 0, 100, 0)

    # Pair Age — ORE/MIN + toggle <60m
    st.markdown("**Pair Age — range**")
    age_unit = st.radio("Unità", ["Ore", "Minuti"], index=0, horizontal=True, key="pairs_age_unit")
    if age_unit == "Ore":
        pairs_age_min_h, pairs_age_max_h = st.slider(
            "Intervallo (ore)", min_value=0.0, max_value=float(AGE_LIMIT_HOURS),
            value=(0.0, float(AGE_LIMIT_HOURS)), step=0.5, key="pairs_age_range_h",
        )
        pairs_age_min_m = int(round(pairs_age_min_h * 60))
        pairs_age_max_m = int(round(pairs_age_max_h * 60))
    else:
        max_min = int(AGE_LIMIT_HOURS * 60)
        pairs_age_min_m, pairs_age_max_m = st.slider(
            "Intervallo (minuti)", min_value=0, max_value=max_min,
            value=(0, max_min), step=1, key="pairs_age_range_m",
        )
        pairs_age_min_h = pairs_age_min_m / 60.0
        pairs_age_max_h = pairs_age_max_m / 60.0

    fresh60 = st.toggle("Solo nuovissime (< 60 min)", value=False, key="fresh60_toggle")
    if fresh60:
        pairs_age_min_h, pairs_age_max_h = 0.0, 1.0
        pairs_age_min_m, pairs_age_max_m = 0, 60
        st.caption("**Override attivo:** filtro età 0–60 minuti.")
    else:
        st.caption(f"Filtro età: {pairs_age_min_h:.2f}–{pairs_age_max_h:.2f} h  ({pairs_age_min_m}–{pairs_age_max_m} min)")

    # Liquidity log-range (solo tabella)
    st.markdown("**Liquidity (USD) — range (log)**")
    pairs_liq_enable = st.toggle("Abilita filtro Liquidity (tabella, log)", value=False)
    pairs_liq_exp_min, pairs_liq_exp_max = st.slider(
        "10^x (min, max) [Liquidity]", min_value=0.0, max_value=12.0,
        value=(0.0, 12.0), step=0.25, disabled=not pairs_liq_enable
    )
    pairs_liq_min = 10 ** pairs_liq_exp_min
    pairs_liq_max = 10 ** pairs_liq_exp_max
    if pairs_liq_enable:
        st.caption(f"Range Liquidity tabella: ${int(pairs_liq_min):,} → ${int(pairs_liq_max):,}".replace(",", "."))
    else:
        st.caption("Filtro liquidity disattivato.")

    # Volume log-range (solo tabella)
    st.markdown("**Volume 24h (USD) — range (log)**")
    pairs_vol_enable = st.toggle("Abilita filtro Volume 24h (tabella, log)", value=False)
    pairs_vol_exp_min, pairs_vol_exp_max = st.slider(
        "10^x (min, max) [Volume 24h]", min_value=0.0, max_value=12.0,
        value=(0.0, 12.0), step=0.25, disabled=not pairs_vol_enable
    )
    pairs_vol_min = 10 ** pairs_vol_exp_min
    pairs_vol_max = 10 ** pairs_vol_exp_max
    if pairs_vol_enable:
        st.caption(f"Range Volume 24h tabella: ${int(pairs_vol_min):,} → ${int(pairs_vol_max):,}".replace(",", "."))
    else:
        st.caption("Filtro volume disattivato.")

    # Survivors 60m
    st.divider(); st.subheader("Survivor Filter")
    survivors_only = st.toggle("Solo Survivors 60m (ROI>0 & PairAge≥60m)", value=False)

    # PAIRS → Diagnostica (NO trading)
    st.divider(); st.subheader("Applica filtri PAIRS → Diagnostica")
    pairs_filters_to_strategy = st.toggle("Applica i filtri PAIRS anche alla diagnostica", value=False)
    col_apply1, col_apply2 = st.columns(2)
    with col_apply1:
        apply_meme_to_strat = st.checkbox("Meme Score min → diagnostica", value=True, disabled=not pairs_filters_to_strategy)
        apply_age_to_strat  = st.checkbox("Pair Age range → diagnostica", value=True, disabled=not pairs_filters_to_strategy)
    with col_apply2:
        apply_liq_to_strat  = st.checkbox("Liquidity (log) → diagnostica", value=False, disabled=not pairs_filters_to_strategy)
        apply_vol_to_strat  = st.checkbox("Volume 24h (log) → diagnostica", value=False, disabled=not pairs_filters_to_strategy)

    # Parametri diagnostici (NO trading)
    st.divider(); st.subheader("Parametri diagnostici (NO trading)")
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

    # Alert Telegram
    st.divider(); st.subheader("Alert Telegram")
    TELEGRAM_BOT_TOKEN = st.text_input("Bot Token", value=os.getenv("TELEGRAM_BOT_TOKEN",""), type="password")
    TELEGRAM_CHAT_ID   = st.text_input("Chat ID", value=os.getenv("TELEGRAM_CHAT_ID",""))
    st.markdown("**Soglie tabella (hit radar)**")
    enable_alerts      = st.toggle("Abilita alert tabella (hit)", value=False)
    alert_tx1h_min     = st.number_input("Soglia txns 1h", min_value=0, value=200, step=10)
    alert_liq_min      = st.number_input("Soglia liquidity USD", min_value=0, value=20000, step=1000)
    alert_meme_min     = st.number_input("Soglia Meme Score (0=disattiva)", min_value=0, max_value=100, value=70, step=5)
    st.markdown("**Trailing-stop alert**")
    enable_trailing    = st.toggle("Abilita trailing-stop alert", value=False)
    trailing_dd_thr    = st.number_input("Soglia Drawdown (%)", value=-15.0, step=1.0)
    st.markdown("**Entry Finder alert**")
    enable_entry_alerts = st.toggle("Abilita alert Entry Finder (🎯)", value=False)
    st.markdown("**Rate-limit**")
    alert_cooldown_min = st.number_input("Cooldown alert (min)", min_value=1, value=30, step=5)
    alert_max_per_run  = st.number_input("Max alert per refresh (hit)", min_value=1, value=3, step=1)

    def _tg_send_test():
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            st.warning("Inserisci BOT_TOKEN e CHAT_ID."); return
        try:
            rq = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                              params={"chat_id": TELEGRAM_CHAT_ID, "text": "✅ Test dal Meme Radar", "disable_web_page_preview": True}, timeout=15)
            st.success("Messaggio di test inviato ✅" if rq.ok else f"Telegram {rq.status_code}.")
        except Exception as e:
            st.error(f"Errore Telegram: {e}")
    if st.button("Test Telegram"): _tg_send_test()

# ---------- Stato Esecuzione ----------
running = bool(st.session_state.get("app_running", True))
st.markdown(f"**Stato:** {'🟢 Running' if running else '⏸️ Pausa'}")

# 🔁 Auto-refresh (semplificato e stabile: niente query_params.update)
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

if "last_refresh_ts" not in st.session_state:
    st.session_state["last_refresh_ts"] = time.time()

_prev_ts = float(st.session_state.get("last_refresh_ts", time.time()))
_next_ts = _prev_ts + max(1, REFRESH_SEC)
secs_left = max(0, int(round(_next_ts - time.time())))

effective_auto_refresh = auto_refresh and running
if effective_auto_refresh:
    if st_autorefresh:
        st_autorefresh(interval=int(REFRESH_SEC * 1000), key="auto_refresh_tick")
    elif "fallback_rerun_thread" not in st.session_state:
        def _delayed_rerun():
            time.sleep(max(1, REFRESH_SEC))
            try:
                st.rerun()
            except Exception:
                pass
        t = threading.Thread(target=_delayed_rerun, daemon=True)
        t.start()
        st.session_state["fallback_rerun_thread"] = True

util_col1, util_col2 = st.columns([5, 1])
with util_col1:
    st.caption(f"Prossimo refresh ~{secs_left}s")
with util_col2:
    if st.button("Aggiorna ora", use_container_width=True):
        st.rerun()

# ================= Helpers =================
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
    if x is None: return default
    try:
        v = float(x); return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        try:
            s = str(x).replace(",", "").strip().replace("%", "")
            v = float(s) if s else default
            return v if math.isfinite(v) else default
        except Exception:
            return default

def to_int0(x, default=0):
    v = to_float0(x, float(default)); return int(round(v))

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
    local_weights = tuple(weights or (20,20,25,20,15))
    f = (local_weights[0]*score_symbol(base) + local_weights[1]*score_age(ageh) +
         local_weights[2]*s_sigmoid(tx1) + local_weights[3]*score_liq((liq or 0.0), liq_min_sweet, liq_max_sweet) +
         local_weights[4]*score_dex(dex))
    return round(100.0 * f / max(1e-6, sum(local_weights)))

# ============== Provider init ==============
if "provider" not in st.session_state:
    prov = MarketDataProvider(refresh_sec=REFRESH_SEC, preserve_on_empty=True)
    prov.set_queries(SEARCH_QUERIES)
    st.session_state["provider"] = prov
    prov.start_auto_refresh()  # aggiorna cache provider (non forza il rerun UI)

provider: MarketDataProvider = st.session_state["provider"]
try:
    if disable_all_filters:
        provider.set_filters(only_raydium=False, min_liq=0, exclude_quotes=[])
    else:
        provider.set_filters(only_raydium=only_raydium, min_liq=min_liq, exclude_quotes=[str(x) for x in (exclude_quotes or [])])
except Exception:
    provider.set_filters(only_raydium=only_raydium if not disable_all_filters else False,
                         min_liq=min_liq if not disable_all_filters else 0,
                         exclude_quotes=[])

df_provider, ts = provider.get_snapshot()
codes = provider.get_last_http_codes()
st.caption(f"Aggiornato: {time.strftime('%H:%M:%S', time.localtime(ts))}" if ts else "Aggiornamento in corso…")

# ============== Watchlist & Volume filtro dataset ==============
df_view = df_provider.copy()
pre_count = len(df_view)

def norm_list(s):
    out = []
    for part in (s or "").replace(" ", "").split(","):
        if not part: continue
        out.append(part.upper() if len(part) <= 8 else part)
    return out

watchlist = norm_list(st.session_state.get("watchlist_input", ""))

def is_watch_hit_row(r):
    base = str(r.get("baseSymbol","")).upper() if hasattr(r,"get") else str(r["baseSymbol"]).upper()
    quote= str(r.get("quoteSymbol","")).upper() if hasattr(r,"get") else str(r["quoteSymbol"]).upper()
    addr = r.get("pairAddress","") if hasattr(r,"get") else r["pairAddress"]
    return (watchlist and (base in watchlist or quote in watchlist or addr in watchlist))

if st.session_state.get("watchlist_only", False) and not df_view.empty:
    mask = df_view.apply(is_watch_hit_row, axis=1)
    df_view = df_view[mask].reset_index(drop=True)
post_watch_count = len(df_view)

vmin = int(st.session_state.get("vol24_min", 0))
vmax = int(st.session_state.get("vol24_max", 0))
if not df_view.empty:
    vol_series = pd.to_numeric(df_view["volume24hUsd"], errors="coerce").fillna(0)
    mask_vol = (vol_series >= vmin) & ((vmax == 0) | (vol_series <= vmax))
    df_view = df_view[mask_vol].reset_index(drop=True)
post_vol_count = len(df_view)

# ============== KPI Base ==============
if df_provider.empty:
    vol24_avg = None; tx1h_avg = None
else:
    top10_raw = df_provider.sort_values(by=["volume24hUsd"], ascending=False).head(10)
    vol24_avg = safe_series_mean(top10_raw["volume24hUsd"])
    tx1h_avg  = safe_series_mean(top10_raw["txns1h"])
if (not vol24_avg or vol24_avg == 0) and (tx1h_avg and tx1h_avg > 0):
    vol24_avg = tx1h_avg * 24 * PROXY_TICKET

# ============== Nuove coin — Birdeye + Fallback ==============
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

# Score mercato + badge
score = "N/D"
if vol24_avg is not None and vol24_avg > 0:
    if vol24_avg > 1_000_000: score = "ON FIRE"
    elif vol24_avg > 200_000: score = "MEDIO"
    else: score = "FIACCO"
tone = {"ON FIRE":"🟢","MEDIO":"🟡","FIACCO":"🔴","N/D":"⚪️"}.get(score,"")

# ============== KPI UI ==============
c1, c2, c3, c4 = st.columns(4)
with c1: st.metric("Score mercato", f"{tone} {score}")
with c2: st.metric("Volume 24h medio Top 10", fmt_int(vol24_avg))
with c3: st.metric("Txns 1h medie Top 10", fmt_int(tx1h_avg))
with c4: st.metric("Nuove coin – Liquidity media", fmt_int(new_liq_avg))

# ============== ROI/ATH/DD helpers ==============
def _addr_key_from_rowdict(rdict):
    for k in ("baseAddress","pairAddress","Base Address","Pair Address"):
        v = rdict.get(k)
        if v: return str(v)
    return rdict.get("Pair") or rdict.get("pair") or None

def update_profit_metrics_from_raw(rdict):
    addr = _addr_key_from_rowdict(rdict)
    if not addr: return None, None, None
    px = rdict.get("priceUsd")
    try:
        px = None if px in (None, "") else float(px)
    except Exception:
        px = None
    if not px or px <= 0: return None, None, None

    if addr not in st.session_state["baseline_px"]:
        st.session_state["baseline_px"][addr] = float(px)
    base = st.session_state["baseline_px"][addr]
    prev_ath = st.session_state["ath_px"].get(addr, base)
    new_ath = max(prev_ath, px)
    st.session_state["ath_px"][addr] = new_ath

    roi_pct = (px/base - 1.0)*100.0 if base>0 else None
    ath_pct = (new_ath/base - 1.0)*100.0 if base>0 else None
    dd_pct  = (px/new_ath - 1.0)*100.0 if new_ath>0 else None
    return roi_pct, ath_pct, dd_pct

# ============== Tabella (build) ==============
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
    if x is None: return None
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
                if val is not None: return val
    return None

def _get_change_pct(r, flat_keys, nested_key, nested_candidates):
    v = _first_or_none(r, flat_keys); v = _to_float_pct(v)
    return v if v is not None else _get_change_pct_from_nested(r, nested_key, nested_candidates)

def build_table(df):
    rows = []
    for r in df.to_dict(orient="records"):
        mscore = compute_meme_score_row(r, (w_symbol, w_age, w_txns, w_liq, w_dex), liq_min_sweet, liq_max_sweet)
        ageh = hours_since_ms(r.get("pairCreatedAt", 0))
        chg_1h = _get_change_pct(r, ["priceChange1hPct","priceChangeH1Pct","pc1h","priceChange1h"], "priceChange", ("h1","1h","m60","60m"))
        chg_4h = _get_change_pct(r, ["priceChange4hPct","priceChangeH4Pct","pc4h","priceChange4h"], "priceChange", ("h4","4h","m240","240m"))
        if show_h6_fallback and chg_4h is None:
            chg_4h = _get_change_pct(r, ["priceChange6hPct","priceChangeH6Pct","pc6h","priceChange6h"], "priceChange", ("h6","6h","m360","360m"))
        chg_24h = _get_change_pct(r, ["priceChange24hPct","priceChangeH24Pct","pc24h","priceChange24h"], "priceChange", ("h24","24h","m1440","1440m"))
        roi_pct, ath_pct, dd_pct = update_profit_metrics_from_raw(r)

        rows.append({
            "Meme Score": mscore,
            "Pair": f"{r.get('baseSymbol','')}/{r.get('quoteSymbol','')}",
            "DEX": r.get("dexId",""),
            "Liquidity (USD)": to_int0(r.get("liquidityUsd"), 0),
            "Txns 1h": to_int0(r.get("txns1h"), 0),
            "Volume 24h (USD)": to_int0(r.get("volume24hUsd"), 0),
            "Price (USD)": (None if r.get("priceUsd") in (None, "") else to_float0(r.get("priceUsd"), None)),
            "ROI (%)": roi_pct, "ATH (%)": ath_pct, "Drawdown (%)": dd_pct,
            "Change 1h (%)": chg_1h, "Change 4h/6h (%)": chg_4h, "Change 24h (%)": chg_24h,
            "Created (UTC)": ms_to_dt(r.get("pairCreatedAt", 0)),
            "Pair Age": fmt_age(ageh), "PairAgeHours": (float(ageh) if ageh is not None else None),
            "Link": r.get("url",""),
            "Base Address": r.get("baseAddress",""), "Pair Address": r.get("pairAddress",""),
            "baseSymbol": r.get("baseSymbol",""), "quoteSymbol": r.get("quoteSymbol",""),
        })
    out = pd.DataFrame(rows)
    if not out.empty and sort_by_meme:
        out = out.sort_values(by=["Meme Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False])
    return out

df_pairs = build_table(df_view)

# === Filtri SOLO tabella ===
df_pairs_table = df_pairs.copy()
if not df_pairs_table.empty and pairs_meme_min > 0:
    df_pairs_table = df_pairs_table[pd.to_numeric(df_pairs_table["Meme Score"], errors="coerce").fillna(0) >= int(pairs_meme_min)]
if not df_pairs_table.empty and "PairAgeHours" in df_pairs_table.columns:
    age_series = pd.to_numeric(df_pairs_table["PairAgeHours"], errors="coerce")
    df_pairs_table = df_pairs_table[age_series.between(float(pairs_age_min_h), float(pairs_age_max_h), inclusive="both")]
if pairs_liq_enable and not df_pairs_table.empty and "Liquidity (USD)" in df_pairs_table.columns:
    liq_series = pd.to_numeric(df_pairs_table["Liquidity (USD)"], errors="coerce").fillna(0)
    df_pairs_table = df_pairs_table[(liq_series >= pairs_liq_min) & (liq_series <= pairs_liq_max)]
if pairs_vol_enable and not df_pairs_table.empty and "Volume 24h (USD)" in df_pairs_table.columns:
    vol_series_tbl = pd.to_numeric(df_pairs_table["Volume 24h (USD)"], errors="coerce").fillna(0)
    df_pairs_table = df_pairs_table[(vol_series_tbl >= pairs_vol_min) & (vol_series_tbl <= pairs_vol_max)]
if survivors_only and not df_pairs_table.empty:
    age_series = pd.to_numeric(df_pairs_table["PairAgeHours"], errors="coerce").fillna(0)
    roi_series = pd.to_numeric(df_pairs_table["ROI (%)"], errors="coerce")
    df_pairs_table = df_pairs_table[(age_series >= 1.0) & (roi_series > 0)]

# === PAIRS → Diagnostica (opzionale) ===
df_pairs_diag = df_pairs.copy()
if pairs_filters_to_strategy and not df_pairs_diag.empty:
    if apply_meme_to_strat and pairs_meme_min > 0:
        df_pairs_diag = df_pairs_diag[pd.to_numeric(df_pairs_diag["Meme Score"], errors="coerce").fillna(0) >= int(pairs_meme_min)]
    if apply_age_to_strat and "PairAgeHours" in df_pairs_diag.columns:
        age_series_str = pd.to_numeric(df_pairs_diag["PairAgeHours"], errors="coerce")
        df_pairs_diag = df_pairs_diag[age_series_str.between(float(pairs_age_min_h), float(pairs_age_max_h), inclusive="both")]
    if apply_liq_to_strat and pairs_liq_enable and "Liquidity (USD)" in df_pairs_diag.columns:
        liq_series_str = pd.to_numeric(df_pairs_diag["Liquidity (USD)"], errors="coerce").fillna(0)
        df_pairs_diag = df_pairs_diag[(liq_series_str >= pairs_liq_min) & (liq_series_str <= pairs_liq_max)]
    if apply_vol_to_strat and pairs_vol_enable and "Volume 24h (USD)" in df_pairs_diag.columns:
        vol_series_str = pd.to_numeric(df_pairs_diag["Volume 24h (USD)"], errors="coerce").fillna(0)
        df_pairs_diag = df_pairs_diag[(vol_series_str >= pairs_vol_min) & (vol_series_str <= pairs_vol_max)]

df_pairs_used = df_pairs_diag if (pairs_filters_to_strategy) else df_pairs

# Pillola riassuntiva PAIRS→Diagnostica
def _fmt_exp_range(lo_exp, hi_exp):
    try: return f"10^{lo_exp:g}–10^{hi_exp:g}"
    except Exception: return f"10^{lo_exp}–10^{hi_exp}"
applied = []
if pairs_filters_to_strategy:
    if apply_meme_to_strat and int(pairs_meme_min) > 0: applied.append(f"Meme ≥ {int(pairs_meme_min)}")
    if apply_age_to_strat and (float(pairs_age_min_h) > 0.0 or float(pairs_age_max_h) < AGE_LIMIT_HOURS):
        if st.session_state.get("fresh60_toggle"): applied.append("Age < 60m")
        elif st.session_state.get("pairs_age_unit", "Ore") == "Minuti": applied.append(f"Age {int(pairs_age_min_m)}–{int(pairs_age_max_m)}m")
        else: applied.append(f"Age {pairs_age_min_h:g}–{pairs_age_max_h:g}h")
    if apply_liq_to_strat and pairs_liq_enable and (pairs_liq_exp_min > 0.0 or pairs_liq_exp_max < 12.0):
        applied.append(f"Liq {_fmt_exp_range(pairs_liq_exp_min, pairs_liq_exp_max)}")
    if apply_vol_to_strat and pairs_vol_enable and (pairs_vol_exp_min > 0.0 or pairs_vol_exp_max < 12.0):
        applied.append(f"Vol24 {_fmt_exp_range(pairs_vol_exp_min, pairs_vol_exp_max)}")
base_n = 0 if df_pairs is None or df_pairs.empty else len(df_pairs)
used_n = 0 if df_pairs_used is None or df_pairs_used.empty else len(df_pairs_used)
pct = (used_n / base_n * 100.0) if base_n > 0 else 0.0
if pairs_filters_to_strategy:
    if applied:
        st.success(f"**PAIRS→Diagnostica: ON** • Filtri: " + ", ".join(applied) + f" • Universo: **{used_n}/{base_n}** (≈ {pct:.1f}%)", icon="🧠")
    else:
        st.info(f"**PAIRS→Diagnostica: ON** • Nessun filtro effettivo. Universo: **{used_n}/{base_n}** (≈ {pct:.1f}%)", icon="ℹ️")
else:
    st.caption("PAIRS→Diagnostica: **OFF** — i filtri PAIRS impattano solo **Tabella/Top 10**, non la diagnostica.")

# ============== Equity helpers & tick ==============
def _addr_from_row(row: dict):
    for k in ("Base Address","Pair Address"):
        v = row.get(k)
        if isinstance(v, str) and v: return v
    return row.get("Pair")

def _select_top_roi(df_pairs_table: pd.DataFrame, topN: int) -> pd.DataFrame:
    if df_pairs_table is None or df_pairs_table.empty: return pd.DataFrame()
    tmp = df_pairs_table.copy()
    tmp["ROI_val"] = pd.to_numeric(tmp["ROI (%)"], errors="coerce")
    tmp = tmp.dropna(subset=["ROI_val"])
    if tmp.empty:
        tmp["c1_val"] = pd.to_numeric(tmp["Change 1h (%)"], errors="coerce")
        tmp = tmp.dropna(subset=["c1_val"]).sort_values(by="c1_val", ascending=False).head(topN)
    else:
        tmp = tmp.sort_values(by="ROI_val", ascending=False).head(topN)
    return tmp

def _equity_tick(df_pairs_table: pd.DataFrame, topN: int = 10) -> None:
    if df_pairs_table is None or df_pairs_table.empty: return
    sel = _select_top_roi(df_pairs_table, topN); 
    if sel.empty: return
    curr_prices = {}
    for _, row in sel.iterrows():
        addr = _addr_from_row(row); px = row.get("Price (USD)")
        try:
            if addr and px is not None and float(px) > 0: curr_prices[addr] = float(px)
        except Exception: pass
    if not curr_prices: return
    prev = st.session_state["eq_last_prices"] or {}
    keys = [k for k in curr_prices.keys() if k in prev and prev[k] > 0]
    if keys:
        rets = []
        for k in keys:
            try: rets.append(curr_prices[k] / prev[k] - 1.0)
            except Exception: pass
        if rets:
            port_ret = sum(rets) / len(rets)
            st.session_state["eq_equity"] *= (1.0 + port_ret)
            st.session_state["eq_history"].append({"ts": time.time(),"equity": st.session_state["eq_equity"],"ret": port_ret,"n": len(keys)})
    else:
        st.session_state["eq_history"].append({"ts": time.time(),"equity": st.session_state["eq_equity"],"ret": 0.0,"n": 0})
    st.session_state["eq_last_prices"] = curr_prices

def _max_drawdown(equity_series: list[float]) -> float:
    peak = -1e18; mdd = 0.0
    for x in equity_series:
        if x > peak: peak = x
        if peak > 0:
            dd = (x / peak) - 1.0
            if dd < mdd: mdd = dd
    return mdd

if running and st.session_state.get("eq_enabled", True):
    topn_tick = int(st.session_state.get("eq_topN_tab", 10))
    _equity_tick(df_pairs_table, topN=topn_tick)

# ============================ TABS ============================
tab_radar, tab_winners, tab_equity, tab_entry, tab_paper = st.tabs(
    ["📡 Radar", "🏆 Winners Now", "📈 Equity Curve", "🎯 Entry Finder", "🧪 Paper Trading"]
)

with tab_radar:
    # Charts
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

    # Tabella PAIRS + Drilldown
    st.markdown("### Pairs (post-filtri)")
    if not df_pairs_table.empty:
        base_for_view = df_pairs_table
        df_pairs_for_view = base_for_view.sort_values(by=["Volume 24h (USD)"], ascending=False).head(10) if show_top10_table else base_for_view

        df_pairs_for_view = df_pairs_for_view.copy()
        if "Select" not in df_pairs_for_view.columns:
            df_pairs_for_view.insert(0, "Select", False)

        display_cols = ["Select","Pair","DEX","Meme Score","Price (USD)",
                        "ROI (%)","ATH (%)","Drawdown (%)",
                        "Change 1h (%)","Change 4h/6h (%)","Change 24h (%)",
                        "Txns 1h","Liquidity (USD)","Volume 24h (USD)","Pair Age","Link"]
        if not show_pair_age and "Pair Age" in display_cols:
            display_cols.remove("Pair Age")

        edited = st.data_editor(
            df_pairs_for_view[display_cols],
            key="pairs_editor",
            hide_index=True,
            use_container_width=True,
            disabled=False,
            column_config={
                "Select": st.column_config.CheckboxColumn(help="Spunta per aprire il pannello drill-down"),
                "Link": st.column_config.LinkColumn("Link"),
                "Liquidity (USD)": st.column_config.NumberColumn(format="%,d"),
                "Volume 24h (USD)": st.column_config.NumberColumn(format="%,d"),
                "Meme Score": st.column_config.NumberColumn(help="0–100"),
                "Price (USD)": st.column_config.NumberColumn(format="%.8f"),
                "ROI (%)": st.column_config.NumberColumn(format="%.2f"),
                "ATH (%)": st.column_config.NumberColumn(format="%.2f"),
                "Drawdown (%)": st.column_config.NumberColumn(format="%.2f"),
                "Change 1h (%)": st.column_config.NumberColumn(format="%.2f"),
                "Change 4h/6h (%)": st.column_config.NumberColumn(format="%.2f"),
                "Change 24h (%)": st.column_config.NumberColumn(format="%.2f"),
            }
        )

        try:
            sel_idx = edited.index[edited["Select"] == True].tolist()
        except Exception:
            sel_idx = []
        selected_row = df_pairs_for_view.loc[sel_idx[-1]] if sel_idx else None

        try:
            n1 = pd.to_numeric(df_pairs_for_view.loc[edited.index, "Change 1h (%)"], errors="coerce").notna().sum()
            n4 = pd.to_numeric(df_pairs_for_view.loc[edited.index, "Change 4h/6h (%)"], errors="coerce").notna().sum()
            nroi = pd.to_numeric(df_pairs_for_view.loc[edited.index, "ROI (%)"], errors="coerce").notna().sum()
            st.caption(f"Diagnostica Change/ROI: 1h {n1}/{len(edited)} • 4h/6h {n4}/{len(edited)} • ROI {nroi}/{len(edited)}")
        except Exception:
            pass
        cap = "Top 10 per Volume 24h (tabella filtrata)." if show_top10_table else "Tutte le coppie (tabella filtrata)."
        cap += "  (Se H4 mancante, mostrata H6)" if show_h6_fallback else ""
        if survivors_only: cap += "  •  Filtro: Survivors 60m"
        st.caption(cap)

        # Drill-down
        def _fetch_pair_details_safely(pair_addr: str):
            if not pair_addr: return None, None
            url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair_addr}"
            data, code = fetch_with_retry(url, tries=3, base_backoff=0.6, headers=UA_HEADERS)
            if data and isinstance(data, dict) and data.get("pairs"):
                return data["pairs"][0], code
            return None, code

        def _hostname(url: str) -> str:
            try: return urlparse(url).hostname or ""
            except Exception: return ""

        def _collect_socials(info: dict) -> dict:
            if not isinstance(info, dict): return {}
            links = {}
            webs = info.get("websites")
            if isinstance(webs, list):
                for w in webs:
                    url = w.get("url") if isinstance(w, dict) else (w if isinstance(w,str) else None)
                    if url and "http" in url: links.setdefault("website", url)
            socs = info.get("socials")
            if isinstance(socs, list):
                for s in socs:
                    if isinstance(s, dict):
                        typ = (s.get("type") or s.get("name") or "").lower()
                        url = s.get("url") or s.get("link")
                    elif isinstance(s, str):
                        typ = ""; url = s
                    else:
                        continue
                    if not (url and "http" in url): continue
                    if "twitter" in typ or typ == "x" or "x.com" in url: links.setdefault("twitter", url)
                    elif "telegram" in typ or "t.me" in url: links.setdefault("telegram", url)
                    elif "discord" in typ: links.setdefault("discord", url)
                    elif "github" in typ: links.setdefault("github", url)
                    elif "medium" in typ: links.setdefault("medium", url)
                    elif "coingecko" in typ: links.setdefault("coingecko", url)
                    elif "coinmarketcap" in typ or "cmc" in typ: links.setdefault("cmc", url)
                    else: links.setdefault("other", url)
            return links

        if selected_row is not None:
            pair_addr = None
            if "Pair Address" in df_pairs.columns and pd.notna(selected_row.get("Pair Address","")):
                pair_addr = str(selected_row.get("Pair Address"))
            if (not pair_addr) and isinstance(selected_row.get("Link",""), str) and "/solana/" in selected_row.get("Link",""):
                try: pair_addr = selected_row["Link"].split("/solana/")[1].split("?")[0]
                except Exception: pass

            st.markdown("#### 🔎 Token drill-down")
            if not pair_addr:
                st.info("Impossibile determinare il Pair Address.")
            else:
                pair, http_code = _fetch_pair_details_safely(pair_addr)
                if not pair:
                    st.warning(f"Nessun dettaglio disponibile (DexScreener {http_code}).")
                else:
                    cA, cB = st.columns([1,3])
                    img_url = pair.get("info",{}).get("imageUrl")
                    if img_url: cA.image(img_url, width=72)
                    base = pair.get("baseToken",{}).get("symbol","")
                    quote = pair.get("quoteToken",{}).get("symbol","")
                    st_pair = f"**{base}/{quote}**  •  `{pair_addr}`"
                    url_dex = pair.get("url","")
                    if url_dex: st_pair += f"  •  [DexScreener]({url_dex})"
                    cB.markdown(st_pair)

                    bA, bB, bC, bD = st.columns(4)
                    baddr = pair.get("baseToken",{}).get("address","")
                    qaddr = pair.get("quoteToken",{}).get("address","")
                    if baddr and qaddr:
                        jup = f"https://jup.ag/swap/{baddr}-{qaddr}"
                        ray = f"https://raydium.io/swap/?inputMint={baddr}&outputMint={qaddr}"
                        bA.link_button("Jupiter", jup, use_container_width=True)
                        bB.link_button("Raydium", ray, use_container_width=True)
                    if baddr:
                        be = f"https://birdeye.so/token/{baddr}?chain=solana"
                        bC.link_button("Birdeye", be, use_container_width=True)
                    if url_dex:
                        bD.link_button("DexScreener", url_dex, use_container_width=True)

                    info_obj = pair.get("info", {}) or {}
                    social_links = _collect_socials(info_obj)
                    if social_links:
                        st.markdown("##### Social & Sito")
                        ordered = [("website","Website"),("twitter","X / Twitter"),("telegram","Telegram"),
                                   ("discord","Discord"),("github","GitHub"),("medium","Medium"),
                                   ("coingecko","CoinGecko"),("cmc","CoinMarketCap"),("other","Altro")]
                        buttons = [(label, social_links[k]) for k, label in ordered if k in social_links]
                        ncols = min(4, max(1, len(buttons)))
                        cols = st.columns(ncols)
                        for i,(label,url) in enumerate(buttons):
                            cols[i % ncols].link_button(label, url, use_container_width=True)
                    else:
                        st.caption("Social non disponibili.")

                    _HUB_DOMAINS = {"linktr.ee","links.linktr.ee","beacons.ai","linkin.bio"}
                    _SUSPICIOUS_DOMAINS = {"forms.gle","docs.google.com","site.google.com","notion.so","notion.site","pastebin.com","pastelink.net"}
                    def _domain_in(set_domains, url):
                        host = _hostname(url).lower()
                        return any(host == d or host.endswith("." + d) for d in set_domains)
                    status = "weak"; reasons=[]
                    keys=set(social_links.keys())
                    if keys == {"other"}:
                        status="suspicious"; reasons.append("Solo link 'other'.")
                    else:
                        if "website" in social_links and _domain_in(_HUB_DOMAINS, social_links["website"]):
                            reasons.append("Website è un link-hub (es. linktr.ee).")
                        if any(_domain_in(_SUSPICIOUS_DOMAINS, u) for u in social_links.values()):
                            reasons.append("Domini sospetti tra i link.")
                        if ("website" in social_links) and (("twitter" in social_links) or ("telegram" in social_links)) and not reasons:
                            status="strong"
                    if status=="strong": st.success("🛡️ Linkset solido (website + social).", icon="🛡️")
                    elif status=="weak": st.warning("🟡 Linkset limitato/medio.", icon="🟡")
                    else: st.error("🔴 Linkset sospetto.", icon="🚩")
                    if reasons:
                        with st.expander("Dettagli valutazione"): 
                            for r in reasons: st.markdown(f"- {r}")

                    st.divider()

                    m1, m2, m3, m4 = st.columns(4)
                    try: m1.metric("Prezzo (USD)", f"{float(pair.get('priceUsd',0) or 0):.8f}")
                    except Exception: m1.metric("Prezzo (USD)", "N/D")
                    try: m2.metric("Liq (USD)", fmt_int(pair.get("liquidity",{}).get("usd",0)))
                    except Exception: m2.metric("Liq (USD)", "N/D")
                    try: m3.metric("Vol 24h (USD)", fmt_int(pair.get("volume",{}).get("h24",0) or 0))
                    except Exception: m3.metric("Vol 24h (USD)", "N/D")
                    try:
                        ch24 = pair.get("priceChange",{}).get("h24", None)
                        ch24 = float(str(ch24).replace("%","")) if ch24 is not None else None
                        m4.metric("Change 24h", f"{ch24:.2f}%" if ch24 is not None else "N/D")
                    except Exception:
                        m4.metric("Change 24h", "N/D")

                    g1, g2, g3 = st.columns(3)
                    try:
                        tfs=[]; vals=[]
                        for tf in ("m5","h1","h6","h24"):
                            v = pair.get("priceChange",{}).get(tf, None)
                            if v is not None:
                                vals.append(float(str(v).replace("%",""))); tfs.append(tf.upper())
                        if tfs:
                            figc = px.bar(pd.DataFrame({"TF": tfs, "Change %": vals}), x="TF", y="Change %", title="Change % by TF")
                            g1.plotly_chart(figc, use_container_width=True)
                        else:
                            g1.info("Change% non disponibile.")
                    except Exception:
                        g1.info("Change% non disponibile.")

                    try:
                        rows_tx=[]
                        for tf in ("m5","h1"):
                            rows_tx.append({"TF": tf.upper(), "Side": "Buys", "Tx": pair.get("txns",{}).get(tf,{}).get("buys",0)})
                            rows_tx.append({"TF": tf.upper(), "Side": "Sells","Tx": pair.get("txns",{}).get(tf,{}).get("sells",0)})
                        dftx = pd.DataFrame(rows_tx)
                        if len(dftx):
                            figt = px.bar(dftx, x="TF", y="Tx", color="Side", barmode="group", title="Buys/Sells")
                            g2.plotly_chart(figt, use_container_width=True)
                        else:
                            g2.info("Tx breakdown non disponibile.")
                    except Exception:
                        g2.info("Tx breakdown non disponibile.")

                    try:
                        v24 = float(pair.get("volume",{}).get("h24",0) or 0)
                        liq = float(pair.get("liquidity",{}).get("usd",0) or 0)
                        dflq = pd.DataFrame({"Metric": ["Vol 24h","Liquidity"], "USD": [v24, liq]})
                        figv = px.bar(dflq, x="Metric", y="USD", title="Vol 24h vs Liquidity")
                        g3.plotly_chart(figv, use_container_width=True)
                    except Exception:
                        g3.info("Vol vs Liq non disponibile.")
    else:
        st.caption("Nessuna coppia disponibile con i filtri attuali.")

    # Diagnostica mercato
    def market_heat_value(df: pd.DataFrame, topN: int) -> float:
        if df is None or df.empty: return 0.0
        if "Volume 24h (USD)" not in df.columns or "Txns 1h" not in df.columns: return 0.0
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
        if vol24_max > 0:
            c_vol = cnt((s["Volume 24h (USD)"] >= vmin) & (s["Volume 24h (USD)"] <= vol24_max))
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

with tab_winners:
    st.markdown("### 🏆 Winners Now")
    if df_pairs_table.empty:
        st.info("Nessuna coppia disponibile con i filtri tabella attuali.")
    else:
        df_roi = df_pairs_table.copy()
        df_roi["ROI_val"] = pd.to_numeric(df_roi["ROI (%)"], errors="coerce")
        df_roi = df_roi.dropna(subset=["ROI_val"]).sort_values(by="ROI_val", ascending=False).head(20)
        cols_keep = ["Pair","DEX","Price (USD)","ROI (%)","ATH (%)","Drawdown (%)","Change 1h (%)","Liquidity (USD)","Volume 24h (USD)","Pair Age","Link"]
        if df_roi.empty:
            st.warning("Nessun ROI calcolabile (prezzi non disponibili).")
        else:
            st.markdown("**Top 20 per ROI (%)**")
            st.dataframe(df_roi[cols_keep], use_container_width=True, hide_index=True)

        st.divider()
        df_c1 = df_pairs_table.copy()
        df_c1["c1_val"] = pd.to_numeric(df_c1["Change 1h (%)"], errors="coerce")
        df_c1 = df_c1.dropna(subset=["c1_val"]).sort_values(by="c1_val", ascending=False).head(20)
        if df_c1.empty:
            st.warning("Change 1h non disponibile.")
        else:
            st.markdown("**Top 20 per Change 1h (%)**")
            st.dataframe(df_c1[["Pair","DEX","Price (USD)","Change 1h (%)","Liquidity (USD)","Volume 24h (USD)","Pair Age","Link"]],
                         use_container_width=True, hide_index=True)
        st.caption(f"Survivors 60m attivo: {'SÌ' if survivors_only else 'NO'}")

with tab_equity:
    st.markdown("### 📈 Equity Curve (paper) — Top ROI Rebalance")
    colA, colB, colC, colD = st.columns(4)
    with colA:
        eq_enabled = st.toggle("Tracking ON/OFF", value=st.session_state.get("eq_enabled", True), key="eq_enabled_tab")
        st.session_state["eq_enabled"] = eq_enabled
    with colB:
        topN_tab = st.number_input("Top N per ROI", min_value=1, max_value=50, value=int(st.session_state.get("eq_topN_tab", 10)), step=1, key="eq_topN_tab")
    with colC:
        init_cap = st.number_input("Capitale iniziale", min_value=100.0, value=float(st.session_state.get("eq_init_capital", 1000.0)), step=100.0, key="eq_init_tab")
    with colD:
        if st.button("🔄 Reset equity"):
            st.session_state["eq_init_capital"] = float(init_cap)
            st.session_state["eq_equity"] = float(init_cap)
            st.session_state["eq_history"] = []
            st.session_state["eq_last_prices"] = {}
            st.success("Equity resettata.")

    hist = st.session_state.get("eq_history", [])
    if len(hist) < 1:
        st.info("Nessun dato ancora: attendi il primo refresh (o clicca 'Aggiorna ora').")
    else:
        df_eq = pd.DataFrame(hist)
        df_eq["t"] = pd.to_datetime(df_eq["ts"], unit="s")
        base = float(st.session_state.get("eq_init_capital", 1000.0))
        last_eq = float(df_eq["equity"].iloc[-1])
        cum_ret = (last_eq / base - 1.0) if base > 0 else 0.0
        mdd = _max_drawdown(df_eq["equity"].tolist())

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Capitale iniziale", f"${base:,.2f}".replace(",", "."))
        m2.metric("Equity attuale", f"${last_eq:,.2f}".replace(",", "."))
        m3.metric("Rendimento cumulato", f"{cum_ret*100:.2f}%")
        m4.metric("Max Drawdown", f"{mdd*100:.2f}%")

        fig_eq = px.line(df_eq, x="t", y="equity", title="Equity Curve (paper)")
        st.plotly_chart(fig_eq, use_container_width=True)

        with st.expander("Dettagli ultimo tick"):
            last = df_eq.iloc[-1].to_dict()
            st.write({
                "Timestamp": str(df_eq["t"].iloc[-1]),
                "Ritorno tick": f"{last.get('ret', 0.0)*100:.3f}%",
                "N token considerati": int(last.get("n", 0))
            })

        csv = df_eq[["t","equity","ret","n"]].to_csv(index=False).encode("utf-8")
        st.download_button("📥 Scarica equity.csv", data=csv, file_name="equity_curve.csv", mime="text/csv")

# ===================== Entry Finder (smart + presets + volume) =====================
with tab_entry:
    st.markdown("### 🎯 Entry Finder — scanner ingressi (smart + presets)")

    # Preset definitions
    ENTRY_PRESETS = {
        "Fiacco": {
            "ms_min": 55, "tx_min": 80,
            "liq_min": 5000, "liq_max": 150_000,
            "age_min_m": 10, "age_max_m": 720,
            "vol_min": 0, "vol_max": 0,
            "ch1_min": -8, "ch1_max": 18,
            "cap_24h": 120,
            "trend_pos": True,
            "allow_missing_ch1": True, "allow_missing_h4": True,
            "survivor": False, "targetN": 10
        },
        "Medio": {
            "ms_min": 65, "tx_min": 150,
            "liq_min": 10_000, "liq_max": 250_000,
            "age_min_m": 5, "age_max_m": 360,
            "vol_min": 0, "vol_max": 0,
            "ch1_min": -5, "ch1_max": 25,
            "cap_24h": 150,
            "trend_pos": True,
            "allow_missing_ch1": True, "allow_missing_h4": True,
            "survivor": False, "targetN": 12
        },
        "On-Fire": {
            "ms_min": 70, "tx_min": 250,
            "liq_min": 10_000, "liq_max": 0,
            "age_min_m": 0, "age_max_m": 240,
            "vol_min": 0, "vol_max": 0,
            "ch1_min": -3, "ch1_max": 35,
            "cap_24h": 200,
            "trend_pos": True,
            "allow_missing_ch1": True, "allow_missing_h4": True,
            "survivor": False, "targetN": 15
        }
    }
    def apply_preset(p):
        for k, v in p.items():
            st.session_state[f"ef_{k}"] = v
        st.toast("Preset applicato ✅"); st.rerun()

    try: _tx_heat = int(float(tx1h_avg or 0))
    except Exception: _tx_heat = 0
    suggested = "Fiacco"
    if _tx_heat >= 900: suggested = "On-Fire"
    elif _tx_heat >= 250: suggested = "Medio"

    cP1, cP2, cP3, cP4 = st.columns([1,1,1,2])
    if cP1.button("🥶 Fiacco"): apply_preset(ENTRY_PRESETS["Fiacco"])
    if cP2.button("🙂 Medio"): apply_preset(ENTRY_PRESETS["Medio"])
    if cP3.button("🔥 On-Fire"): apply_preset(ENTRY_PRESETS["On-Fire"])
    if cP4.button(f"🤖 Suggerisci preset (heat={_tx_heat})"):
        apply_preset(ENTRY_PRESETS[suggested])

    st.caption("Tip: puoi anche ignorare i filtri PAIRS per scansionare l’universo grezzo.")

    base_choice = st.radio(
        "Sorgente dati",
        ["Usa tabella filtrata (PAIRS)", "Ignora PAIRS (universo grezzo)"],
        horizontal=True, key="ef_source"
    )
    df_base_for_entry = df_pairs_table if st.session_state.get("ef_source","").startswith("Usa") else df_pairs
    st.caption(f"Universo di lavoro: {0 if df_base_for_entry is None else len(df_base_for_entry)}")

    if df_base_for_entry is None or df_base_for_entry.empty:
        st.info("Nessuna coppia disponibile con la sorgente selezionata.")
    else:
        # Widgets con chiavi persistenti (preset-friendly)
        c1, c2, c3 = st.columns(3)
        ms_min = c1.slider("Meme Score ≥", 0, 100, st.session_state.get("ef_ms_min", 60), 1, key="ef_ms_min")
        tx_min = c2.number_input("Txns 1h ≥", min_value=0, value=st.session_state.get("ef_tx_min", 150), step=25, key="ef_tx_min")
        liq_min_e = c3.number_input("Liquidity USD min", min_value=0, value=st.session_state.get("ef_liq_min", 0), step=1000, key="ef_liq_min")

        c4, c5, c6 = st.columns(3)
        liq_max_e = c4.number_input("Liquidity USD max (0 = ∞)", min_value=0, value=st.session_state.get("ef_liq_max", 0), step=5000, key="ef_liq_max")
        age_min_m = c5.number_input("Età min (min)", min_value=0, value=st.session_state.get("ef_age_min_m", 0), step=1, key="ef_age_min_m")
        age_max_m = c6.number_input("Età max (min)", min_value=0, value=st.session_state.get("ef_age_max_m", 360), step=5, key="ef_age_max_m")

        c7, c8, c9 = st.columns(3)
        ch1_min = c7.number_input("Change 1h min (%)", value=st.session_state.get("ef_ch1_min", -3), step=1, key="ef_ch1_min")
        ch1_max = c8.number_input("Change 1h max (%)", value=st.session_state.get("ef_ch1_max", 25), step=1, key="ef_ch1_max")
        trend_pos = c9.toggle("Richiedi H4/H6 > 0", value=st.session_state.get("ef_trend_pos", True), key="ef_trend_pos")

        c10, c11, c12 = st.columns(3)
        survivors_gate = c10.toggle("Richiedi Survivor 60m (ROI>0 & Age≥60m)", value=st.session_state.get("ef_survivor", False), key="ef_survivor")
        cap_24h = c11.number_input("Limita overextension (Change 24h max %)", min_value=0, value=st.session_state.get("ef_cap_24h", 150), step=10, key="ef_cap_24h")
        sort_mode = c12.radio("Ordina per", ["Momentum (1h %)", "Qualità (Meme Score)", "Freschezza (Age)"], horizontal=True, key="ef_sort_mode")

        c13, c14, c15 = st.columns(3)
        allow_missing_ch1 = c13.toggle("Consenti H1 mancante", value=st.session_state.get("ef_allow_missing_ch1", True), key="ef_allow_missing_ch1")
        allow_missing_h4  = c14.toggle("Consenti H4/H6 mancante", value=st.session_state.get("ef_allow_missing_h4", True), key="ef_allow_missing_h4")
        auto_relax        = c15.toggle("Auto-relax fino a N risultati", value=st.session_state.get("ef_auto_relax", True), key="ef_auto_relax")
        targetN = st.number_input("Target risultati", min_value=1, max_value=100, value=st.session_state.get("ef_targetN", 10), step=1, key="ef_targetN")

        # 🔎 Nuovi: filtri Volume 24h
        c16, c17 = st.columns(2)
        vol_min_e = c16.number_input("Volume 24h USD min", min_value=0, value=st.session_state.get("ef_vol_min", 0), step=10000, key="ef_vol_min")
        vol_max_e = c17.number_input("Volume 24h USD max (0 = ∞)", min_value=0, value=st.session_state.get("ef_vol_max", 0), step=100000, key="ef_vol_max")

        topN_show = st.number_input("Mostra prime N", min_value=1, max_value=100, value=st.session_state.get("ef_topN_show", 25), step=1, key="ef_topN_show")

        # Cast
        dfC = df_base_for_entry.copy()
        def _num(s, col, default=0.0):
            if col not in s.columns: return pd.Series([default]*len(s))
            return pd.to_numeric(s[col], errors="coerce").fillna(default)
        age_h   = pd.to_numeric(dfC.get("PairAgeHours"), errors="coerce")
        ms_col  = _num(dfC, "Meme Score")
        tx_col  = _num(dfC, "Txns 1h")
        liq_col = _num(dfC, "Liquidity (USD)")
        vol_col = _num(dfC, "Volume 24h (USD)")
        ch1_col = _num(dfC, "Change 1h (%)", default=-9999)
        ch4_col = _num(dfC, "Change 4h/6h (%)", default=-9999)
        ch24_col= _num(dfC, "Change 24h (%)", default=-9999)
        roi_col = _num(dfC, "ROI (%)", default=-9999)

        # Maschere
        m_ms  = (ms_col >= ms_min)
        m_tx  = (tx_col >= tx_min)
        m_liq = (liq_col >= liq_min_e) & ((liq_max_e == 0) | (liq_col <= liq_max_e))
        m_age = age_h.between(age_min_m/60.0, age_max_m/60.0, inclusive="both")
        m_vol = (vol_col >= vol_min_e) & ((vol_max_e == 0) | (vol_col <= vol_max_e))

        has_ch1 = ch1_col > -9998
        m_ch1 = ((~has_ch1) | ch1_col.between(ch1_min, ch1_max, inclusive="both")) if allow_missing_ch1 else (has_ch1 & ch1_col.between(ch1_min, ch1_max, inclusive="both"))
        has_h4 = ch4_col > -9998
        if trend_pos:
            m_ch4 = ((~has_h4) & allow_missing_h4) | (has_h4 & (ch4_col > 0))
        else:
            m_ch4 = (has_h4 | allow_missing_h4)
        m_ch24 = (ch24_col <= cap_24h) | (ch24_col < -9998)

        mask = m_ms & m_tx & m_liq & m_age & m_vol & m_ch1 & m_ch4 & m_ch24
        if survivors_gate:
            mask = mask & ((age_h >= 1.0) & (roi_col > 0))

        dfE = dfC[mask].copy()

        # Auto-relax (non aggressivo, include volume)
        relax_applied = False
        if auto_relax and (dfE.empty or len(dfE) < targetN):
            relax_applied = True
            _tx, _ch1min, _ch1max = int(tx_min), int(ch1_min), int(ch1_max)
            _liqmin, _liqmax = int(liq_min_e), int(liq_max_e)
            _volmin, _volmax = int(vol_min_e), int(vol_max_e)
            _agemin, _agemax = int(age_min_m), int(age_max_m)
            _cap24 = int(cap_24h)

            for _ in range(10):
                _tx = max(50, _tx - 50)
                _ch1min -= 3; _ch1max += 5
                _liqmin = max(0, _liqmin - 5000)
                _liqmax = 0 if _liqmax == 0 else min(1_000_000_000, int(_liqmax * 2))
                _volmin = max(0, _volmin - 20_000)
                _volmax = 0 if _volmax == 0 else min(2_000_000_000, int(_volmax * 2))
                _agemin = max(0, _agemin - 5)
                _agemax = min(720, _agemax + 60)
                _cap24  = min(300, _cap24 + 30)

                m_tx  = (tx_col >= _tx)
                m_liq = (liq_col >= _liqmin) & ((_liqmax == 0) | (liq_col <= _liqmax))
                m_age = age_h.between(_agemin/60.0, _agemax/60.0, inclusive="both")
                m_vol = (vol_col >= _volmin) & ((_volmax == 0) | (vol_col <= _volmax))
                m_ch1 = ((~(ch1_col > -9998)) | ch1_col.between(_ch1min, _ch1max, inclusive="both")) if allow_missing_ch1 else ( (ch1_col > -9998) & ch1_col.between(_ch1min, _ch1max, inclusive="both") )
                m_ch4 = ((~(ch4_col > -9998)) | (ch4_col > 0)) if trend_pos else ( (ch4_col > -9998) | allow_missing_h4 )
                m_c24 = (ch24_col <= _cap24) | (ch24_col < -9998)

                m_relaxed = m_ms & m_tx & m_liq & m_age & m_vol & m_ch1 & m_ch4 & m_c24
                if survivors_gate: m_relaxed &= ((age_h >= 1.0) & (roi_col > 0))
                dfE = dfC[m_relaxed].copy()
                if len(dfE) >= targetN or len(dfE) > 0:
                    st.session_state["ef_tx_min"] = _tx
                    st.session_state["ef_ch1_min"] = _ch1min
                    st.session_state["ef_ch1_max"] = _ch1max
                    st.session_state["ef_liq_min"] = _liqmin
                    st.session_state["ef_liq_max"] = _liqmax
                    st.session_state["ef_vol_min"] = _volmin
                    st.session_state["ef_vol_max"] = _volmax
                    st.session_state["ef_age_min_m"] = _agemin
                    st.session_state["ef_age_max_m"] = _agemax
                    st.session_state["ef_cap_24h"] = _cap24
                    break

        # Diagnostica rapida
        def _diag_counts():
            return {
                "Universe": len(dfC),
                "H1 n/d": int((ch1_col <= -9998).sum()),
                "H4/6 n/d": int((ch4_col <= -9998).sum())
            }
        diag = _diag_counts()
        cols = st.columns(3)
        cols[0].metric("Universe", diag.get("Universe", 0))
        cols[1].metric("H1 n/d", diag.get("H1 n/d", 0))
        cols[2].metric("H4/6 n/d", diag.get("H4/6 n/d", 0))

        # Entry grade + badge sintetico
        def _entry_grade(row) -> tuple[int, str]:
            # 0–100 + badge
            ms = float(row.get("Meme Score", 0) or 0)
            tx = float(row.get("Txns 1h", 0) or 0)
            liq= float(row.get("Liquidity (USD)", 0) or 0)
            vol= float(row.get("Volume 24h (USD)", 0) or 0)
            c1 = row.get("Change 1h (%)", None)
            c4 = row.get("Change 4h/6h (%)", None)
            c24= row.get("Change 24h (%)", None)

            heat = s_sigmoid(tx) * 100
            sweet_liq = score_liq(liq, liq_min_sweet, liq_max_sweet) * 100
            sweet_vol = 100.0 if (vol >= vol_min_e and (vol_max_e==0 or vol <= vol_max_e)) else (70.0 if vol>0 else 30.0)
            mom_ok = 100.0 if (c1 is not None and ch1_min <= float(c1) <= ch1_max) else (60.0 if (c1 is None and allow_missing_ch1) else 20.0)
            trend_ok = 100.0 if (c4 is not None and float(c4) > 0) else (60.0 if allow_missing_h4 else 20.0)
            overext = 100.0 if (c24 is None or float(c24) <= cap_24h) else 40.0

            grade = 0.25*ms + 0.2*heat + 0.15*sweet_liq + 0.1*sweet_vol + 0.15*mom_ok + 0.1*trend_ok + 0.05*overext
            badge = "🟢" if grade >= 70 else ("🟡" if grade >= 55 else "🔴")
            return int(round(grade)), badge

        if dfE.empty:
            st.warning("Nessun candidato con questi parametri. Prova un preset o allarga i range.")
        else:
            # Reasons
            reasons = []
            grades = []
            badges = []
            for _, r in dfE.iterrows():
                rs = []
                try:
                    if float(r.get("Meme Score", 0)) >= ms_min: rs.append("MS✓")
                    if float(r.get("Txns 1h", 0)) >= tx_min: rs.append("Tx1h✓")
                    L = float(r.get("Liquidity (USD)", 0))
                    if (L >= liq_min_e) and ((liq_max_e == 0) or (L <= liq_max_e)): rs.append("Liq✓")
                    V = float(r.get("Volume 24h (USD)", 0))
                    if (V >= vol_min_e) and ((vol_max_e == 0) or (V <= vol_max_e)): rs.append("Vol24✓")
                    v1 = r.get("Change 1h (%)", None)
                    if pd.isna(v1):
                        if allow_missing_ch1: rs.append("H1 n/d✓")
                    else:
                        if ch1_min <= float(v1) <= ch1_max: rs.append("H1✓")
                    v4 = r.get("Change 4h/6h (%)", None)
                    if pd.isna(v4):
                        if allow_missing_h4: rs.append("H4/6 n/d✓")
                    else:
                        if (not trend_pos) or float(v4) > 0: rs.append("H4/6✓")
                    if float(r.get("Change 24h (%)", 9999)) <= cap_24h: rs.append("24h≤cap")
                    if survivors_gate and float(r.get("ROI (%)", -999))>0 and float(r.get("PairAgeHours",0))>=1.0:
                        rs.append("Survivor✓")
                except Exception:
                    pass
                reasons.append(", ".join(rs))
                g, b = _entry_grade(r)
                grades.append(g); badges.append(b)

            dfE["Reasons"] = reasons
            dfE["Entry Grade"] = grades
            dfE["Badge"] = badges

            # Ordinamento & show
            if sort_mode.startswith("Momentum"):
                dfE = dfE.sort_values(by=["Change 1h (%)","Entry Grade","Meme Score"], ascending=[False, False, False])
            elif sort_mode.startswith("Qualità"):
                dfE = dfE.sort_values(by=["Entry Grade","Meme Score","Txns 1h"], ascending=[False, False, False])
            else:
                dfE = dfE.sort_values(by=["PairAgeHours","Entry Grade","Meme Score"], ascending=[True, False, False])

            keep_cols = ["Badge","Entry Grade","Pair","DEX","Meme Score","Price (USD)","Txns 1h",
                         "Liquidity (USD)","Volume 24h (USD)",
                         "Change 1h (%)","Change 4h/6h (%)","Change 24h (%)",
                         "ROI (%)","ATH (%)","Drawdown (%)",
                         "Pair Age","Link","Reasons"]
            show_cols = [c for c in keep_cols if c in dfE.columns]
            topN_show = int(st.session_state.get("ef_topN_show", 25))
            st.success(f"Candidati: {len(dfE)} — mostrati i primi {min(topN_show, len(dfE))}", icon="🎯")
            st.dataframe(dfE[show_cols].head(topN_show), use_container_width=True, hide_index=True)
            st.caption("Badge: 🟢 forte | 🟡 medio | 🔴 debole. Tip: apri **📡 Radar** e spunta la riga per il drill-down Jupiter/Raydium.")

            # === ALERT TELEGRAM: Entry Finder ===
            def tg_send(text: str):
                if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID): return False, "missing-credentials"
                try:
                    r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                     params={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}, timeout=15)
                    return (True, None) if r.ok else (False, f"status={r.status_code}")
                except Exception as e:
                    return False, str(e)

            if "tg_sent" not in st.session_state: st.session_state["tg_sent"] = {}
            cooldown = int(alert_cooldown_min) * 60
            now = time.time()
            if running and enable_entry_alerts and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                max_send = int(alert_max_per_run)
                sent = 0
                for _, row in dfE.head(max_send*2).iterrows():
                    if sent >= max_send: break
                    addr = str(row.get("Base Address","")) or row.get("Pair")
                    last_ts = st.session_state["tg_sent"].get(("entry", addr), 0)
                    if now - last_ts < cooldown: continue
                    pair = row.get("Pair",""); dex = row.get("DEX","")
                    ms   = int(row.get("Meme Score",0) or 0)
                    tx1  = int(row.get("Txns 1h",0) or 0)
                    liq  = int(row.get("Liquidity (USD)",0) or 0)
                    vol  = int(row.get("Volume 24h (USD)",0) or 0)
                    px   = row.get("Price (USD)", None)
                    ch1  = row.get("Change 1h (%)", None)
                    link = row.get("Link","")
                    grade= int(row.get("Entry Grade",0) or 0)
                    badge= row.get("Badge","")
                    txt = (f"🎯 ENTRY SIGNAL {badge} — {pair}\n"
                           f"DEX: {dex} | Grade: {grade} | MS: {ms}\n"
                           f"Tx1h: {tx1:,} | Liq: ${liq:,} | Vol24h: ${vol:,}")
                    if isinstance(px,(int,float)) and px: txt += f"\nPrice: {px:.8f}"
                    if ch1 is not None:
                        try: txt += f"  |  H1: {float(ch1):.2f}%"
                        except Exception: pass
                    if link: txt += f"\n{link}"
                    ok, err = tg_send(txt)
                    if ok:
                        st.session_state["tg_sent"][("entry", addr)] = now
                        sent += 1

# ===================== Paper Trading (vanilla) =====================
with tab_paper:
    st.markdown("### 🧪 Paper Trading — entry selezionate (vanilla)")
    st.caption("Nota: simulazione didattica. Nessun trading reale.")
    if df_pairs_table.empty:
        st.info("Serve almeno una lista dalla tabella filtrata (PAIRS).")
    else:
        # Candidati “buoni” = ROI>0 OR Change1h>0 AND badge 🟢/🟡
        dfPT = df_pairs_table.copy()
        ch1 = pd.to_numeric(dfPT.get("Change 1h (%)"), errors="coerce")
        roi = pd.to_numeric(dfPT.get("ROI (%)"), errors="coerce")
        dfPT["posMomentum"] = (ch1 > 0) | (roi > 0)
        # Entry grade semplice per ranking
        def _pt_grade(row):
            ms = float(row.get("Meme Score",0) or 0)
            tx = float(row.get("Txns 1h",0) or 0)
            liq= float(row.get("Liquidity (USD)",0) or 0)
            heat = s_sigmoid(tx)*100
            return 0.5*ms + 0.5*heat + 10*score_liq(liq, liq_min_sweet, liq_max_sweet)
        dfPT["PT Grade"] = dfPT.apply(_pt_grade, axis=1)

        good = dfPT[dfPT["posMomentum"]==True].sort_values(by=["PT Grade","Meme Score","Txns 1h"], ascending=[False,False,False])

        colO1, colO2 = st.columns(2)
        default_open = max(1, min(5, len(good)))
        topN_open = colO1.number_input("Apri prime N (🟢)", min_value=1, max_value=100, value=default_open, step=1)
        risk_per_pos = colO2.number_input("Risk% eq. per posizione", min_value=0.5, max_value=10.0, value=2.0, step=0.5)

        st.markdown("**Candidati ordinati (PT Grade)**")
        show_cols = ["Pair","DEX","Price (USD)","Meme Score","Txns 1h","Liquidity (USD)","Volume 24h (USD)","Change 1h (%)","ROI (%)","PT Grade","Link"]
        st.dataframe(good[show_cols].head(50), use_container_width=True, hide_index=True)

        if st.button("📌 Simula entrata sulle prime N"):
            # Semplice: alloca capitale per N posizioni, poi registra come 'aperto'
            N = int(topN_open)
            if N < 1 or good.empty:
                st.warning("Nessun candidato o N<1.")
            else:
                cap = float(st.session_state.get("eq_equity", 1000.0))
                risk_cap = cap * (risk_per_pos/100.0)
                alloc = risk_cap  # qui 'risk' = allocazione flat
                opened = []
                for _, r in good.head(N).iterrows():
                    px = r.get("Price (USD)")
                    pair = r.get("Pair")
                    if px is None or px==0: continue
                    opened.append({"pair": pair, "price": float(px), "alloc": float(alloc)})
                st.success(f"Aperte {len(opened)} posizioni paper (alloc≈${alloc:,.2f} cad.).".replace(",", "."))
                with st.expander("Dettagli"):
                    st.write(opened)

# ============== Alert Telegram (global) ==============
def tg_send(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID): return False, "missing-credentials"
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                         params={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}, timeout=15)
        return (True, None) if r.ok else (False, f"status={r.status_code}")
    except Exception as e:
        return False, str(e)

if "tg_sent" not in st.session_state: st.session_state["tg_sent"] = {}
tg_sent_now = 0
cooldown = int(alert_cooldown_min) * 60
now = time.time()

# (A) Alert "hit radar"
if running and enable_alerts and (df_pairs_table is not None) and not df_pairs_table.empty and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    try:
        df_alert = df_pairs_table.copy()
        mask = (df_alert["Txns 1h"] >= int(alert_tx1h_min)) & (df_alert["Liquidity (USD)"] >= int(alert_liq_min))
        if int(alert_meme_min) > 0: mask &= (df_alert["Meme Score"] >= int(alert_meme_min))
        df_alert = df_alert[mask]
        if not df_alert.empty:
            df_alert = df_alert.sort_values(by=["Meme Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False])
            max_send = int(alert_max_per_run)
            for _, row in df_alert.head(max_send*2).iterrows():
                addr = str(row.get("Base Address","")) or row.get("Pair")
                last_ts = st.session_state["tg_sent"].get(("hit", addr), 0)
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
                    st.session_state["tg_sent"][("hit", addr)] = now
                    tg_sent_now += 1
                    if tg_sent_now >= max_send: break
    except Exception as e:
        st.caption(f"Alert Telegram (hit): errore — {e}")

# (B) Trailing-stop alert su Drawdown
if running and enable_trailing and (df_pairs_table is not None) and not df_pairs_table.empty and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    try:
        df_tr = df_pairs_table.copy()
        dd_series = pd.to_numeric(df_tr["Drawdown (%)"], errors="coerce")
        roi_series = pd.to_numeric(df_tr["ROI (%)"], errors="coerce")
        mask_tr = (dd_series <= float(trailing_dd_thr)) & (roi_series >= 0)
        df_tr = df_tr[mask_tr]
        for _, row in df_tr.iterrows():
            addr = str(row.get("Base Address","")) or row.get("Pair")
            last_ts = st.session_state["tg_sent"].get(("trail", addr), 0)
            if now - last_ts < cooldown: continue
            pair = row.get("Pair",""); dd = row.get("Drawdown (%)"); roi=row.get("ROI (%)"); link=row.get("Link","")
            txt = f"⚠️ Trailing stop — {pair}\nDD: {dd:.1f}%  |  ROI: {roi:.1f}%"
            if link: txt += f"\n{link}"
            ok, err = tg_send(txt)
            if ok:
                st.session_state["tg_sent"][("trail", addr)] = now
                tg_sent_now += 1
    except Exception as e:
        st.caption(f"Alert Telegram (trailing): errore — {e}")

# ============== Diagnostica finale ==============
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
    f"Stato: {'🟢 Running' if running else '⏸️ Pausa'} • Refresh: {REFRESH_SEC}s • "
    f"TG alerts (run): {tg_sent_now} • Ticket proxy: ${PROXY_TICKET:.0f} • "
    f"PAIRS→Diagnostica: {'ON' if pairs_filters_to_strategy else 'OFF'} • "
    f"EquityCurve: tracking={'ON' if st.session_state.get('eq_enabled', True) else 'OFF'} • "
    f"TopN={int(st.session_state.get('eq_topN_tab', 10))} • Equity=${st.session_state.get('eq_equity', 0):,.2f}".replace(",", ".")
)

st.session_state["last_refresh_ts"] = time.time()
