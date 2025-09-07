# streamlit_app.py — Meme Radar (Streamlit) v10.2
# Novità v10.2: auto-refresh lato client + countdown "Prossimo refresh" + pulsante "Aggiorna ora".
# Confermati: Bubblemaps anti-cluster, GetMoni social filter, anti-duplicati, cooldown, time-stop,
# LIVE Pump.fun, fallback Moralis, Meme Score, grafici, watchlist, Telegram base.

import os, time, math, random, datetime, json, threading
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

try:
    import websocket  # pip install websocket-client
except Exception:
    websocket = None

from market_data import MarketDataProvider
from trading import RiskConfig, StratConfig, TradeEngine
from bubblemaps_client import check_wallet_clusters
from getmoni_client import SocialSentimentAnalyzer

# ---------------- Config ----------------
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

st.set_page_config(page_title="Meme Radar — Solana", layout="wide")
st.title("Solana Meme Coin Radar")

# Stato per stimare il prossimo refresh
if "last_refresh_ts" not in st.session_state:
    st.session_state["last_refresh_ts"] = time.time()

# ---------------- Sidebar ----------------
with st.sidebar:
    st.header("Impostazioni")
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

    st.divider()
    st.subheader("Watchlist")
    wl_default = os.getenv("WATCHLIST", "")
    watchlist_input = st.text_input("Simboli o address (comma-separated)", value=wl_default, help="Es: WIF,BONK,So111...,<pairAddress>")
    watchlist_only = st.toggle("Mostra solo watchlist", value=False)

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

    st.divider()
    st.subheader("Tabella")
    show_pair_age = st.toggle("Mostra colonna 'Pair Age' (min/ore)", value=True)

    st.divider()
    st.subheader("Alert Telegram (base)")
    TELEGRAM_BOT_TOKEN = st.text_input("Bot Token", value=os.getenv("TELEGRAM_BOT_TOKEN",""), type="password")
    TELEGRAM_CHAT_ID   = st.text_input("Chat ID", value=os.getenv("TELEGRAM_CHAT_ID",""))
    alert_tx1h_min     = st.number_input("Soglia txns 1h", min_value=0, value=200, step=10)
    alert_liq_min      = st.number_input("Soglia liquidity USD", min_value=0, value=20000, step=1000)
    alert_meme_min     = st.number_input("Soglia Meme Score (0=disattiva)", min_value=0, max_value=100, value=70, step=5)
    enable_alerts      = st.toggle("Abilita alert Telegram", value=False)

    st.divider()
    st.subheader("LIVE Pump.fun")
    pump_enable = st.toggle("Abilita feed live (subscribeNewToken)", value=False)
    pump_buffer = st.number_input("Buffer massimo eventi", min_value=50, max_value=1000, value=200, step=50)
    pump_keywords = st.text_input("Filtra per keyword (opz, virgola)", value="", help="Esempio: cat,dog,elon,wif")
    pump_alert_enable = st.toggle("Alert Telegram su match keyword", value=False, help="Richiede BOT_TOKEN e CHAT_ID sopra")

    st.divider()
    st.subheader("Fallback HTTP (Moralis)")
    moralis_enable = st.toggle("Mostra ultimi token via Moralis se WS è vuoto", value=True)
    MORALIS_API_KEY = st.text_input("MORALIS_API_KEY", value=os.getenv("MORALIS_API_KEY",""), type="password")
    moralis_exchange = st.selectbox("Exchange", options=["pumpfun","pump"], index=0)
    moralis_limit = st.slider("Quanti token recenti (fallback)", 10, 50, 20, step=5)

    st.divider()
    st.subheader("Trading (Paper) + Bubblemaps + GetMoni")
    # preset strategia
    preset = st.selectbox("Preset strategia", ["Prudente","Neutra","Aggressiva"], index=1)
    if st.button("Applica preset"):
        if preset == "Prudente":
            st.session_state.update({"pos_usd":30.0,"max_pos":2,"stop_pct":25,"tp_pct":60,"trail_pct":15,"day_loss":150.0,"strat_meme":75,"strat_txns":200})
        elif preset == "Neutra":
            st.session_state.update({"pos_usd":50.0,"max_pos":3,"stop_pct":20,"tp_pct":40,"trail_pct":15,"day_loss":200.0,"strat_meme":65,"strat_txns":150})
        else:
            st.session_state.update({"pos_usd":60.0,"max_pos":4,"stop_pct":18,"tp_pct":35,"trail_pct":12,"day_loss":250.0,"strat_meme":55,"strat_txns":100})
        st.rerun()

    colA, colB = st.columns(2)
    with colA:
        pos_usd = st.number_input("Posizione fissa (USD)", min_value=10.0, value=st.session_state.get("pos_usd", 50.0), step=10.0, key="pos_usd")
        max_pos = st.number_input("Max posizioni aperte", min_value=1, value=st.session_state.get("max_pos", 3), step=1, key="max_pos")
        stop_pct = st.slider("Stop Loss %", 5, 60, st.session_state.get("stop_pct", 20), key="stop_pct")
    with colB:
        tp_pct   = st.slider("Take Profit %", 10, 200, st.session_state.get("tp_pct", 40), key="tp_pct")
        trail_pct= st.slider("Trailing %", 5, 60, st.session_state.get("trail_pct", 15), key="trail_pct")
        day_loss = st.number_input("Daily loss limit (USD)", min_value=50.0, value=st.session_state.get("day_loss", 200.0), step=50.0, key="day_loss")
    strat_meme = st.slider("Soglia Meme Score", 0, 100, st.session_state.get("strat_meme", 75), key="strat_meme")
    strat_txns = st.number_input("Soglia Txns 1h", min_value=0, value=st.session_state.get("strat_txns", 300), step=50, key="strat_txns")

    # --- Regole anti-duplicato & timing ---
    st.markdown("**Regole anti-duplicato & timing**")
    colx1, colx2, colx3 = st.columns(3)
    with colx1:
        max_per_symbol = st.number_input("Max per simbolo", min_value=1, value=int(st.session_state.get("max_per_symbol", 1)), step=1, key="max_per_symbol")
    with colx2:
        cooldown_min = st.number_input("Cooldown (min)", min_value=0, value=int(st.session_state.get("cooldown_min", 20)), step=5, key="cooldown_min")
    with colx3:
        time_stop_min = st.number_input("Time-stop (min, 0=off)", min_value=0, value=int(st.session_state.get("time_stop_min", 60)), step=5, key="time_stop_min")

    colp1, colp2 = st.columns(2)
    with colp1:
        allow_pyr = st.toggle("Abilita pyramiding", value=bool(st.session_state.get("allow_pyr", False)), key="allow_pyr")
    with colp2:
        add_on_pct = st.slider("Trigger add-on (%)", 1, 25, int(st.session_state.get("add_on_pct", 8)), key="add_on_pct",
                               help="Apre un'add-on solo se il prezzo è già salito di almeno questa % dall'ultimo ingresso")

    BUBBLEMAPS_API_KEY = st.text_input("BUBBLEMAPS_API_KEY", value=os.getenv("BUBBLEMAPS_API_KEY",""), type="password", help="Filtro anti-cluster")

    # ---------- GetMoni (nuovo) ----------
    st.markdown("**GetMoni — Social Filter**")
    GETMONI_API_KEY = st.text_input("GETMONI_API_KEY", value=os.getenv("GETMONI_API_KEY",""), type="password")
    gm_header = st.selectbox("Header auth", ["X-API-Key","Authorization"], index=0, help="Se 'Authorization', usa Bearer <key>")
    gm_enable = st.toggle("Abilita filtro Social (GetMoni)", value=False)
    colg1, colg2, colg3 = st.columns(3)
    with colg1:
        gm_mentions = st.number_input("Min Mentions 24h", min_value=0, value=50, step=10)
    with colg2:
        gm_smarts = st.number_input("Min Smarts engagement", min_value=0, value=10, step=5, help="# di account 'smart' che interagiscono")
    with colg3:
        gm_sent = st.slider("Min Sentiment (Moni score)", 0, 100, 30)
    st.caption("Serve una mappa SYMBOL→@username. Formato: una riga per voce, 'SYMBOL=@handle'")

    gm_map_text = st.text_area(
        "Mappatura simboli → Twitter (GetMoni)",
        value=os.getenv("GETMONI_MAP", "WIF=@dogwifcoin\nBONK=@bonk_inu"),
        height=100
    )

# 🔁 Auto-refresh (pausa se WS attivo). Usa streamlit-autorefresh se disponibile.
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

def _tick_query_param():
    # Mantiene un "tick" nell'URL per evitare cache e aiutare la diagnosi
    try:
        st.query_params.update({"_": str(int(time.time() // max(1, REFRESH_SEC)))})
    except Exception:
        pass

# Stima countdown: prossimo refresh = ultimo_rerun + REFRESH_SEC
_prev_ts = float(st.session_state.get("last_refresh_ts", time.time()))
_next_ts = _prev_ts + max(1, REFRESH_SEC)
secs_left = max(0, int(round(_next_ts - time.time())))

if auto_refresh and not pump_enable:
    if st_autorefresh:
        # vero timer lato client
        st_autorefresh(interval=int(REFRESH_SEC * 1000), key="auto_refresh_tick")
    else:
        # fallback: rerun asincrono con thread
        import threading
        def _delayed_rerun():
            time.sleep(max(1, REFRESH_SEC))
            try:
                st.rerun()
            except Exception:
                pass
        if "fallback_rerun_thread" not in st.session_state:
            t = threading.Thread(target=_delayed_rerun, daemon=True)
            t.start()
            st.session_state["fallback_rerun_thread"] = True
    _tick_query_param()
else:
    if pump_enable:
        st.caption("Auto-refresh sospeso mentre il LIVE Pump.fun è attivo.")

# Barra utility: countdown + refresh manuale
util_col1, util_col2 = st.columns([5, 1])
with util_col1:
    st.caption(f"Prossimo refresh ~{secs_left}s")
with util_col2:
    if st.button("Aggiorna ora", use_container_width=True):
        st.rerun()

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
         local_weights[2]*s_sigmoid(tx1) + local_weights[3]*score_liq(liq, liq_min_sweet, liq_max_sweet) +
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

# Watchlist
def norm_list(s):
    out = []
    for part in (s or "").replace(" ", "").split(","):
        if not part: continue
        out.append(part.upper() if len(part) <= 8 else part)
    return out
watchlist = norm_list(watchlist_input)
def is_watch_hit_row(r):
    base = str(r.get("baseSymbol","")).upper() if hasattr(r,"get") else str(r["baseSymbol"]).upper()
    quote= str(r.get("quoteSymbol","")).upper() if hasattr(r,"get") else str(r["quoteSymbol"]).upper()
    addr = r.get("pairAddress","") if hasattr(r,"get") else r["pairAddress"]
    return (watchlist and (base in watchlist or quote in watchlist or addr in watchlist))

df_view = df_provider.copy()
pre_count = len(df_view)
if watchlist_only and not df_view.empty:
    mask = df_view.apply(is_watch_hit_row, axis=1)
    df_view = df_view[mask].reset_index(drop=True)
post_count = len(df_view)

# ---------------- KPI base ----------------
if df_view.empty:
    vol24_avg = None; tx1h_avg = None
else:
    top10 = df_view.sort_values(by=["volume24hUsd"], ascending=False).head(10)
    vol24_avg = safe_series_mean(top10["volume24hUsd"])
    tx1h_avg  = safe_series_mean(top10["txns1h"])
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
            x = float(v); 
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

# ---------------- Charts ----------------
left, right = st.columns(2)
with left:
    if not df_view.empty:
        df_top = df_view.sort_values(by=["volume24hUsd"], ascending=False).head(10)
        df_chart = pd.DataFrame({"Token": df_top["baseSymbol"], "Volume 24h": df_top["volume24hUsd"].fillna(0)})
        fig = px.bar(df_chart, x="Token", y="Volume 24h", title="Top 10 Volume 24h (post-filtri)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Nessuna coppia disponibile con i filtri attuali.")
with right:
    if bird_ok and bird_tokens:
        names, liqs = [], []
        for t in bird_tokens[:20]:
            names.append(t.get("name") or t.get("symbol") or (t.get("mint") or "")[:6])
            liqs.append(liquidity_from_birdeye_token(t) or 0)
        df_liq = pd.DataFrame({"Token": names, "Liquidity": liqs})
        fig2 = px.bar(df_liq, x="Token", y="Liquidity", title="Ultime 20 Nuove Coin – Liquidity (Birdeye)")
        st.plotly_chart(fig2, use_container_width=True)

# ---------------- Tabella pairs con Meme Score ----------------
def build_table(df):
    rows = []
    for r in df.to_dict(orient="records"):
        mscore = compute_meme_score_row(r, (w_symbol, w_age, w_txns, w_liq, w_dex), liq_min_sweet, liq_max_sweet)
        ageh = hours_since_ms(r.get("pairCreatedAt", 0))
        rows.append({
            "Meme Score": mscore,
            "Pair": f"{r.get('baseSymbol','')}/{r.get('quoteSymbol','')}",
            "DEX": r.get("dexId",""),
            "Liquidity (USD)": int(round(r.get("liquidityUsd") or 0)),
            "Txns 1h": int(r.get("txns1h") or 0),
            "Volume 24h (USD)": int(round(r.get("volume24hUsd") or 0)),
            "Price (USD)": r.get("priceUsd"),
            "Change 24h (%)": r.get("priceChange24hPct"),
            "Created (UTC)": ms_to_dt(r.get("pairCreatedAt", 0)),
            "Pair Age": fmt_age(ageh),
            "Link": r.get("url",""),
            "Base Address": r.get("baseAddress",""),
            "baseSymbol": r.get("baseSymbol",""),
        })
    out = pd.DataFrame(rows)
    if not out.empty and sort_by_meme:
        out = out.sort_values(by=["Meme Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False])
    return out

df_pairs = build_table(df_view)

st.markdown("### Pairs (post-filtri)")
if not df_pairs.empty:
    display_cols = [c for c in df_pairs.columns if c != "baseSymbol"]
    if not show_pair_age and "Pair Age" in display_cols:
        display_cols.remove("Pair Age")
    st.dataframe(
        df_pairs[display_cols],
        use_container_width=True,
        column_config={
            "Link": st.column_config.LinkColumn("Link", help="Apri su DexScreener"),
            "Liquidity (USD)": st.column_config.NumberColumn(format="%,d"),
            "Volume 24h (USD)": st.column_config.NumberColumn(format="%,d"),
            "Meme Score": st.column_config.NumberColumn(help="0–100: più alto = più 'meme' + momentum"),
            "Price (USD)": st.column_config.NumberColumn(format="%.8f"),
            "Change 24h (%)": st.column_config.NumberColumn(format="%.2f"),
        }
    )

# ---------------- Top 10 per Meme Score ----------------
st.markdown("### Top 10 per Meme Score")
if not df_pairs.empty:
    top10_meme = df_pairs.sort_values(by=["Meme Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False]).head(10)
    fig3 = px.bar(top10_meme, x="Pair", y="Meme Score",
                  hover_data=["DEX","Txns 1h","Liquidity (USD)","Volume 24h (USD)","Price (USD)","Change 24h (%)","Created (UTC)","Pair Age","Base Address"],
                  title="Top 10 per Meme Score")
    fig3.update_layout(yaxis_range=[0, 100]); fig3.update_xaxes(tickangle=-30)
    st.plotly_chart(fig3, use_container_width=True)

# ---------------- Trading (Paper) + Bubblemaps + GetMoni ----------------
st.markdown("## 🧪 Trading — Paper Mode (Bubblemaps + GetMoni)")

if "trade_engine" not in st.session_state:
    st.session_state["trade_engine"] = None
if "bm_cache" not in st.session_state:
    st.session_state["bm_cache"] = {}
if "gm_analyzer" not in st.session_state:
    st.session_state["gm_analyzer"] = None
if "gm_calls" not in st.session_state:
    st.session_state["gm_calls"] = 0

def bm_check_cached(addr: str):
    key = f"{addr}"
    hit = st.session_state["bm_cache"].get(key)
    now = time.time()
    if hit and now - hit["ts"] < 3600:  # 1h TTL
        return hit["res"]
    if not BUBBLEMAPS_API_KEY:
        return {"is_high_risk": False, "reason": "no-key", "top1": None, "top3": None, "score": None}
    res = check_wallet_clusters(addr, chain="solana", api_key=BUBBLEMAPS_API_KEY)
    st.session_state["bm_cache"][key] = {"ts": now, "res": res}
    return res

def parse_map(text: str) -> dict:
    mp = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"): 
            continue
        if "=" in line:
            sym, user = line.split("=", 1)
        elif ":" in line:
            sym, user = line.split(":", 1)
        else:
            continue
        mp[sym.strip().upper()] = user.strip()
    return mp

# init SocialSentimentAnalyzer
if gm_enable and GETMONI_API_KEY:
    mapping = parse_map(gm_map_text)
    if st.session_state["gm_analyzer"] is None:
        st.session_state["gm_analyzer"] = SocialSentimentAnalyzer(
            GETMONI_API_KEY,
            auth_header=gm_header,
            symbol_to_user=mapping,
            cache_ttl_sec=600,
            min_mentions_24h=int(gm_mentions),
            min_smart_engagement=int(gm_smarts),
            min_sentiment_score=float(gm_sent),
        )
    else:
        a = st.session_state["gm_analyzer"]
        a.api_key = GETMONI_API_KEY
        a.auth_header = gm_header
        a.update_mapping(mapping)
        a.th_mentions = int(gm_mentions)
        a.th_smarts = int(gm_smarts)
        a.th_sent = float(gm_sent)
else:
    st.session_state["gm_analyzer"] = None

def sent_check(symbol: str, base_addr: str):
    if not gm_enable or not st.session_state["gm_analyzer"]:
        return {"passes": True}
    st.session_state["gm_calls"] += 1
    return st.session_state["gm_analyzer"].analyze(symbol)

# costruzione TradeEngine
r_cfg = RiskConfig(
    position_usd=float(st.session_state.get("pos_usd",50.0)),
    max_positions=int(st.session_state.get("max_pos",3)),
    stop_loss_pct=float(st.session_state.get("stop_pct",20))/100.0,
    take_profit_pct=float(st.session_state.get("tp_pct",40))/100.0,
    trailing_pct=float(st.session_state.get("trail_pct",15))/100.0,
    daily_loss_limit_usd=float(st.session_state.get("day_loss",200.0)),
    # NEW anti-duplicati
    max_positions_per_symbol=int(st.session_state.get("max_per_symbol",1)),
    symbol_cooldown_min=int(st.session_state.get("cooldown_min",20)),
    allow_pyramiding=bool(st.session_state.get("allow_pyr", False)),
    pyramid_add_on_trigger_pct=float(st.session_state.get("add_on_pct",8))/100.0,
    time_stop_min=int(st.session_state.get("time_stop_min",60)),
)
s_cfg = StratConfig(meme_score_min=int(st.session_state.get("strat_meme",75)), txns1h_min=int(st.session_state.get("strat_txns",300)),
                    liq_min=float(liq_min_sweet), liq_max=float(liq_max_sweet),
                    allow_dex=("raydium","orca","meteora"))

if st.session_state["trade_engine"] is None:
    st.session_state["trade_engine"] = TradeEngine(r_cfg, s_cfg, bm_check=bm_check_cached, sent_check=sent_check)
else:
    eng = st.session_state["trade_engine"]
    eng.risk.cfg = r_cfg
    eng.strategy.cfg = s_cfg
    eng.bm_check = bm_check_cached
    eng.sent_check = sent_check

eng = st.session_state["trade_engine"]
if df_pairs is None or df_pairs.empty:
    st.info("Nessun dato per la strategia al momento.")
else:
    df_signals, df_open, df_closed = eng.step(df_pairs)

    st.markdown("**Segnali (candidati all'ingresso)**")
    if not df_signals.empty:
        st.dataframe(df_signals.head(12), use_container_width=True)
    else:
        st.caption("Nessun segnale valido: filtri tecnici, Bubblemaps e/o GetMoni possono aver escluso i candidati.")

    st.markdown("**Posizioni aperte (Paper)**")
    if not df_open.empty:
        cols = st.columns([3,2,2,2,2,2,2])
        cols[0].write("Pair"); cols[1].write("Entry"); cols[2].write("Last"); cols[3].write("PnL $"); cols[4].write("PnL %"); cols[5].write("Aperta da"); cols[6].write("Azioni")
        for _, r in df_open.iterrows():
            c = st.columns([3,2,2,2,2,2,2])
            c[0].write(f"{r['symbol']} ({r['label']})")
            c[1].write(f"{r['entry']:.8f}")
            c[2].write(f"{r['last']:.8f}")
            c[3].write(f"{r['pnl_usd']:.2f}")
            c[4].write(f"{r['pnl_pct']*100:.2f}%")
            c[5].write(r['opened_ago'])
            if c[6].button("Chiudi", key=f"close_{r['id']}"):
                try:
                    px = float(r["last"]); eng.close_by_id(str(r["id"]), px)
                except Exception:
                    pass
        st.caption(f"Posizioni aperte: {len(df_open)} / max {st.session_state['max_pos']}")
    else:
        st.caption("Nessuna posizione aperta.")

    st.markdown("**Ultime chiusure**")
    if not df_closed.empty:
        st.dataframe(df_closed, use_container_width=True)
    else:
        st.caption("Nessuna chiusura registrata (ancora).")

    # Performance rapida
    st.markdown("**Performance (Paper)**")
    st.metric("Daily PnL", f"{eng.risk.state.daily_pnl:.2f} USD",
              help=f"Stop nuove entrate se PnL giornaliero ≤ -{eng.risk.cfg.daily_loss_limit_usd:.0f} USD")
    if not df_closed.empty:
        try:
            pnl_series = pd.to_numeric(df_closed["pnl_usd"], errors="coerce").fillna(0).cumsum()
            perf_df = pd.DataFrame({"Trade #": range(1, len(pnl_series)+1), "CumPnL": pnl_series})
            figp = px.line(perf_df, x="Trade #", y="CumPnL", title="Cumulative PnL (closed)")
            st.plotly_chart(figp, use_container_width=True)
        except Exception:
            st.caption("Impossibile generare la curva PnL.")
    else:
        st.caption("Nessuna chiusura → curva PnL in attesa.")

# ---------------- LIVE Pump.fun (WS) + Fallback Moralis ----------------
class PumpFunLive:
    def __init__(self, max_rows=200, api_key: str | None = None, ua: str | None = None):
        self.max_rows = int(max_rows); self.api_key = api_key
        base = "wss://pumpportal.fun/api/data"
        self.url = f"{base}?api-key={api_key}" if api_key else base
        self.headers = ["Origin: https://pumpportal.fun", f"User-Agent: {ua or 'MemeRadar/1.0 (+streamlit)'}"]
        self._rows = []; self._lock = threading.Lock(); self._stop = threading.Event()
        self._ws = None; self._thread = None; self._last_err = None; self._connected = False; self._last_close = None
    def start(self):
        if self._thread and self._thread.is_alive(): return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="PumpFunLive", daemon=True); self._thread.start()
    def reconnect_now(self):
        self.stop(); time.sleep(0.2); self.start()
    def stop(self):
        self._stop.set()
        try:
            if self._ws: self._ws.close()
        except Exception: pass
    def _on_open(self, ws):
        self._connected = True; self._last_err = None
        try: ws.send(json.dumps({"method": "subscribeNewToken"}))
        except Exception as e: self._last_err = f"on_open send err: {e}"
    def _on_message(self, ws, message: str):
        try:
            data = json.loads(message) if isinstance(message, (str, bytes, bytearray)) else message
            mint   = data.get("mint") or data.get("token") or data.get("mintAddress") or data.get("address")
            name   = data.get("name") or data.get("tokenName") or data.get("symbol") or ""
            symbol = data.get("symbol") or ""
            creator= data.get("creator") or data.get("user") or data.get("owner") or ""
            ts = (data.get("createdTimestamp") or data.get("createdOn") or data.get("createdAt") or int(time.time()))
            if not mint: return
            row = {"Mint": mint,"Name": name,"Symbol": symbol,"Creator": creator,
                   "Created (UTC)": ms_to_dt(ts),"Age": fmt_age(hours_since_ms(ts)),
                   "Pump.fun": f"https://pump.fun/coin/{mint}","Solscan": f"https://solscan.io/token/{mint}"}
            with self._lock:
                if any(r["Mint"] == mint for r in self._rows): return
                self._rows.insert(0, row); 
                if len(self._rows) > self.max_rows: self._rows = self._rows[:self.max_rows]
        except Exception as e: self._last_err = f"on_message err: {e}"
    def _on_error(self, ws, error): self._last_err = f"{error}"
    def _on_close(self, ws, code, msg): self._connected = False; self._last_close = f"code={code}, reason={msg}"
    def _run(self):
        while not self._stop.is_set():
            try:
                self._ws = websocket.WebSocketApp(self.url, header=self.headers,
                    on_open=self._on_open, on_message=self._on_message, on_error=self._on_error, on_close=self._on_close)
                self._ws.run_forever(ping_interval=20, ping_timeout=10, ping_payload="ping")
            except Exception as e: self._last_err = f"run_forever err: {e}"
            if not self._stop.is_set(): time.sleep(1.5 + random.uniform(0, 2.0))
    def snapshot_df(self) -> pd.DataFrame:
        with self._lock: return pd.DataFrame(self._rows).copy()
    def status(self):
        info = []
        if self._last_err: info.append(f"err={self._last_err}")
        if self._last_close: info.append(f"close={self._last_close}")
        return ("connected" if self._connected else "disconnected", " • ".join(info) if info else None)

# init live
if "pump_live" not in st.session_state: st.session_state["pump_live"] = None
if pump_enable and websocket is None:
    st.warning("Installa `websocket-client` in requirements.txt per attivare il feed live.")
elif pump_enable and websocket is not None:
    if st.session_state["pump_live"] is None:
        st.session_state["pump_live"] = PumpFunLive(max_rows=pump_buffer, api_key=os.getenv("PUMP_API_KEY","")); st.session_state["pump_live"].start()
    else:
        st.session_state["pump_live"] = st.session_state["pump_live"]
        st.session_state["pump_live"].max_rows = int(pump_buffer)
else:
    if st.session_state["pump_live"] is not None:
        st.session_state["pump_live"].stop(); st.session_state["pump_live"] = None

def moralis_new_tokens(exchange: str, limit: int = 20, api_key: str | None = None) -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame(columns=["Mint","Name","Symbol","Created (UTC)","Age","Pump.fun","Solscan"])
    url = f"https://solana-gateway.moralis.io/token/mainnet/exchange/{exchange}/new?limit={int(limit)}"
    headers = {"X-API-Key": api_key, **UA_HEADERS}
    data, code = fetch_with_retry(url, headers=headers)
    if not data:
        return pd.DataFrame(columns=["Mint","Name","Symbol","Created (UTC)","Age","Pump.fun","Solscan"])
    items = data.get("result") or data.get("data") or data.get("tokens") or data.get("items") or data
    if not isinstance(items, list): items = []
    rows = []
    for t in items:
        mint = t.get("mint") or t.get("tokenAddress") or t.get("token_address") or t.get("address") or t.get("id")
        name = t.get("name") or t.get("tokenName") or t.get("symbol") or ""
        symbol = t.get("symbol") or ""
        ts = t.get("createdAt") or t.get("created_time") or t.get("creationTime") or t.get("createdTimestamp") or 0
        rows.append({"Mint": mint or "", "Name": name, "Symbol": symbol,
                     "Created (UTC)": ms_to_dt(ts) if ts else "", "Age": fmt_age(hours_since_ms(ts)) if ts else "",
                     "Pump.fun": f"https://pump.fun/coin/{mint}" if mint else "", "Solscan": f"https://solscan.io/token/{mint}" if mint else ""})
    df = pd.DataFrame(rows)
    if not df.empty: df = df.drop_duplicates(subset=["Mint"]).reset_index(drop=True)
    return df

st.markdown("## 🔴 LIVE: New on Pump.fun")
if pump_enable and st.session_state["pump_live"] and websocket is not None:
    p = st.session_state["pump_live"]
    status, last_info = p.status()
    col_stat, col_btn = st.columns([3,1])
    with col_stat:
        st.caption(f"WebSocket: {status}" + (f" • {last_info}" if last_info else ""))
    with col_btn:
        if st.button("🔁 Riconnetti WS"): p.reconnect_now()

    df_live = p.snapshot_df()
    keys = [k.strip().lower() for k in (pump_keywords or "").split(",") if k.strip()]
    if not df_live.empty and keys:
        def match_row(r):
            s = (str(r.get("Name","")) + " " + str(r.get("Symbol",""))).lower()
            return any(k in s for k in keys)
        df_live = df_live[df_live.apply(match_row, axis=1)].reset_index(drop=True)

    if not df_live.empty:
        st.dataframe(df_live.head(50), use_container_width=True,
                     column_config={"Pump.fun": st.column_config.LinkColumn("Pump.fun"),
                                    "Solscan": st.column_config.LinkColumn("Solscan")})
        # (opz) invio Telegram su match keyword
        if pump_alert_enable and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and keys:
            try:
                hit = df_live.iloc[0].to_dict()
                text = f"🆕 Pump.fun: {hit.get('Name','')} ({hit.get('Symbol','')})\n{hit.get('Pump.fun','')}"
                requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                             params={"chat_id": TELEGRAM_CHAT_ID, "text": text})
            except Exception:
                pass
    else:
        st.info("In ascolto… nessun evento finora.")
        if moralis_enable and MORALIS_API_KEY:
            df_fb = moralis_new_tokens(moralis_exchange, moralis_limit, MORALIS_API_KEY)
            if not df_fb.empty:
                st.markdown("**Ultimi token (HTTP fallback — Moralis)**")
                st.dataframe(df_fb.head(moralis_limit), use_container_width=True,
                             column_config={"Pump.fun": st.column_config.LinkColumn("Pump.fun"),
                                            "Solscan": st.column_config.LinkColumn("Solscan")})
        elif moralis_enable and not MORALIS_API_KEY:
            st.caption("Per il fallback Moralis inserisci una MORALIS_API_KEY (free).")
else:
    st.caption("Attiva “Abilita feed live (subscribeNewToken)” nella sidebar per ascoltare i nuovi token Pump.fun in tempo reale.")

# ---------------- Diagnostica ----------------
st.subheader("Diagnostica")
d1, d2, d3, d4 = st.columns(4)
with d1: st.text(f"Query provider: {len(SEARCH_QUERIES)}  •  HTTP: {codes if codes else '—'}")
with d2: st.text(f"Righe provider (post-filtri provider): {pre_count}")
with d3: st.text(f"Righe dopo watchlist: {post_count}")
with d4: 
    src = 'Birdeye' if (bird_ok and bird_tokens) else 'DexScreener (fallback)'
    st.text(f"Nuove coin source: {src}")
st.caption(f"Refresh: {REFRESH_SEC}s • GetMoni calls: {st.session_state.get('gm_calls',0)} • Ticket proxy: ${PROXY_TICKET:.0f}")

# 🔚 Aggiorna il timestamp dell'ultimo run per il prossimo countdown
st.session_state["last_refresh_ts"] = time.time()
