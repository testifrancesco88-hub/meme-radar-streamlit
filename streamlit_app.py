# streamlit_app.py — Meme Radar (Streamlit) v8.2
# Fix: st.query_params al posto di experimental_set_query_params + filtri provider robusti

import os, time, math, random, datetime
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from market_data import MarketDataProvider  # v8.2

# ---------------- Config ----------------
REFRESH_SEC   = int(os.getenv("REFRESH_SEC", "60"))
PROXY_TICKET  = float(os.getenv("PROXY_TICKET_USD", "150"))
BIRDEYE_URL   = "https://public-api.birdeye.so/defi/tokenlist?chain=solana&sort=createdBlock&order=desc&limit=50"

SEARCH_QUERIES = [
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

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MemeRadar/1.0 Chrome/120 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

st.set_page_config(page_title="Meme Radar — Solana", layout="wide")
st.title("Solana Meme Coin Radar")

# ---------------- Sidebar ----------------
with st.sidebar:
    st.header("Impostazioni")
    auto_refresh = st.toggle("Auto-refresh", value=True)
    disable_all_filters = st.toggle("Disattiva filtri provider (mostra tutto)", value=False)
    only_raydium = st.toggle("Solo Raydium (dexId=raydium)", value=False, disabled=disable_all_filters)
    min_liq = st.number_input("Min liquidity (USD)", min_value=0, value=0, step=1000, disabled=disable_all_filters)
    exclude_quotes = st.multiselect(
        "Escludi quote (stable/major)",
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
    liq_max_sweet = st.number_input("Sweet spot liquidity MAX", min_value=0, value=150000, step=5000)

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

# 🔁 sostituisce experimental_set_query_params
if auto_refresh:
    try:
        st.query_params.update({"_": str(int(time.time() // REFRESH_SEC))})
    except Exception:
        pass

# ---------------- Helpers ----------------
def fetch_with_retry(url, tries=3, base_backoff=0.7, headers=None):
    last = (None, None)
    for i in range(tries):
        try:
            r = requests.get(url, headers=headers or UA_HEADERS, timeout=15)
            code = r.status_code
            if r.ok:
                return r.json(), code
            last = (None, code)
            if code in (429, 500, 502, 503, 504):
                time.sleep(base_backoff*(i+1) + random.uniform(0,0.3))
                continue
            break
        except Exception:
            last = (None, "ERR")
            time.sleep(base_backoff*(i+1) + random.uniform(0,0.3))
    return last

def avg(lst):
    xs = [x for x in lst if x is not None]
    return (sum(xs)/len(xs)) if xs else None

def fmt_int(n):
    return f"{int(round(n)):,}".replace(",", ".") if n is not None else "N/D"

def hours_since_ms(ms):
    if not ms: return None
    try:
        return max(0.0, (time.time() - int(ms)/1000.0) / 3600.0)
    except Exception:
        return None

def ms_to_dt(ms):
    if not ms: return ""
    try:
        return datetime.datetime.utcfromtimestamp(int(ms)/1000).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ms)

def fmt_age(hours):
    if hours is None: return ""
    if hours < 1: return f"{int(round(hours*60))}m"
    if hours < 48:
        h = int(hours); m = int(round((hours - h) * 60))
        return f"{h}h {m}m"
    d = int(hours // 24); h = int(hours % 24)
    return f"{d}d {h}h"

def norm_list(s):
    out = []
    for part in (s or "").replace(" ", "").split(","):
        if not part: continue
        out.append(part.upper() if len(part) <= 8 else part)
    return out

def safe_series_mean(s):
    vals = []
    for x in s:
        try:
            if pd.notna(x):
                vals.append(float(x))
        except Exception:
            pass
    return (sum(vals)/len(vals)) if vals else None

def safe_sort(df, col, ascending=False):
    if df is None or df.empty or col not in df.columns:
        return df
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
    if mn <= liq <= mx: return 1.0
    if liq < mn: return max(0.0, liq / mn)
    return max(0.0, mx / liq)
def score_dex(d): return DEX_WEIGHTS.get((d or "").lower(), 0.6)

def compute_meme_score_row(r, weights, sweet_min, sweet_max):
    base = r.get("baseSymbol","") if hasattr(r, "get") else r["baseSymbol"]
    dex  = r.get("dexId","") if hasattr(r, "get") else r["dexId"]
    liq  = r.get("liquidityUsd", None) if hasattr(r, "get") else r["liquidityUsd"]
    tx1  = r.get("txns1h", 0) if hasattr(r, "get") else r["txns1h"]
    ageh = hours_since_ms(r.get("pairCreatedAt", 0) if hasattr(r, "get") else r["pairCreatedAt"])
    f = (
        weights[0]*score_symbol(base) +
        weights[1]*score_age(ageh) +
        weights[2]*s_sigmoid(tx1) +
        weights[3]*score_liq(liq, sweet_min, sweet_max) +
        weights[4]*score_dex(dex)
    )
    total = max(1e-6, sum(weights))
    return round(100.0 * f / total)

# ---------------- Provider init (una sola volta) ----------------
if "provider" not in st.session_state:
    prov = MarketDataProvider(
        refresh_sec=REFRESH_SEC,
        preserve_on_empty=True
    )
    prov.set_queries(SEARCH_QUERIES)
    st.session_state["provider"] = prov
    prov.start_auto_refresh()

provider: MarketDataProvider = st.session_state["provider"]

# Applica/azzera filtri del provider in base al toggle
if disable_all_filters:
    provider.set_filters(only_raydium=False, min_liq=0, exclude_quotes=[])
else:
    # passa una LIST, non set — il provider gestisce uppercase/flatten
    provider.set_filters(only_raydium=only_raydium, min_liq=min_liq, exclude_quotes=list(exclude_quotes))

# Snapshot dal provider
df_provider, ts = provider.get_snapshot()
codes = provider.get_last_http_codes()

st.caption(f"Aggiornato: {time.strftime('%H:%M:%S', time.localtime(ts))}" if ts else "Aggiornamento in corso…")

# Watchlist
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
    vol24_avg = None
    tx1h_avg = None
else:
    top10 = df_view.sort_values(by=["volume24hUsd"], ascending=False).head(10)
    vol24_avg = safe_series_mean(top10["volume24hUsd"])
    tx1h_avg  = safe_series_mean(top10["txns1h"])

if (not vol24_avg or vol24_avg == 0) and (tx1h_avg and tx1h_avg > 0):
    vol24_avg = tx1h_avg * 24 * PROXY_TICKET

# ---------------- Nuove coin — Birdeye + Fallback sicuro ----------------
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
    new_source = "Birdeye"
else:
    new_source = "DexScreener (fallback)"
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

# ---------------- UI: KPI ----------------
c1, c2, c3, c4 = st.columns(4)
with c1:
    tone = {"ON FIRE":"🟢","MEDIO":"🟡","FIACCO":"🔴","N/D":"⚪️"}.get(score,"")
    st.metric("Score mercato", f"{tone} {score}")
with c2: st.metric("Volume 24h medio Top 10", fmt_int(vol24_avg))
with c3: st.metric("Txns 1h medie Top 10", fmt_int(tx1h_avg))
with c4: st.metric("Nuove coin – Liquidity media", fmt_int(new_liq_avg))

# ---------------- UI: Charts ----------------
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
    else:
        recents = safe_sort(df_provider, "pairCreatedAt", ascending=False)
        if recents is not None and not recents.empty:
            rec_h = recents.head(20)
            df_liq = pd.DataFrame({"Token": rec_h["baseSymbol"], "Liquidity": rec_h["liquidityUsd"].fillna(0)})
            fig2 = px.bar(df_liq, x="Token", y="Liquidity", title="Ultime 20 Nuove Pairs – Liquidity (Dex fallback)")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Nessun token nuovo disponibile (Birdeye 401 o fallback vuoto).")

# ---------------- UI: Tabella con link + Meme Score ----------------
weights = (w_symbol, w_age, w_txns, w_liq, w_dex)

def build_table(df):
    rows = []
    for r in df.to_dict(orient="records"):
        mscore = compute_meme_score_row(r, weights, liq_min_sweet, liq_max_sweet)
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
            "Watch": "✅" if is_watch_hit_row(r) else "",
            "_pairAddress": r.get("pairAddress",""),
        })
    out = pd.DataFrame(rows)
    if not out.empty and sort_by_meme:
        out = out.sort_values(by=["Meme Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False])
    return out

df_pairs = build_table(df_view)

st.markdown("### Pairs (post-filtri)")
if not df_pairs.empty:
    display_cols = [c for c in df_pairs.columns if c not in ["_pairAddress"]]
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

    # Export CSV
    export_cols = [c for c in df_pairs.columns if c not in ["_pairAddress"]]
    if not show_pair_age and "Pair Age" in export_cols:
        export_cols.remove("Pair Age")
    csv_bytes = df_pairs[export_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Scarica CSV filtrato",
        data=csv_bytes,
        file_name="solana-meme-radar.csv",
        mime="text/csv",
        help="Esporta la tabella (filtri e ordinamenti già applicati)"
    )
else:
    st.info("Nessuna pair da mostrare (post-filtri).")

# ---------------- UI: Top 10 per Meme Score ----------------
st.markdown("### Top 10 per Meme Score")
if not df_pairs.empty:
    top10_meme = df_pairs.sort_values(by=["Meme Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False]).head(10)
    fig3 = px.bar(
        top10_meme,
        x="Pair",
        y="Meme Score",
        hover_data=["DEX","Txns 1h","Liquidity (USD)","Volume 24h (USD)","Price (USD)","Change 24h (%)","Created (UTC)","Pair Age"],
        title="Top 10 per Meme Score"
    )
    fig3.update_layout(yaxis_range=[0, 100])
    fig3.update_xaxes(tickangle=-30)
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Nessuna pair disponibile per calcolare il Meme Score.")

# ---------------- Alert Telegram (base) ----------------
if "sent_alerts" not in st.session_state:
    st.session_state["sent_alerts"] = set()

def send_telegram(bot_token, chat_id, text):
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        r = requests.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=10)
        return r.ok, r.status_code
    except Exception:
        return False, "ERR"

def should_alert(row):
    cond = (row["Txns 1h"] >= alert_tx1h_min) and (row["Liquidity (USD)"] >= alert_liq_min)
    if alert_meme_min > 0:
        cond = cond and (row["Meme Score"] >= alert_meme_min)
    return cond

alerts_to_send = []
if enable_alerts and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and not df_pairs.empty:
    for _, r in df_pairs.iterrows():
        if should_alert(r):
            addr = str(r.get("_pairAddress", ""))
            if addr and addr not in st.session_state["sent_alerts"]:
                alerts_to_send.append(r)

    for r in alerts_to_send:
        text = (
            f"🔥 *Meme Radar Trigger*\n"
            f"Pair: {r['Pair']} ({r['DEX']})\n"
            f"Meme Score: {r['Meme Score']}\n"
            f"Txns 1h: {r['Txns 1h']} • Liq: ${r['Liquidity (USD)']:,}\n"
            f"Vol24h: ${r['Volume 24h (USD)']:,}\n"
            f"{r['Link']}"
        )
        ok, code = send_telegram(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, text)
        if ok:
            st.session_state["sent_alerts"].add(str(r.get("_pairAddress","")))
    if alerts_to_send:
        st.success(f"Alert inviati: {len(alerts_to_send)}")
    else:
        st.caption("Nessun nuovo alert da inviare (già notificati o sotto soglia).")

# ---------------- Diagnostica ----------------
st.subheader("Diagnostica")
d1, d2, d3, d4 = st.columns(4)
with d1: st.text(f"Query provider: {len(SEARCH_QUERIES)}  •  HTTP: {codes if codes else '—'}")
with d2: st.text(f"Righe provider (post-filtri provider): {pre_count}")
with d3: st.text(f"Righe dopo watchlist: {post_count}")
with d4: st.text(f"Nuove coin source: {'Birdeye' if (bird_ok and bird_tokens) else 'DexScreener (fallback)'}")
st.caption(f"Refresh: {REFRESH_SEC}s • Ticket proxy: ${PROXY_TICKET:.0f}")
