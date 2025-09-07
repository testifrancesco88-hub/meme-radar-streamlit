# streamlit_app.py — Meme Radar (Streamlit) v5
# Multi-search DexScreener + Tabella con link + Watchlist + Alert Telegram (base)

import os, time, math, random, datetime
import requests
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- Config ----------------
REFRESH_SEC   = int(os.getenv("REFRESH_SEC", "60"))
PROXY_TICKET  = float(os.getenv("PROXY_TICKET_USD", "150"))

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
DEXSEARCH_BASE = "https://api.dexscreener.com/latest/dex/search?q="
BIRDEYE_URL    = "https://public-api.birdeye.so/defi/tokenlist?chain=solana&sort=createdBlock&order=desc&limit=50"

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MemeRadar/1.0 Chrome/120 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9"
}

st.set_page_config(page_title="Meme Radar — Solana", layout="wide")
st.title("Solana Meme Coin Radar")

# ---------------- Sidebar ----------------
with st.sidebar:
    st.header("Impostazioni")
    auto_refresh = st.toggle("Auto-refresh", value=True)
    disable_all_filters = st.toggle("Disattiva tutti i filtri", value=True)
    only_raydium = st.toggle("Solo Raydium (dexId=raydium)", value=False, disabled=disable_all_filters)
    min_liq = st.number_input("Min liquidity (USD)", min_value=0, value=0, step=1000, disabled=disable_all_filters)
    exclude_quotes = st.multiselect(
        "Escludi quote (stable/major)",
        options=["USDC","USDT","USDH","SOL","wSOL","stSOL"],
        default=[] if disable_all_filters else ["USDC","USDT"],
        disabled=disable_all_filters
    )
    st.caption(f"Proxy ticket (USD): {PROXY_TICKET:.0f} • Refresh: {REFRESH_SEC}s")

    st.divider()
    st.subheader("Watchlist")
    wl_default = os.getenv("WATCHLIST", "")  # es: "WIF,BONK,So11111111111111111111111111111111111111112"
    watchlist_input = st.text_input("Simboli o address (comma-separated)", value=wl_default, help="Esempio: WIF,BONK,So111...,<pairAddress>")
    watchlist_only = st.toggle("Mostra solo watchlist", value=False)

    st.divider()
    st.subheader("Alert Telegram (base)")
    TELEGRAM_BOT_TOKEN = st.text_input("Bot Token", value=os.getenv("TELEGRAM_BOT_TOKEN",""), type="password")
    TELEGRAM_CHAT_ID   = st.text_input("Chat ID", value=os.getenv("TELEGRAM_CHAT_ID",""))
    alert_tx1h_min     = st.number_input("Soglia txns 1h", min_value=0, value=200, step=10)
    alert_liq_min      = st.number_input("Soglia liquidity USD", min_value=0, value=20000, step=1000)
    enable_alerts      = st.toggle("Abilita alert Telegram", value=False)

if auto_refresh:
    st.experimental_set_query_params(_=int(time.time() // REFRESH_SEC))

# ---------------- Helpers ----------------
def fetch_json(url, headers=None, timeout=15):
    try:
        r = requests.get(url, headers=headers or {}, timeout=timeout)
        code = r.status_code
        if r.ok:
            return r.json(), code
        return None, code
    except Exception:
        return None, "ERR"

def fetch_with_retry(url, tries=3, base_backoff=0.7, headers=None):
    last = (None, None)
    for i in range(tries):
        data, code = fetch_json(url, headers=headers or UA_HEADERS, timeout=15)
        last = (data, code)
        if code == 200 and data:
            return data, code
        if code in (429, 500, 502, 503, 504, "ERR"):
            time.sleep(base_backoff*(i+1) + random.uniform(0,0.3))
            continue
        break
    return last

def is_solana_pair(p): 
    return str((p or {}).get("chainId","solana")).lower() == "solana"

def txns1h(p):
    h1 = ((p or {}).get("txns") or {}).get("h1") or {}
    return (h1.get("buys") or 0) + (h1.get("sells") or 0)

def vol24(p):
    return (((p or {}).get("volume") or {}).get("h24"))

def liq_usd_from_pair(p):
    return (((p or {}).get("liquidity") or {}).get("usd"))

def first_num(*vals):
    for v in vals:
        try:
            n = float(v)
            if not math.isnan(n):
                return n
        except Exception:
            pass
    return None

def filter_pairs(pairs, only_raydium=False, min_liq=0, exclude_quotes=None):
    if not pairs: return []
    if disable_all_filters:
        return [p for p in pairs if is_solana_pair(p)]
    out, exq = [], set(map(str.upper, exclude_quotes or []))
    for p in pairs:
        if not is_solana_pair(p): continue
        if only_raydium and (p.get("dexId") or "").lower() != "raydium": continue
        qsym = ((p.get("quoteToken") or {}).get("symbol") or "").upper()
        if qsym in exq: continue
        liq = liq_usd_from_pair(p)
        if min_liq and (liq is None or liq < min_liq): continue
        out.append(p)
    return out

def avg(lst):
    xs = [x for x in lst if x is not None]
    return (sum(xs)/len(xs)) if xs else None

def fmt_int(n):
    return f"{int(round(n)):,}".replace(",", ".") if n is not None else "N/D"

def ms_to_dt(ms):
    if not ms: return ""
    try:
        return datetime.datetime.utcfromtimestamp(int(ms)/1000).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ms)

def norm_list(s):
    """split per virgola/spazi; upper per simboli; mantiene address/lunghi così com’è."""
    out = []
    for part in (s or "").replace(" ", "").split(","):
        if not part: continue
        if len(part) <= 8:
            out.append(part.upper())
        else:
            out.append(part)  # presumibilmente address
    return out

# Telegram
def send_telegram(bot_token, chat_id, text):
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        r = requests.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=10)
        return r.ok, r.status_code
    except Exception:
        return False, "ERR"

if "sent_alerts" not in st.session_state:
    st.session_state["sent_alerts"] = set()  # pairAddress inviati in questa sessione

# ---------------- Multi-search DexScreener ----------------
def run_multi_search(queries):
    all_pairs, seen, http_codes = [], set(), []
    for q in queries:
        url = DEXSEARCH_BASE + requests.utils.quote(q, safe="")
        data, code = fetch_with_retry(url, headers=UA_HEADERS)
        http_codes.append(code)
        pairs = (data or {}).get("pairs", []) if data else []
        for p in pairs:
            if not is_solana_pair(p): 
                continue
            addr = p.get("pairAddress") or p.get("url") or (p.get("baseToken",{}).get("address"))
            key = str(addr)
            if key and key not in seen:
                seen.add(key)
                all_pairs.append(p)
    return all_pairs, http_codes

pairs_all, http_codes = run_multi_search(SEARCH_QUERIES)
pre_filter_count = len(pairs_all)

# ---------------- Watchlist filtering/highlight ----------------
watchlist = norm_list(watchlist_input)
def is_watch_hit(p):
    base = ((p.get("baseToken") or {}).get("symbol") or "")
    quote = ((p.get("quoteToken") or {}).get("symbol") or "")
    addr  = p.get("pairAddress") or ""
    base_addr = (p.get("baseToken") or {}).get("address") or ""
    if not watchlist: return False
    # match per symbol (upper) o address/pairAddress
    if base.upper() in watchlist or quote.upper() in watchlist:
        return True
    if addr in watchlist or base_addr in watchlist:
        return True
    return False

# Applica filtri di mercato
pairs_filtered = filter_pairs(pairs_all, only_raydium=only_raydium, min_liq=min_liq, exclude_quotes=exclude_quotes)
if watchlist_only:
    pairs = [p for p in pairs_filtered if is_watch_hit(p)]
else:
    pairs = pairs_filtered
post_filter_count = len(pairs)

# ---------------- Nuove Coin ----------------
be_headers = {"accept": "application/json"}
be_key = os.getenv("BE_API_KEY","")
if be_key:
    be_headers["x-api-key"] = be_key
bird_data, bird_code = fetch_with_retry(BIRDEYE_URL, headers={**UA_HEADERS, **be_headers})
bird_tokens, bird_ok = [], False
if bird_data and "data" in bird_data:
    if isinstance(bird_data["data"], dict) and isinstance(bird_data["data"].get("tokens"), list):
        bird_tokens = bird_data["data"]["tokens"]; bird_ok = True
    elif isinstance(bird_data["data"], list):
        bird_tokens = bird_data["data"]; bird_ok = True

new_source = "Birdeye"
dex_new_pairs = []
if (not bird_ok) or (bird_code == 401) or (len(bird_tokens) == 0):
    new_source = "DexScreener (fallback)"
    recents = [p for p in pairs_all if is_solana_pair(p)]
    recents.sort(key=lambda p: (p.get("pairCreatedAt") or 0), reverse=True)
    dex_new_pairs = recents[:20]

# ---------------- KPI ----------------
top = sorted([{
        "base": (((p or {}).get("baseToken") or {}).get("symbol")) or "",
        "vol24": first_num(vol24(p)),
        "tx1h": txns1h(p)
    } for p in pairs], key=lambda x: (x["vol24"] or 0), reverse=True)[:10]

vols = [x["vol24"] for x in top if x["vol24"] is not None]
txs  = [x["tx1h"]  for x in top if x["tx1h"]  is not None]

vol24_avg = avg(vols)
tx1h_avg  = avg(txs)
if (vol24_avg is None or vol24_avg == 0) and (tx1h_avg is not None and tx1h_avg > 0):
    vol24_avg = tx1h_avg * 24 * PROXY_TICKET

def liquidity_from_birdeye_token(t):
    return first_num(t.get("liquidity"), t.get("liquidityUsd"), t.get("liquidityUSD"))
def liquidity_from_dex_pair(p):
    return first_num(((p.get("liquidity") or {}).get("usd")))

new_liq_values = []
if new_source == "Birdeye" and bird_tokens:
    for t in bird_tokens[:20]:
        new_liq_values.append(liquidity_from_birdeye_token(t))
else:
    for p in dex_new_pairs:
        new_liq_values.append(liquidity_from_dex_pair(p))
new_liq_values = [v for v in new_liq_values if v is not None]
new_liq_avg = avg(new_liq_values)

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
    if top:
        df_top = pd.DataFrame({"Token":[x["base"] or "" for x in top],
                               "Volume 24h":[x["vol24"] or 0 for x in top]})
        fig = px.bar(df_top, x="Token", y="Volume 24h", title="Top 10 Volume 24h (post-filtri)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Nessuna coppia disponibile con i filtri attuali.")

with right:
    if new_source == "Birdeye" and bird_tokens:
        names, liqs = [], []
        for t in bird_tokens[:20]:
            names.append(t.get("name") or t.get("symbol") or (t.get("mint") or "")[:6])
            liqs.append(liquidity_from_birdeye_token(t) or 0)
        df_liq = pd.DataFrame({"Token": names, "Liquidity": liqs})
        fig2 = px.bar(df_liq, x="Token", y="Liquidity", title="Ultime 20 Nuove Coin – Liquidity (Birdeye)")
        st.plotly_chart(fig2, use_container_width=True)
    elif dex_new_pairs:
        names, liqs = [], []
        for p in dex_new_pairs:
            names.append(((p.get("baseToken") or {}).get("symbol")) or "")
            liqs.append(liquidity_from_dex_pair(p) or 0)
        df_liq = pd.DataFrame({"Token": names, "Liquidity": liqs})
        fig2 = px.bar(df_liq, x="Token", y="Liquidity", title="Ultime 20 Nuove Pairs – Liquidity (Dex fallback)")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Nessun token nuovo disponibile (Birdeye 401 e fallback vuoto).")

# ---------------- UI: Tabella con link ----------------
def row_for_table(p):
    base = ((p.get("baseToken") or {}).get("symbol") or "")
    quote = ((p.get("quoteToken") or {}).get("symbol") or "")
    dex  = (p.get("dexId") or "")
    liq  = liq_usd_from_pair(p) or 0
    tx1  = txns1h(p)
    v24  = first_num(vol24(p)) or 0
    created = p.get("pairCreatedAt") or 0
    url = p.get("url") or ""
    return {
        "Pair": f"{base}/{quote}",
        "DEX": dex,
        "Liquidity (USD)": int(round(liq)),
        "Txns 1h": int(tx1),
        "Volume 24h (USD)": int(round(v24)),
        "Created (UTC)": ms_to_dt(created),
        "Link": url,
        "Watch": "✅" if is_watch_hit(p) else "",
        "_pairAddress": p.get("pairAddress") or "",
    }

table_rows = [row_for_table(p) for p in pairs]
df_pairs = pd.DataFrame(table_rows)

st.markdown("### Pairs (post-filtri)")
if not df_pairs.empty:
    # evidenziazione watchlist con stile semplice
    def highlight_watch(s):
        return ['background-color: #d1fae5' if v == "✅" else '' for v in s]
    st.dataframe(
        df_pairs.drop(columns=["_pairAddress"]),
        use_container_width=True,
        column_config={
            "Link": st.column_config.LinkColumn("Link", help="Apri su DexScreener"),
            "Liquidity (USD)": st.column_config.NumberColumn(format="%,d"),
            "Volume 24h (USD)": st.column_config.NumberColumn(format="%,d"),
        }
    )
else:
    st.info("Nessuna pair da mostrare (post-filtri).")

# ---------------- Alert Telegram (base) ----------------
def should_alert(row):
    return (row["Txns 1h"] >= alert_tx1h_min) and (row["Liquidity (USD)"] >= alert_liq_min)

alerts_to_send = []
if enable_alerts and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and not df_pairs.empty:
    for _, r in df_pairs.iterrows():
        if should_alert(r):
            # dedup per sessione su pairAddress
            addr = str(r.get("_pairAddress", ""))
            if addr and addr not in st.session_state["sent_alerts"]:
                alerts_to_send.append(r)

    for r in alerts_to_send:
        text = (
            f"🔥 *Meme Radar Trigger*\n"
            f"Pair: {r['Pair']} ({r['DEX']})\n"
            f"Txns 1h: {r['Txns 1h']} • Liq: ${r['Liquidity (USD)']:,}\n"
            f"Vol24h: ${r['Volume 24h (USD)']:,}\n"
            f"{r['Link']}"
        )
        ok, code = send_telegram(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, text)
        if ok:
            st.session_state["sent_alerts"].add(str(r.get("_pairAddress","")))
        # feedback minimale in UI
    if alerts_to_send:
        st.success(f"Alert inviati: {len(alerts_to_send)}")
    else:
        st.caption("Nessun nuovo alert da inviare (già notificati o sotto soglia).")

# ---------------- Diagnostica ----------------
st.subheader("Diagnostica")
d1, d2, d3, d4 = st.columns(4)
with d1: st.text(f"Query eseguite: {len(SEARCH_QUERIES)}  •  Codici: {http_codes}")
with d2: st.text(f"Pairs (pre-filtri): {pre_filter_count}")
with d3: st.text(f"Pairs (post-filtri): {post_filter_count}")
with d4: st.text(f"Nuove coin source: {'Birdeye' if bird_tokens else 'DexScreener (fallback)'}")
st.caption(f"Refresh: {REFRESH_SEC}s • Ticket proxy: ${PROXY_TICKET:.0f}")
