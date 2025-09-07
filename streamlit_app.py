# streamlit_app.py — Meme Radar (Streamlit) v2: SEARCH primaria + filtri safe + diagnostica estesa
import os
import time
import requests
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- Config ----------------
DS_API_SEARCH    = 'https://api.dexscreener.com/latest/dex/search?q=chain:solana'
DS_API_PAIRS     = 'https://api.dexscreener.com/latest/dex/pairs/solana'  # ora fallback
BE_API           = 'https://public-api.birdeye.so/defi/tokenlist?chain=solana&sort=createdBlock&order=desc&limit=50'

REFRESH_SEC  = int(os.getenv('REFRESH_SEC', '60'))
PROXY_TICKET = float(os.getenv('PROXY_TICKET_USD', '150'))

st.set_page_config(page_title='Meme Radar — Solana', layout='wide')
st.title('Solana Meme Coin Radar')

# ---------------- Sidebar (controlli) ----------------
with st.sidebar:
    st.header("Impostazioni")
    auto_refresh = st.toggle('Auto-refresh', value=True, help='Aggiorna ogni REFRESH_SEC secondi')
    disable_all_filters = st.toggle('Disattiva tutti i filtri', value=True)
    only_raydium = st.toggle('Solo Raydium (dexId=raydium)', value=False, disabled=disable_all_filters)
    min_liq = st.number_input('Min liquidity (USD)', min_value=0, value=0, step=1000, disabled=disable_all_filters)
    exclude_quotes = st.multiselect(
        'Escludi quote (stable/major)',
        options=['USDC','USDT','USDH','SOL','wSOL','stSOL'],
        default=[] if disable_all_filters else ['USDC','USDT']
    , disabled=disable_all_filters)
    st.caption(f"Proxy ticket (USD): {PROXY_TICKET:.0f} • Refresh: {REFRESH_SEC}s")

if auto_refresh:
    st.experimental_set_query_params(_=int(time.time() // REFRESH_SEC))

# ---------------- Helpers ----------------
def fetch_json(url, headers=None, timeout=15):
    try:
        r = requests.get(url, headers=headers or {}, timeout=timeout)
        if r.ok:
            return r.json(), r.status_code
        return None, r.status_code
    except Exception:
        return None, 'ERR'

def is_solana_pair(p):
    return str((p or {}).get('chainId','solana')).lower() == 'solana'

def txns1h(p):
    h1 = ((p or {}).get('txns') or {}).get('h1') or {}
    return (h1.get('buys') or 0) + (h1.get('sells') or 0)

def vol24(p):
    return (((p or {}).get('volume') or {}).get('h24'))

def liq_usd_from_pair(p):
    return (((p or {}).get('liquidity') or {}).get('usd'))

def first_num(*vals):
    for v in vals:
        try:
            n = float(v)
            if pd.isna(n): continue
            return n
        except Exception:
            continue
    return None

def filter_pairs(pairs, only_raydium=False, min_liq=0, exclude_quotes=None):
    if not pairs: return []
    if disable_all_filters:
        return [p for p in pairs if is_solana_pair(p)]
    out = []
    exq = set(map(str.upper, exclude_quotes or []))
    for p in pairs:
        if not is_solana_pair(p): continue
        if only_raydium and (p.get('dexId') or '').lower() != 'raydium': continue
        qsym = ((p.get('quoteToken') or {}).get('symbol') or '').upper()
        if qsym in exq: continue
        liq = liq_usd_from_pair(p)
        if min_liq and (liq is None or liq < min_liq): continue
        out.append(p)
    return out

def avg(lst):
    lst2 = [x for x in lst if x is not None]
    return (sum(lst2)/len(lst2)) if lst2 else None

def fmt_int(n):
    return f"{int(round(n)):,}".replace(',', '.') if n is not None else 'N/D'

# ---------------- Data fetch ----------------
# 1) Pairs principali — ORA: SEARCH come primaria, PAIRS come fallback
search_data, search_code = fetch_json(DS_API_SEARCH)
pairs_search = (search_data or {}).get('pairs', []) if search_data else []

pairs_all = [p for p in (pairs_search or []) if is_solana_pair(p)]
primary_used = "DexScreener SEARCH"

if not pairs_all:
    pairs_data, pairs_code = fetch_json(DS_API_PAIRS)
    pairs_pairs = (pairs_data or {}).get('pairs', []) if pairs_data else []
    pairs_all = [p for p in (pairs_pairs or []) if is_solana_pair(p)]
    primary_used = "DexScreener PAIRS"
    search_code = pairs_code  # per diagnosi unifichiamo nello stesso campo

pre_filter_count = len(pairs_all)
pairs = filter_pairs(pairs_all, only_raydium=only_raydium, min_liq=min_liq, exclude_quotes=exclude_quotes)
post_filter_count = len(pairs)

# 2) Nuove coin — Birdeye se disponibile, altrimenti Dex SEARCH “new pairs”
bird_data, bird_code = fetch_json(BE_API, headers={'accept':'application/json'})
bird_tokens = []
bird_ok = False
if bird_data:
    if isinstance(bird_data.get('data'), dict) and isinstance(bird_data['data'].get('tokens'), list):
        bird_tokens = bird_data['data']['tokens']; bird_ok = True
    elif isinstance(bird_data.get('data'), list):
        bird_tokens = bird_data['data']; bird_ok = True

new_source = "Birdeye"
dex_new_pairs = []
if not bird_ok or bird_code == 401 or len(bird_tokens) == 0:
    new_source = "DexScreener (fallback)"
    dsn_data, dsn_code = fetch_json(DS_API_SEARCH)
    dsn_pairs = (dsn_data or {}).get('pairs', []) if dsn_data else []
    dsn_pairs = [p for p in dsn_pairs if is_solana_pair(p)]
    dsn_pairs.sort(key=lambda p: (p.get('pairCreatedAt') or 0), reverse=True)
    dex_new_pairs = dsn_pairs[:20]

# ---------------- KPI compute ----------------
top = sorted([
    {
        'base': (((p or {}).get('baseToken') or {}).get('symbol')) or '',
        'vol24': first_num(vol24(p)),
        'tx1h': txns1h(p)
    }
    for p in pairs
], key=lambda x: (x['vol24'] or 0), reverse=True)[:10]

vols = [x['vol24'] for x in top if x['vol24'] is not None]
txs  = [x['tx1h']  for x in top if x['tx1h']  is not None]

vol24_avg = avg(vols)
tx1h_avg  = avg(txs)

if (vol24_avg is None or vol24_avg == 0) and (tx1h_avg is not None and tx1h_avg > 0):
    vol24_avg = tx1h_avg * 24 * PROXY_TICKET

# liquidity media “nuove coin”
def liquidity_from_birdeye_token(t):
    return first_num(t.get('liquidity'), t.get('liquidityUsd'), t.get('liquidityUSD'))

def liquidity_from_dex_pair(p):
    return first_num(((p.get('liquidity') or {}).get('usd')))

new_liq_values = []
if new_source == "Birdeye" and bird_tokens:
    for t in bird_tokens[:20]: new_liq_values.append(liquidity_from_birdeye_token(t))
else:
    for p in dex_new_pairs: new_liq_values.append(liquidity_from_dex_pair(p))
new_liq_values = [v for v in new_liq_values if v is not None]
new_liq_avg = avg(new_liq_values)

# Score
score = 'N/D'
if vol24_avg is not None and vol24_avg > 0:
    if vol24_avg > 1_000_000: score = 'ON FIRE'
    elif vol24_avg > 200_000: score = 'MEDIO'
    else: score = 'FIACCO'

# ---------------- Layout ----------------
col1, col2, col3, col4 = st.columns(4)
with col1:
    tone = {'ON FIRE':'🟢','MEDIO':'🟡','FIACCO':'🔴','N/D':'⚪️'}.get(score,'')
    st.metric('Score mercato', f"{tone} {score}")
with col2:
    st.metric('Volume 24h medio Top 10', fmt_int(vol24_avg))
with col3:
    st.metric('Txns 1h medie Top 10', fmt_int(tx1h_avg))
with col4:
    st.metric('Nuove coin – Liquidity media', fmt_int(new_liq_avg))

left, right = st.columns(2)
with left:
    if top:
        df_top = pd.DataFrame({
            'Token': [x['base'] or '' for x in top],
            'Volume 24h': [x['vol24'] or 0 for x in top],
        })
        fig = px.bar(df_top, x='Token', y='Volume 24h', title='Top 10 Volume 24h (post-filtri)')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info('Nessuna coppia disponibile dai servizi DEX con i filtri attuali.')

with right:
    if new_source == "Birdeye" and bird_tokens:
        names, liqs = [], []
        for t in bird_tokens[:20]:
            names.append(t.get('name') or t.get('symbol') or (t.get('mint') or '')[:6])
            liqs.append(liquidity_from_birdeye_token(t) or 0)
        df_liq = pd.DataFrame({'Token': names, 'Liquidity': liqs})
        fig2 = px.bar(df_liq, x='Token', y='Liquidity', title='Ultime 20 Nuove Coin – Liquidity (Birdeye)')
        st.plotly_chart(fig2, use_container_width=True)
    elif new_source != "Birdeye" and dex_new_pairs:
        names, liqs = [], []
        for p in dex_new_pairs:
            names.append(((p.get('baseToken') or {}).get('symbol')) or '')
            liqs.append(liquidity_from_dex_pair(p) or 0)
        df_liq = pd.DataFrame({'Token': names, 'Liquidity': liqs})
        fig2 = px.bar(df_liq, x='Token', y='Liquidity', title='Ultime 20 Nuove Pairs – Liquidity (Dex fallback)')
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info('Nessun token nuovo disponibile (Birdeye 401 e fallback vuoto).')

# ---------------- Diagnostica estesa ----------------
st.subheader('Diagnostica')
d1, d2, d3, d4 = st.columns(4)
with d1:
    st.text(f'Fonte coppie primaria: {primary_used}')
with d2:
    st.text(f'Codice sorgente coppie: {search_code}')
with d3:
    st.text(f'Pairs (pre-filtri): {pre_filter_count}')
with d4:
    st.text(f'Pairs (post-filtri): {post_filter_count}')

st.caption(f'Nuove coin source: {new_source} • Refresh: {REFRESH_SEC}s • Ticket proxy: ${PROXY_TICKET:.0f}')

# Anteprima grezza primi 10 (aiuta debugging quando “0 coppie”)
if pre_filter_count:
    preview_rows = []
    for p in pairs_all[:10]:
        preview_rows.append({
            "base": ((p.get('baseToken') or {}).get('symbol') or ''),
            "quote": ((p.get('quoteToken') or {}).get('symbol') or ''),
            "dexId": (p.get('dexId') or ''),
            "liq.usd": liq_usd_from_pair(p) or 0
        })
    st.markdown("**Anteprima prime 10 coppie (pre-filtri):**")
    st.dataframe(pd.DataFrame(preview_rows))
