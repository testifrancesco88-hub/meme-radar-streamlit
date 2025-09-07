import os
import time
import json
import requests
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- Config ----------------
DS_API_PRIMARY  = 'https://api.dexscreener.com/latest/dex/pairs/solana'
DS_API_FALLBACK = 'https://api.dexscreener.com/latest/dex/search?q=chain:solana'
BE_API          = 'https://public-api.birdeye.so/defi/tokenlist?chain=solana&sort=createdBlock&order=desc&limit=50'

REFRESH_SEC = int(os.getenv('REFRESH_SEC', '60'))
PROXY_TICKET = float(os.getenv('PROXY_TICKET_USD', '150'))

st.set_page_config(page_title='Meme Radar — Solana', layout='wide')
st.title('Solana Meme Coin Radar')

# auto refresh
st_autorefresh = st.sidebar.toggle('Auto-refresh', value=True, help='Aggiorna automaticamente i dati')
if st_autorefresh:
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
        return None, 'ERR'

def fetch_dex():
    data, code = fetch_json(DS_API_PRIMARY)
    pairs = (data or {}).get('pairs', []) if data else []
    if not pairs:
        data2, code2 = fetch_json(DS_API_FALLBACK)
        pairs2 = (data2 or {}).get('pairs', []) if data2 else []
        # filter solana
        pairs2 = [p for p in pairs2 if str((p or {}).get('chainId','')).lower() == 'solana']
        return {'pairs': pairs2}, code2
    return {'pairs': pairs}, code

def fetch_bird():
    data, code = fetch_json(BE_API, headers={'accept':'application/json'})
    tokens = []
    if data:
        if isinstance(data.get('data'), dict) and isinstance(data['data'].get('tokens'), list):
            tokens = data['data']['tokens']
        elif isinstance(data.get('data'), list):
            tokens = data['data']
    return {'tokens': tokens}, code

def num(x):
    try:
        n = float(x)
        if pd.isna(n):
            return None
        return n
    except Exception:
        return None

def avg(lst):
    lst2 = [x for x in lst if x is not None]
    return sum(lst2)/len(lst2) if lst2 else None

def fmt_int(n):
    return f"{int(round(n)):,}".replace(',', '.') if n is not None else 'N/D'

# ---------------- Data fetch ----------------
dex, dex_code = fetch_dex()
bird, bird_code = fetch_bird()

pairs = dex.get('pairs', [])
tokens = bird.get('tokens', [])

# ---------------- KPI compute ----------------
# Top 10 by volume.h24 (or proxy if missing)
def txns1h(p):
    h1 = ((p or {}).get('txns') or {}).get('h1') or {}
    return (h1.get('buys') or 0) + (h1.get('sells') or 0)

def vol24(p):
    return (((p or {}).get('volume') or {}).get('h24'))

top = sorted([
    {
        'base': (((p or {}).get('baseToken') or {}).get('symbol')) or '',
        'vol24': num(vol24(p)),
        'tx1h': txns1h(p)
    }
    for p in pairs
    if str((p or {}).get('chainId','solana')).lower() == 'solana'
], key=lambda x: (x['vol24'] or 0), reverse=True)[:10]

vols = [x['vol24'] for x in top if x['vol24'] is not None]
txs  = [x['tx1h'] for x in top if x['tx1h'] is not None]

vol24_avg = avg(vols)
tx1h_avg = avg(txs)

# proxy volume if missing
if (vol24_avg is None or vol24_avg == 0) and (tx1h_avg is not None and tx1h_avg > 0):
    vol24_avg = tx1h_avg * 24 * PROXY_TICKET

# new tokens liquidity avg
liq_vals = []
for t in tokens[:20]:
    for key in ['liquidity','liquidityUsd','liquidityUSD']:
        v = num(t.get(key))
        if v is not None:
            liq_vals.append(v)
            break
new_liq_avg = avg(liq_vals)

# Score
score = 'N/D'
if vol24_avg is not None and vol24_avg > 0:
    if vol24_avg > 1_000_000:
        score = 'ON FIRE'
    elif vol24_avg > 200_000:
        score = 'MEDIO'
    else:
        score = 'FIACCO'

# ---------------- Layout ----------------
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric('Score mercato', score)
with col2:
    st.metric('Volume 24h medio Top 10', fmt_int(vol24_avg) if vol24_avg is not None else 'N/D')
with col3:
    st.metric('Txns 1h medie Top 10', fmt_int(tx1h_avg) if tx1h_avg is not None else 'N/D')
with col4:
    st.metric('Nuove coin – Liquidity media', fmt_int(new_liq_avg) if new_liq_avg is not None else 'N/D')

# Charts
left, right = st.columns(2)
with left:
    if top:
        df_top = pd.DataFrame({
            'Token': [x['base'] or '' for x in top],
            'Volume 24h': [x['vol24'] or 0 for x in top],
        })
        fig = px.bar(df_top, x='Token', y='Volume 24h', title='Top 10 Volume 24h (SOL pairs)')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info('Nessuna coppia disponibile dai servizi DEX al momento.')

with right:
    if tokens:
        names = []
        liqs = []
        for t in tokens[:20]:
            names.append(t.get('name') or t.get('symbol') or (t.get('mint') or '')[:6])
            v = None
            for key in ['liquidity','liquidityUsd','liquidityUSD']:
                v = num(t.get(key))
                if v is not None:
                    break
            liqs.append(v or 0)
        df_liq = pd.DataFrame({'Token': names, 'Liquidity': liqs})
        fig2 = px.bar(df_liq, x='Token', y='Liquidity', title='Ultime 20 Nuove Coin – Liquidity')
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info('Nessun token nuovo disponibile da Birdeye (o API limitata).')

# Diagnostics
st.subheader('Diagnostica')
d1, d2, d3 = st.columns(3)
with d1:
    st.text(f'DEX Screener status: {dex_code}')
with d2:
    st.text(f'Birdeye status: {bird_code}')
with d3:
    st.text(f'Pairs ricevute: {len(pairs)}')
st.caption(f'Refresh: {REFRESH_SEC}s • Ticket proxy: ${PROXY_TICKET}')
