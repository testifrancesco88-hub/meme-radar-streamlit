# streamlit_app.py — Meme Radar (Streamlit) v9.2
# Novità v9.2: integrazione GetMoni (SocialSentimentAnalyzer) con filtro su segnali + colonne social.
# Confermati: Bubblemaps anti-cluster, LIVE Pump.fun, fallback Moralis, Meme Score, grafici, watchlist, CSV, alert Telegram.

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
from getmoni_client import SocialSentimentAnalyzer  # <--- NEW

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
    MORALIS_API_KEY = st.text_input("MORALIS_API_KEY", value=os.getenv("MORALIS
