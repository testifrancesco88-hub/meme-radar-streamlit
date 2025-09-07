# streamlit_app.py — Meme Radar (Streamlit) v12.4
# - Scanner Solana via MarketDataProvider (DexScreener)
# - KPI, Meme Score, grafici, watchlist
# - Filtro Volume 24h (MIN/MAX)
# - DEX consentiti (multiselect)
# - StrategyMomentumV2 + Diagnostica + Adaptive Relax
# - Trading Paper (risk mgmt, BE lock, trailing, pyramiding)
# - Alert Telegram dalla tabella
# - LIVE Trading (Jupiter): deeplink o autosign (locale)
# - Tabella: Top 10 per Volume 24h + Change 1h + Change 4h con fallback H6 (nested-aware)
# - Start / Stop: pausa globale (strategia, alert, live mirror)
# - Compatibile con Streamlit >= 1.33 (usa st.query_params)

import os, time, math, random, datetime, json, threading, base64
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from market_data import MarketDataProvider
from trading import RiskConfig, StratConfig, TradeEngine  # Strategy V2

# =========================== Jupiter LIVE Connector (inline) ===========================
JUP_QUOTE = "https://quote-api.jup.ag/v6/quote"
JUP_SWAP  = "https://quote-api.jup.ag/v6/swap"
JUP_TOKENS= "https://token.jup.ag/all"
JUP_PRICE = "https://price.jup.ag/v6/price?ids=SOL"

MINT_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
MINT_SOL  = "So11111111111111111111111111111111111111112"  # wSOL

def _jget(url, params=None, timeout=20):
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _jpost(url, data, timeout=20):
    r = requests.post(url, headers={"Content-Type":"application/json"}, data=json.dumps(data), timeout=timeout)
    r.raise_for_status()
    return r.json()

class _TokenRegistry:
    _cache = None
    _by_mint = {}
    _ts = 0

    @classmethod
    def _ensure(cls):
        if cls._cache and time.time()-cls._ts < 3600:
            return
        data = _jget(JUP_TOKENS)
        cls._cache = data
        cls._by_mint = {t["address"]: t for t in data}
        cls._ts = time.time()

    @classmethod
    def decimals(cls, mint: str, default: int = 9) -> int:
        cls._ensure()
        t = cls._by_mint.get(mint)
        return int(t.get("decimals", default)) if t else default

    @classmethod
    def symbol(cls, mint: str, default: str = "?") -> str:
        cls._ensure()
        t = cls._by_mint.get(mint)
        return t.get("symbol", default) if t else default

def get_sol_usd(default=150.0) -> float:
    try:
        j = _jget(JUP_PRICE)
        return float(j["data"]["SOL"]["price"])
    except Exception:
        return float(default)

class LiveConfig:
    def __init__(self, mode: str, slippage_bps: int = 100, rpc_url: str | None = None, public_key: str | None = None, private_key_b58: str | None = None):
        self.mode = mode              # "off" | "deeplink" | "autosign"
        self.slippage_bps = int(slippage_bps)
        self.rpc_url = rpc_url
        self.public_key = public_key
        self.private_key_b58 = private_key_b58

class JupiterConnector:
    def __init__(self, cfg: LiveConfig):
        self.cfg = cfg

    def build_buy(self, quote_mint: str, base_mint: str, amount_quote_usd: float, price_usd_base: float | None):
        if self.cfg.mode == "off":
            return ("off", "Live trading OFF")

        if quote_mint == MINT_USDC:
            dec = _TokenRegistry.decimals(MINT_USDC, 6)
            amount_in = int(round(amount_quote_usd * (10 ** dec)))
            input_mint = MINT_USDC
        elif quote_mint == MINT_SOL:
            sol_usd = get_sol_usd()
            sol_amount = amount_quote_usd / max(0.01, sol_usd)
            dec = 9
            amount_in = int(round(sol_amount * (10 ** dec)))
            input_mint = MINT_SOL
        else:
            dec = _TokenRegistry.decimals(MINT_USDC, 6)
            amount_in = int(round(amount_quote_usd * (10 ** dec)))
            input_mint = MINT_USDC

        if self.cfg.mode == "deeplink":
            human = amount_in / (10 ** dec)
            url = f"https://jup.ag/swap/{_TokenRegistry.symbol(input_mint) or 'USDC'}-{_TokenRegistry.symbol(base_mint)}?amount={human}&slippageBps={self.cfg.slippage_bps}"
            return ("deeplink", url)

        if self.cfg.mode == "autosign":
            return self._autosign_swap(input_mint, base_mint, amount_in, "ExactIn")

        return ("off", "Unsupported mode")

    def build_sell(self, base_mint: str, quote_mint: str, base_amount_tokens: float):
        if self.cfg.mode == "off":
            return ("off", "Live trading OFF")
        dec = _TokenRegistry.decimals(base_mint, 9)
        amount_in = int(round(base_amount_tokens * (10 ** dec)))
        if self.cfg.mode == "deeplink":
            url = f"https://jup.ag/swap/{_TokenRegistry.symbol(base_mint)}-{_TokenRegistry.symbol(quote_mint)}?amount={base_amount_tokens}&slippageBps={self.cfg.slippage_bps}"
            return ("deeplink", url)
        if self.cfg.mode == "autosign":
            return self._autosign_swap(base_mint, quote_mint, amount_in, "ExactIn")
        return ("off", "Unsupported mode")

    def _autosign_swap(self, input_mint: str, output_mint: str, amount_in: int, swap_mode: str):
        if not (self.cfg.rpc_url and self.cfg.public_key and self.cfg.private_key_b58):
            return ("error", "Autosign richiede RPC_URL, PUBLIC_KEY e PRIVATE_KEY")
        q = _jget(JUP_QUOTE, params={
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": amount_in,
            "slippageBps": self.cfg.slippage_bps,
            "swapMode": swap_mode,
            "onlyDirectRoutes": "false",
        })
        s = _jpost(JUP_SWAP, {
            "userPublicKey": self.cfg.public_key,
            "wrapAndUnwrapSol": True,
            "quoteResponse": q,
            "asLegacyTransaction": True,
            "dynamicComputeUnitLimit": True,
            "useSharedAccounts": True,
        })
        tx_b64 = s.get("swapTransaction")
        if not tx_b64:
            return ("error", f"swapTransaction mancante: {s}")

        try:
            from solana.rpc.api import Client
            from solana.keypair import Keypair
            from solders.keypair import Keypair as SKeypair
            from solana.transaction import Transaction
        except Exception as e:
            return ("error", f"Librerie non presenti (solana, solders). Aggiungi a requirements.txt. Dettagli: {e}")

        try:
            client = Client(self.cfg.rpc_url)
            raw = base64.b64decode(tx_b64)
            tx  = Transaction.deserialize(raw)
            try:
                kp = SKeypair.from_base58_string(self.cfg.private_key_b58)
                secret = bytes(kp)
                keypair = Keypair.from_secret_key(secret)
            except Exception:
                keypair = Keypair.from_secret_key(base64.b64decode(self.cfg.private_key_b58))
            tx.sign(keypair)
            sig = client.send_raw_transaction(tx.serialize(), skip_preflight=False).value
            return ("sent", str(sig))
        except Exception as e:
            return ("error", f"Invio fallito: {e}")

# =============================== App Config/UI =================================
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

# Stato esecuzione
if "app_running" not in st.session_state:
    st.session_state["app_running"] = True  # default ON
running = bool(st.session_state.get("app_running", True))
st.markdown(f"**Stato:** {'🟢 Running' if running else '⏸️ Pausa'}")

if "last_refresh_ts" not in st.session_state:
    st.session_state["last_refresh_ts"] = time.time()

# ---------------- Sidebar ----------------
with st.sidebar:
    st.header("Impostazioni")

    # --- Controllo esecuzione (Start / Stop) ---
    st.subheader("Esecuzione")
    col_run1, col_run2 = st.columns(2)
    if col_run1.button("▶️ Start", disabled=st.session_state["app_running"]):
        st.session_state["app_running"] = True
        st.toast("Esecuzione avviata", icon="✅")
    if col_run2.button("⏹ Stop", type="primary", disabled=not st.session_state["app_running"]):
        st.session_state["app_running"] = False
        st.session_state["live_positions"] = {}  # prudente
        st.toast("Esecuzione in pausa", icon="⏸️")
    running = bool(st.session_state["app_running"])

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

    st.divider()
    st.subheader("Watchlist")
    wl_default = os.getenv("WATCHLIST", "")
    watchlist_input = st.text_input("Simboli o address (comma-separated)", value=wl_default,
                                    help="Es: WIF,BONK,So111...,<pairAddress>", key="watchlist_input")
    watchlist_only = st.toggle("Mostra solo watchlist", value=False, key="watchlist_only")

    st.divider()
    st.subheader("Filtro Volume 24h (USD)")
    vol24_min = st.number_input("Volume 24h MIN", min_value=0, value=st.session_state.get("vol24_min", 0), step=10000, key="vol24_min")
    vol24_max = st.number_input("Volume 24h MAX (0 = illimitato)", min_value=0, value=st.session_state.get("vol24_max", 0), step=100000, key="vol24_max")

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

    # --- DEX consentiti ---
    allowed_dex = st.multiselect(
        "DEX consentiti",
        ["raydium", "orca", "meteora", "lifinity"],
        default=st.session_state.get("allowed_dex", ["raydium", "orca", "meteora", "lifinity"])
    )
    st.session_state["allowed_dex"] = allowed_dex

    # --- Tabella ---
    st.divider()
    st.subheader("Tabella")
    show_pair_age = st.toggle("Mostra colonna 'Pair Age' (min/ore)", value=True)
    show_top10_table = st.toggle("Tabella: mostra solo Top 10 per Volume 24h", value=True)
    show_h6_fallback = st.toggle("Fallback: mostra Change H6 se H4 mancante", value=True)

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

    st.divider()
    st.subheader("Trading (Paper)")
    preset = st.selectbox("Preset strategia", ["Prudente","Neutra","Aggressiva"], index=1)
    if st.button("Applica preset"):
        if preset == "Prudente":
            st.session_state.update({"pos_usd":30.0,"max_pos":2,"stop_pct":25,"tp_pct":60,"trail_pct":15,"day_loss":150.0,
                                     "strat_meme":75,"strat_txns":250,"strat_turnover":1.4,"strat_heat_avg":140})
        elif preset == "Neutra":
            st.session_state.update({"pos_usd":50.0,"max_pos":3,"stop_pct":20,"tp_pct":40,"trail_pct":15,"day_loss":200.0,
                                     "strat_meme":65,"strat_txns":200,"strat_turnover":1.2,"strat_heat_avg":120})
        else:
            st.session_state.update({"pos_usd":60.0,"max_pos":4,"stop_pct":18,"tp_pct":35,"trail_pct":12,"day_loss":250.0,
                                     "strat_meme":55,"strat_txns":150,"strat_turnover":1.0,"strat_heat_avg":100})
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
    st.markdown("**Strategia V2 — parametri chiave**")
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

    st.markdown("**Regole anti-duplicato & timing**")
    colx1, colx2, colx3 = st.columns(3)
    with colx1:
        max_per_symbol = st.number_input("Max per simbolo", min_value=1, value=int(st.session_state.get("max_per_symbol", 1)), step=1, key="max_per_symbol")
    with colx2:
        cooldown_min = st.number_input("Cooldown (min)", min_value=0, value=int(st.session_state.get("cooldown_min", 20)), step=5, key="cooldown_min")
    with colx3:
        time_stop_min = st.number_input("Time-stop (min, 0=off)", min_value=0, value=int(st.session_state.get("time_stop_min", 60)), step=5, key="time_stop_min")

    st.markdown("**Break-even lock (difesa profitto)**")
    colbe1, colbe2, colbe3 = st.columns(3)
    with colbe1:
        be_trig = st.slider("Trigger BE (%)", 5, 40, 10, key="be_trig")
    with colbe2:
        be_lock = st.slider("Lock minimo (%)", 0, 20, 2, key="be_lock")
    with colbe3:
        dd_lock = st.slider("Drawdown lock (%)", 2, 30, 6, key="dd_lock")

    colp1, colp2 = st.columns(2)
    with colp1:
        allow_pyr = st.toggle("Abilita pyramiding", value=bool(st.session_state.get("allow_pyr", False)), key="allow_pyr")
    with colp2:
        add_on_pct = st.slider("Trigger add-on (%)", 1, 25, int(st.session_state.get("add_on_pct", 8)), key="add_on_pct",
                               help="Apre un'add-on solo se il prezzo è a favore di almeno questa %")

    st.divider()
    st.subheader("Live Trading (Jupiter)")
    live_mode = st.selectbox("Modalità", ["off","deeplink","autosign"], index=0,
                             help="Deeplink: confermi dal wallet. Autosign: firma e invia (solo su macchina tua, non cloud).")
    slip_bps  = st.number_input("Slippage (bps)", min_value=10, value=100, step=10)
    rpc_url   = st.text_input("RPC URL (solo autosign)", value=os.getenv("SOL_RPC_URL",""))
    pub_key   = st.text_input("WALLET Public Key (solo autosign)", value=os.getenv("WALLET_PUB",""))
    priv_b58  = st.text_input("WALLET Private Key (base58) • NON su cloud", value=os.getenv("WALLET_PRIV",""), type="password")

# 🔁 Auto-refresh (condizionato a Running)
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

def _tick_query_param():
    try:
        st.query_params.update({"_": str(int(time.time() // max(1, REFRESH_SEC)))})
    except Exception:
        pass

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

# ---- Cast sicuri per evitare ValueError su NaN / stringhe
def to_float0(x, default=0.0):
    """Cast robusto a float con fallback su default. Gestisce None, stringhe e NaN."""
    if x is None:
        return default
    try:
        v = float(x)
        if not math.isfinite(v):  # NaN / inf
            return default
        return v
    except (TypeError, ValueError):
        try:
            s = str(x).replace(",", "").strip().replace("%", "")
            v = float(s) if s else default
            return v if math.isfinite(v) else default
        except Exception:
            return default

def to_int0(x, default=0):
    """Cast robusto a int con arrotondamento e gestione NaN."""
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
         local_weights[2]*s_sigmoid(tx1) + local_weights[3]*score_liq( (liq or 0.0), liq_min_sweet, liq_max_sweet) +
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

# Watchlist & Volume filter pipeline
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

# Applica watchlist_only
if st.session_state.get("watchlist_only", False) and not df_view.empty:
    mask = df_view.apply(is_watch_hit_row, axis=1)
    df_view = df_view[mask].reset_index(drop=True)
post_watch_count = len(df_view)

# Applica filtro Volume 24h
vmin = int(st.session_state.get("vol24_min", 0))
vmax = int(st.session_state.get("vol24_max", 0))
if not df_view.empty:
    vol_series = pd.to_numeric(df_view["volume24hUsd"], errors="coerce").fillna(0)
    mask_vol = (vol_series >= vmin) & ((vol_series <= vmax) if vmax > 0 else True)
    df_view = df_view[mask_vol].reset_index(drop=True)
post_vol_count = len(df_view)

# ---------------- KPI base (calcolati prima del filtro volume per stabilità) ----------------
if df_provider.empty:
    vol24_avg = None; tx1h_avg = None
else:
    top10 = df_provider.sort_values(by=["volume24hUsd"], ascending=False).head(10)
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

# ---------------- Charts (post volume filter) ----------------
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

# ---------------- Tabella pairs con Meme Score (nested-aware + cast sicuri) ----------------
def _first_or_none(d, keys):
    # Cerca al livello piatto
    for k in keys:
        try:
            v = d.get(k) if hasattr(d, "get") else d[k]
            if v is not None:
                return v
        except Exception:
            pass
    return None

def _to_float_pct(x):
    # '12.34' o '12.34%' -> 12.34; altrimenti None
    if x is None:
        return None
    try:
        s = str(x).replace("%", "").strip()
        return float(s) if s != "" else None
    except Exception:
        return None

def _get_change_pct_from_nested(r, nested_key, candidates):
    # Cerca dentro un oggetto annidato (es. r['priceChange'])
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
    # 1) flat; 2) nested priceChange[h1/h4/h6/…]
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

        # Change 1h: flat -> nested priceChange['h1'/'1h'/'m60'/...]
        chg_1h = _get_change_pct(
            r,
            flat_keys=["priceChange1hPct","priceChangeH1Pct","pc1h","priceChange1h"],
            nested_key="priceChange",
            nested_candidates=("h1","1h","m60","60m")
        )

        # Change 4h con fallback a H6 se mancante
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

st.markdown("### Pairs (post-filtri)")
if not df_pairs.empty:
    # Vista "Top 10 per Volume 24h" SOLO per la resa tabellare se richiesto
    df_pairs_for_view = df_pairs.sort_values(by=["Volume 24h (USD)"], ascending=False).head(10) if show_top10_table else df_pairs

    display_cols = [c for c in df_pairs_for_view.columns if c not in ("baseSymbol","quoteSymbol")]
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
    cap = "Top 10 per Volume 24h." if show_top10_table else "Tutte le coppie post-filtri."
    cap += "  (Se H4 mancante, mostrata H6)" if show_h6_fallback else ""
    st.caption(cap)

    # Diagnostica popolamento Change
    try:
        n1 = pd.to_numeric(df_pairs_for_view["Change 1h (%)"], errors="coerce").notna().sum()
        n4 = pd.to_numeric(df_pairs_for_view["Change 4h/6h (%)"], errors="coerce").notna().sum()
        st.caption(f"Diagnostica Change: 1h valorizzati {n1}/{len(df_pairs_for_view)} • 4h/6h valorizzati {n4}/{len(df_pairs_for_view)}")
    except Exception:
        pass
else:
    st.caption("Nessuna coppia disponibile con i filtri attuali.")

# ---------------- Diagnostica Strategia ----------------
def market_heat_value(df: pd.DataFrame, topN: int) -> float:
    if df is None or df.empty: 
        return 0.0
    top = df.sort_values(by=["Volume 24h (USD)"], ascending=False).head(max(1, int(topN)))
    return float(pd.to_numeric(top["Txns 1h"], errors="coerce").fillna(0).mean())

st.markdown("### Diagnostica strategia")
if df_pairs is None or df_pairs.empty:
    st.caption("Nessuna coppia post-filtri provider/watchlist/volume.")
else:
    heat_val = market_heat_value(df_pairs, int(st.session_state.get("heat_topN", 10)))
    heat_thr = float(st.session_state.get("strat_heat_avg", 120))
    st.caption(f"Market heat (media **Txns1h** top {int(st.session_state.get('heat_topN', 10))} per **Volume 24h**): "
               f"**{int(heat_val)}** vs soglia **{int(heat_thr)}** → "
               f"{'OK ✅' if heat_val >= heat_thr else 'BLOCCO ⛔️'}")

    s = df_pairs.copy()
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

# === Adaptive Relax ============================================================
def _count_candidates(df: pd.DataFrame, cfg) -> int:
    if df is None or df.empty: 
        return 0
    def _reasons(row):
        reasons = []
        try:
            liq = float(row.get("Liquidity (USD)", 0) or 0)
            vol = float(row.get("Volume 24h (USD)", 0) or 0)
            tx1 = int(row.get("Txns 1h", 0) or 0)
            meme = int(row.get("Meme Score", 0) or 0)
            chg = row.get("Change 24h (%)", None); chg = float(chg) if chg is not None else 0.0
            dex = str(row.get("DEX", "")).lower()
        except Exception:
            return ["parse"]
        if not (float(cfg.liq_min) <= liq <= float(cfg.liq_max)): reasons.append("liq")
        if cfg.allow_dex and len(cfg.allow_dex) > 0 and dex not in cfg.allow_dex: reasons.append("dex")
        if meme < int(cfg.meme_score_min): reasons.append("meme")
        if tx1 < int(cfg.txns1h_min): reasons.append("tx1h")
        turnover = (vol / max(1.0, liq)) if liq > 0 else 0.0
        if turnover < float(cfg.turnover_min): reasons.append("turnover")
        if chg < float(cfg.chg24_min) or chg > float(cfg.chg24_max): reasons.append("chg24")
        return reasons
    return sum(1 for _, r in df.iterrows() if len(_reasons(r)) == 0)

def relax_strategy_if_empty(df: pd.DataFrame, cfg):
    allowed = tuple(st.session_state.get("allowed_dex", ["raydium","orca","meteora","lifinity"]))
    base = dict(
        allow_dex=allowed,
        heat_tx1h_topN=cfg.heat_tx1h_topN,
        heat_tx1h_avg_min=cfg.heat_tx1h_avg_min,
    )
    steps = [
        cfg,
        StratConfig(**base, meme_score_min=max(0, int(cfg.meme_score_min) - 10),
                    txns1h_min=int(cfg.txns1h_min), liq_min=float(cfg.liq_min), liq_max=float(cfg.liq_max),
                    turnover_min=float(cfg.turnover_min), chg24_min=float(cfg.chg24_min), chg24_max=float(cfg.chg24_max)),
        StratConfig(**base, meme_score_min=int(cfg.meme_score_min),
                    txns1h_min=max(0, int(cfg.txns1h_min * 0.8)), liq_min=float(cfg.liq_min), liq_max=float(cfg.liq_max),
                    turnover_min=float(cfg.turnover_min), chg24_min=float(cfg.chg24_min), chg24_max=float(cfg.chg24_max)),
        StratConfig(**base, meme_score_min=int(cfg.meme_score_min),
                    txns1h_min=int(cfg.txns1h_min), liq_min=float(cfg.liq_min), liq_max=float(cfg.liq_max),
                    turnover_min=max(0.0, float(cfg.turnover_min) * 0.8), chg24_min=float(cfg.chg24_min), chg24_max=float(cfg.chg24_max)),
        StratConfig(**base, meme_score_min=int(cfg.meme_score_min),
                    txns1h_min=int(cfg.txns1h_min), liq_min=max(0.0, float(cfg.liq_min) * 0.5), liq_max=float(cfg.liq_max) * 1.5,
                    turnover_min=float(cfg.turnover_min), chg24_min=float(cfg.chg24_min), chg24_max=float(cfg.chg24_max)),
        StratConfig(**base, meme_score_min=int(cfg.meme_score_min),
                    txns1h_min=int(cfg.txns1h_min), liq_min=float(cfg.liq_min), liq_max=float(cfg.liq_max),
                    turnover_min=float(cfg.turnover_min), chg24_min=float(cfg.chg24_min) - 10, chg24_max=float(cfg.chg24_max) + 50),
    ]
    chosen = steps[0]
    for i, c in enumerate(steps):
        if _count_candidates(df, c) > 0:
            chosen = c
            if i > 0: st.info(f"Adaptive relax attivo: step {i} (soglie alleggerite).")
            break
    return chosen

# ---------------- Trading (Paper) ----------------
st.markdown("## 🧪 Trading — Paper Mode")

r_cfg = RiskConfig(
    position_usd=float(st.session_state.get("pos_usd",50.0)),
    max_positions=int(st.session_state.get("max_pos",3)),
    stop_loss_pct=float(st.session_state.get("stop_pct",20))/100.0,
    take_profit_pct=float(st.session_state.get("tp_pct",40))/100.0,
    trailing_pct=float(st.session_state.get("trail_pct",15))/100.0,
    daily_loss_limit_usd=float(st.session_state.get("day_loss",200.0)),
    max_positions_per_symbol=int(st.session_state.get("max_per_symbol",1)),
    symbol_cooldown_min=int(st.session_state.get("cooldown_min",20)),
    allow_pyramiding=bool(st.session_state.get("allow_pyr", False)),
    pyramid_add_on_trigger_pct=float(st.session_state.get("add_on_pct",8))/100.0,
    time_stop_min=int(st.session_state.get("time_stop_min",60)),
    be_trigger_pct=float(st.session_state.get("be_trig",10))/100.0,
    be_lock_profit_pct=float(st.session_state.get("be_lock",2))/100.0,
    dd_lock_pct=float(st.session_state.get("dd_lock",6))/100.0,
)

s_cfg = StratConfig(
    meme_score_min=int(st.session_state.get("strat_meme",70)),
    txns1h_min=int(st.session_state.get("strat_txns",250)),
    liq_min=float(liq_min_sweet),
    liq_max=float(liq_max_sweet),
    turnover_min=float(st.session_state.get("strat_turnover",1.2)),
    chg24_min=float(st.session_state.get("chg_min",-8)),
    chg24_max=float(st.session_state.get("chg_max",180)),
    allow_dex=tuple(st.session_state.get("allowed_dex", ["raydium","orca","meteora","lifinity"])),
    heat_tx1h_topN=int(st.session_state.get("heat_topN",10)),
    heat_tx1h_avg_min=float(st.session_state.get("strat_heat_avg",120)),
)

active_s_cfg = s_cfg
if df_pairs is not None and not df_pairs.empty:
    active_s_cfg = relax_strategy_if_empty(df_pairs, s_cfg)

if "trade_engine" not in st.session_state or st.session_state["trade_engine"] is None:
    st.session_state["trade_engine"] = TradeEngine(r_cfg, active_s_cfg, bm_check=None, sent_check=None)
else:
    eng_tmp = st.session_state["trade_engine"]
    eng_tmp.risk.cfg = r_cfg
    eng_tmp.strategy.cfg = active_s_cfg
    eng_tmp.bm_check = None
    eng_tmp.sent_check = None

eng = st.session_state["trade_engine"]

# In pausa: non eseguire step
if df_pairs is None or df_pairs.empty:
    st.info("Nessun dato per la strategia al momento.")
    df_signals = pd.DataFrame(); df_open = pd.DataFrame(); df_closed = pd.DataFrame()
else:
    if running:
        df_signals, df_open, df_closed = eng.step(df_pairs)
    else:
        df_signals = pd.DataFrame(); df_open = pd.DataFrame(); df_closed = pd.DataFrame()
        st.info("Strategia in pausa — premi ▶️ Start per riattivare.")

st.markdown("**Segnali (candidati all'ingresso)**")
if not df_signals.empty: st.dataframe(df_signals.head(12), use_container_width=True)
else: st.caption("Nessun segnale valido con i parametri attuali.")

st.markdown("**Posizioni aperte (Paper)**")
if not df_open.empty:
    cols = st.columns([3,2,2,2,2,2,2])
    cols[0].write("Pair"); cols[1].write("Entry"); cols[2].write("Last"); cols[3].write("PnL $ (aperto)"); cols[4].write("PnL % (aperto)"); cols[5].write("Aperta da"); cols[6].write("Azioni")
    for _, r in df_open.iterrows():
        c = st.columns([3,2,2,2,2,2,2])
        c[0].write(f"{r['symbol']} ({r['label']})")
        c[1].write(f"{r['entry']:.8f}"); c[2].write(f"{r['last']:.8f}")
        c[3].write(f"{r['pnl_usd']:.2f}"); c[4].write(f"{r['pnl_pct']*100:.2f}%"); c[5].write(r['opened_ago'])
        if c[6].button("Chiudi", key=f"close_{r['id']}"):
            try: px = float(r["last"]); eng.close_by_id(str(r["id"]), px)
            except Exception: pass
    st.caption(f"Posizioni aperte: {len(df_open)} / max {st.session_state['max_pos']}")
else:
    st.caption("Nessuna posizione aperta.")

st.markdown("**Ultime chiusure**")
if not df_closed.empty: st.dataframe(df_closed, use_container_width=True)
else: st.caption("Nessuna chiusura registrata (ancora).")

st.markdown("**Performance (Paper)**")
open_pnl = float(pd.to_numeric(df_open["pnl_usd"], errors="coerce").fillna(0).sum()) if not df_open.empty else 0.0
realized = float(eng.risk.state.daily_pnl) if eng else 0.0
total_today = open_pnl + realized
m1, m2, m3 = st.columns(3)
with m1: st.metric("PnL Aperto (Unrealized)", f"{open_pnl:.2f} USD")
with m2: st.metric("PnL Giornaliero (Realizzato)", f"{realized:.2f} USD")
with m3: st.metric("Totale Oggi", f"{total_today:.2f} USD")

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
if running and enable_alerts and (df_pairs is not None) and not df_pairs.empty and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    try:
        df_alert = df_pairs.copy()
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

# ---------------- LIVE MIRROR (Jupiter) ----------------
st.markdown("#### Live Mirror")
if "live_positions" not in st.session_state:
    st.session_state["live_positions"] = {}  # id_trade -> dict(mints, qty_base, last_sig/url)

live_cfg = LiveConfig(mode=live_mode, slippage_bps=int(slip_bps),
                      rpc_url=rpc_url or None, public_key=pub_key or None, private_key_b58=priv_b58 or None)
jup = JupiterConnector(live_cfg)

def find_mints_for_symbol(symbol: str) -> tuple[str | None, str | None]:
    try:
        base_sym, quote_sym = (symbol or "").split("/", 1)
    except Exception:
        return (None, None)
    hit = None
    try:
        dfh = df_view[(df_view["baseSymbol"]==base_sym) & (df_view["quoteSymbol"]==quote_sym)]
        if not dfh.empty: hit = dfh.iloc[0]
    except Exception: pass
    base_mint = hit.get("baseAddress", None) if hit is not None else None
    quote_mint = hit.get("quoteAddress", None) if hit is not None else None
    if not quote_mint:
        qs = quote_sym.upper()
        quote_mint = MINT_USDC if qs in ("USDC","USDT") else (MINT_SOL if qs in ("SOL","WSOL") else MINT_USDC)
    return (base_mint, quote_mint)

if not running:
    st.caption("Live: app in pausa — nessuna operazione verrà replicata finché non riattivi ▶️ Start.")
else:
    live_log = []

    curr_ids = set()
    if not df_open.empty and "id" in df_open.columns:
        curr_ids = set(str(x) for x in df_open["id"].tolist())
    prev_ids = set(st.session_state["live_positions"].keys())

    # APERTURE
    new_ids = curr_ids - prev_ids
    for tid in list(new_ids):
        try:
            r = df_open[df_open["id"].astype(str) == tid].iloc[0]
            symbol = r.get("symbol","")
            base_mint, quote_mint = find_mints_for_symbol(symbol)
            if not base_mint:
                live_log.append(f"BUY {symbol} → mint base non trovato")
                continue
            size_usd = float(st.session_state.get("pos_usd", 50.0))
            px_usd   = float(r.get("entry") or r.get("last") or 0.0)
            mode, payload = jup.build_buy(quote_mint, base_mint, size_usd, px_usd)
            qty_base_est = size_usd / max(px_usd, 1e-9) if px_usd else 0.0
            st.session_state["live_positions"][tid] = {"symbol":symbol, "base_mint": base_mint, "quote_mint": quote_mint,
                                                       "qty_base": qty_base_est, "last_sig": None, "last_url": None}
            if mode == "deeplink":
                st.session_state["live_positions"][tid]["last_url"] = payload
                live_log.append(f"BUY {symbol} → [Jupiter link]({payload})")
            elif mode == "sent":
                st.session_state["live_positions"][tid]["last_sig"] = payload
                live_log.append(f"BUY {symbol} → tx: `{payload}`")
            else:
                live_log.append(f"BUY {symbol} → {payload}")
        except Exception as e:
            live_log.append(f"BUY error: {e}")

    # CHIUSURE
    closed_ids = prev_ids - curr_ids
    for tid in list(closed_ids):
        info = st.session_state["live_positions"].get(tid)
        if not info: 
            continue
        try:
            symbol = info.get("symbol","?")
            base_mint = info["base_mint"]; quote_mint = info["quote_mint"]; qty_base = float(info.get("qty_base", 0))
            mode, payload = jup.build_sell(base_mint, quote_mint, qty_base)
            if mode == "deeplink":
                live_log.append(f"SELL {symbol} → [Jupiter link]({payload})")
            elif mode == "sent":
                live_log.append(f"SELL {symbol} → tx: `{payload}`")
            else:
                live_log.append(f"SELL {symbol} → {payload}")
        except Exception as e:
            live_log.append(f"SELL error: {e}")
        finally:
            st.session_state["live_positions"].pop(tid, None)

    if live_cfg.mode == "off":
        st.caption("Live: OFF")
    elif not live_log:
        st.caption("Live: nessuna operazione da replicare in questo tick.")
    else:
        for line in live_log: st.write("• " + line)

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
st.caption(f"Stato esecuzione: {'🟢 Running' if running else '⏸️ Pausa'} • Refresh: {REFRESH_SEC}s • TG alerts (run): {tg_sent_now} • Ticket proxy: ${PROXY_TICKET:.0f}")

st.session_state["last_refresh_ts"] = time.time()
