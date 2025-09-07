# trading.py — motore di paper trading + strategia Momentum V2 + break-even lock
from __future__ import annotations
import time, math, uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ---------------- Risk Manager ----------------
@dataclass
class RiskConfig:
    position_usd: float = 50.0
    max_positions: int = 3
    stop_loss_pct: float = 0.20
    take_profit_pct: float = 0.40
    trailing_pct: float = 0.15
    daily_loss_limit_usd: float = 200.0
    slippage_pct: float = 0.01
    # anti-duplicati & timing
    max_positions_per_symbol: int = 1
    symbol_cooldown_min: int = 20
    allow_pyramiding: bool = False
    pyramid_add_on_trigger_pct: float = 0.08
    time_stop_min: int = 60
    # --- NEW: Break-even lock (difesa utile del profitto) ---
    be_trigger_pct: float = 0.10       # quando PnL% >= 10% abilita il lock
    be_lock_profit_pct: float = 0.02   # non mollare sotto +2% complessivi
    dd_lock_pct: float = 0.06          # se drawdown dal max >=6% mentre BE è attivo -> chiudi

@dataclass
class RiskState:
    daily_pnl: float = 0.0
    last_reset_day: int = 0

class RiskManager:
    def __init__(self, cfg: RiskConfig):
        self.cfg = cfg
        self.state = RiskState(last_reset_day=self._day_idx())

    def _day_idx(self) -> int:
        return int(time.time() // 86400)

    def maybe_reset_day(self):
        today = self._day_idx()
        if today != self.state.last_reset_day:
            self.state.daily_pnl = 0.0
            self.state.last_reset_day = today

    def can_open(self, open_positions: int) -> bool:
        self.maybe_reset_day()
        if open_positions >= self.cfg.max_positions:
            return False
        if self.state.daily_pnl <= -abs(self.cfg.daily_loss_limit_usd):
            return False
        return True

    def apply_fill_slippage(self, price: float, side: str) -> float:
        slip = self.cfg.slippage_pct
        if side.lower() == "buy":
            return price * (1 + slip)
        else:
            return price * (1 - slip)

    def update_daily_pnl(self, delta: float):
        self.maybe_reset_day()
        self.state.daily_pnl += float(delta)

# ---------------- Paper Broker ----------------
@dataclass
class Position:
    id: str
    symbol: str
    label: str
    entry_price: float
    qty: float
    side: str = "long"
    opened_ts: float = field(default_factory=lambda: time.time())
    max_price_seen: float = 0.0
    be_armed: bool = False  # NEW: break-even lock abilitato?

class PaperBroker:
    def __init__(self, risk: RiskManager):
        self.risk = risk
        self.positions: Dict[str, Position] = {}
        self.closed: List[Dict] = []

    def _mk_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def open_long(self, symbol: str, label: str, price: float, usd_amount: float) -> Optional[Position]:
        if price <= 0: return None
        if not self.risk.can_open(len(self.positions)): return None
        fill = self.risk.apply_fill_slippage(price, "buy")
        qty = usd_amount / fill
        pos = Position(id=self._mk_id(), symbol=symbol, label=label, entry_price=fill, qty=qty, side="long")
        pos.max_price_seen = fill
        self.positions[pos.id] = pos
        return pos

    def mark_to_market(self, prices: Dict[str, float]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        rows = []
        to_close = []
        for pos in list(self.positions.values()):
            px = float(prices.get(pos.symbol, 0) or 0)
            if px <= 0:
                rows.append(self._row(pos, px, 0.0))
                continue

            if px > pos.max_price_seen:
                pos.max_price_seen = px

            pnl = (px - pos.entry_price) * pos.qty
            pnl_pct = (px / pos.entry_price - 1.0) if pos.entry_price > 0 else 0.0
            rows.append(self._row(pos, px, pnl))

            # Time-stop (se attivo) chiude posizioni in perdita oltre X minuti
            age_sec = time.time() - pos.opened_ts
            if self.risk.cfg.time_stop_min and age_sec >= self.risk.cfg.time_stop_min * 60 and pnl <= 0:
                to_close.append((pos, px, "TIME"))
                continue

            # Break-even lock (nuovo): dopo un certo guadagno, non concedere più di X drawdown e non scendere sotto +Y%
            if pnl_pct >= abs(self.risk.cfg.be_trigger_pct):
                pos.be_armed = True
            if pos.be_armed and pos.max_price_seen > 0:
                drawdown = 1.0 - (px / pos.max_price_seen)
                if (drawdown >= abs(self.risk.cfg.dd_lock_pct)) and (pnl_pct <= abs(self.risk.cfg.be_lock_profit_pct)):
                    to_close.append((pos, px, "BE_LOCK"))
                    continue

            # SL / TP canonici
            if pnl_pct <= -abs(self.risk.cfg.stop_loss_pct):
                to_close.append((pos, px, "SL")); continue
            if pnl_pct >= abs(self.risk.cfg.take_profit_pct):
                to_close.append((pos, px, "TP")); continue

            # Trailing stop classico
            if pos.max_price_seen > 0:
                drawdown = 1 - (px / pos.max_price_seen)
                if drawdown >= abs(self.risk.cfg.trailing_pct):
                    to_close.append((pos, px, "TRAIL"))

        for pos, px, reason in to_close:
            self.close(pos.id, px, reason)

        df_open = pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
            "id","symbol","label","entry","last","pnl_usd","pnl_pct","opened_ago"
        ])
        df_closed = pd.DataFrame(self.closed[-50:]) if self.closed else pd.DataFrame(columns=[
            "closed_ts","symbol","label","exit","pnl_usd","reason"
        ])
        return df_open, df_closed

    def _row(self, pos: Position, last: float, pnl: float) -> Dict:
        ago = int(time.time() - pos.opened_ts)
        return {
            "id": pos.id,
            "symbol": pos.symbol,
            "label": pos.label,
            "entry": pos.entry_price,
            "last": last,
            "pnl_usd": pnl,
            "pnl_pct": (last/pos.entry_price - 1.0) if pos.entry_price > 0 and last>0 else 0.0,
            "opened_ago": f"{ago//60}m {ago%60}s",
        }

    def close(self, pos_id: str, price: float, reason: str = "MANUAL"):
        pos = self.positions.pop(pos_id, None)
        if not pos: return
        fill = self.risk.apply_fill_slippage(price, "sell") if price>0 else pos.entry_price
        pnl = (fill - pos.entry_price) * pos.qty
        self.risk.update_daily_pnl(pnl)
        self.closed.append({
            "closed_ts": time.strftime("%H:%M:%S"),
            "symbol": pos.symbol,
            "label": pos.label,
            "exit": fill,
            "pnl_usd": pnl,
            "reason": reason,
        })

# ---------------- Strategia ----------------
@dataclass
class StratConfig:
    meme_score_min: int = 70
    txns1h_min: int = 250
    liq_min: float = 8000.0
    liq_max: float = 250000.0
    turnover_min: float = 1.2            # NEW: Volume24h/Liquidity minimo
    chg24_min: float = -8.0              # NEW: evitare dump profondi
    chg24_max: float = 180.0             # NEW: evitare eccesso di estensione
    allow_dex: Tuple[str,...] = ("raydium","orca","meteora")
    heat_tx1h_topN: int = 10             # NEW: mercato "heat" su topN per volume
    heat_tx1h_avg_min: int = 120         # NEW: media Txns1h minima su topN

class StrategyMomentumV2:
    """
    Regole d'ingresso:
      - Meme Score >= soglia
      - Txns1h >= soglia
      - Liquidity tra [liq_min, liq_max]
      - Turnover (Vol24h/Liq) >= turnover_min
      - Change24h entro [chg24_min, chg24_max]
      - DEX whitelisted
    """
    def __init__(self, cfg: StratConfig):
        self.cfg = cfg

    def signal_long(self, row: dict) -> bool:
        try:
            meme = int(row.get("Meme Score", 0) or 0)
            tx1h = int(row.get("Txns 1h", 0) or 0)
            liq  = float(row.get("Liquidity (USD)", 0) or 0.0)
            vol  = float(row.get("Volume 24h (USD)", 0) or 0.0)
            dex  = str(row.get("DEX","")).lower()
            chg  = row.get("Change 24h (%)")
            chg  = float(chg) if chg is not None else 0.0
        except Exception:
            return False

        if not (self.cfg.liq_min <= liq <= self.cfg.liq_max):
            return False
        if self.cfg.allow_dex and dex not in self.cfg.allow_dex:
            return False
        if meme < self.cfg.meme_score_min:
            return False
        if tx1h < self.cfg.txns1h_min:
            return False

        turnover = (vol / max(1.0, liq)) if liq > 0 else 0.0
        if turnover < self.cfg.turnover_min:
            return False

        if chg < self.cfg.chg24_min or chg > self.cfg.chg24_max:
            return False

        return True

# ---------------- Trade Engine ----------------
class TradeEngine:
    """
    Collega strategia + broker + risk.
    Supporta:
      - bm_check(token_address) -> dict    # Bubblemaps anti-cluster
      - sent_check(symbol, base_addr, base_symbol) -> dict  # GetMoni social
    """
    def __init__(self, risk_cfg: RiskConfig, strat_cfg: StratConfig, bm_check=None, sent_check=None):
        self.risk = RiskManager(risk_cfg)
        self.broker = PaperBroker(self.risk)
        self.strategy = StrategyMomentumV2(strat_cfg)
        self.bm_check = bm_check
        self.sent_check = sent_check
        self.last_entry_by_symbol: Dict[str, float] = {}

    def _market_heat_ok(self, df_pairs: pd.DataFrame) -> bool:
        """
        Usa la media Txns1h dei top N per volume come proxy del momentum di mercato.
        """
        try:
            N = max(3, int(self.strategy.cfg.heat_tx1h_topN))
            top = df_pairs.sort_values(by=["Volume 24h (USD)"], ascending=False).head(N)
            if top.empty: return False
            avg_tx = float(pd.to_numeric(top["Txns 1h"], errors="coerce").fillna(0).mean())
            return avg_tx >= float(self.strategy.cfg.heat_tx1h_avg_min)
        except Exception:
            return True  # in caso di errore non bloccare
    def step(self, df_pairs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        prices: Dict[str, float] = {}
        candidates: List[Dict] = []

        # 0) filtro di mercato (heat)
        if not self._market_heat_ok(df_pairs):
            # aggiorna comunque marcatura PnL / trailing / stop
            for r in df_pairs.to_dict(orient="records"):
                prices[r.get("Pair","")] = float(r.get("Price (USD)") or 0.0)
            df_open, df_closed = self.broker.mark_to_market(prices)
            return pd.DataFrame(columns=["Pair","DEX","Score","Txns 1h","Liquidity (USD)","Price (USD)","Link","Base Address","BM Risk","Moni User","Mentions 24h","Smarts","Sentiment","Moni Score"]), df_open, df_closed

        # 1) scan segnali (tecnici)
        for r in df_pairs.to_dict(orient="records"):
            sym = r.get("Pair","")
            last = float(r.get("Price (USD)") or 0.0)
            prices[sym] = last
            if self.strategy.signal_long(r):
                candidates.append({
                    "Pair": sym,
                    "DEX": r.get("DEX",""),
                    "Score": int(r.get("Meme Score", 0) or 0),
                    "Txns 1h": int(r.get("Txns 1h", 0) or 0),
                    "Liquidity (USD)": int(r.get("Liquidity (USD)", 0) or 0),
                    "Volume 24h (USD)": int(r.get("Volume 24h (USD)", 0) or 0),
                    "Price (USD)": last,
                    "Change 24h (%)": r.get("Change 24h (%)"),
                    "Link": r.get("Link",""),
                    "Base Address": r.get("Base Address",""),
                    "Base Symbol": (sym.split("/")[0] if sym else r.get("baseSymbol","")),
                })

        # 2) Bubblemaps anti-cluster
        filtered = []
        for c in candidates:
            addr = c.get("Base Address") or ""
            bm_ok = "N/A"
            if self.bm_check and addr:
                try:
                    bm = self.bm_check(addr)
                    if bm.get("is_high_risk"):
                        bm_ok = "HIGH"
                        continue
                    else:
                        bm_ok = "OK"
                except Exception:
                    bm_ok = "ERR"
            c["BM Risk"] = bm_ok
            filtered.append(c)

        # 3) GetMoni — filtro social (facoltativo)
        filtered2 = []
        for c in filtered:
            social = {"passes": True}
            if self.sent_check:
                try:
                    social = self.sent_check(c.get("Base Symbol"), c.get("Base Address"))
                except Exception:
                    social = {"passes": True, "err": True}
            if not social.get("passes", True):
                continue
            c["Moni User"] = social.get("username")
            c["Mentions 24h"] = social.get("mentions_24h")
            c["Smarts"] = social.get("smarts_count")
            c["Sentiment"] = social.get("sentiment_score")
            c["Moni Score"] = social.get("moni_score")
            filtered2.append(c)

        sort_cols = ["Score","Txns 1h","Liquidity (USD)","Mentions 24h","Smarts","Sentiment"]
        df_signals = (pd.DataFrame(filtered2)
                        .sort_values(by=[c for c in sort_cols if (filtered2 and c in filtered2[0].keys())],
                                     ascending=[False]*len(sort_cols) if filtered2 else True)
                        .reset_index(drop=True)
                      ) if filtered2 else pd.DataFrame(columns=["Pair","DEX","Score","Txns 1h","Liquidity (USD)","Volume 24h (USD)","Price (USD)","Change 24h (%)","Link","Base Address","BM Risk","Moni User","Mentions 24h","Smarts","Sentiment","Moni Score"])

        # 4) apri max 1 posizione per step (rispettando vincoli per-simbolo)
        if not df_signals.empty and self.risk.can_open(len(self.broker.positions)):
            top = df_signals.iloc[0].to_dict()
            sym = top["Pair"]
            px = float(top["Price (USD)"] or 0.0)
            if px > 0:
                now = time.time()
                # quante posizioni già aperte su questo simbolo?
                open_count_sym = sum(1 for p in self.broker.positions.values() if p.symbol == sym)
                open_allowed = True

                # limite per-simbolo
                if open_count_sym >= max(1, int(self.risk.cfg.max_positions_per_symbol)):
                    open_allowed = False

                # cooldown
                last_ts = self.last_entry_by_symbol.get(sym, 0)
                if self.risk.cfg.symbol_cooldown_min and (now - last_ts) < self.risk.cfg.symbol_cooldown_min * 60:
                    open_allowed = False

                # pyramiding
                if open_count_sym > 0 and not self.risk.cfg.allow_pyramiding:
                    open_allowed = False
                elif open_count_sym > 0 and self.risk.cfg.allow_pyramiding:
                    last_entry_price = 0.0
                    try:
                        last_entry_price = max(p.entry_price for p in self.broker.positions.values() if p.symbol == sym)
                    except Exception:
                        pass
                    trig = (1.0 + float(self.risk.cfg.pyramid_add_on_trigger_pct or 0.08))
                    if not (px >= last_entry_price * trig):
                        open_allowed = False

                if open_allowed:
                    pos = self.broker.open_long(sym, label=f"{top.get('DEX','')} | S{top.get('Score',0)}", price=px, usd_amount=self.risk.cfg.position_usd)
                    if pos:
                        self.last_entry_by_symbol[sym] = now

        # 5) mark-to-market & uscite
        df_open, df_closed = self.broker.mark_to_market(prices)
        return df_signals, df_open, df_closed

    def close_by_id(self, pid: str, last_price: float):
        self.broker.close(pid, last_price, "MANUAL")
