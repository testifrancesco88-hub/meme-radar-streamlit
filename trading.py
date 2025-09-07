# trading.py — motore di paper trading + risk manager + strategia momentum per meme coin
from __future__ import annotations
import time, math, uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ---------------- Risk Manager ----------------
@dataclass
class RiskConfig:
    position_usd: float = 50.0           # dimensione fissa per trade
    max_positions: int = 3               # quante posizioni aperte al massimo
    stop_loss_pct: float = 0.20          # 20% stop
    take_profit_pct: float = 0.40        # 40% take profit
    trailing_pct: float = 0.15           # trailing 15% dal massimo
    daily_loss_limit_usd: float = 200.0  # blocca nuove entrate se raggiunto
    slippage_pct: float = 0.01           # per la simulazione di esecuzione

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
        """
        prices: mappa symbol->prezzo
        Ritorna: (df_open, df_closed_recent)
        """
        rows = []
        to_close = []
        for pos in self.positions.values():
            px = float(prices.get(pos.symbol, 0) or 0)
            if px <= 0: 
                rows.append(self._row(pos, px, 0.0))
                continue

            # aggiorna massimo visto (per trailing)
            if px > pos.max_price_seen:
                pos.max_price_seen = px

            pnl = (px - pos.entry_price) * pos.qty  # USD
            pnl_pct = (px / pos.entry_price - 1.0) if pos.entry_price > 0 else 0.0

            rows.append(self._row(pos, px, pnl))

            # regole di uscita: stop/take/trailing
            if pnl_pct <= -abs(self.risk.cfg.stop_loss_pct):
                to_close.append((pos, px, "SL"))
            elif pnl_pct >= abs(self.risk.cfg.take_profit_pct):
                to_close.append((pos, px, "TP"))
            else:
                if pos.max_price_seen > 0:
                    drawdown = 1 - (px / pos.max_price_seen)
                    if drawdown >= abs(self.risk.cfg.trailing_pct):
                        to_close.append((pos, px, "TRAIL"))

        # applica chiusure
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

# ---------------- Strategia: Momentum + Meme Score ----------------
@dataclass
class StratConfig:
    meme_score_min: int = 75
    txns1h_min: int = 300
    liq_min: float = 10000.0
    liq_max: float = 200000.0
    allow_dex: Tuple[str,...] = ("raydium","orca","meteora")

class StrategyMomentumV1:
    def __init__(self, cfg: StratConfig):
        self.cfg = cfg

    def signal_long(self, row: dict) -> bool:
        try:
            meme = int(row.get("Meme Score", 0) or 0)
            tx1h = int(row.get("Txns 1h", 0) or 0)
            liq  = float(row.get("Liquidity (USD)", 0) or 0.0)
            dex  = str(row.get("DEX","")).lower()
            chg  = float(row.get("Change 24h (%)", 0) or 0.0)
        except Exception:
            return False

        # filtri base
        if meme < self.cfg.meme_score_min: return False
        if tx1h < self.cfg.txns1h_min: return False
        if liq < self.cfg.liq_min or liq > self.cfg.liq_max: return False
        if self.cfg.allow_dex and dex not in self.cfg.allow_dex: return False

        # piccolo vincolo di momentum addizionale: prezzo 24h non negativo
        if chg is not None and chg < -5.0:  # evita già in dump
            return False
        return True

# ---------------- Trade Engine ----------------
class TradeEngine:
    """
    Collega strategia + broker + risk. Gestisce open/close su DataFrame di segnali.
    """
class TradeEngine:
    def __init__(self, risk_cfg: RiskConfig, strat_cfg: StratConfig, bm_check=None):
        self.risk = RiskManager(risk_cfg)
        self.broker = PaperBroker(self.risk)
        self.strategy = StrategyMomentumV1(strat_cfg)
        self.bm_check = bm_check  # funzione(token_address) -> dict

    def step(self, df_pairs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Valuta segnali long e marca a mercato le posizioni aperte.
        df_pairs è la tabella 'Pairs (post-filtri)' prodotta dall'app.
        """
        prices: Dict[str, float] = {}
        candidates: List[Dict] = []

        # 1) scan segnali
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
                    "Price (USD)": last,
                    "Link": r.get("Link",""),
                })

        df_signals = pd.DataFrame(candidates).sort_values(
            by=["Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False]
        ).reset_index(drop=True) if candidates else pd.DataFrame(columns=["Pair","DEX","Score","Txns 1h","Liquidity (USD)","Price (USD)","Link"])

        # 1) scan segnali
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
                    "Price (USD)": last,
                    "Link": r.get("Link",""),
                    "Base Address": r.get("Base Address","") or r.get("baseAddress",""),
                })

        # --- Bubblemaps anti-cluster (filtra i candidati ad alto rischio) ---
        filtered = []
        for c in candidates:
            addr = c.get("Base Address") or ""
            bm_ok = "N/A"
            if self.bm_check and addr:
                try:
                    bm = self.bm_check(addr)
                    if bm.get("is_high_risk"):
                        # scarta (non tradabile)
                        bm_ok = "HIGH"
                        # NON aggiungere a filtered
                        continue
                    else:
                        bm_ok = "OK"
                except Exception:
                    bm_ok = "ERR"
            c["BM Risk"] = bm_ok  # visibile in UI
            filtered.append(c)

        df_signals = (pd.DataFrame(filtered)
                        .sort_values(by=["Score","Txns 1h","Liquidity (USD)"], ascending=[False, False, False])
                        .reset_index(drop=True)
                      ) if filtered else pd.DataFrame(columns=["Pair","DEX","Score","Txns 1h","Liquidity (USD)","Price (USD)","Link","Base Address","BM Risk"])
