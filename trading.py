# trading.py — Engine semplice, robusto e compatibile con streamlit_app.py v15+
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Optional
import pandas as pd
import math
import uuid


# ============================== Config dataclasses ==============================

@dataclass
class RiskConfig:
    # dimensionamento & limiti
    position_usd: float = 50.0
    max_positions: int = 3
    max_positions_per_symbol: int = 1
    daily_loss_limit_usd: float = 200.0

    # gestione rischio/uscite
    stop_loss_pct: float = 0.20          # 20%
    take_profit_pct: float = 0.40        # 40% (se non usi R-multiple)
    trailing_pct: float = 0.15           # 15%
    time_stop_min: int = 60

    # break-even lock
    be_trigger_pct: float = 0.10         # +10% attiva lock
    be_lock_profit_pct: float = 0.02     # lock a +2%
    dd_lock_pct: float = 0.06            # se ritraccia più di 6% da HWM → chiudi

    # anti-duplicato
    symbol_cooldown_min: int = 20
    allow_pyramiding: bool = False
    pyramid_add_on_trigger_pct: float = 0.08

    # direzione e TP a multipli di R
    direction: str = "long"              # "long" | "short" | "both" (short solo paper)
    use_r_multiple: bool = True
    tp_r_multiple: float = 2.0

    # partial take profit
    partial_tp_enable: bool = True
    partial_tp_fraction: float = 0.5
    # scalini extra: [(3.0, 0.25), (5.0, 0.25)] = chiudi 25% a 3R e 25% a 5R
    partial_scales: List[Tuple[float, float]] = field(default_factory=list)


@dataclass
class StratConfig:
    meme_score_min: int = 70
    txns1h_min: int = 200
    liq_min: float = 10_000.0
    liq_max: float = 200_000.0
    turnover_min: float = 1.2
    chg24_min: float = -8.0
    chg24_max: float = 180.0
    allow_dex: Tuple[str, ...] = ("raydium", "orca", "meteora", "lifinity")

    # market heat gate
    heat_tx1h_topN: int = 10
    heat_tx1h_avg_min: float = 120.0


# ================================ State models =================================

@dataclass
class _RiskState:
    daily_pnl: float = 0.0
    last_action_ts: Dict[str, float] = field(default_factory=dict)   # per symbol cooldown


@dataclass
class _Position:
    id: str
    symbol: str           # "BASE/QUOTE"
    direction: str        # "long" | "short"
    entry: float
    qty: float            # quantità base (semplice proxy paper)
    opened_ts: float
    last: float
    hwm: float            # high-water-mark sul prezzo (per trailing/lock)
    label: str            # es. "raydium | LONG"
    partial_done_2r: bool = False
    closed: bool = False
    pnl_usd: float = 0.0
    pnl_pct: float = 0.0
    # per scalini extra: set di multipli già eseguiti
    scales_hit: set = field(default_factory=set)


# ================================ TradeEngine ==================================

class TradeEngine:
    def __init__(self, risk: RiskConfig, strategy: StratConfig,
                 bm_check=None, sent_check=None):
        self.risk = type("RiskWrap", (), {"cfg": risk, "state": _RiskState()})
        self.strategy = type("StratWrap", (), {"cfg": strategy})
        self.bm_check = bm_check      # hook opzionali (non usati in questa build)
        self.sent_check = sent_check

        self.positions: Dict[str, _Position] = {}
        self.closed_trades: List[Dict] = []

    # ----------------------------- API pubblica -----------------------------

    def step(self, df_pairs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        - genera segnali dal df filtrato
        - apre/gestisce/chiude posizioni paper
        - ritorna (signals_df, open_df, closed_df)
        """
        now = time.time()

        # 0) market heat gate
        if not self._market_heat_ok(df_pairs):
            # aggiorna mark-to-market delle posizioni e applica time-stop/lock
            self._mtm_and_manage(df_pairs, now)
            return self._signals_df([]), self._open_df(), self._closed_df()

        # 1) genera candidati strategia
        candidates = self._candidates(df_pairs)

        # 2) apri posizioni se c'è capienza
        to_open = self._select_to_open(candidates)
        for row in to_open:
            self._try_open(row, now)

        # 3) mark-to-market & regole di uscita
        self._mtm_and_manage(df_pairs, now)

        return self._signals_df(candidates), self._open_df(), self._closed_df()

    def close_by_id(self, pid: str, price: Optional[float] = None):
        """Chiusura manuale da UI."""
        pos = self.positions.get(str(pid))
        if not pos or pos.closed:
            return
        px = float(price) if price and price > 0 else pos.last
        self._close_position(pos, px, reason="manual")

    # ---------------------------- Costruttori DF ----------------------------

    def _signals_df(self, rows: List[dict]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()
        cols = ["Pair", "DEX", "Price (USD)", "Meme Score", "Txns 1h",
                "Liquidity (USD)", "Volume 24h (USD)", "Change 24h (%)", "label"]
        out = []
        for r in rows:
            out.append({
                "Pair": r.get("Pair"),
                "DEX": r.get("DEX"),
                "Price (USD)": _to_float(r.get("Price (USD)")),
                "Meme Score": int(_to_float(r.get("Meme Score"), 0)),
                "Txns 1h": int(_to_float(r.get("Txns 1h"), 0)),
                "Liquidity (USD)": int(_to_float(r.get("Liquidity (USD)"), 0)),
                "Volume 24h (USD)": int(_to_float(r.get("Volume 24h (USD)"), 0)),
                "Change 24h (%)": _to_float(r.get("Change 24h (%)")),
                "label": f"{str(r.get('DEX','')).lower()} | {self._dir_label()}",
            })
        return pd.DataFrame(out, columns=cols)

    def _open_df(self) -> pd.DataFrame:
        if not self.positions:
            return pd.DataFrame()
        rows = []
        for p in self.positions.values():
            if p.closed:
                continue
            opened_ago = _fmt_ago(time.time() - p.opened_ts)
            rows.append({
                "id": p.id,
                "symbol": p.symbol,
                "label": p.label,
                "entry": p.entry,
                "last": p.last,
                "pnl_usd": p.pnl_usd,
                "pnl_pct": p.pnl_pct,
                "opened_ago": opened_ago,
            })
        return pd.DataFrame(rows)

    def _closed_df(self) -> pd.DataFrame:
        if not self.closed_trades:
            return pd.DataFrame()
        return pd.DataFrame(self.closed_trades)

    # ------------------------------- Logica --------------------------------

    def _market_heat_ok(self, df: pd.DataFrame) -> bool:
        """Controlla la media Txns1h sui top-N per Volume 24h."""
        c = self.strategy.cfg
        if df is None or df.empty:
            return False
        if "Volume 24h (USD)" not in df.columns or "Txns 1h" not in df.columns:
            return False
        top = df.sort_values(by=["Volume 24h (USD)"], ascending=False).head(max(1, int(c.heat_tx1h_topN)))
        avg_tx = float(pd.to_numeric(top["Txns 1h"], errors="coerce").fillna(0).mean())
        return (avg_tx >= float(c.heat_tx1h_avg_min))

    def _candidates(self, df: pd.DataFrame) -> List[dict]:
        """Filtra il df secondo StratConfig e ritorna una lista di righe (dict)."""
        c = self.strategy.cfg
        if df is None or df.empty:
            return []

        s = df.copy()

        def _num(series_name, default=0.0):
            return pd.to_numeric(s[series_name], errors="coerce").fillna(default)

        # filtri
        s = s[s["DEX"].str.lower().isin([d.lower() for d in c.allow_dex])]
        s = s[_num("Meme Score") >= int(c.meme_score_min)]
        s = s[_num("Txns 1h") >= int(c.txns1h_min)]
        s = s[(_num("Liquidity (USD)") >= float(c.liq_min)) & (_num("Liquidity (USD)") <= float(c.liq_max))]
        vol = _num("Volume 24h (USD)")
        liq = _num("Liquidity (USD)").replace(0, 1)
        s = s[(vol / liq) >= float(c.turnover_min)]
        chg24 = _num("Change 24h (%)", 0.0)
        s = s[(chg24 >= float(c.chg24_min)) & (chg24 <= float(c.chg24_max))]

        if s.empty:
            return []

        # ranking semplice: Meme Score desc, poi Txns1h desc, poi Vol24 desc
        s = s.sort_values(by=["Meme Score", "Txns 1h", "Volume 24h (USD)"], ascending=[False, False, False])
        return s.to_dict(orient="records")

    def _select_to_open(self, candidates: List[dict]) -> List[dict]:
        """Sceglie quali candidati aprire rispettando capienza/duplicati/cooldown."""
        if not candidates:
            return []
        cfg = self.risk.cfg

        # capienza globale
        free_slots = max(0, int(cfg.max_positions) - sum(1 for p in self.positions.values() if not p.closed))
        if free_slots <= 0:
            return []

        # anti-duplicato e cooldown
        chosen = []
        per_symbol_count: Dict[str, int] = {}
        now = time.time()

        for r in candidates:
            if len(chosen) >= free_slots:
                break
            pair = str(r.get("Pair", ""))
            if not pair or "/" not in pair:
                continue
            base = pair.split("/", 1)[0]

            # limiti per simbolo
            per_symbol_count.setdefault(base, 0)
            active_same = sum(1 for p in self.positions.values()
                              if (not p.closed) and p.symbol.startswith(base + "/"))
            if active_same >= int(cfg.max_positions_per_symbol):
                continue

            # cooldown
            last_ts = self.risk.state.last_action_ts.get(base, 0.0)
            if now - last_ts < cfg.symbol_cooldown_min * 60:
                continue

            chosen.append(r)
            per_symbol_count[base] += 1

        return chosen

    def _try_open(self, row: dict, now: float):
        """Apre una posizione paper."""
        cfg = self.risk.cfg
        side = self._decide_side()  # "long" | "short"
        price = _to_float(row.get("Price (USD)"), None)
        if price is None or price <= 0:
            return  # prezzo non valido

        pair = str(row.get("Pair", ""))
        if not pair:
            return
        base, quote = pair.split("/", 1)
        label = f"{str(row.get('DEX','')).lower()} | {side.upper()}"

        # sizing paper
        qty = cfg.position_usd / max(1e-9, price)

        pid = uuid.uuid4().hex[:8].upper()
        pos = _Position(
            id=pid,
            symbol=pair,
            direction=side,
            entry=price,
            qty=qty,
            opened_ts=now,
            last=price,
            hwm=price,
            label=label,
        )
        self.positions[pid] = pos
        # aggiorna cooldown per il base
        self.risk.state.last_action_ts[base] = now

    def _mtm_and_manage(self, df: pd.DataFrame, now: float):
        """Aggiorna P&L delle posizioni e applica stop/TP/trailing/time-stop/lock."""
        if not self.positions:
            return
        cfg = self.risk.cfg

        # mappa Pair -> prezzo corrente (se disponibile)
        px_map: Dict[str, float] = {}
        if df is not None and not df.empty and "Pair" in df.columns and "Price (USD)" in df.columns:
            for r in df.to_dict(orient="records"):
                pair = str(r.get("Pair", ""))
                px = _to_float(r.get("Price (USD)"))
                if pair and px and px > 0:
                    px_map[pair] = px

        to_close: List[Tuple[_Position, float, str]] = []

        for p in list(self.positions.values()):
            if p.closed:
                continue

            price = px_map.get(p.symbol, p.last)
            p.last = price
            if price <= 0:
                continue

            # direzione: per "short" invertiamo la percentuale
            raw_ret = (price / p.entry - 1.0)
            ret_pct = raw_ret if p.direction == "long" else (-raw_ret)
            p.pnl_pct = ret_pct
            p.pnl_usd = ret_pct * cfg.position_usd

            # HWM aggiornato (per trailing su LONG; su SHORT usiamo LWM equivalente)
            if p.direction == "long":
                p.hwm = max(p.hwm, price)
            else:
                # per short, hwm è il "low-water-mark": prezzo più basso
                p.hwm = min(p.hwm, price)

            # regole di uscita
            # 1) Stop loss
            if ret_pct <= -cfg.stop_loss_pct:
                to_close.append((p, price, "stop"))
                continue

            # 2) TP classico (se non uso multipli di R)
            if not cfg.use_r_multiple and ret_pct >= cfg.take_profit_pct:
                to_close.append((p, price, "tp"))
                continue

            # 3) Partial TP a 2R (se abilitato)
            if cfg.use_r_multiple and cfg.partial_tp_enable and not p.partial_done_2r:
                r_mult = (ret_pct / max(1e-9, cfg.stop_loss_pct))
                if r_mult >= cfg.tp_r_multiple:  # es. 2R
                    # chiudiamo frazione e spostiamo entry in avanti come se fossimo lockati
                    cash_out = cfg.partial_tp_fraction * p.qty * (price - p.entry)
                    self.risk.state.daily_pnl += cash_out
                    # riduci qty (runner rimasto)
                    p.qty *= (1.0 - cfg.partial_tp_fraction)
                    p.partial_done_2r = True
                    # break-even lock: aggiorna entry simulando BE+lock
                    be_price = p.entry * (1.0 + cfg.be_lock_profit_pct if p.direction == "long"
                                          else 1.0 - cfg.be_lock_profit_pct)
                    p.entry = min(be_price, price) if p.direction == "long" else max(be_price, price)

            # 4) Scalini extra a R multipli (runner)
            if cfg.use_r_multiple and cfg.partial_scales and p.qty > 0:
                r_now = ret_pct / max(1e-9, cfg.stop_loss_pct)
                for r_mult, frac in cfg.partial_scales:
                    if r_now >= r_mult and (r_mult not in p.scales_hit) and 0.0 < frac < 1.0:
                        cash_out = frac * p.qty * (price - p.entry)
                        self.risk.state.daily_pnl += cash_out
                        p.qty *= (1.0 - frac)
                        p.scales_hit.add(r_mult)

            # 5) Trailing / Drawdown lock
            if cfg.trailing_pct > 0:
                if p.direction == "long":
                    trail_stop = p.hwm * (1.0 - cfg.trailing_pct)
                    if price <= trail_stop:
                        to_close.append((p, price, "trailing"))
                        continue
                else:
                    trail_stop = p.hwm * (1.0 + cfg.trailing_pct)  # per short p.hwm è LWM
                    if price >= trail_stop:
                        to_close.append((p, price, "trailing"))
                        continue

            # 6) Break-even trigger + drawdown lock da HWM
            if ret_pct >= cfg.be_trigger_pct:
                # drawdown dal massimo rendimento
                if p.direction == "long":
                    peak_ret = (p.hwm / p.entry - 1.0)
                else:
                    peak_ret = (p.entry / p.hwm - 1.0)  # per short
                if peak_ret - ret_pct >= cfg.dd_lock_pct:
                    to_close.append((p, price, "dd-lock"))
                    continue

            # 7) Time stop
            if cfg.time_stop_min > 0 and (now - p.opened_ts) >= cfg.time_stop_min * 60:
                to_close.append((p, price, "time-stop"))
                continue

            # 8) Daily loss limit (sommatoria realized + unrealized)
            if (self.risk.state.daily_pnl + self._unrealized_sum()) <= -abs(cfg.daily_loss_limit_usd):
                to_close.append((p, price, "day-loss-limit"))
                continue

        for p, px, reason in to_close:
            self._close_position(p, px, reason)

    def _close_position(self, p: _Position, price: float, reason: str):
        if p.closed:
            return
        cfg = self.risk.cfg
        # realized pnl: su tutto il residuo
        ret_pct = (price / p.entry - 1.0) if p.direction == "long" else (p.entry / price - 1.0)
        realized = ret_pct * cfg.position_usd * (p.qty)  # qty in “quote” semplificato
        self.risk.state.daily_pnl += realized
        p.closed = True
        p.last = price
        p.pnl_usd = realized
        p.pnl_pct = ret_pct
        self.closed_trades.append({
            "closed_ts": time.strftime("%H:%M:%S"),
            "symbol": p.symbol,
            "label": p.label,
            "reason": reason,
            "pnl_usd": round(realized, 4),
            "pnl_pct": round(ret_pct * 100.0, 2),
        })

    # ------------------------------ Utilità --------------------------------

    def _dir_label(self) -> str:
        d = self.risk.cfg.direction
        if d == "long":
            return "LONG"
        if d == "short":
            return "SHORT"
        return "LONG/SHORT"

    def _decide_side(self) -> str:
        d = self.risk.cfg.direction
        if d in ("long", "short"):
            return d
        # "both": per ora preferiamo LONG su spot; SHORT rimane paper/manuale
        return "long"

    def _unrealized_sum(self) -> float:
        return sum(p.pnl_usd for p in self.positions.values() if not p.closed)


# ================================ Helpers =====================================

def _to_float(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).replace(",", "").strip().replace("%", "")
        return float(s) if s else default
    except Exception:
        return default


def _fmt_ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"
