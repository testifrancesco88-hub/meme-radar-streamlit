# trading.py — Strategy V2 con SHORT + TP a R-multiple + Multi-Partial (2R/3R/5R)
# - RiskConfig:
#     direction ('long'|'short'|'both'), use_r_multiple, tp_r_multiple
#     partial_tp_enable, partial_tp_fraction (es. 0.5 a 2R)
#     partial_scales: lista di (multiple, fraction) es. [(3.0, 0.25), (5.0, 0.25)]
# - TradeEngine:
#     • Segnali long/short (short = solo paper)
#     • Partial TP multipli: quando raggiunge il target, realizza la frazione, riduce size e imposta stop a BE+lock
#     • Trailing, BE lock, time-stop
#     • Registra chiusure parziali (note: "partial 25% @ 3.0R")

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict, Any
import time, math
import pandas as pd

# ========================= Config =========================

@dataclass
class RiskConfig:
    position_usd: float = 50.0
    max_positions: int = 3
    stop_loss_pct: float = 0.20            # 20% -> 0.20
    take_profit_pct: float = 0.40          # legacy se use_r_multiple=False
    trailing_pct: float = 0.15
    daily_loss_limit_usd: float = 200.0
    max_positions_per_symbol: int = 1
    symbol_cooldown_min: int = 20
    allow_pyramiding: bool = False
    pyramid_add_on_trigger_pct: float = 0.08
    time_stop_min: int = 60
    be_trigger_pct: float = 0.10           # legacy trigger BE (non centrale qui)
    be_lock_profit_pct: float = 0.02       # lock minimo su BE (dopo partial)
    dd_lock_pct: float = 0.06              # (non usato esplicitamente qui)

    # Direzione e R-multipli
    direction: str = "long"                # 'long' | 'short' | 'both'
    use_r_multiple: bool = True
    tp_r_multiple: float = 2.0             # prima scala (es. 2R)
    partial_tp_enable: bool = True
    partial_tp_fraction: float = 0.5       # frazione alla prima scala (es. 50%)

    # Scalini addizionali: [(3.0, 0.25), (5.0, 0.25)]
    partial_scales: List[Tuple[float, float]] = field(default_factory=list)

@dataclass
class StratConfig:
    meme_score_min: int = 70
    txns1h_min: int = 250
    liq_min: float = 10_000.0
    liq_max: float = 200_000.0
    turnover_min: float = 1.2
    chg24_min: float = -8.0
    chg24_max: float = 180.0
    allow_dex: tuple[str, ...] = ("raydium","orca","meteora","lifinity")
    heat_tx1h_topN: int = 10
    heat_tx1h_avg_min: float = 120.0

# ========================= Utils =========================

def _now() -> float: return time.time()

def _fmt_ago(ts: float) -> str:
    d = max(0, _now() - ts)
    m = int(d // 60); s = int(d % 60)
    if m >= 60:
        h = m // 60; m = m % 60
        return f"{h}h {m}m"
    return f"{m}m {s}s"

def _to_float(x, default=None):
    if x is None: return default
    try:
        v = float(x)
        if not math.isfinite(v): return default
        return v
    except Exception:
        try:
            s = str(x).replace(",", "").replace("%","").strip()
            v = float(s) if s else default
            return v if (v is not None and math.isfinite(v)) else default
        except Exception:
            return default

def _row_price(row: pd.Series) -> Optional[float]:
    return _to_float(row.get("Price (USD)"), None)

# ========================= Engine =========================

class TradeEngine:

    class _RiskState:
        def __init__(self):
            self.daily_pnl: float = 0.0
            self.symbol_last_open: Dict[str, float] = {}
            self.per_symbol_open_count: Dict[str, int] = {}

    def __init__(self, risk: RiskConfig, strategy: StratConfig, bm_check=None, sent_check=None):
        self.risk = type("Risk", (), {})()
        self.risk.cfg = risk
        self.risk.state = TradeEngine._RiskState()

        self.strategy = type("Strategy", (), {})()
        self.strategy.cfg = strategy

        self.bm_check = bm_check
        self.sent_check = sent_check

        self._positions: Dict[str, Dict[str, Any]] = {}
        self._closed: List[Dict[str, Any]] = []
        self._last_id = 0

    # ---------- API ----------

    def close_by_id(self, pos_id: str, last_price: float | None = None):
        pos = self._positions.pop(str(pos_id), None)
        if not pos:
            return
        if last_price is not None:
            pos["last"] = float(last_price)
        pnl = self._compute_pnl_usd(pos, pos.get("last"))
        pos["pnl_usd"] = pnl
        pos["pnl_pct"] = self._compute_pnl_pct(pos, pos.get("last"))
        pos["closed_at"] = _now()
        self._closed.append({
            "symbol": pos["symbol"],
            "label": pos["label"],
            "side": pos["side"],
            "pnl_usd": pos["pnl_usd"],
            "pnl_pct": pos["pnl_pct"],
            "duration": _fmt_ago(pos["opened_at"]),
            "note": "full close",
        })
        self.risk.state.daily_pnl += pnl
        sym = pos["symbol"]
        self.risk.state.per_symbol_open_count[sym] = max(0, self.risk.state.per_symbol_open_count.get(sym, 1) - 1)

    def step(self, df_pairs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        self._update_positions(df_pairs.copy() if df_pairs is not None else pd.DataFrame())

        if self.risk.state.daily_pnl <= -abs(self.risk.cfg.daily_loss_limit_usd):
            return (pd.DataFrame(), self._df_open(), self._flush_closed())

        signals = self._generate_signals(df_pairs)
        self._open_from_signals(signals)

        return (self._df_signals(signals), self._df_open(), self._flush_closed())

    # ---------- Segnali ----------

    def _generate_signals(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        s = self.strategy.cfg
        rk = self.risk.cfg
        if df is None or df.empty:
            return []

        d = df.copy()
        try:
            d = d[d["DEX"].str.lower().isin(s.allow_dex)]
        except Exception:
            pass
        d = d[pd.to_numeric(d["Meme Score"], errors="coerce").fillna(0) >= s.meme_score_min]
        d = d[pd.to_numeric(d["Txns 1h"], errors="coerce").fillna(0) >= s.txns1h_min]

        liq = pd.to_numeric(d["Liquidity (USD)"], errors="coerce").fillna(0)
        d = d[(liq >= s.liq_min) & (liq <= s.liq_max)]

        vol = pd.to_numeric(d["Volume 24h (USD)"], errors="coerce").fillna(0)
        turnover = (vol / liq.replace(0, 1)).fillna(0)
        d = d[turnover >= s.turnover_min]

        chg24 = pd.to_numeric(d["Change 24h (%)"], errors="coerce")
        d = d[(chg24 >= s.chg24_min) & (chg24 <= s.chg24_max)]

        if rk.direction == "long":
            d = d[pd.to_numeric(d["Change 1h (%)"], errors="coerce").fillna(0) >= 0]
        elif rk.direction == "short":
            c1 = pd.to_numeric(d["Change 1h (%)"], errors="coerce").fillna(0) <= -3.0
            c4 = pd.to_numeric(d["Change 4h/6h (%)"], errors="coerce").fillna(0) <= 0.0
            d = d[c1 & c4]
        else:
            pass  # both

        if d.empty:
            return []

        d = d.sort_values(by=["Meme Score", "Txns 1h", "Liquidity (USD)"], ascending=[False, False, False])

        signals: List[Dict[str, Any]] = []
        for _, r in d.iterrows():
            price = _row_price(r)
            if price is None or price <= 0:
                continue
            pair = r.get("Pair", "")
            dex = r.get("DEX", "")
            label = dex
            ch1 = _to_float(r.get("Change 1h (%)"), None)
            ch4 = _to_float(r.get("Change 4h/6h (%)"), None)

            # side
            side = "long"
            if rk.direction == "short":
                side = "short"
            elif rk.direction == "both":
                side = "short" if ((ch1 is not None and ch1 <= -3.0) and (ch4 is None or ch4 <= 0.0)) else "long"

            if not self._can_open_symbol(pair):
                continue

            signals.append({
                "symbol": pair,
                "label": label,
                "side": side,
                "entry": float(price),
                "mscore": int(_to_float(r.get("Meme Score"), 0) or 0),
                "tx1h": int(_to_float(r.get("Txns 1h"), 0) or 0),
                "liq": float(_to_float(r.get("Liquidity (USD)"), 0) or 0),
                "vol24": float(_to_float(r.get("Volume 24h (USD)"), 0) or 0),
                "url": r.get("Link", ""),
            })
            if len(signals) >= 20:
                break

        return signals

    def _can_open_symbol(self, symbol: str) -> bool:
        if self.risk.state.per_symbol_open_count.get(symbol, 0) >= max(1, self.risk.cfg.max_positions_per_symbol):
            return False
        last_ts = self.risk.state.symbol_last_open.get(symbol, 0)
        if (_now() - last_ts) < (self.risk.cfg.symbol_cooldown_min * 60):
            return False
        return True

    # ---------- Apertura / Update ----------

    def _open_from_signals(self, signals: List[Dict[str, Any]]):
        if not signals:
            return
        space = max(0, self.risk.cfg.max_positions - len(self._positions))
        if space <= 0:
            return
        for sig in signals:
            if space <= 0:
                break
            if not self._can_open_symbol(sig["symbol"]):
                continue
            self._open_position(sig)
            space -= 1

    def _open_position(self, sig: Dict[str, Any]):
        self._last_id += 1
        pos_id = str(self._last_id)

        side = sig["side"]
        entry = float(sig["entry"])
        size_usd = float(self.risk.cfg.position_usd)
        qty_base = size_usd / max(1e-9, entry)

        sl_pct = float(self.risk.cfg.stop_loss_pct)
        tp_pct = float(self.risk.cfg.take_profit_pct)

        # Stop simmetrico
        if side == "long":
            stop_price = entry * (1 - sl_pct)
            tp_price_legacy = entry * (1 + tp_pct)
        else:
            stop_price = entry * (1 + sl_pct)
            tp_price_legacy = entry * (1 - tp_pct)

        # Costruisci lista scalini (prima 2R, poi eventuali extra)
        partials: List[Dict[str, Any]] = []
        if self.risk.cfg.use_r_multiple and self.risk.cfg.partial_tp_enable:
            base_mult = float(self.risk.cfg.tp_r_multiple)
            base_frac = max(0.05, min(0.95, float(self.risk.cfg.partial_tp_fraction)))
            price_base = entry * (1 + base_mult * sl_pct) if side == "long" else entry * (1 - base_mult * sl_pct)
            partials.append({"multiple": base_mult, "price": price_base, "fraction": base_frac, "hit": False})
            # extra scalini
            for mult, frac in (self.risk.cfg.partial_scales or []):
                m = float(mult); f = max(0.05, min(0.95, float(frac)))
                price_m = entry * (1 + m * sl_pct) if side == "long" else entry * (1 - m * sl_pct)
                partials.append({"multiple": m, "price": price_m, "fraction": f, "hit": False})

            # ordina per multiple crescente per sicurezza
            partials.sort(key=lambda x: x["multiple"])

        pos = {
            "id": pos_id,
            "symbol": sig["symbol"],
            "label": sig["label"],
            "side": side,
            "entry": entry,
            "last": entry,
            "size_usd": size_usd,
            "qty": qty_base,
            "opened_at": _now(),
            "stop_price": stop_price,
            "tp_price_legacy": tp_price_legacy if not (self.risk.cfg.use_r_multiple and self.risk.cfg.partial_tp_enable) else None,
            "partials": partials,      # lista target R multipli
            "best": entry,
        }
        self._positions[pos_id] = pos

        self.risk.state.symbol_last_open[sig["symbol"]] = _now()
        self.risk.state.per_symbol_open_count[sig["symbol"]] = self.risk.state.per_symbol_open_count.get(sig["symbol"], 0) + 1

    def _update_positions(self, df: pd.DataFrame):
        if not self._positions:
            return

        by_pair = {}
        try:
            for _, r in df.iterrows():
                by_pair[r.get("Pair","")] = _row_price(r)
        except Exception:
            pass

        to_close: List[Tuple[str, Dict[str, Any]]] = []
        for pid, pos in list(self._positions.items()):
            price = by_pair.get(pos["symbol"], pos["last"])
            if price is None: price = pos["last"]
            pos["last"] = float(price)

            # best per trailing
            if pos["side"] == "long":
                pos["best"] = max(pos["best"], price)
            else:
                pos["best"] = min(pos["best"], price)

            # ===== Partial multipli (2R / 3R / 5R / ...) =====
            if pos.get("partials"):
                for tgt in pos["partials"]:
                    if tgt["hit"]:
                        continue
                    reached = (price >= tgt["price"]) if pos["side"] == "long" else (price <= tgt["price"])
                    if reached:
                        f = max(0.05, min(0.95, float(tgt["fraction"])))
                        pnl_pct_total = self._compute_pnl_pct(pos, price)
                        # realizza sul notional corrente (size_usd rimasto)
                        realized_usd = float(pos["size_usd"]) * pnl_pct_total * f
                        self.risk.state.daily_pnl += realized_usd
                        self._closed.append({
                            "symbol": pos["symbol"],
                            "label": pos["label"],
                            "side": pos["side"],
                            "pnl_usd": realized_usd,
                            "pnl_pct": pnl_pct_total * f,
                            "duration": _fmt_ago(pos["opened_at"]),
                            "note": f"partial {int(f*100)}% @ {tgt['multiple']:.1f}R",
                        })
                        # riduci posizione (runner rimane)
                        pos["qty"] = float(pos["qty"]) * (1.0 - f)
                        pos["size_usd"] = float(pos["size_usd"]) * (1.0 - f)
                        tgt["hit"] = True

                        # BE+lock dopo OGNI partial
                        be_lock = float(self.risk.cfg.be_lock_profit_pct)
                        sl_pct = float(self.risk.cfg.stop_loss_pct)
                        lock = max(be_lock, sl_pct)
                        if pos["side"] == "long":
                            pos["stop_price"] = max(pos["stop_price"], pos["entry"] * (1 + lock))
                        else:
                            pos["stop_price"] = min(pos["stop_price"], pos["entry"] * (1 - lock))

                        # se size residua è troppo piccola, chiudi
                        if pos["size_usd"] <= 1e-6 or pos["qty"] <= 1e-18:
                            to_close.append((pid, pos))
                        # continua a valutare altri scalini nello stesso tick

            # trailing dinamico
            trail = float(self.risk.cfg.trailing_pct)
            if trail > 0:
                if pos["side"] == "long":
                    trail_price = pos["best"] * (1 - trail)
                    pos["stop_price"] = max(pos["stop_price"], trail_price)
                else:
                    trail_price = pos["best"] * (1 + trail)
                    pos["stop_price"] = min(pos["stop_price"], trail_price)

            # time-stop
            if self.risk.cfg.time_stop_min > 0:
                age_min = (_now() - pos["opened_at"]) / 60.0
                if age_min >= self.risk.cfg.time_stop_min:
                    r = float(self.risk.cfg.stop_loss_pct)
                    pnl_pct = self._compute_pnl_pct(pos, price)
                    if pnl_pct < 0.5 * r:
                        to_close.append((pid, pos))

            # SL / BE / Trail
            if (pos["side"] == "long" and price <= pos["stop_price"]) or (pos["side"] == "short" and price >= pos["stop_price"]):
                to_close.append((pid, pos))
                continue

            # Legacy TP% (se niente R-multipli)
            if (not self.risk.cfg.use_r_multiple) and pos.get("tp_price_legacy") is not None:
                if (pos["side"] == "long" and price >= pos["tp_price_legacy"]) or (pos["side"] == "short" and price <= pos["tp_price_legacy"]):
                    to_close.append((pid, pos))

        for pid, pos in to_close:
            self.close_by_id(pid, pos.get("last"))

    # ---------- PnL ----------

    def _compute_pnl_pct(self, pos: Dict[str, Any], last: float | None) -> float:
        if last is None: return 0.0
        e = float(pos["entry"])
        if e <= 0: return 0.0
        move = (last - e) / e
        return move if pos["side"] == "long" else (-move)

    def _compute_pnl_usd(self, pos: Dict[str, Any], last: float | None) -> float:
        if last is None: return 0.0
        pct = self._compute_pnl_pct(pos, last)
        return float(pos["size_usd"]) * pct

    # ---------- Output DF ----------

    def _df_open(self) -> pd.DataFrame:
        rows = []
        for pid, p in self._positions.items():
            rows.append({
                "id": pid,
                "symbol": p["symbol"],
                "label": f"{p['label']} • {p['side']}",
                "entry": float(p["entry"]),
                "last": float(p["last"]),
                "pnl_usd": self._compute_pnl_usd(p, p["last"]),
                "pnl_pct": self._compute_pnl_pct(p, p["last"]),
                "opened_ago": _fmt_ago(p["opened_at"]),
            })
        return pd.DataFrame(rows)

    def _df_signals(self, signals: List[Dict[str, Any]]) -> pd.DataFrame:
        if not signals:
            return pd.DataFrame()
        rows = []
        for s in signals:
            rows.append({
                "symbol": s["symbol"],
                "side": s["side"],
                "entry": s["entry"],
                "mscore": s["mscore"],
                "tx1h": s["tx1h"],
                "liq": s["liq"],
                "vol24": s["vol24"],
                "url": s["url"],
            })
        return pd.DataFrame(rows)

    def _flush_closed(self) -> pd.DataFrame:
        if not self._closed:
            return pd.DataFrame()
        df = pd.DataFrame(self._closed)
        self._closed = []
        return df
