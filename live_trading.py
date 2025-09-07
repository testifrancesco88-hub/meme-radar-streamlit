# live_trading.py — Jupiter connector (deeplink + autosign beta)
import base64, json, time
from dataclasses import dataclass
from typing import Optional, Tuple

import requests

JUP_QUOTE = "https://quote-api.jup.ag/v6/quote"
JUP_SWAP  = "https://quote-api.jup.ag/v6/swap"
JUP_TOKENS= "https://token.jup.ag/all"
JUP_PRICE = "https://price.jup.ag/v6/price?ids=SOL"

# Mints utili
MINT_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
MINT_SOL  = "So11111111111111111111111111111111111111112"  # wSOL

def _get(url, params=None, timeout=20):
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _post(url, data, timeout=20):
    r = requests.post(url, headers={"Content-Type":"application/json"}, data=json.dumps(data), timeout=timeout)
    r.raise_for_status()
    return r.json()

class TokenRegistry:
    _cache = None
    _by_mint = {}
    _ts = 0

    @classmethod
    def _ensure(cls):
        if cls._cache and time.time()-cls._ts < 3600:
            return
        data = _get(JUP_TOKENS)
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
        j = _get(JUP_PRICE)
        return float(j["data"]["SOL"]["price"])
    except Exception:
        return float(default)

@dataclass
class LiveConfig:
    mode: str                 # "off" | "deeplink" | "autosign"
    slippage_bps: int = 100   # 1% = 100 bps
    rpc_url: Optional[str] = None
    public_key: Optional[str] = None
    private_key_b58: Optional[str] = None  # solo per autosign

class JupiterConnector:
    def __init__(self, cfg: LiveConfig):
        self.cfg = cfg

    # ===== High-level: BUY / SELL =====
    def build_buy(self, quote_mint: str, base_mint: str, amount_quote_usd: float, price_usd_base: Optional[float], prefer_quote="USDC") -> Tuple[str, str]:
        if self.cfg.mode == "off":
            return ("off", "Live trading OFF")

        # Determina inputMint e amountIn (unità minime) da USD
        if quote_mint == MINT_USDC:
            in_dec = TokenRegistry.decimals(MINT_USDC, 6)
            amount_in = int(round(amount_quote_usd * (10 ** in_dec)))
            input_mint = MINT_USDC
        elif quote_mint == MINT_SOL:
            sol_usd = get_sol_usd()
            sol_amount = amount_quote_usd / max(0.01, sol_usd)
            in_dec = 9
            amount_in = int(round(sol_amount * (10 ** in_dec)))
            input_mint = MINT_SOL
        else:
            in_dec = TokenRegistry.decimals(MINT_USDC, 6)
            amount_in = int(round(amount_quote_usd * (10 ** in_dec)))
            input_mint = MINT_USDC

        if self.cfg.mode == "deeplink":
            human = amount_in / (10 ** in_dec)
            url = f"https://jup.ag/swap/{TokenRegistry.symbol(input_mint) or 'USDC'}-{TokenRegistry.symbol(base_mint)}?amount={human}&slippageBps={self.cfg.slippage_bps}"
            return ("deeplink", url)

        if self.cfg.mode == "autosign":
            return self._autosign_swap(input_mint, base_mint, amount_in, "ExactIn")

        return ("off", "Unsupported mode")

    def build_sell(self, base_mint: str, quote_mint: str, base_amount_tokens: float) -> Tuple[str, str]:
        if self.cfg.mode == "off":
            return ("off", "Live trading OFF")

        out_dec = TokenRegistry.decimals(base_mint, 9)
        amount_in = int(round(base_amount_tokens * (10 ** out_dec)))
        if self.cfg.mode == "deeplink":
            human = base_amount_tokens
            url = f"https://jup.ag/swap/{TokenRegistry.symbol(base_mint)}-{TokenRegistry.symbol(quote_mint)}?amount={human}&slippageBps={self.cfg.slippage_bps}"
            return ("deeplink", url)
        if self.cfg.mode == "autosign":
            return self._autosign_swap(base_mint, quote_mint, amount_in, "ExactIn")
        return ("off", "Unsupported mode")

    # ===== Autosign (beta) =====
    def _autosign_swap(self, input_mint: str, output_mint: str, amount_in: int, swap_mode: str):
        if not (self.cfg.rpc_url and self.cfg.public_key and self.cfg.private_key_b58):
            return ("error", "Autosign richiede RPC_URL, PUBLIC_KEY e PRIVATE_KEY")

        # 1) Quote
        q = _get(JUP_QUOTE, params={
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": amount_in,
            "slippageBps": self.cfg.slippage_bps,
            "swapMode": swap_mode,
            "onlyDirectRoutes": "false",
        })
        # 2) Swap (build tx)
        payload = {
            "userPublicKey": self.cfg.public_key,
            "wrapAndUnwrapSol": True,
            "quoteResponse": q,
            "asLegacyTransaction": True,
            "dynamicComputeUnitLimit": True,
            "useSharedAccounts": True,
        }
        s = _post(JUP_SWAP, payload)
        tx_b64 = s.get("swapTransaction")
        if not tx_b64:
            return ("error", f"Jupiter non ha fornito swapTransaction: {s}")

        # 3) Firma e invio — serve solana & solders (requirements.txt)
        try:
            from solana.rpc.api import Client
            from solana.keypair import Keypair
            from solders.keypair import Keypair as SKeypair
            from solana.transaction import Transaction
        except Exception as e:
            return ("error", f"Librerie non presenti (solana, solders). Aggiungi a requirements.txt. Dettagli: {e}")

        client = Client(self.cfg.rpc_url)
        try:
            raw = base64.b64decode(tx_b64)
            tx  = Transaction.deserialize(raw)

            # Carica keypair
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
