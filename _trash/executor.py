# executor.py
"""
Motore esecutivo del Coordinator:
- legge/ricalcola equity utente
- chiama Pyth (Hermes) per il prezzo
- calcola size (notional / qty)
- (stub) prepara esecuzione on-chain via Ponte
"""

import os
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Se vogliamo in futuro usare Web3:
try:
    from web3 import Web3
except ImportError:
    Web3 = None  # opzionale per ora


# ======================
# CONFIG & CONSTANTS
# ======================

PYTH_HERMES_URL = os.getenv("PYTH_HERMES_URL", "https://hermes.pyth.network")

ARBITRUM_RPC_URL = os.getenv("ARBITRUM_RPC_URL")  # es. https://arb-sepolia.g.alchemy.com/v2/...
PONTE_ADDRESS    = os.getenv("PONTE_ADDRESS")        # lo stesso usato per App Manager (Sepolia)
RELAYER_PK       = os.getenv("RELAYER_PRIVATE_KEY")  # chiave privata del dev wallet (TEST_USER_001)
CHAIN_ID         = int(os.getenv("CHAIN_ID", "421614"))  # Arbitrum Sepolia di default

MAX_LEVERAGE_FX      = float(os.getenv("MAX_LEVERAGE_FX", "40"))
MAX_LEVERAGE_CRYPTO  = float(os.getenv("MAX_LEVERAGE_CRYPTO", "40"))
MAX_LEVERAGE_METAL   = float(os.getenv("MAX_LEVERAGE_METAL", "40"))
MAX_LEVERAGE_INDEX   = float(os.getenv("MAX_LEVERAGE_INDEX", "20"))

# Mapping simbolo → feed_id Pyth (DA RIEMPIRE con gli ID reali).
# Copia 1:1 da live/price_adapter_pyth.py quando vuoi essere “production ready”.
PYTH_FEEDS = {
    "EURUSD":       "0xa995d00bb36a63cef7fd2c287dc105fc8f3d93779f062f09551b0af3e81ec30b",
    "GBPUSD":       "0x84c2dde9633d93d1bcad84e7dc41c9d56578b7ec52fabedc1f335d673df0a7c1",
    "AUDUSD":       "0x67a6f93030420c1c9e3fe37c1ab6b77966af82f995944a9fefce357a22854a80",
    "USDJPY":       "0xef2c98c804ba503c6a707e38be4dfbb16683775f195b091252bf24693042fd52",
    "USDCHF":       "0x0b1e3297e69f162877b577b0d6a47a0d63b2392bc8499e6540da4187a63e28f8",
    "USDCAD":       "0x3112b03a41c910ed446852aacf67118cb1bec67b2cd0b9a214c58cc0eaa2ecca",

    "EURJPY":       "0xd8c874fa511b9838d094109f996890642421e462c3b29501a2560cecf82c2eb4",
    "AUDJPY":       "0x8dbbb66dff44114f0bfc34a1d19f0fe6fc3906dcc72f7668d3ea936e1d6544ce",
    "CADJPY":       "0x9e19cbf0b363b3ce3fa8533e171f449f605a7ca5bb272a9b80df4264591c4cbb",
    "GBPJPY":       "0xcfa65905787703c692c3cac2b8a009a1db51ce68b54f5b206ce6a55bfa2c3cd1",

    "DOLLARIDXUSD": "0x710afe0041a07156bfd71971160c78a326bf8121403e0d4e140d06bea0353b7f",

    "XAGUSD":       "0xf2fb02c32b055c805e7238d628e5e9dadef274376114eb1f012337cabe93871e",
    "XAUUSD":       "0x765d2ba906dbc32ca17cc11f5310a89e9ee1f6420508c63861f2f8ba4ee34bb2",

    "LIGHTCMDUSD":  "0x925ca92ff005ae943c158e3563f59698ce7e75c5a8c8dd43303a0a154887b3e6",

    "BTCUSD":       "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
    "ETHUSD":       "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
}

SYMBOL_UNIVERSE = [
    "AUDJPY", "AUDUSD", "BTCUSD", "CADJPY", "DOLLARIDXUSD",
    "ETHUSD", "EURJPY", "EURUSD", "GBPJPY", "GBPUSD",
    "LIGHTCMDUSD", "USATECHIDXUSD",  # questa in realtà non è nel v3, ma lasciamo comment: vedi sotto
    "USDCAD", "USDCHF", "USDJPY", "XAGUSD", "XAUUSD",
]

# In realtà l’universo vero v3 è quello che hai riportato:
COSCIENZA_SYMBOLS_V3 = [
    "EURUSD", "GBPUSD", "AUDUSD", "USDJPY", "USDCHF", "USDCAD",
    "EURJPY", "AUDJPY", "CADJPY", "GBPJPY",
    "XAUUSD", "XAGUSD", "LIGHTCMDUSD", "DOLLARIDXUSD",
    "BTCUSD", "ETHUSD",
]


def _asset_class(symbol: str) -> str:
    """Ritorna la 'classe' dell'asset per decidere la leva."""
    if symbol in ("BTCUSD", "ETHUSD"):
        return "CRYPTO"
    if symbol in ("XAUUSD", "XAGUSD", "LIGHTCMDUSD"):
        return "METAL"
    if symbol == "DOLLARIDXUSD":
        return "INDEX"
    # tutto il resto lo trattiamo come FX
    return "FX"


def _max_leverage_for(symbol: str) -> float:
    cls = _asset_class(symbol)
    if cls == "CRYPTO":
        return MAX_LEVERAGE_CRYPTO
    if cls == "METAL":
        return MAX_LEVERAGE_METAL
    if cls == "INDEX":
        return MAX_LEVERAGE_INDEX
    return MAX_LEVERAGE_FX


# ======================
# DB HELPERS
# ======================

def get_or_create_signal(engine: Engine, intent: Dict[str, Any]) -> uuid.UUID:
    """
    Salva il segnale in tabella signals e ritorna l'id.
    Per ora score/p/e/r/v sono placeholder (veri valori li potremo passare nel meta).
    """
    symbol = intent["symbol"]
    created_at = datetime.now(timezone.utc)

    # genera l'UUID UNA VOLTA qui
    str_id = str(uuid.uuid4())

    with engine.begin() as conn:
        # inserimento veloce; in futuro p,e,r,v possono venire da intent["meta"]
        conn.execute(
            text("""
                INSERT INTO signals (id, symbol, score, p, e, r, v, created_at)
                VALUES (:id, :symbol, :score, :p, :e, :r, :v, :created_at)
            """),
            {
                "id": str_id,
                "symbol": symbol,
                "score": 1.0,
                "p": 0.9,
                "e": 0.9,
                "r": 0.9,
                "v": 0.9,
                "created_at": created_at,
            }
        )

    return uuid.UUID(str_id)


def ensure_test_user(engine: Engine, user_id: str, test_wallet: str) -> None:
    """
    Assicura che esista un tenant + contract per TEST_USER_001.
    user_id lo usiamo come email pseudo-univoca, ad es. "TEST_USER_001".
    test_wallet = indirizzo Arbitrum dove vive il wallet di laboratorio.
    """
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id FROM tenants WHERE email = :email"),
            {"email": user_id}
        ).scalar_one_or_none()

        if row is None:
            tenant_uuid = str(uuid.uuid4())
            conn.execute(
                text("""
                    INSERT INTO tenants (id, email, stripe_customer_id, created_at)
                    VALUES (:id, :email, :stripe_customer_id, :created_at)
                """),
                {
                    "id": tenant_uuid,
                    "email": user_id,
                    "stripe_customer_id": None,
                    "created_at": datetime.now(timezone.utc),
                }
            )
        else:
            tenant_uuid = row

        # assicura un contract entry
        c = conn.execute(
            text("""
                SELECT id FROM contracts
                WHERE tenant_id = :tenant_id AND arbitrum_address = :addr
            """),
            {"tenant_id": tenant_uuid, "addr": test_wallet}
        ).scalar_one_or_none()

        if c is None:
            conn.execute(
                text("""
                    INSERT INTO contracts (id, tenant_id, arbitrum_address, status, deposit_cap_eur, created_at)
                    VALUES (:id, :tenant_id, :addr, :status, :cap, :created_at)
                """),
                {
                    "id": str(uuid.uuid4()),
                    "tenant_id": tenant_uuid,
                    "addr": test_wallet,
                    "status": "active",
                    "cap": 100000,  # 100k EUR cap fittizio
                    "created_at": datetime.now(timezone.utc),
                }
            )


def get_equity_for_user(engine: Engine, user_id: str) -> float:
    """
    Placeholder: in futuro leggeremo equity reale (da tabella accounts, PnL, ecc.).
    Per ora leggiamo da tenants/qualche tabella, oppure restituiamo un valore fisso.
    """
    # TODO: sostituire con equity reale
    # Per test: 10k
    return 10_000.0


# ======================
# PYTH / PRICE
# ======================

async def fetch_pyth_price(symbol: str) -> float:
    feed_id = PYTH_FEEDS.get(symbol)
    if not feed_id:
        raise ValueError(f"no pyth feed configured for symbol={symbol}")

    url = PYTH_HERMES_URL.rstrip("/") + "/api/latest_price_feeds"
    params = {"ids[]": [feed_id]}

    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()

    if not data:
        raise RuntimeError(f"pyth price not found for symbol={symbol}")

    item = data[0]
    price_obj = item.get("price", {})
    px = price_obj.get("price")
    expo = price_obj.get("expo", 0)

    if px is None:
        raise RuntimeError(f"pyth price missing 'price' field for {symbol}")

    # Pyth: valore = px * 10^expo (expo negativo di solito)
    price = float(px) * (10 ** float(expo))
    return price


# ======================
# SIZE & ONCHAIN (stub)
# ======================

def compute_position_size(equity: float, risk_pct: float, price: float, symbol: str) -> Dict[str, float]:
    """
    Calcola notional e quantity sulla base di:
    - equity utente
    - risk_pct (es. 0.008 = 0.8%)
    - prezzo corrente
    - max_leverage per asset class
    """
    max_lev = _max_leverage_for(symbol)
    risk_amount = equity * risk_pct
    notional = risk_amount * max_lev
    if notional <= 0 or price <= 0:
        raise ValueError("invalid notional/price for size calc")
    qty = notional / price
    return {
        "equity": equity,
        "risk_amount": risk_amount,
        "max_leverage": max_lev,
        "notional": notional,
        "qty": qty,
        "price": price,
    }


def _get_web3() -> Optional["Web3"]:
    if not (ARBITRUM_RPC_URL and RELAYER_PK and PONTE_ADDRESS):
        logging.warning("onchain execution disabled: missing ARBITRUM_RPC_URL/RELAYER_PK/PONTE_ADDRESS")
        return None
    if Web3 is None:
        logging.warning("web3 not installed, skipping onchain execution")
        return None
    w3 = Web3(Web3.HTTPProvider(ARBITRUM_RPC_URL))
    return w3


def execute_onchain_trade(symbol: str, direction: str, size: Dict[str, float]) -> Optional[str]:
    """
    STUB: qui dovrai implementare la chiamata a Ponte.sol (relay vero).
    Per ora ritorniamo un finto tx_hash se web3 non è configurato.
    """
    w3 = _get_web3()
    if w3 is None:
        # stub: nessuna tx reale, ma vogliamo comunque un ID per logging
        fake_tx = f"0xSTUB-{uuid.uuid4().hex}"
        logging.info(f"[STUB] would execute trade {symbol} {direction} size={size} -> {fake_tx}")
        return fake_tx

    acct = w3.eth.account.from_key(RELAYER_PK)

    # TODO: carica ABI Ponte (da file JSON) e costruisci la tx reale.
    # Esempio:
    # with open(os.getenv("PONTE_ABI_PATH", "/app/Ponte.json")) as f:
    #     ponte_abi = json.load(f)["abi"]
    # ponte = w3.eth.contract(address=PONTE_ADDRESS, abi=ponte_abi)
    # tx = ponte.functions.relayTrade(...).build_transaction({
    #     "from": acct.address,
    #     "nonce": w3.eth.get_transaction_count(acct.address),
    #     "chainId": CHAIN_ID,
    #     "gas": 600000,
    #     "maxFeePerGas": ...,
    #     "maxPriorityFeePerGas": ...,
    # })
    # signed = acct.sign_transaction(tx)
    # tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction).hex()
    # return tx_hash

    fake_tx = f"0xSTUB-{uuid.uuid4().hex}"
    logging.info(f"[STUB/web3] would send real tx for {symbol} {direction}, size={size} -> {fake_tx}")
    return fake_tx


# ======================
# ENTRY POINT DALLE API
# ======================

async def process_trade_intent(engine: Engine, intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Punto di ingresso chiamato da main.py /v1/trade-intent.
    1) Assicura tenant TEST_USER_001 (o quello passato)
    2) Salva il segnale in tabella signals
    3) Calcola equity e price via Pyth
    4) Calcola size
    5) (Stub) esegue on-chain via Ponte
    """
    user_id = intent.get("user_id") or "TEST_USER_001"
    symbol = intent["symbol"]
    risk_pct = float(intent["risk_pct"])

    # 1) tenant + contract entry
    test_wallet = os.getenv("TEST_USER_WALLET")  # indirizzo del wallet di laboratorio (lo associ al ponte)
    if not test_wallet:
        # puoi usare direttamente il relayer come test wallet se vuoi
        test_wallet = os.getenv("RELAYER_ADDRESS", "")
    if not test_wallet:
        raise RuntimeError("TEST_USER_WALLET or RELAYER_ADDRESS not configured")

    ensure_test_user(engine, user_id, test_wallet)

    # 2) salva segnale
    signal_id = get_or_create_signal(engine, intent)

    # 3) equity + price
    equity = get_equity_for_user(engine, user_id)
    price = await fetch_pyth_price(symbol)

    # 4) calcolo size
    size_info = compute_position_size(equity, risk_pct, price, symbol)

    # 5) esecuzione on-chain (stub o reale)
    direction = intent["direction"]
    tx_hash = execute_onchain_trade(symbol, direction, size_info)

    intent_id = str(uuid.uuid4())

    # in futuro: salva anche jobs + trade log
    return {
        "intent_id": intent_id,
        "status": "RECEIVED",
        "message": "intent accepted",
        "symbol": symbol,
        "user_id": user_id,
        "risk_pct": risk_pct,
        "equity": size_info["equity"],
        "price": size_info["price"],
        "size": size_info,
        "tx_hash": tx_hash,
        "signal_id": str(signal_id),
    }
