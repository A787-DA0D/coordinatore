from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import os
import logging
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Float, DateTime, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime, timezone
from google.cloud import tasks_v2
import json
import httpx

app = FastAPI(title="Coscienza Coordinatore")
PYTH_HERMES_URL = os.getenv("PYTH_HERMES_URL", "https://hermes.pyth.network")


def _project_id():
    return (os.getenv('GOOGLE_CLOUD_PROJECT') or
            os.getenv('GCP_PROJECT') or
            os.getenv('PROJECT_ID') or
            os.getenv('COSCIENZA_PROJECT_ID'))


SQL_USER = os.getenv("SQL_DB_USER")
SQL_PASS = os.getenv("SQL_DB_PASS")
SQL_NAME = os.getenv("SQL_DB_NAME")
INSTANCE_CONNECTION_NAME = os.getenv("SQL_INSTANCE_CONN")  # es. new-cerbero:europe-west8:coscienza-db

HOST = f"/cloudsql/{INSTANCE_CONNECTION_NAME}"
DB_URL = f"postgresql+psycopg://{SQL_USER}:{SQL_PASS}@/{SQL_NAME}?host={HOST}"

engine = create_engine(DB_URL, pool_pre_ping=True)
metadata = MetaData()

tenants = Table(
    "tenants", metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("email", String(255), nullable=False, unique=True),
    Column("stripe_customer_id", String(255)),
    Column("created_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False),
)

contracts = Table(
    "contracts", metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("tenant_id", UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False),
    Column("arbitrum_address", String(64), nullable=False),
    Column("status", String(32), nullable=False, default="inactive"),
    Column("deposit_cap_eur", Integer, nullable=False, default=999),
    Column("created_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False),
)

signals = Table(
    "signals", metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("symbol", String(16), nullable=False),
    Column("score", Float, nullable=False),
    Column("p", Float, nullable=False),
    Column("e", Float, nullable=False),
    Column("r", Float, nullable=False),
    Column("v", Float, nullable=False),
    Column("created_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False),
)

jobs = Table(
    "jobs", metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("tenant_id", UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False),
    Column("signal_id", UUID(as_uuid=True), ForeignKey("signals.id")),
    Column("status", String(32), nullable=False, default="pending"),
    Column("created_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False),
)

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.post("/tick", response_class=PlainTextResponse)
async def tick():
    try:
        REGION_TASKS = "europe-west1"
        QUEUE_ID = "coordinatore-dispatch"
        SERVICE_URL = os.getenv("SERVICE_URL")
        SA_TASKS = os.getenv("SA_TASKS_EMAIL")
        PROJECT = _project_id()
        if not PROJECT:
            raise RuntimeError("Missing project id env")

        # 1) Radar
        cand = mock_radar(SYMBOLS)

        # 2) Pyth (mock)
        prices = mock_pyth_prices(cand)

        # 3) Coscienza (mock)
        scores = mock_coscienza_score(cand, prices)

        # 4) Rank & risk
        picks = [(sym, s) for sym, s in top_signals(scores, k=3) if risk_manager(s)]
        if not picks:
            return "tick-queued(0 tenants, 0 signals)"

        # 5) Carica tenants attivi dal DB (se vuoto, crea 3 dummy)
        with engine.begin() as conn:
            cnt = conn.execute(text("SELECT COUNT(*) FROM tenants")).scalar_one()
            if cnt == 0:
                conn.execute(text("INSERT INTO tenants (id, email) VALUES (:id, :email)"),
                             [{"id": str(uuid.uuid4()), "email": f"demo{i}@cerbero.ai"} for i in range(1,4)])
            rows = conn.execute(text("SELECT id, email FROM tenants ORDER BY created_at ASC LIMIT 3")).mappings().all()

        # 6) Enqueue 1 job per tenant con il miglior segnale
        best_sym, best = picks[0]
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(PROJECT, REGION_TASKS, QUEUE_ID)
        url = SERVICE_URL.rstrip("/") + "/worker"

        enq = 0
        for r in rows:
            payload = {
                "tenant_id": str(r["id"]),
                "email": r["email"],
                "symbol": best_sym,
                "p": best["p"],
                "e": best["e"],
                "r": best["r"],
                "v": best["v"],
                "ts": datetime.now(timezone.utc).isoformat()
            }
            task = {
                "http_request": {
                    "http_method": tasks_v2.HttpMethod.POST,
                    "url": url,
                    "oidc_token": {
                        "service_account_email": SA_TASKS,
                        "audience": SERVICE_URL
                    },
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(payload).encode("utf-8"),
                }
            }
            client.create_task(request={"parent": parent, "task": task})
            enq += 1

        return f"tick-queued({enq} tenants, best={best_sym})"

    except Exception as e:
        logging.exception("tick failed")
        return PlainTextResponse(f"tick-error: {e}", status_code=500)


@app.get("/dbtest", response_class=PlainTextResponse)
def dbtest():
    with engine.connect() as conn:
        one = conn.execute(text("SELECT 1")).scalar_one()
        return f"db:{one}"

@app.post("/migrate", response_class=PlainTextResponse)
def migrate():
    metadata.create_all(engine)
    return "migrated"


@app.post("/worker", response_class=PlainTextResponse)
def worker():
    # qui in futuro processeremo il job (trade singolo utente)
    return "worker-ack"


# ====== MOCK INTELLIGENCE LAYER (Radar → Pyth → Coscienza → Risk) ======

SYMBOLS = ["EURUSD","GBPUSD","USDJPY","USDCHF","USDCAD",
           "AUDUSD","NZDUSD","XAUUSD","XAGUSD","WTI",
           "DAX","SPX","NDX","BTCUSD","ETHUSD",
           "SOLUSD","ADAUSD","LINKUSD","AVAXUSD","DOGEUSD"]

def mock_radar(symbols):
    # Filtra 3-6 simboli "interessanti" a caso (qui deterministico su primi 5)
    return symbols[:5]

def mock_pyth_prices(symbols):
    # Mock prezzi: numeri fissi (in futuro: query Pyth)
    base = {
        "EURUSD":1.07,"GBPUSD":1.25,"USDJPY":150.0,"XAUUSD":1995.0,"BTCUSD":65000.0
    }
    return {s: base.get(s, 1.0) for s in symbols}

def mock_coscienza_score(symbols, prices):
    # Score fittizio: (price % 7)/7 → [0..1)
    out = {}
    for s in symbols:
        p = float(prices.get(s,1.0))
        score = (p % 7.0) / 7.0
        out[s] = {
            "p": min(0.99, 0.50 + score*0.40),  # probability
            "e": min(0.99, 0.60 + score*0.30),  # edge
            "r": 0.9,                            # risk (placeholder: Manager calcola V/R reali)
            "v": 0.9
        }
    return out

def risk_manager(signal):
    # veto semplice: se p*e < 0.6 scarta
    se = signal["p"] * signal["e"]
    return se >= 0.6

def top_signals(scores, k=3):
    # rank per p*e, prendi top-k
    ranked = sorted(scores.items(), key=lambda kv: (kv[1]["p"]*kv[1]["e"]), reverse=True)
    return ranked[:k]


# --- ARBI CHECK ENDPOINT ---
import os, json, urllib.request
from fastapi import HTTPException

@app.get("/arbi/block")
def arbi_block():
    url = os.environ.get("ALCHEMY_HTTP_ARBITRUM")
    if not url:
        raise HTTPException(status_code=500, detail="ALCHEMY_HTTP_ARBITRUM not set")
    body = {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"content-type":"application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        payload = json.loads(r.read().decode())
    res = payload.get("result")
    height = int(res, 16) if isinstance(res, str) else None
    return {"ok": True, "height": height, "result": res}



from fastapi import HTTPException, Query

@app.get("/pyth/price")
async def pyth_price(id: str = Query(..., description="Pyth price feed id (0x...)")):
    """Chiama Hermes con un singolo feed id e ritorna il last price."""
    if not id.startswith("0x") or len(id) < 10:
        raise HTTPException(status_code=400, detail="invalid feed id")
    url = PYTH_HERMES_URL.rstrip("/") + "/api/latest_price_feeds"
    params = {"ids[]": [id]}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
        if not data:
            return {"ok": False, "id": id, "reason": "not found"}
        item = data[0]
        price = item.get("price", {})
        px, expo = price.get("price"), price.get("expo", 0)
        out = float(px) * (10 ** float(expo)) if px is not None else None
        return {"ok": True, "id": id, "price": out, "raw": item}
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"hermes error: {e}") from e


from fastapi import HTTPException, Query

@app.get("/pyth/price")
async def pyth_price(id: str = Query(..., description="Pyth price feed id (0x...)")):
    """Chiama Hermes con un singolo feed id e ritorna il last price."""
    try:
        url_base = (globals().get("PYTH_HERMES_URL")
                    or os.environ.get("PYTH_HERMES_URL")
                    or "https://hermes.pyth.network")
        if not id.startswith("0x") or len(id) < 10:
            raise HTTPException(status_code=400, detail="invalid feed id")
        url = url_base.rstrip("/") + "/api/latest_price_feeds"
        params = {"ids[]": [id]}
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
        if not data:
            return {"ok": False, "id": id, "reason": "not found"}
        item = data[0]
        price = item.get("price", {})
        px, expo = price.get("price"), price.get("expo", 0)
        out = float(px) * (10 ** float(expo)) if px is not None else None
        return {"ok": True, "id": id, "price": out, "raw": item}
    except httpx.HTTPError as e:
        logging.exception("hermes http error")
        raise HTTPException(status_code=502, detail=f"hermes error: {e}") from e
    except Exception as e:
        logging.exception("pyth_price failed")
        raise HTTPException(status_code=500, detail=f"internal error: {e}") from e
