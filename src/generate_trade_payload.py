import json
import datetime
import uuid
import random

# Enterprise Reference Data
COUNTERPARTIES = ["JPM_LDN", "GS_NY", "MS_HK", "DB_FRA", "BARC_LDN"]
TRADERS = ["T-1023", "T-4402", "T-5591", "ALGO-BOT-01"]
VENUES = ["BLOOMBERG", "REUTERS", "VOICE", "ELECTRONIC"]

def generate_trade_payload(
        trade_id=None,
        version=1,
        maturity_date_str=None,
        price=100.0,
        amount=1000,
        symbol="SPY",
        status="NEW"
):
    """
    Generates a realistic financial trade payload.

    Args:
        trade_id (str): Optional. If None, generates a new UUID.
        version (int): The version of the trade (1, 2, 3...).
        maturity_date_str (str): 'YYYY-MM-DD'. If None, defaults to future (Valid).
        price (float): The execution price.
        amount (int): The quantity traded.
        symbol (str): Ticker symbol (e.g., SPY).
        status (str): Trade status (NEW, AMEND, CANCEL).
    """

    # 1. Identity Management
    if not trade_id:
        trade_id = f"TRD-{uuid.uuid4().hex[:8].upper()}"

    # 2. Maturity Logic (Default to 1 year future if not specified)
    if not maturity_date_str:
        today = datetime.date.today()
        maturity_date_str = (today + datetime.timedelta(days=365)).isoformat()

    # 3. Timestamping (UTC)
    timestamp = datetime.datetime.utcnow().isoformat()

    # 4. Construct Payload
    trade = {
        "trade_id": trade_id,
        "version": str(version),
        "status": status,
        "trade_type": random.choice(["BUY", "SELL"]),
        "instrument_type": "ETF",
        "ccy_pair": "USD",
        "symbol": symbol,
        "amount": float(amount),
        "price": float(price),
        "trade_date": datetime.date.today().isoformat(),
        "maturity_date": maturity_date_str, # Critical for 'Expired' vs 'Reject' rules
        "timestamp": timestamp,
        "counterparty": random.choice(COUNTERPARTIES),
        "trader_id": random.choice(TRADERS),
        "execution_venue": random.choice(VENUES)
    }

    return json.dumps(trade).encode("utf-8")