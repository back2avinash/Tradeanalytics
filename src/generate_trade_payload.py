import json
import datetime

def generate_trade_payload(trade_id, version, maturity_days_offset):
    """
    Generates a trade dictionary and converts it to bytes for transmission.
    """
    maturity_date = (datetime.date.today() + datetime.timedelta(days=maturity_days_offset)).isoformat()

    trade = {
        "trade_id": trade_id,
        "version": version,
        "maturity_date": maturity_date,
        "timestamp": datetime.datetime.utcnow().isoformat()
    }

    return json.dumps(trade).encode("utf-8")