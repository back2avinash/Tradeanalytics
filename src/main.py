import time
import random
import yfinance as yf
import google.auth
import datetime
from generate_trade_payload import generate_trade_payload
from pubsub_manager import PubSubManager

# --- CONFIGURATION ---
TOPIC_ID = "trade-events"
SYMBOL = "SPY"  # S&P 500 ETF (Real Data Source)

def get_real_market_data():
    """
    Fetches the last trading day's minute-level data for SPY.
    """
    print(f"📥 Fetching real market data for {SYMBOL} from Yahoo Finance...")
    try:
        # Fetch 1 day of data at 1-minute intervals
        df = yf.download(SYMBOL, period="1d", interval="1m", progress=False)

        if df.empty:
            print("⚠️ No data found (Market might be closed). Generating mock prices.")
            return [{"price": 450.0 + i*0.5, "amount": 1000} for i in range(100)]

        # Convert to list of dicts
        market_data = []
        for index, row in df.iterrows():
            # Handle standard pandas series vs scalar
            price = float(row['Close'].iloc[0]) if hasattr(row['Close'], 'iloc') else float(row['Close'])
            volume = int(row['Volume'].iloc[0]) if hasattr(row['Volume'], 'iloc') else int(row['Volume'])

            market_data.append({
                "price": round(price, 2),
                "amount": volume,
                "timestamp": index
            })
        return market_data
    except Exception as e:
        print(f"⚠️ Data fetch error: {e}. Using fallback data.")
        return [{"price": 450.0, "amount": 1000} for _ in range(50)]

def run_simulation():
    # 1. Setup Pub/Sub Connection
    try:
        _, project_id = google.auth.default()
    except:
        project_id = "trade-analytics-481714" # Hardcoded fallback

    ps_manager = PubSubManager(project_id, TOPIC_ID)

    # 2. Load Real Data
    ticks = get_real_market_data()
    print(f"🚀 Starting Enterprise Data Simulation: {len(ticks)} ticks loaded.")

    # 3. Simulation Loop
    for i, tick in enumerate(ticks):

        # --- CASE A: STANDARD VALID TRADES ---
        # 90% of traffic is normal
        valid_payload = generate_trade_payload(
            price=tick['price'],
            amount=tick['amount'],
            symbol=SYMBOL,
            version=1,
            status="NEW"
        )
        ps_manager.publish_message(valid_payload)
        print(f"[{i}] ✅ Sent Valid Trade: ${tick['price']}")

        # --- CASE B: VERSIONING LOGIC (Amendments) ---
        # "Replace trades with the same version" or "Reject lower version"
        if i % 10 == 0:
            test_id = f"VER-TEST-{i}"

            # Step 1: Send Version 2 (The "Current" Truth)
            v2_payload = generate_trade_payload(trade_id=test_id, version=2, price=tick['price'], status="AMEND")
            ps_manager.publish_message(v2_payload)
            print(f"   ➤ Sent {test_id} (Version 2) - Expect Update")

            time.sleep(0.2)

            # Step 2: Send Version 1 (The "Late" Packet)
            # Business Rule: "Reject trades with a lower version than existing"
            v1_payload = generate_trade_payload(trade_id=test_id, version=1, price=tick['price'], status="NEW")
            ps_manager.publish_message(v1_payload)
            print(f"   ⚠️ Sent {test_id} (Version 1) - Expect REJECTION (Lower Version)")

        # --- CASE C: MATURITY RULES ---
        if i % 15 == 0:
            # 1. "Reject trades with a maturity date earlier than today"
            # This simulates a user trying to book a new trade with a bad date
            past_date_reject = (datetime.date.today() - datetime.timedelta(days=2)).isoformat()
            reject_payload = generate_trade_payload(
                maturity_date_str=past_date_reject,
                price=tick['price'],
                status="NEW" # Intent is to create NEW trade
            )
            ps_manager.publish_message(reject_payload)
            print(f"   ⛔ Sent NEW Trade with Past Maturity ({past_date_reject}) -> Expect REJECTION")

            # 2. "Mark trades as expired if the maturity date has passed"
            # This simulates processing a valid historical record
            # (In a real stream, this distinction depends on the pipeline logic interpreting 'status')
            past_date_expire = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
            expire_payload = generate_trade_payload(
                maturity_date_str=past_date_expire,
                price=tick['price'],
                status="EXPIRED_RECORD" # Intent is history loading
            )
            ps_manager.publish_message(expire_payload)
            print(f"   📉 Sent HISTORICAL Trade ({past_date_expire}) -> Expect 'EXPIRED' Status")

        # Flow Control
        time.sleep(0.5)

if __name__ == "__main__":
    run_simulation()