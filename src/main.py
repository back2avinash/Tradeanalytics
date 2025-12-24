import google.auth
from generate_trade_payload import generate_trade_payload
from pubsub_manager import PubSubManager

def run_simulation():
    # 1. Automatically detect the Project ID from the environment
    _, project_id = google.auth.default()

    # 2. Define Topic ID (consistent across environments)
    TOPIC_ID = "trade-events"

    # Initialize the GCP client with discovered project_id
    ps_manager = PubSubManager(project_id, TOPIC_ID)

    # Define the mock events
    trade_events = [
        generate_trade_payload("T1", 1, 10),  # Valid
        generate_trade_payload("T2", 1, -5),  # Maturity in past (Reject)
        generate_trade_payload("T1", 2, 10)   # New version (Update)
    ]

    print(f"Starting publication in project {project_id} to {TOPIC_ID}...")

    for event in trade_events:
        try:
            msg_id = ps_manager.publish_message(event)
            print(f"Published message ID: {msg_id}")
        except Exception as e:
            print(f"Failed to publish: {e}")

if __name__ == "__main__":
    run_simulation()