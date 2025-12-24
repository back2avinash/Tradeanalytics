from google.cloud import pubsub_v1
"""
This module handles the connection to Google Cloud. It wraps the Pub/Sub client so it can 
be reused across different scripts.
"""
class PubSubManager:
    def __init__(self, project_id, topic_id):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish_message(self, data):
        """
        Publishes data to the configured Pub/Sub topic.
        """
        future = self.publisher.publish(self.topic_path, data)
        # Ensure the message is sent
        message_id = future.result()
        return message_id