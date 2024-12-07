from __future__ import annotations
import json
from datetime import datetime
from concurrent import futures
from google.cloud import pubsub_v1
from singer_sdk.sinks import BatchSink
from singer_sdk.target_base import Target
import uuid

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class CloudPubSubSink(BatchSink):
    """PubSub target sink class."""

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(
            self.config["project_id"], self.config["topic"]
        )

    def process_batch(self, context: dict) -> None:
        batch_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        batch_id = f"{self.config.get('batch_id_prefix', '')}_{batch_timestamp}_{str(uuid.uuid4())[:8]}"
        
        res = []
        for record in context["records"]:
            enriched_message = {
                "client_id": self.config.get("client_id"),
                "source": self.config.get("source"),
                "schema_version": self.config.get("schema_version", "2.0.0"),
                "payload": record,
                "metadata": {
                    "batch_id": batch_id,
                    "job_id": self.config.get("job_id"),
                    "stream": self.stream_name,
                    "timestamp": datetime.utcnow().isoformat()
                }
            }
            
            message_data = json.dumps(enriched_message, cls=DateTimeEncoder).encode("utf-8")
            fut = self.publisher.publish(self.topic_path, message_data)
            res.append(fut)

        futures.wait(res, return_when=futures.ALL_COMPLETED)
