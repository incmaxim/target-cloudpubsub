# sinks.py

from __future__ import annotations
import json
import logging
from concurrent import futures
from typing import Optional, Dict, Any
from datetime import datetime
import uuid

from google.cloud import pubsub_v1
from singer_sdk.sinks import BatchSink

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class CloudPubSubSink(BatchSink):
    """PubSub target sink class."""

    max_size = 1000  # Maximum number of records to process in one batch

    def __init__(self, target, stream_name, schema, key_properties, config=None):
        """Initialize sink with config."""
        super().__init__(target, stream_name, schema, key_properties)
        
        if not config:
            raise ValueError("Config must be provided")
            
        self._config = config
        self.client_id = self._config["client_id"]
        self.project_id = self._config["project_id"]
        self.topic = self._config["topic"]
        self.schema_version = self._config.get("schema_version", "2.0.0")
        self.tap_version = self._config.get("tap_version", "1.0.0")
        
        # Initialize PubSub client
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic)
        
        logger.info(f"Initialized sink for stream {stream_name}")

    @property
    def key_properties(self):
        """Get key properties."""
        return self._key_properties

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        # Add metadata
        record["_sdc_client_id"] = self.client_id
        record["_sdc_schema_version"] = self.schema_version
        record["_sdc_tap_version"] = self.tap_version
        
        # Add record to the batch
        self.records.append(record)
        
        logger.info(f"Processing record for stream {self.stream_name} with client_id: {self.client_id}")

    def process_batch(self, context: dict) -> None:
        """Process a batch of records."""
        if not self.records:
            return
            
        publish_futures = []
        
        try:
            for i, record in enumerate(self.records):
                try:
                    # Add metadata fields if not present
                    if "_sdc_schema_version" not in record:
                        record["_sdc_schema_version"] = self.schema_version
                    if "_sdc_tap_version" not in record:
                        record["_sdc_tap_version"] = self.tap_version
                    
                    # Convert record to JSON string
                    data = json.dumps(record, cls=DateTimeEncoder).encode("utf-8")
                    
                    # Publish message with metadata
                    future = self.publisher.publish(
                        self.topic_path,
                        data,
                        client_id=str(self.client_id),
                        stream_name=self.stream_name
                    )
                    publish_futures.append(future)
                    
                    if i % 100 == 0:  # Log progress every 100 records
                        logger.debug(f"Processed {i}/{len(self.records)} records for stream {self.stream_name}")
                    
                except Exception as e:
                    logger.error(f"Error preparing message {i}/{len(self.records)}: {str(e)}")
                    raise
            
            # Wait for all messages to be published
            futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
            logger.info(f"Published {len(publish_futures)} messages for stream {self.stream_name}")
                
        except Exception as e:
            logger.error(
                f"Error in batch processing for stream {self.stream_name} "
                f"with client_id {self.client_id}: {str(e)}"
            )
            raise
        finally:
            self.records.clear()

    def _convert_datetime_to_string(self, obj):
        """Recursively convert datetime objects to ISO format strings."""
        if isinstance(obj, dict):
            return {key: self._convert_datetime_to_string(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_datetime_to_string(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj