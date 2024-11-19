"""PubSub target class."""

from __future__ import annotations
import logging
from typing import Optional, Dict, Any
from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_cloudpubsub.sinks import CloudPubSubSink

logger = logging.getLogger(__name__)


class TargetCloudPubSub(Target):
    """Target for Cloud Pub/Sub."""

    name = "target-cloudpubsub"
    default_sink_class = CloudPubSubSink

    config_jsonschema = th.PropertiesList(
        th.Property("project_id", th.StringType, required=True),
        th.Property("topic", th.StringType, required=True),
        th.Property("client_id", th.StringType, required=True),
        th.Property("schema_version", th.StringType, default="2.0.0"),
        th.Property("tap_version", th.StringType, default="1.0.0"),
    ).to_dict()

    def _get_sink(self, stream_name: str) -> CloudPubSubSink:
        """Return a sink for the given stream name."""
        if stream_name not in self._sinks:
            key_properties = self._schemas[stream_name].get("key_properties", [])
            sink_config = {
                "client_id": self.config["client_id"],
                "project_id": self.config["project_id"],
                "topic": self.config["topic"],
                "schema_version": self.config.get("schema_version", "2.0.0"),
                "tap_version": self.config.get("tap_version", "1.0.0")
            }
            self._sinks[stream_name] = CloudPubSubSink(
                target=self,
                stream_name=stream_name,
                schema=self._schemas[stream_name],
                key_properties=key_properties,
                config=sink_config
            )
            logger.info(f"Created sink for stream {stream_name}")
        return self._sinks[stream_name]


if __name__ == "__main__":
    TargetCloudPubSub.cli()