"""PubSub target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_cloudpubsub.sinks import (
    CloudPubSubSink,
)


class TargetCloudPubSub(Target):
    """Sample target for PubSub."""

    name = "target-pubsub"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "project_id",
            th.StringType,
            description="The name of the GCP project to write to",
        ),
        th.Property(
            "topic",
            th.StringType,
            description="The name of the PubSub topic to write to",
        ),
        th.Property(
            "client_id",
            th.StringType,
            description="Client identifier (usually email)",
        ),
        th.Property(
            "source",
            th.StringType,
            description="Source system identifier",
        ),
        th.Property(
            "schema_version",
            th.StringType,
            default="2.0.0",
            description="Schema version",
        ),
        th.Property(
            "job_id",
            th.StringType,
            description="Unique job identifier",
        ),
        th.Property(
            "batch_id_prefix",
            th.StringType,
            description="Prefix for batch ID",
        ),
    ).to_dict()

    default_sink_class = CloudPubSubSink


if __name__ == "__main__":
    TargetCloudPubSub.cli()
