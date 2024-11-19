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
            "file_naming_scheme",
            th.StringType,
            description="The scheme with which output files will be named",
        ),
        th.Property(
            "auth_token",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="The path to the target output file",
        ),
    ).to_dict()

    default_sink_class = CloudPubSubSink


if __name__ == "__main__":
    TargetCloudPubSub.cli()
