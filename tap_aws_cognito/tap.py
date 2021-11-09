"""AwsCognito tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_aws_cognito.streams import UsersStream

STREAM_TYPES = [
    UsersStream
]


class TapAwsCognito(Tap):
    """AwsCognito tap class."""
    name = "tap-aws-cognito"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "pool_id",
            th.StringType,
            required=True,
            description="Cognito Pool Id"
        )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
