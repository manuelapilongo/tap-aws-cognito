"""REST client handling, including AwsCognitoStream base class."""

from pathlib import Path
from typing import Any, Dict, Optional, Union

from boto3 import client
from singer.schema import Schema
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import Stream

MAX_PAGE_SIZE = 60
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AwsCognitoStream(Stream):
    """AwsCognito stream class."""

    _page_size = MAX_PAGE_SIZE
    _client: Any

    @property
    def schema_filepath(self) -> str:
        return SCHEMAS_DIR / f"{self.name}.json"

    def __init__(
        self,
        tap: TapBaseClass,
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, Any], Schema]] = None
    ) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            path: URL path for this entity stream.
        """
        super().__init__(name=name, schema=schema, tap=tap)

        self._client = client('cognito-idp')
