"""Stream type classes for tap-aws-cognito."""

from typing import Any, Dict, Iterable, Optional

from botocore.paginate import Paginator

from tap_aws_cognito.client import AwsCognitoStream


class UsersStream(AwsCognitoStream):
    """Define custom stream."""
    name = "Users"
    primary_keys = ["Username"]
    replication_key = None

    @staticmethod
    def _get_user_attribute(attributes, attribute_name: str) -> str:
        """Returns value from attributes array or none"""

        return ([item for item in attributes if item.get(
            'Name') == attribute_name] or [{'Value': None}])[0]['Value']

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """

        row['Email'] = self._get_user_attribute(row['Attributes'], 'email')
        row['EmailVerified'] = self._get_user_attribute(
            row['Attributes'], 'email_verified') == 'true'

        return row

    def get_pages(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        paginator: Paginator = self._client.get_paginator('list_users')
        page_iterator = paginator.paginate(
            UserPoolId=self.config.get('pool_id'), PaginationConfig={'PageSize': self._page_size})

        for page in page_iterator:
            yield page

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for page in self.get_pages(context):
            for record in page.get('Users'):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record
