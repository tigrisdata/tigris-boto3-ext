"""Unit tests for helper functions in helpers.py."""

from unittest.mock import MagicMock, patch

from tigris_boto3_ext import delete_bucket


class TestDeleteBucket:
    def test_default_no_force(self):
        """Without force, just calls boto3's delete_bucket."""
        client = MagicMock()
        delete_bucket(client, "my-bucket")
        client.delete_bucket.assert_called_once_with(Bucket="my-bucket")

    def test_force_registers_header_injector(self):
        """With force=True, registers the Tigris-Force-Delete header injector."""
        client = MagicMock()
        injector = MagicMock()
        with patch(
            "tigris_boto3_ext.helpers.create_header_injector", return_value=injector
        ) as mock_factory:
            delete_bucket(client, "my-bucket", force=True)

        mock_factory.assert_called_once_with(
            client, "DeleteBucket", {"Tigris-Force-Delete": "true"}
        )
        injector.register.assert_called_once()
        injector.unregister.assert_called_once()
        client.delete_bucket.assert_called_once_with(Bucket="my-bucket")

    def test_force_unregisters_on_error(self):
        """The header injector must be unregistered even if delete_bucket raises."""
        client = MagicMock()
        client.delete_bucket.side_effect = RuntimeError("boom")
        injector = MagicMock()
        with patch(
            "tigris_boto3_ext.helpers.create_header_injector", return_value=injector
        ):
            try:
                delete_bucket(client, "my-bucket", force=True)
            except RuntimeError:
                pass
        injector.unregister.assert_called_once()
