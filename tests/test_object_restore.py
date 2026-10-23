"""Unit tests for archive restore: restore_object and get_restore_info."""

from datetime import datetime, timezone

import pytest

from tigris_boto3_ext import get_restore_info, restore_deleted_object, restore_object
from tigris_boto3_ext._internal import create_header_injector, has_active_injector

RESTORED = 'ongoing-request="false", expiry-date="Wed, 07 Oct 2026 00:00:00 GMT"'


class TestRestoreObject:
    def test_sends_the_restore_request(self, mock_s3_client):
        mock_s3_client.restore_object.return_value = {"ResponseMetadata": {}}

        result = restore_object(mock_s3_client, "archive", "cold.txt", days=3)

        assert result == {"ResponseMetadata": {}}
        mock_s3_client.restore_object.assert_called_once_with(
            Bucket="archive", Key="cold.txt", RestoreRequest={"Days": 3}
        )
        assert mock_s3_client.meta.events.register.call_count == 1
        assert mock_s3_client.meta.events.unregister.call_count == 1

    def test_defaults_to_one_day_and_passes_the_version(self, mock_s3_client):
        restore_object(mock_s3_client, "archive", "cold.txt", version_id="v1")

        mock_s3_client.restore_object.assert_called_once_with(
            Bucket="archive", Key="cold.txt", RestoreRequest={"Days": 1}, VersionId="v1"
        )

    @pytest.mark.parametrize(
        ("args", "kwargs", "error", "message"),
        [
            (("", "cold.txt"), {}, ValueError, "bucket_name is required"),
            (("archive", ""), {}, ValueError, "key is required"),
            (("archive", "cold.txt", 0), {}, ValueError, "at least 1"),
            (("archive", "cold.txt", -2), {}, ValueError, "at least 1"),
            (("archive", "cold.txt", 1.5), {}, TypeError, "whole number of days"),
            (("archive", "cold.txt", "3"), {}, TypeError, "whole number of days"),
            (("archive", "cold.txt", True), {}, TypeError, "whole number of days"),
        ],
    )
    def test_rejects_bad_arguments(self, mock_s3_client, args, kwargs, error, message):
        with pytest.raises(error, match=message):
            restore_object(mock_s3_client, *args, **kwargs)

        mock_s3_client.restore_object.assert_not_called()

    def test_holds_the_operation_while_in_flight(self, mock_s3_client):
        seen = {}

        def restore(**_: object) -> dict:
            seen["active"] = has_active_injector(mock_s3_client, "RestoreObject")
            with pytest.raises(RuntimeError, match="already active"):
                restore_deleted_object(mock_s3_client, "archive", "cold.txt", "123")
            return {"ResponseMetadata": {}}

        mock_s3_client.restore_object.side_effect = restore

        restore_object(mock_s3_client, "archive", "cold.txt")

        assert seen["active"] is True
        assert has_active_injector(mock_s3_client, "RestoreObject") is False

    def test_releases_the_operation_when_the_call_raises(self, mock_s3_client):
        mock_s3_client.restore_object.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            restore_object(mock_s3_client, "archive", "cold.txt")

        assert has_active_injector(mock_s3_client, "RestoreObject") is False

    def test_refuses_while_a_soft_delete_restore_is_in_flight(self, mock_s3_client):
        other = create_header_injector(
            mock_s3_client, "RestoreObject", {"X-Tigris-Restore-Type": "soft-delete"}
        )
        other.register()
        try:
            with pytest.raises(RuntimeError, match="is active on this client"):
                restore_object(mock_s3_client, "archive", "cold.txt")
        finally:
            other.unregister()

        mock_s3_client.restore_object.assert_not_called()


class TestGetRestoreInfo:
    @pytest.mark.parametrize(
        ("response", "expected"),
        [
            ({"StorageClass": "GLACIER"}, {"status": "archived", "expires_at": None}),
            (
                {"StorageClass": "GLACIER", "Restore": 'ongoing-request="true"'},
                {"status": "in-progress", "expires_at": None},
            ),
            (
                {"StorageClass": "GLACIER", "Restore": RESTORED},
                {
                    "status": "restored",
                    "expires_at": datetime(2026, 10, 7, tzinfo=timezone.utc),
                },
            ),
            (
                {"Restore": 'ongoing-request="false"'},
                {"status": "restored", "expires_at": None},
            ),
            ({"ContentLength": 8}, None),
            ({"StorageClass": "STANDARD_IA"}, None),
            ({"StorageClass": "GLACIER_IR"}, None),
        ],
    )
    def test_maps_storage_class_and_restore(self, mock_s3_client, response, expected):
        mock_s3_client.head_object.return_value = {
            **response,
            "ResponseMetadata": {"HTTPHeaders": {}},
        }

        assert get_restore_info(mock_s3_client, "archive", "cold.txt") == expected

        mock_s3_client.head_object.assert_called_once_with(
            Bucket="archive", Key="cold.txt"
        )

    def test_passes_the_version(self, mock_s3_client):
        mock_s3_client.head_object.return_value = {"StorageClass": "GLACIER"}

        get_restore_info(mock_s3_client, "archive", "cold.txt", version_id="v1")

        mock_s3_client.head_object.assert_called_once_with(
            Bucket="archive", Key="cold.txt", VersionId="v1"
        )

    @pytest.mark.parametrize(
        ("bucket_name", "key", "message"),
        [
            ("", "cold.txt", "bucket_name is required"),
            ("archive", "", "key is required"),
        ],
    )
    def test_rejects_empty_arguments(self, mock_s3_client, bucket_name, key, message):
        with pytest.raises(ValueError, match=message):
            get_restore_info(mock_s3_client, bucket_name, key)

        mock_s3_client.head_object.assert_not_called()
