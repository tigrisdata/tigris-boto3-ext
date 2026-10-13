"""Unit tests for bucket creation options."""

import pytest

from tigris_boto3_ext import TigrisBucketOptions, create_bucket, with_bucket_options

EVENT_NAME = "before-sign.s3.CreateBucket"


def _injected_headers(mock_s3_client, mock_request_class):
    event_name, handler = mock_s3_client.meta.events.register.call_args[0]
    assert event_name == EVENT_NAME
    request = mock_request_class()
    handler(request)
    return request.headers


class TestTigrisBucketOptions:
    @pytest.mark.parametrize(
        ("options", "expected"),
        [
            (
                {"default_storage_class": "STANDARD_IA"},
                {"X-Amz-Storage-Class": "STANDARD_IA"},
            ),
            ({"regions": "iad"}, {"X-Tigris-Regions": "iad"}),
            ({"regions": ["iad", "sjc"]}, {"X-Tigris-Regions": "iad,sjc"}),
            ({"regions": ("usa",)}, {"X-Tigris-Regions": "usa"}),
            (
                {"public_list_objects": True},
                {"X-Amz-Acl-Public-List-Objects-Enabled": "true"},
            ),
            (
                {"public_list_objects": False},
                {"X-Amz-Acl-Public-List-Objects-Enabled": "false"},
            ),
            (
                {
                    "default_storage_class": "GLACIER",
                    "regions": "eur",
                    "public_list_objects": False,
                },
                {
                    "X-Amz-Storage-Class": "GLACIER",
                    "X-Tigris-Regions": "eur",
                    "X-Amz-Acl-Public-List-Objects-Enabled": "false",
                },
            ),
        ],
    )
    def test_injects_the_requested_headers(
        self, mock_s3_client, mock_request_class, options, expected
    ):
        with TigrisBucketOptions(mock_s3_client, **options):
            assert _injected_headers(mock_s3_client, mock_request_class) == expected

        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_no_options_registers_nothing(self, mock_s3_client):
        with TigrisBucketOptions(mock_s3_client):
            pass

        mock_s3_client.meta.events.register.assert_not_called()
        mock_s3_client.meta.events.unregister.assert_not_called()

    @pytest.mark.parametrize("storage_class", ["standard", "DEEP_ARCHIVE", ""])
    def test_rejects_unknown_storage_class(self, mock_s3_client, storage_class):
        with pytest.raises(ValueError, match="default_storage_class must be one of"):
            TigrisBucketOptions(mock_s3_client, default_storage_class=storage_class)

        mock_s3_client.meta.events.register.assert_not_called()

    @pytest.mark.parametrize("regions", ["", [], ["", " "]])
    def test_rejects_empty_regions(self, mock_s3_client, regions):
        with pytest.raises(ValueError, match="at least one region"):
            TigrisBucketOptions(mock_s3_client, regions=regions)

    @pytest.mark.parametrize("value", ["true", 1, 0])
    def test_rejects_non_bool_public_list_objects(self, mock_s3_client, value):
        with pytest.raises(TypeError, match="must be a bool"):
            TigrisBucketOptions(mock_s3_client, public_list_objects=value)


class TestWithBucketOptionsDecorator:
    def test_wraps_function_in_bucket_options(self, mock_s3_client, mock_request_class):
        @with_bucket_options(default_storage_class="STANDARD_IA", regions="iad")
        def make_bucket(client, name):
            headers = _injected_headers(client, mock_request_class)
            client.create_bucket(Bucket=name)
            return headers

        headers = make_bucket(mock_s3_client, "my-bucket")

        assert headers == {
            "X-Amz-Storage-Class": "STANDARD_IA",
            "X-Tigris-Regions": "iad",
        }
        mock_s3_client.create_bucket.assert_called_once_with(Bucket="my-bucket")
        mock_s3_client.meta.events.unregister.assert_called_once()


class TestCreateBucketHelper:
    def test_public_access_sets_the_acl(self, mock_s3_client):
        mock_s3_client.create_bucket.return_value = {"Location": "/my-bucket"}

        result = create_bucket(mock_s3_client, "my-bucket", access="public")

        assert result == {"Location": "/my-bucket"}
        mock_s3_client.create_bucket.assert_called_once_with(
            Bucket="my-bucket", ACL="public-read"
        )
        mock_s3_client.meta.events.register.assert_not_called()

    @pytest.mark.parametrize("access", [None, "private"])
    def test_private_access_sends_no_acl(self, mock_s3_client, access):
        create_bucket(mock_s3_client, "my-bucket", access=access)

        mock_s3_client.create_bucket.assert_called_once_with(Bucket="my-bucket")

    def test_options_are_injected_and_kwargs_passed_through(
        self, mock_s3_client, mock_request_class
    ):
        create_bucket(
            mock_s3_client,
            "my-bucket",
            access="public",
            default_storage_class="STANDARD_IA",
            regions=["iad", "sjc"],
            public_list_objects=True,
            ObjectLockEnabledForBucket=True,
        )

        mock_s3_client.create_bucket.assert_called_once_with(
            Bucket="my-bucket", ACL="public-read", ObjectLockEnabledForBucket=True
        )
        assert _injected_headers(mock_s3_client, mock_request_class) == {
            "X-Amz-Storage-Class": "STANDARD_IA",
            "X-Tigris-Regions": "iad,sjc",
            "X-Amz-Acl-Public-List-Objects-Enabled": "true",
        }
        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_unregisters_when_create_bucket_raises(self, mock_s3_client):
        mock_s3_client.create_bucket.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            create_bucket(mock_s3_client, "my-bucket", regions="iad")

        mock_s3_client.meta.events.unregister.assert_called_once()

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"access": "public-read"}, "access must be 'public' or 'private'"),
            ({"access": "public", "ACL": "private"}, "either access or ACL"),
            ({"public_list_objects": True}, "requires access='public'"),
            (
                {"access": "private", "public_list_objects": True},
                "requires access='public'",
            ),
            ({"default_storage_class": "COLD"}, "default_storage_class must be one of"),
        ],
    )
    def test_rejects_invalid_combinations(self, mock_s3_client, kwargs, message):
        with pytest.raises(ValueError, match=message):
            create_bucket(mock_s3_client, "my-bucket", **kwargs)

        mock_s3_client.create_bucket.assert_not_called()
        mock_s3_client.meta.events.register.assert_not_called()

    def test_rejects_empty_bucket_name(self, mock_s3_client):
        with pytest.raises(ValueError, match="bucket_name is required"):
            create_bucket(mock_s3_client, "")

        mock_s3_client.create_bucket.assert_not_called()
