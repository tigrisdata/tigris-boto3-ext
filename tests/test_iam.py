"""Unit tests for the IAM helpers (boto3 IAM client wrapper)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from tigris_boto3_ext._iam import (
    DEFAULT_IAM_ENDPOINT,
    _build_iam_client,
    _policy_document,
    create_scoped_access_key,
    delete_scoped_access_key,
)


@pytest.fixture
def s3_client():
    client = MagicMock()
    client.meta.region_name = "auto"
    creds = MagicMock(access_key="ak", secret_key="sk", token=None)
    client._request_signer._credentials.get_frozen_credentials.return_value = creds
    return client


@pytest.fixture
def iam_client():
    client = MagicMock()
    client.create_access_key.return_value = {
        "AccessKey": {
            "AccessKeyId": "AKIAEXAMPLE",
            "SecretAccessKey": "supersecret",
            "UserName": "auto",
        }
    }
    client.create_policy.return_value = {
        "Policy": {"Arn": "arn:aws:iam::tigris:policy/bucket-policy"}
    }
    return client


# -- Policy document construction --


class TestPolicyDocument:
    def test_editor_grants_all_s3(self):
        doc = json.loads(_policy_document("my-bucket", "Editor"))
        stmt = doc["Statement"][0]
        assert stmt["Action"] == "s3:*"
        assert stmt["Resource"] == [
            "arn:aws:s3:::my-bucket",
            "arn:aws:s3:::my-bucket/*",
        ]
        assert stmt["Effect"] == "Allow"

    def test_readonly_only_grants_reads(self):
        doc = json.loads(_policy_document("my-bucket", "ReadOnly"))
        actions = doc["Statement"][0]["Action"]
        assert "s3:GetObject" in actions
        assert "s3:ListBucket" in actions
        assert "s3:PutObject" not in actions
        assert "s3:*" not in actions

    def test_invalid_role_raises(self):
        with pytest.raises(ValueError, match="role must be"):
            _policy_document("b", "Admin")


# -- Build client --


class TestBuildIamClient:
    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_uses_default_endpoint_and_credentials(self, mock_factory, s3_client):
        _build_iam_client(s3_client)
        mock_factory.assert_called_once()
        kwargs = mock_factory.call_args.kwargs
        assert mock_factory.call_args.args[0] == "iam"
        assert kwargs["endpoint_url"] == DEFAULT_IAM_ENDPOINT
        assert kwargs["aws_access_key_id"] == "ak"
        assert kwargs["aws_secret_access_key"] == "sk"
        assert kwargs["region_name"] == "auto"

    @patch.dict("os.environ", {"TIGRIS_IAM_ENDPOINT": "https://iam.test"})
    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_endpoint_overridable_via_env(self, mock_factory, s3_client):
        _build_iam_client(s3_client)
        assert (
            mock_factory.call_args.kwargs["endpoint_url"] == "https://iam.test"
        )


# -- create_scoped_access_key --


class TestCreateScopedAccessKey:
    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_full_provisioning_flow(self, mock_factory, s3_client, iam_client):
        mock_factory.return_value = iam_client

        result = create_scoped_access_key(
            s3_client, "ws-1-key", "ws-1", "Editor"
        )

        assert result["access_key_id"] == "AKIAEXAMPLE"
        assert result["secret_access_key"] == "supersecret"
        # Tigris IAM uses the access key id as the user handle.
        assert result["user_name"] == "AKIAEXAMPLE"
        assert result["policy_arn"] == "arn:aws:iam::tigris:policy/bucket-policy"

        iam_client.create_access_key.assert_called_once_with()
        iam_client.create_policy.assert_called_once()
        policy_kwargs = iam_client.create_policy.call_args.kwargs
        assert policy_kwargs["PolicyName"] == "ws-1-key-policy"
        assert "ws-1" in policy_kwargs["PolicyDocument"]
        iam_client.attach_user_policy.assert_called_once_with(
            UserName="AKIAEXAMPLE",
            PolicyArn="arn:aws:iam::tigris:policy/bucket-policy",
        )

    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_rolls_back_on_attach_failure(
        self, mock_factory, s3_client, iam_client
    ):
        iam_client.attach_user_policy.side_effect = RuntimeError("denied")
        mock_factory.return_value = iam_client

        with pytest.raises(RuntimeError, match="denied"):
            create_scoped_access_key(s3_client, "k", "b", "Editor")

        # Both the orphaned access key and the policy must be cleaned up.
        iam_client.delete_access_key.assert_called_once()
        iam_client.delete_policy.assert_called_once_with(
            PolicyArn="arn:aws:iam::tigris:policy/bucket-policy"
        )

    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_rolls_back_when_policy_creation_fails(
        self, mock_factory, s3_client, iam_client
    ):
        iam_client.create_policy.side_effect = RuntimeError("policy error")
        mock_factory.return_value = iam_client

        with pytest.raises(RuntimeError, match="policy error"):
            create_scoped_access_key(s3_client, "k", "b", "Editor")

        iam_client.delete_access_key.assert_called_once()
        # No policy ARN was returned, so delete_policy must not run.
        iam_client.delete_policy.assert_not_called()

    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_invalid_role_raises_before_iam_calls(
        self, mock_factory, s3_client, iam_client
    ):
        mock_factory.return_value = iam_client
        with pytest.raises(ValueError, match="role must be"):
            create_scoped_access_key(s3_client, "k", "b", "Admin")
        iam_client.create_access_key.assert_not_called()


# -- delete_scoped_access_key --


class TestDeleteScopedAccessKey:
    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_full_teardown_order(self, mock_factory, s3_client, iam_client):
        mock_factory.return_value = iam_client

        delete_scoped_access_key(
            s3_client,
            access_key_id="AKIA",
            user_name="auto",
            policy_arn="arn:policy/p",
        )

        iam_client.detach_user_policy.assert_called_once_with(
            UserName="auto", PolicyArn="arn:policy/p"
        )
        iam_client.delete_policy.assert_called_once_with(PolicyArn="arn:policy/p")
        iam_client.delete_access_key.assert_called_once_with(AccessKeyId="AKIA")

    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_continues_after_detach_failure(
        self, mock_factory, s3_client, iam_client
    ):
        iam_client.detach_user_policy.side_effect = RuntimeError("err")
        mock_factory.return_value = iam_client

        delete_scoped_access_key(
            s3_client,
            access_key_id="AKIA",
            user_name="auto",
            policy_arn="arn:policy/p",
        )

        iam_client.delete_policy.assert_called_once()
        iam_client.delete_access_key.assert_called_once()

    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_skips_detach_and_policy_delete_without_arn(
        self, mock_factory, s3_client, iam_client
    ):
        mock_factory.return_value = iam_client

        delete_scoped_access_key(
            s3_client, access_key_id="AKIA", user_name="auto"
        )

        iam_client.detach_user_policy.assert_not_called()
        iam_client.delete_policy.assert_not_called()
        iam_client.delete_access_key.assert_called_once_with(AccessKeyId="AKIA")

    @patch("tigris_boto3_ext._iam.boto3.client")
    def test_no_user_name_means_no_username_kwarg(
        self, mock_factory, s3_client, iam_client
    ):
        mock_factory.return_value = iam_client

        delete_scoped_access_key(s3_client, access_key_id="AKIA")
        iam_client.delete_access_key.assert_called_once_with(AccessKeyId="AKIA")
