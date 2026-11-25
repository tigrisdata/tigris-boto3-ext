"""Unit tests for access key scope."""

from unittest.mock import MagicMock

import pytest

from tigris_boto3_ext import (
    TigrisAccessKeyScope,
    create_access_key,
    with_access_key_scope,
)
from tigris_boto3_ext._internal import create_header_injector, has_active_injector

EVENT = "before-sign.iam.CreateAccessKey"


@pytest.fixture
def iam_client():
    client = MagicMock()
    client.meta.service_model.service_name = "iam"
    return client


def _created(scope="no_default_allow"):
    headers = {"x-tigris-access-key-scope": scope} if scope else {}
    return {
        "AccessKey": {"AccessKeyId": "tid_example", "UserName": "ci-deploy"},
        "ResponseMetadata": {"HTTPHeaders": headers},
    }


def _sent_headers(client, mock_request_class):
    event_name, handler = client.meta.events.register.call_args[0]
    request = mock_request_class()
    handler(request)
    return event_name, request.headers


class TestTigrisAccessKeyScope:
    @pytest.mark.parametrize("scope", ["no_default_allow", "standard"])
    def test_registers_the_scope_header_on_iam(
        self, iam_client, mock_request_class, scope
    ):
        with TigrisAccessKeyScope(iam_client, scope):
            assert _sent_headers(iam_client, mock_request_class) == (
                EVENT,
                {"X-Tigris-Access-Key-Scope": scope},
            )

        iam_client.meta.events.unregister.assert_called_once()

    def test_defaults_to_no_default_allow(self, iam_client, mock_request_class):
        with TigrisAccessKeyScope(iam_client):
            assert _sent_headers(iam_client, mock_request_class)[1] == {
                "X-Tigris-Access-Key-Scope": "no_default_allow"
            }

    @pytest.mark.parametrize("scope", ["NO_DEFAULT_ALLOW", "bogus", ""])
    def test_rejects_other_scopes(self, iam_client, scope):
        with pytest.raises(ValueError, match="scope must be"):
            TigrisAccessKeyScope(iam_client, scope)

        iam_client.meta.events.register.assert_not_called()


class TestWithAccessKeyScope:
    def test_wraps_function_in_the_scope(self, iam_client, mock_request_class):
        @with_access_key_scope("no_default_allow")
        def registered(client, name):
            return _sent_headers(client, mock_request_class)

        assert registered(iam_client, "ci-deploy") == (
            EVENT,
            {"X-Tigris-Access-Key-Scope": "no_default_allow"},
        )
        iam_client.meta.events.unregister.assert_called_once()

    def test_rejects_other_scopes_at_decoration(self):
        with pytest.raises(ValueError, match="scope must be"):
            with_access_key_scope("bogus")


class TestCreateAccessKey:
    def test_creates_with_the_scope_and_reports_the_applied_one(
        self, iam_client, mock_request_class
    ):
        def create(**_: object) -> dict:
            assert _sent_headers(iam_client, mock_request_class) == (
                EVENT,
                {"X-Tigris-Access-Key-Scope": "no_default_allow"},
            )
            return _created("no_default_allow")

        iam_client.create_access_key.side_effect = create

        result = create_access_key(iam_client, "ci-deploy")

        iam_client.create_access_key.assert_called_once_with(UserName="ci-deploy")
        assert result["AccessKey"]["AccessKeyId"] == "tid_example"
        assert result["Scope"] == "no_default_allow"
        assert has_active_injector(iam_client, "CreateAccessKey") is False

    def test_standard_scope_and_extra_arguments(self, iam_client):
        iam_client.create_access_key.return_value = _created("standard")

        result = create_access_key(iam_client, "ci-deploy", "standard")

        assert result["Scope"] == "standard"

    def test_scope_is_none_without_the_echo(self, iam_client):
        iam_client.create_access_key.return_value = _created(None)

        assert create_access_key(iam_client, "ci-deploy")["Scope"] is None

    def test_unregisters_when_the_call_raises(self, iam_client):
        iam_client.create_access_key.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            create_access_key(iam_client, "ci-deploy")

        assert has_active_injector(iam_client, "CreateAccessKey") is False

    @pytest.mark.parametrize(
        ("args", "message"),
        [(("",), "user_name is required"), (("ci-deploy", "bogus"), "scope must be")],
    )
    def test_rejects_bad_arguments(self, iam_client, args, message):
        with pytest.raises(ValueError, match=message):
            create_access_key(iam_client, *args)

        iam_client.create_access_key.assert_not_called()

    def test_refuses_while_another_scope_is_active(self, iam_client):
        other = create_header_injector(
            iam_client, "CreateAccessKey", {"X-Tigris-Access-Key-Scope": "standard"}
        )
        other.register()
        try:
            with pytest.raises(RuntimeError, match="already active"):
                create_access_key(iam_client, "ci-deploy")
        finally:
            other.unregister()

        iam_client.create_access_key.assert_not_called()
