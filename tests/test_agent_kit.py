"""Unit tests for agent_kit helpers (workspaces, forks, checkpoints, coordination)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from tigris_boto3_ext import (
    Checkpoint,
    Credentials,
    Fork,
    ForkSet,
    Workspace,
    checkpoint,
    create_forks,
    create_workspace,
    list_checkpoints,
    restore,
    setup_coordination,
    teardown_coordination,
    teardown_forks,
    teardown_workspace,
)


@pytest.fixture
def s3_client():
    return MagicMock()


# -- create_workspace --


class TestCreateWorkspace:
    def test_basic_creates_plain_bucket(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot_bucket") as mock_snap,
            patch("tigris_boto3_ext.agent_kit.patch_bucket_settings") as mock_patch,
        ):
            ws = create_workspace(s3_client, "ws-1")
        assert ws == Workspace(bucket="ws-1")
        mock_snap.assert_not_called()
        mock_patch.assert_not_called()
        s3_client.create_bucket.assert_called_once_with(Bucket="ws-1")

    def test_enable_snapshots_uses_helper(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot_bucket") as mock_snap,
            patch("tigris_boto3_ext.agent_kit.patch_bucket_settings") as mock_patch,
        ):
            ws = create_workspace(s3_client, "ws-2", enable_snapshots=True)
        assert ws.bucket == "ws-2"
        mock_snap.assert_called_once_with(s3_client, "ws-2")
        s3_client.create_bucket.assert_not_called()
        mock_patch.assert_not_called()

    def test_ttl_days_sets_lifecycle_rule(self, s3_client):
        with patch("tigris_boto3_ext.agent_kit.patch_bucket_settings") as mock_patch:
            create_workspace(s3_client, "ws-3", ttl_days=7)

        mock_patch.assert_called_once()
        bucket_arg = mock_patch.call_args.args[1]
        body = mock_patch.call_args.args[2]
        assert bucket_arg == "ws-3"
        rules = body["lifecycle_rules"]
        assert len(rules) == 1
        assert rules[0]["expiration"] == {"days": 7, "enabled": True}
        assert rules[0]["status"] == 1
        assert rules[0]["id"].startswith("workspace-ttl-")

    def test_ttl_combined_with_snapshots(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot_bucket") as mock_snap,
            patch("tigris_boto3_ext.agent_kit.patch_bucket_settings") as mock_patch,
        ):
            create_workspace(s3_client, "ws-4", ttl_days=1, enable_snapshots=True)
        mock_snap.assert_called_once_with(s3_client, "ws-4")
        mock_patch.assert_called_once()

    def test_zero_ttl_raises(self, s3_client):
        with pytest.raises(ValueError, match="ttl_days must be positive"):
            create_workspace(s3_client, "ws-bad", ttl_days=0)

    def test_negative_ttl_raises(self, s3_client):
        with pytest.raises(ValueError, match="ttl_days must be positive"):
            create_workspace(s3_client, "ws-bad", ttl_days=-1)


# -- teardown_workspace --


class TestTeardownWorkspace:
    def test_force_empties_then_deletes(self, s3_client):
        s3_client.get_paginator.return_value.paginate.return_value = iter([])
        teardown_workspace(s3_client, Workspace(bucket="ws-x"))
        s3_client.delete_bucket.assert_called_once_with(Bucket="ws-x")

    def test_no_force_skips_empty(self, s3_client):
        teardown_workspace(s3_client, Workspace(bucket="ws-x"), force=False)
        s3_client.get_paginator.assert_not_called()
        s3_client.delete_bucket.assert_called_once_with(Bucket="ws-x")


# -- create_forks --


class TestCreateForks:
    def test_creates_n_forks(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot") as mock_snap,
            patch(
                "tigris_boto3_ext.agent_kit.get_snapshot_version",
                return_value="snap-abc",
            ),
            patch("tigris_boto3_ext.agent_kit.create_fork") as mock_fork,
        ):
            mock_snap.return_value = {}
            result = create_forks(s3_client, "base", 3, prefix="exp")

        assert result.base_bucket == "base"
        assert result.snapshot_id == "snap-abc"
        assert [f.bucket for f in result.forks] == ["exp-0", "exp-1", "exp-2"]
        mock_snap.assert_called_once_with(s3_client, "base")
        assert mock_fork.call_count == 3
        for i, call in enumerate(mock_fork.call_args_list):
            assert call.args[1] == f"exp-{i}"
            assert call.args[2] == "base"
            assert call.kwargs == {"snapshot_version": "snap-abc"}

    def test_default_prefix_includes_timestamp(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot") as mock_snap,
            patch(
                "tigris_boto3_ext.agent_kit.get_snapshot_version",
                return_value="snap-1",
            ),
            patch("tigris_boto3_ext.agent_kit.create_fork"),
        ):
            mock_snap.return_value = {}
            result = create_forks(s3_client, "base", 1)

        assert result.forks[0].bucket.startswith("base-fork-")
        assert result.forks[0].bucket.endswith("-0")

    def test_count_zero_raises(self, s3_client):
        with pytest.raises(ValueError, match="count must be >= 1"):
            create_forks(s3_client, "base", 0)

    def test_count_negative_raises(self, s3_client):
        with pytest.raises(ValueError, match="count must be >= 1"):
            create_forks(s3_client, "base", -1)

    def test_missing_snapshot_version_raises(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot", return_value={}),
            patch(
                "tigris_boto3_ext.agent_kit.get_snapshot_version", return_value=None
            ),
        ):
            with pytest.raises(RuntimeError, match="Could not read snapshot version"):
                create_forks(s3_client, "base", 1)

    def test_partial_failure_skips_then_continues(self, s3_client):
        # Third fork fails; remaining attempts still proceed (no early exit).
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot", return_value={}),
            patch(
                "tigris_boto3_ext.agent_kit.get_snapshot_version", return_value="v1"
            ),
            patch("tigris_boto3_ext.agent_kit.create_fork") as mock_fork,
        ):
            mock_fork.side_effect = [None, None, RuntimeError("boom"), None, None]
            result = create_forks(s3_client, "base", 5, prefix="p")

        # p-2 is skipped; the other four were still attempted and recorded.
        assert [f.bucket for f in result.forks] == ["p-0", "p-1", "p-3", "p-4"]
        assert mock_fork.call_count == 5

    def test_all_fail_raises(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot", return_value={}),
            patch(
                "tigris_boto3_ext.agent_kit.get_snapshot_version", return_value="v1"
            ),
            patch(
                "tigris_boto3_ext.agent_kit.create_fork",
                side_effect=RuntimeError("nope"),
            ),
        ):
            with pytest.raises(RuntimeError, match="Failed to create any forks"):
                create_forks(s3_client, "base", 2)


# -- teardown_forks --


class TestTeardownForks:
    def test_deletes_each_fork(self, s3_client):
        s3_client.get_paginator.return_value.paginate.return_value = iter([])
        fs = ForkSet(
            base_bucket="base",
            snapshot_id="v1",
            forks=[Fork(bucket="f0"), Fork(bucket="f1")],
        )
        teardown_forks(s3_client, fs)
        deleted = [c.kwargs["Bucket"] for c in s3_client.delete_bucket.call_args_list]
        assert deleted == ["f0", "f1"]

    def test_continues_on_error(self, s3_client):
        s3_client.get_paginator.return_value.paginate.return_value = iter([])
        s3_client.delete_bucket.side_effect = [RuntimeError("boom"), None]
        fs = ForkSet(
            base_bucket="base",
            snapshot_id="v1",
            forks=[Fork(bucket="f0"), Fork(bucket="f1")],
        )
        teardown_forks(s3_client, fs)
        assert s3_client.delete_bucket.call_count == 2


# -- checkpoint / restore / list_checkpoints --


class TestCheckpoint:
    def test_returns_dataclass_with_version_and_name(self, s3_client):
        with (
            patch(
                "tigris_boto3_ext.agent_kit.create_snapshot",
                return_value={"some": "response"},
            ) as mock_snap,
            patch(
                "tigris_boto3_ext.agent_kit.get_snapshot_version",
                return_value="snap-99",
            ),
        ):
            ck = checkpoint(s3_client, "training-data", name="epoch-50")

        mock_snap.assert_called_once_with(
            s3_client, "training-data", snapshot_name="epoch-50"
        )
        assert ck.snapshot_id == "snap-99"
        assert ck.name == "epoch-50"
        assert isinstance(ck.created_at, datetime)
        assert ck.created_at.tzinfo == timezone.utc

    def test_no_name_passes_none_to_helper(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot") as mock_snap,
            patch(
                "tigris_boto3_ext.agent_kit.get_snapshot_version",
                return_value="snap-1",
            ),
        ):
            mock_snap.return_value = {}
            ck = checkpoint(s3_client, "b")
        mock_snap.assert_called_once_with(s3_client, "b", snapshot_name=None)
        assert ck.name is None

    def test_missing_snapshot_version_raises(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot", return_value={}),
            patch(
                "tigris_boto3_ext.agent_kit.get_snapshot_version", return_value=None
            ),
        ):
            with pytest.raises(RuntimeError, match="Could not read snapshot version"):
                checkpoint(s3_client, "b")


class TestRestore:
    def test_default_fork_name(self, s3_client):
        with patch("tigris_boto3_ext.agent_kit.create_fork") as mock_fork:
            new_bucket = restore(s3_client, "training", "snap-1")
        assert new_bucket.startswith("training-restore-")
        mock_fork.assert_called_once()
        assert mock_fork.call_args.args[1] == new_bucket
        assert mock_fork.call_args.args[2] == "training"
        assert mock_fork.call_args.kwargs == {"snapshot_version": "snap-1"}

    def test_custom_fork_name(self, s3_client):
        with patch("tigris_boto3_ext.agent_kit.create_fork") as mock_fork:
            new_bucket = restore(
                s3_client, "training", "snap-1", fork_name="retry-1"
            )
        assert new_bucket == "retry-1"
        assert mock_fork.call_args.args[1] == "retry-1"


class TestListCheckpoints:
    def test_parses_unnamed_versions(self, s3_client):
        created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        with patch(
            "tigris_boto3_ext.agent_kit.list_snapshots",
            return_value={
                "Buckets": [
                    {"Name": "snap-aaa", "CreationDate": created},
                    {"Name": "snap-bbb", "CreationDate": created},
                ]
            },
        ):
            ckpts = list_checkpoints(s3_client, "b")
        assert ckpts == [
            Checkpoint(snapshot_id="snap-aaa", name=None, created_at=created),
            Checkpoint(snapshot_id="snap-bbb", name=None, created_at=created),
        ]

    def test_parses_named_snapshot(self, s3_client):
        with patch(
            "tigris_boto3_ext.agent_kit.list_snapshots",
            return_value={
                "Buckets": [
                    {"Name": "snap-xyz; name=epoch-10", "CreationDate": None},
                ]
            },
        ):
            ckpts = list_checkpoints(s3_client, "b")
        assert ckpts[0].snapshot_id == "snap-xyz"
        assert ckpts[0].name == "epoch-10"

    def test_empty_listing(self, s3_client):
        with patch(
            "tigris_boto3_ext.agent_kit.list_snapshots", return_value={"Buckets": []}
        ):
            assert list_checkpoints(s3_client, "b") == []

    def test_skips_entries_without_name(self, s3_client):
        with patch(
            "tigris_boto3_ext.agent_kit.list_snapshots",
            return_value={"Buckets": [{"Name": "", "CreationDate": None}]},
        ):
            assert list_checkpoints(s3_client, "b") == []

    def test_equality_ignores_created_at(self):
        """A checkpoint() result must equal the same logical entry from list."""
        from datetime import datetime, timezone

        client_side = Checkpoint(
            snapshot_id="snap-1",
            name="ep-1",
            created_at=datetime(2026, 4, 27, tzinfo=timezone.utc),
        )
        server_side = Checkpoint(
            snapshot_id="snap-1",
            name="ep-1",
            created_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
        )
        assert client_side == server_side


# -- coordination --


class TestSetupCoordination:
    def test_minimal(self, s3_client):
        with patch("tigris_boto3_ext.agent_kit.patch_bucket_settings") as mock_patch:
            setup_coordination(s3_client, "b", webhook_url="https://hook")
        mock_patch.assert_called_once()
        body = mock_patch.call_args.args[2]
        assert body == {
            "object_notifications": {
                "enabled": True,
                "web_hook": "https://hook",
                "filter": "",
            }
        }

    def test_with_filter(self, s3_client):
        with patch("tigris_boto3_ext.agent_kit.patch_bucket_settings") as mock_patch:
            setup_coordination(
                s3_client,
                "b",
                webhook_url="https://hook",
                event_filter='WHERE `key` REGEXP "^results/"',
            )
        body = mock_patch.call_args.args[2]
        assert body["object_notifications"]["filter"] == 'WHERE `key` REGEXP "^results/"'

    def test_with_token_auth(self, s3_client):
        with patch("tigris_boto3_ext.agent_kit.patch_bucket_settings") as mock_patch:
            setup_coordination(
                s3_client, "b", webhook_url="https://hook", auth_token="tok"
            )
        body = mock_patch.call_args.args[2]
        assert body["object_notifications"]["auth"] == {"token": "tok"}

    def test_with_basic_auth(self, s3_client):
        with patch("tigris_boto3_ext.agent_kit.patch_bucket_settings") as mock_patch:
            setup_coordination(
                s3_client,
                "b",
                webhook_url="https://hook",
                auth_username="u",
                auth_password="p",
            )
        body = mock_patch.call_args.args[2]
        assert body["object_notifications"]["auth"] == {
            "basic_user": "u",
            "basic_pass": "p",
        }

    def test_empty_url_raises(self, s3_client):
        with pytest.raises(ValueError, match="webhook_url is required"):
            setup_coordination(s3_client, "b", webhook_url="")

    def test_token_with_basic_raises(self, s3_client):
        with pytest.raises(ValueError, match="cannot be combined"):
            setup_coordination(
                s3_client,
                "b",
                webhook_url="https://hook",
                auth_token="t",
                auth_username="u",
                auth_password="p",
            )

    def test_partial_basic_auth_raises(self, s3_client):
        with pytest.raises(ValueError, match="must be provided together"):
            setup_coordination(
                s3_client,
                "b",
                webhook_url="https://hook",
                auth_username="u",
            )


class TestTeardownCoordination:
    def test_clears_notifications(self, s3_client):
        with patch("tigris_boto3_ext.agent_kit.patch_bucket_settings") as mock_patch:
            teardown_coordination(s3_client, "b")
        mock_patch.assert_called_once_with(s3_client, "b", {"object_notifications": {}})


# -- Credentials --


def _ak(name="bucket-key"):
    return {
        "access_key_id": "AKIAEXAMPLE",
        "secret_access_key": "supersecret",  # noqa: S106
        "name": name,
    }


class TestCreateWorkspaceCredentials:
    def test_no_role_no_credentials(self, s3_client):
        with patch(
            "tigris_boto3_ext.agent_kit.create_access_key_with_buckets_role"
        ) as mock_create:
            ws = create_workspace(s3_client, "ws-x")
        mock_create.assert_not_called()
        assert ws.credentials is None

    def test_editor_role_provisions_key(self, s3_client):
        with patch(
            "tigris_boto3_ext.agent_kit.create_access_key_with_buckets_role",
            return_value=_ak("ws-1-key"),
        ) as mock_create:
            ws = create_workspace(s3_client, "ws-1", credentials_role="Editor")

        mock_create.assert_called_once_with(
            s3_client, "ws-1-key", [{"bucket": "ws-1", "role": "Editor"}]
        )
        assert ws.credentials == Credentials(
            access_key_id="AKIAEXAMPLE",
            secret_access_key="supersecret",  # noqa: S106
        )

    def test_readonly_role_provisions_key(self, s3_client):
        with patch(
            "tigris_boto3_ext.agent_kit.create_access_key_with_buckets_role",
            return_value=_ak(),
        ) as mock_create:
            create_workspace(s3_client, "ws-2", credentials_role="ReadOnly")
        assert (
            mock_create.call_args.args[2][0]["role"] == "ReadOnly"
        )

    def test_invalid_role_raises(self, s3_client):
        with pytest.raises(ValueError, match="must be 'Editor' or 'ReadOnly'"):
            create_workspace(s3_client, "ws-bad", credentials_role="Admin")


class TestTeardownWorkspaceCredentials:
    def test_revokes_then_deletes(self, s3_client):
        s3_client.get_paginator.return_value.paginate.return_value = iter([])
        ws = Workspace(
            bucket="ws-1",
            credentials=Credentials(
                access_key_id="AKIA",
                secret_access_key="s",  # noqa: S106
            ),
        )
        with patch(
            "tigris_boto3_ext.agent_kit.delete_access_key"
        ) as mock_delete:
            teardown_workspace(s3_client, ws)
        mock_delete.assert_called_once_with(s3_client, "AKIA")
        s3_client.delete_bucket.assert_called_once_with(Bucket="ws-1")

    def test_revoke_failure_still_deletes_bucket(self, s3_client):
        s3_client.get_paginator.return_value.paginate.return_value = iter([])
        ws = Workspace(
            bucket="ws-1",
            credentials=Credentials(
                access_key_id="AKIA",
                secret_access_key="s",  # noqa: S106
            ),
        )
        with patch(
            "tigris_boto3_ext.agent_kit.delete_access_key",
            side_effect=RuntimeError("iam down"),
        ):
            teardown_workspace(s3_client, ws)
        s3_client.delete_bucket.assert_called_once_with(Bucket="ws-1")


class TestCreateForksCredentials:
    def test_per_fork_credentials(self, s3_client):
        with (
            patch("tigris_boto3_ext.agent_kit.create_snapshot", return_value={}),
            patch(
                "tigris_boto3_ext.agent_kit.get_snapshot_version", return_value="v1"
            ),
            patch("tigris_boto3_ext.agent_kit.create_fork"),
            patch(
                "tigris_boto3_ext.agent_kit.create_access_key_with_buckets_role",
                side_effect=[_ak("f-0-key"), _ak("f-1-key")],
            ) as mock_create,
        ):
            result = create_forks(
                s3_client, "base", 2, prefix="f", credentials_role="ReadOnly"
            )

        assert mock_create.call_count == 2
        for fork in result.forks:
            assert fork.credentials is not None
            assert fork.credentials.access_key_id == "AKIAEXAMPLE"
        assert mock_create.call_args_list[0].args[1] == "f-0-key"
        assert mock_create.call_args_list[1].args[1] == "f-1-key"
        assert mock_create.call_args_list[0].args[2] == [
            {"bucket": "f-0", "role": "ReadOnly"}
        ]


class TestEmptyBucket:
    """Cover _empty_bucket internals via teardown_workspace."""

    def test_empty_bucket_deletes_versions_and_markers(self, s3_client):
        # The versioned paginator returns one page with versions + delete markers.
        s3_client.get_paginator.return_value.paginate.return_value = iter(
            [
                {
                    "Versions": [
                        {"Key": "k1", "VersionId": "v1"},
                        {"Key": "k2", "VersionId": "v2"},
                    ],
                    "DeleteMarkers": [{"Key": "k3", "VersionId": "vm1"}],
                }
            ]
        )
        teardown_workspace(s3_client, Workspace(bucket="b"))
        s3_client.get_paginator.assert_called_with("list_object_versions")
        s3_client.delete_objects.assert_called_once_with(
            Bucket="b",
            Delete={
                "Objects": [
                    {"Key": "k1", "VersionId": "v1"},
                    {"Key": "k2", "VersionId": "v2"},
                    {"Key": "k3", "VersionId": "vm1"},
                ]
            },
        )
        s3_client.delete_bucket.assert_called_once_with(Bucket="b")

    def test_unversioned_continues_after_per_object_failure(self, s3_client):
        """A single delete_object failure must not strand the rest of the page."""
        v_paginator = MagicMock()
        v_paginator.paginate.side_effect = RuntimeError("not versioned")
        unv_paginator = MagicMock()
        unv_paginator.paginate.return_value = iter(
            [{"Contents": [{"Key": "a"}, {"Key": "b"}, {"Key": "c"}]}]
        )

        def get_paginator(op):
            return v_paginator if op == "list_object_versions" else unv_paginator

        s3_client.get_paginator.side_effect = get_paginator
        # Object "b" can't be deleted; "a" and "c" still must.
        s3_client.delete_object.side_effect = [None, RuntimeError("perm"), None]

        teardown_workspace(s3_client, Workspace(bucket="b"))

        keys = [c.kwargs["Key"] for c in s3_client.delete_object.call_args_list]
        assert keys == ["a", "b", "c"]

    def test_falls_back_to_unversioned_on_failure(self, s3_client):
        # Versioned listing raises (e.g. unversioned bucket); fallback runs.
        v_paginator = MagicMock()
        v_paginator.paginate.side_effect = RuntimeError("not versioned")
        unv_paginator = MagicMock()
        unv_paginator.paginate.return_value = iter(
            [{"Contents": [{"Key": "a"}, {"Key": "b"}]}]
        )

        def get_paginator(op):
            if op == "list_object_versions":
                return v_paginator
            return unv_paginator

        s3_client.get_paginator.side_effect = get_paginator

        teardown_workspace(s3_client, Workspace(bucket="b"))
        deleted = [c.kwargs["Key"] for c in s3_client.delete_object.call_args_list]
        assert deleted == ["a", "b"]


class TestTeardownForksCredentials:
    def test_revokes_each_credential(self, s3_client):
        s3_client.get_paginator.return_value.paginate.return_value = iter([])
        fs = ForkSet(
            base_bucket="base",
            snapshot_id="v1",
            forks=[
                Fork(
                    bucket="f0",
                    credentials=Credentials(
                        access_key_id="AKIA0",
                        secret_access_key="s",  # noqa: S106
                    ),
                ),
                Fork(
                    bucket="f1",
                    credentials=Credentials(
                        access_key_id="AKIA1",
                        secret_access_key="s",  # noqa: S106
                    ),
                ),
            ],
        )
        with patch(
            "tigris_boto3_ext.agent_kit.delete_access_key"
        ) as mock_delete:
            teardown_forks(s3_client, fs)
        deleted_keys = [c.args[1] for c in mock_delete.call_args_list]
        assert deleted_keys == ["AKIA0", "AKIA1"]
