"""Unit tests for the helpers shared across resources."""

from datetime import datetime, timezone

import pytest

from tigris_boto3_ext._utils import parse_timestamp


class TestParseTimestamp:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (
                "2026-10-14T13:56:39.395637075Z",
                datetime(2026, 10, 14, 13, 56, 39, 395637, tzinfo=timezone.utc),
            ),
            (
                "2026-10-14T13:56:39Z",
                datetime(2026, 10, 14, 13, 56, 39, tzinfo=timezone.utc),
            ),
            (
                "2026-10-14T13:56:39.5+00:00",
                datetime(2026, 10, 14, 13, 56, 39, 500000, tzinfo=timezone.utc),
            ),
            (
                "2026-10-14T13:56:39",
                datetime(2026, 10, 14, 13, 56, 39, tzinfo=timezone.utc),
            ),
            (
                "Wed, 07 Oct 2026 00:00:00 GMT",
                datetime(2026, 10, 7, tzinfo=timezone.utc),
            ),
            ("", None),
            (None, None),
            (12345, None),
            ("not a date", None),
        ],
    )
    def test_gateway_timestamp_formats(self, value, expected):
        assert parse_timestamp(value) == expected
