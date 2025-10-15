"""Shared pytest fixtures for all tests."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for unit tests."""
    mock = MagicMock()
    mock.meta.events = MagicMock()
    mock.meta.events.register = MagicMock()
    mock.meta.events.unregister = MagicMock()
    return mock


@pytest.fixture
def mock_request():
    """Mock request object with headers dict."""
    mock = MagicMock()
    mock.headers = {}
    return mock


class MockRequest:
    """Simple mock request class for testing header injection."""

    def __init__(self):
        self.headers = {}


@pytest.fixture
def mock_request_class():
    """Return MockRequest class for instantiation in tests."""
    return MockRequest
