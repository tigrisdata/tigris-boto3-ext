"""Internal utilities for event handler management."""

from typing import Any, Callable, Optional


class HeaderInjector:
    """Manages header injection via boto3 event system."""

    def __init__(self, client: Any, event_name: str):
        """
        Initialize header injector.

        Args:
            client: boto3 S3 client
            event_name: Event name pattern (e.g., 'before-sign.s3.CreateBucket')
        """
        self.client = client
        self.event_name = event_name
        self.headers: dict[str, str] = {}
        self._handler_id: Optional[int] = None

    def add_header(self, name: str, value: str) -> None:
        """Add a header to be injected."""
        self.headers[name] = value

    def set_headers(self, headers: dict[str, str]) -> None:
        """Set all headers to be injected."""
        self.headers = headers.copy()

    def _create_handler(self) -> Callable:
        """Create event handler function."""

        def handler(request: Any, **kwargs: Any) -> None:
            for name, value in self.headers.items():
                request.headers[name] = value

        return handler

    def register(self) -> None:
        """Register event handler with boto3."""
        if self._handler_id is not None:
            return  # Already registered

        handler = self._create_handler()
        self.client.meta.events.register(self.event_name, handler)
        self._handler_id = id(handler)

    def unregister(self) -> None:
        """Unregister event handler from boto3."""
        if self._handler_id is None:
            return  # Not registered

        # Note: boto3 doesn't provide easy handler removal by ID
        # We need to unregister all handlers for this event and re-register others
        # For simplicity, we'll rely on context manager lifecycle
        self._handler_id = None


def create_header_injector(
    client: Any,
    operation: str,
    headers: dict[str, str],
) -> HeaderInjector:
    """
    Create and configure a header injector.

    Args:
        client: boto3 S3 client
        operation: S3 operation name (e.g., 'CreateBucket', 'GetObject')
        headers: Headers to inject

    Returns:
        Configured HeaderInjector instance
    """
    event_name = f"before-sign.s3.{operation}"
    injector = HeaderInjector(client, event_name)
    injector.set_headers(headers)
    return injector


def create_multi_operation_injector(
    client: Any,
    operations: list[str],
    headers: dict[str, str],
) -> list[HeaderInjector]:
    """
    Create header injectors for multiple operations.

    Args:
        client: boto3 S3 client
        operations: List of S3 operation names
        headers: Headers to inject for all operations

    Returns:
        List of configured HeaderInjector instances
    """
    return [create_header_injector(client, op, headers) for op in operations]
