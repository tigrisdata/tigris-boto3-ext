"""Internal utilities for event handler management."""

from typing import Any, Callable

# Global registry to track handlers across all HeaderInjector instances
# Key: (client_id, event_name) -> (handler_function, reference_count)
_handler_registry: dict[tuple[int, str], tuple[Callable, int]] = {}


class HeaderInjector:
    """Manages header injection via boto3 event system with support for nesting."""

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
        self._registry_key = (id(client), event_name)

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
        """Register event handler with boto3, using reference counting for nested contexts."""
        if self._registry_key in _handler_registry:
            # Handler already registered, increment reference count
            handler, ref_count = _handler_registry[self._registry_key]
            _handler_registry[self._registry_key] = (handler, ref_count + 1)
        else:
            # First registration, create and register handler
            handler = self._create_handler()
            self.client.meta.events.register(self.event_name, handler)
            _handler_registry[self._registry_key] = (handler, 1)

    def unregister(self) -> None:
        """Unregister event handler from boto3, respecting nested contexts."""
        if self._registry_key not in _handler_registry:
            return  # Not registered

        handler, ref_count = _handler_registry[self._registry_key]

        if ref_count > 1:
            # Still have nested contexts, just decrement
            _handler_registry[self._registry_key] = (handler, ref_count - 1)
        else:
            # Last reference, actually unregister
            self.client.meta.events.unregister(self.event_name, handler)
            del _handler_registry[self._registry_key]


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
