"""Internal utilities for event handler management."""

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

# Global registry to track shared handlers and active injectors
# Key: (client_id, event_name) -> (handler_function, set of active injector IDs)
_handler_registry: dict[tuple[int, str], tuple[Callable, set[int]]] = {}


class HeaderInjector:
    """Manages header injection via boto3 event system with support for nesting."""

    def __init__(self, client: S3Client, event_name: str):
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
        self._instance_id = id(self)

    def add_header(self, name: str, value: str) -> None:
        """Add a header to be injected."""
        self.headers[name] = value

    def set_headers(self, headers: dict[str, str]) -> None:
        """Set all headers to be injected."""
        self.headers = headers.copy()

    def _create_shared_handler(self) -> Callable:
        """Create a shared event handler that gets headers from the first active injector."""

        def handler(request: Any, **kwargs: Any) -> None:
            # Inject headers from this instance (first registered wins)
            for name, value in self.headers.items():
                request.headers[name] = value

        return handler

    def register(self) -> None:
        """Register event handler with boto3, sharing handler across nested contexts."""
        if self._registry_key in _handler_registry:
            # Handler already exists, just add this instance to the active set
            handler, active_injectors = _handler_registry[self._registry_key]
            if self._instance_id not in active_injectors:
                active_injectors.add(self._instance_id)
        else:
            # First registration, create and register shared handler
            handler = self._create_shared_handler()
            self.client.meta.events.register(self.event_name, handler)
            _handler_registry[self._registry_key] = (handler, {self._instance_id})

    def unregister(self) -> None:
        """Unregister event handler from boto3, only removing when no active contexts remain."""
        if self._registry_key not in _handler_registry:
            return  # Not registered

        handler, active_injectors = _handler_registry[self._registry_key]

        # Remove this instance from the active set
        active_injectors.discard(self._instance_id)

        # If no more active injectors, unregister the handler
        if not active_injectors:
            self.client.meta.events.unregister(self.event_name, handler)
            del _handler_registry[self._registry_key]


def create_header_injector(
    client: S3Client,
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
    client: S3Client,
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
