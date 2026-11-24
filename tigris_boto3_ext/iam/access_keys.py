"""Access key scope: whether a new key gets the default allowed operations."""

from functools import wraps
from typing import Any, Callable, TypeVar, cast

from tigris_boto3_ext._internal import (
    _guard_lock,
    create_header_injector,
    has_active_injector,
)

F = TypeVar("F", bound=Callable[..., Any])

SCOPE_HEADER = "X-Tigris-Access-Key-Scope"
ACCESS_KEY_SCOPES = ("standard", "no_default_allow")


def _validate_scope(scope: str) -> str:
    if scope not in ACCESS_KEY_SCOPES:
        msg = f"scope must be 'standard' or 'no_default_allow', got {scope!r}"
        raise ValueError(msg)
    return scope


""" Context Managers """


class TigrisAccessKeyScope:
    """
    Context manager that sets the scope of access keys created with
    ``create_access_key`` on an IAM client.

    ``no_default_allow`` keys can do only what their bucket roles and IAM
    policies allow; ``standard`` keys also get the default allowed operations,
    such as creating and listing buckets and managing their own keys.

    The header applies to every ``create_access_key`` issued through this
    client while the block is active, from any thread. Do not share the client
    with other threads that create keys during the block.

    Usage:
        iam = boto3.client('iam', endpoint_url='https://iam.storage.dev')
        with TigrisAccessKeyScope(iam, 'no_default_allow'):
            iam.create_access_key(UserName='ci-deploy')
    """

    def __init__(self, iam_client: Any, scope: str = "no_default_allow"):
        """
        Initialize context manager.

        Args:
            iam_client: boto3 IAM client pointed at the Tigris IAM endpoint
            scope: ``"no_default_allow"`` (default) or ``"standard"``

        Raises:
            ValueError: If ``scope`` is anything else
        """
        self.client = iam_client
        self.scope = _validate_scope(scope)
        self._injector = create_header_injector(
            iam_client, "CreateAccessKey", {SCOPE_HEADER: scope}
        )

    def __enter__(self) -> "TigrisAccessKeyScope":
        """Enter context and register event handler."""
        self._injector.register()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unregister event handler."""
        self._injector.unregister()


""" Decorators """


def with_access_key_scope(scope: str = "no_default_allow") -> Callable[[F], F]:
    """
    Decorator that sets the scope of access keys created inside the function.
    See ``TigrisAccessKeyScope``.

    The decorated function must accept an IAM client as its first argument.

    Usage:
        @with_access_key_scope('no_default_allow')
        def create_ci_key(iam_client, name):
            return iam_client.create_access_key(UserName=name)
    """
    _validate_scope(scope)

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(iam_client: Any, *args: Any, **kwargs: Any) -> Any:
            with TigrisAccessKeyScope(iam_client, scope):
                return func(iam_client, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator


""" Helpers """


def create_access_key(
    iam_client: Any,
    user_name: str,
    scope: str = "no_default_allow",
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Create an access key with a scope.

    The scope is fixed when the key is created: ``update_access_key`` does not
    change it, and ``list_access_keys`` does not report it. Tigris echoes the
    applied scope on the response, which this helper returns as ``Scope``.

    Args:
        iam_client: boto3 IAM client pointed at the Tigris IAM endpoint
        user_name: Name of the new key
        scope: ``"no_default_allow"`` (default) or ``"standard"``
        **kwargs: Additional arguments to pass to ``create_access_key``

    Returns:
        Response from ``create_access_key``, plus ``Scope``: the scope Tigris
        applied, or None when the response does not say

    Raises:
        ValueError: If ``user_name`` is empty or ``scope`` is not one of the two values
        RuntimeError: If another scope injection is active on this client
        botocore.exceptions.ClientError: From the gateway, for example a 403
            when the calling key may not create keys

    Usage:
        key = create_access_key(iam_client, 'ci-deploy')
        print(key['AccessKey']['AccessKeyId'], key['Scope'])
    """
    if not user_name:
        msg = "user_name is required"
        raise ValueError(msg)
    injector = create_header_injector(
        iam_client, "CreateAccessKey", {SCOPE_HEADER: _validate_scope(scope)}
    )

    # Check and register under one lock so two threads cannot both pass the check.
    with _guard_lock:
        if has_active_injector(iam_client, "CreateAccessKey"):
            msg = (
                "a CreateAccessKey header injection is already active on this client; "
                "wait for the other call to finish or use a separate client"
            )
            raise RuntimeError(msg)
        injector.register()

    try:
        response = cast(
            "dict[str, Any]",
            iam_client.create_access_key(UserName=user_name, **kwargs),
        )
    finally:
        injector.unregister()

    headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    return {**response, "Scope": headers.get(SCOPE_HEADER.lower())}
