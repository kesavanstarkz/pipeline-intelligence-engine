"""File browser registry pattern for plugin-based source support."""

from typing import Callable, Any
from collections import defaultdict

_REGISTRY: dict[str, Callable] = {}


def register(source_type: str):
    """Decorator to register a file browser for a source type.
    
    Usage:
        @register("s3")
        async def list_files(connection: dict, max_files: int) -> list[str]:
            ...
    """
    def decorator(fn: Callable) -> Callable:
        _REGISTRY[source_type] = fn
        return fn
    return decorator


def get_browser(source_type: str) -> Callable:
    """Retrieve a registered file browser function.
    
    Raises ValueError if source_type not found in registry.
    """
    browser = _REGISTRY.get(source_type)
    if not browser:
        raise ValueError(
            f"No file browser registered for source type: {source_type}. "
            f"Available: {list(_REGISTRY.keys())}"
        )
    return browser


def list_registered_sources() -> list[str]:
    """Return list of all registered source types."""
    return list(_REGISTRY.keys())
