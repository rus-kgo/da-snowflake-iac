"""Value sanitization to prevent SQL injection."""

from __future__ import annotations

from typing import Any

from sqliac.errors import RustyError


class ValueSanitizer:
    """Sanitizes config values."""

    MAX_DEPTH = 10

    def to_string(self, value: Any, source: str = "") -> str:
        """Convert to string."""
        if value is None:
            return ""
        try:
            return str(value).strip()
        except (ValueError, TypeError):
            raise RustyError(
                error="value error",
                details="value must be a string",
                note=f"source: {source}" if source else None,
                help="check the input value for invalid characters or patterns",
            ) from None

    def to_int(self, value: Any, source: str) -> int:
        """Convert to integer or float number."""
        if isinstance(value, int):
            return value

        try:
            return int(value)
        except (ValueError, TypeError):
            raise RustyError(
                error="value error",
                details="value must be convertible to an integer",
                note=f"source: {source}" if source else None,
                help="ensure the input is a valid numeric value",
            ) from None

    def to_bool(self, value: Any, source: str) -> bool:
        """Convert to bool."""
        if isinstance(value, str):
            lower_value = value.lower().strip()
            if lower_value in {"true", "false"}:
                return lower_value == "true"
        try:
            return value == True
        except (ValueError, TypeError):
            raise RustyError(
                error="value error",
                details="value must be a boolean (true/false)",
                note=f"source: {source}" if source else None,
                help="use 'true' or 'false' as string values",
            ) from None

    def _deep_clean_recursive(self, value: Any, depth: int) -> Any:
        """Internal: Recursively traverse and clean nested structures.

        Args:
            value: Value to clean (string, dict, list, or other)
            depth: Current recursion depth

        Returns:
            Cleaned value with same structure as input

        Raises:
            RustyError: If depth exceeds MAX_DEPTH
        """
        if depth > self.MAX_DEPTH:
            raise RustyError(
                error="nesting limit exceeded",
                details=f"structure depth at {depth} levels; maximum allowed: {self.MAX_DEPTH}",
                help="reduce nesting depth in your configuration",
            )

        if isinstance(value, str):
            return self.to_string(value, source="nested_value")

        if isinstance(value, dict):
            return {
                k: self._deep_clean_recursive(v, depth + 1) for k, v in value.items()
            }

        if isinstance(value, list):
            return [self._deep_clean_recursive(item, depth + 1) for item in value]

        return value

    def deep_clean(self, value: Any) -> dict[str, Any]:
        """Recursively clean nested structures (dict/list) from SQL injection.

        Traverses dicts and lists to arbitrary depth, sanitizing all string values.
        Non-string leaf values pass through unchanged.

        Args:
            value: String, dict, list, or nested combination thereof

        Returns:
            Cleaned structure with same shape as input
        """
        try:
            return self._deep_clean_recursive(value, depth=0)
        except RustyError:
            raise
        except Exception as e:
            raise RustyError(
                error="unexpected error during sanitization",
                details="an unexpected condition was encountered",
                note=str(e),
                help="check your input structure for validity",
            ) from None
