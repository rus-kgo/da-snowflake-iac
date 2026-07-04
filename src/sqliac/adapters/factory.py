"""Adapter factory for creating database adapters."""

from __future__ import annotations

from importlib.metadata import entry_points

from sqliac.errors import RustyError


class AdapterFactory:
    GROUP = "sqliac.adapters"

    @classmethod
    def list_adapters(cls) -> list[str]:
        return sorted(ep.name for ep in entry_points(group=cls.GROUP))

    @classmethod
    def get_adapter(cls, provider: str):
        adapters = {ep.name: ep for ep in entry_points(group=cls.GROUP)}
        try:
            adapter_cls = adapters[provider]
        except KeyError:
            available = ", ".join(sorted(adapters)) or "none installed"
            raise RustyError(
                error=f"adapter `{provider}` is not installed",
                help=f"install it, e.g. `pip install sqliac-{provider}`",
                details=f"available adapters: {available}",
            )

        return adapter_cls.load()()
