"""Custom error classes for structured diagnostics.

This module provides exception classes that format error messages
in the style of Rust compiler diagnostics, with support for:
- error messages
- file locations
- detailed explanations
- help text
- additional notes
"""

from rich.console import Console
from rich.markdown import Markdown
from rich.text import Text


class RustyError(Exception):
    """Rust-style diagnostics."""

    def __init__(
        self,
        *,
        error: str,
        details: str | Markdown | None = None,
        file: str | Markdown | None = None,
        help: str | Markdown | None = None,  # noqa: A002
        note: str | Markdown | None = None,
    ):
        """Initialize with structured error information.

        Args:
            error: What failed
            file: Where it failed
            details: Why it failed
            help: How to fix it
            note: Additional context
        """
        self.error = error
        self.details = details
        self.file = file
        self.help = help
        self.note = note

        # Build plain-text message for __str__ (fallback)
        msg_lines = [f"\nerror: {error}"]
        if file:
            msg_lines.append(f"\n    --> {file}")
        if details:
            msg_lines.append(f"\ndetails: {details}")
        if help:
            msg_lines.append(f"\nhelp: {help}")
        if note:
            msg_lines.append(f"\nnote: {note}")
        super().__init__("\n".join(msg_lines))

    def print(self):
        """Print the error with Markdown rendering using Rich.

        Args:
            console: Rich Console instance for output.
        """
        console = Console(force_terminal=True)

        console.print(Text())
        console.print(Markdown(f"**error**: {self.error}"))
        if self.file:
            console.print(Text())
            console.print(Text(f"    --> {self.file}"))
        if self.details:
            console.print(Text())
            console.print(Markdown(f"**details**: {self.details}"))
        if self.help:
            console.print(Text())
            console.print(Markdown(f"**help**: {self.help}"))
        if self.note:
            console.print(Text())
            console.print(Markdown(f"**note**: {self.note}"))
