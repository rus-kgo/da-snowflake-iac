"""Custom error classes for structured diagnostics.

This module provides exception classes that format error messages
in the style of Rust compiler diagnostics, with support for:
- error messages
- file locations
- detailed explanations
- help text
- additional notes
"""

from sqliac.utils import box_message


class RustyError(Exception):
    """Rust-style diagnostics."""

    def __init__(
        self,
        *,
        error: str,
        details: str | None = None,
        file: str | None = None,
        help: str | None = None,
        note: str | None = None,
        sql: str | None = None,
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
        self.sql = sql

        msg_lines = [f"\nerror: {error}"]
        if file:
            msg_lines.append(f"\n    --> {file}")
        if details:
            msg_lines.append(f"\ndetails: {details}")
        if sql:
            msg_lines.append(f"\nstatement: {box_message(message=sql)}")
        if help:
            msg_lines.append(f"\nhelp: {help}")
        if note:
            msg_lines.append(f"\nnote: {note}")
        msg_lines.append("\n")
        super().__init__(
            box_message(title="Error", message="\n".join(msg_lines), width=100)
        )
