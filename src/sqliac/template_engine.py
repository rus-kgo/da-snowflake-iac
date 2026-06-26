"""SQL template rendering using Jinja2."""

from __future__ import annotations

from typing import Any, Literal, overload

import sqlparse
from jinja2 import (
    Environment,
    StrictUndefined,
    TemplateError,
    TemplateSyntaxError,
    UndefinedError,
)
from rich.console import Console
from rich.syntax import Syntax
from rich.text import Text

from sqliac import TemplateType
from sqliac.errors import RustyError
from sqliac.value_sanitizer import ValueSanitizer


class TemplateEngine:
    """Renders Jinja2 templates to formatted SQL statements."""

    def __init__(self):
        """Initialize the TemplateEngine."""
        self.sanitizer = ValueSanitizer()
        self.env = Environment(undefined=StrictUndefined)

        # Register the custom filter into the environment
        self.env.filters["sql_escape"] = self._sql_escape_string

    def _sql_escape_string(self, value: Any) -> str:
        """Custom filter to safely wrap and escape a string value for standard SQL."""
        if value is None:
            return "NULL"
        # Basic ANSI SQL escape: double-up single quotes
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def render(
        self,
        template: str,
        template_type: str,
        context: dict[str, Any] | None = None,
    ) -> str:
        """Render a Jinja2 template to SQL."""

        sanitized_context = self.sanitizer.deep_clean(context or {})

        self._validate_context(sanitized_context, template_type)

        try:
            sql_template = self.env.from_string(template)
            return sql_template.render(**sanitized_context)

        except (
            TemplateSyntaxError,
            UndefinedError,
            TemplateError,
            TypeError,
            AttributeError,
        ) as err:
            lineno = getattr(err, "lineno", None)
            line_help = (
                f"the error is at line {lineno}" if lineno else "check your template"
            )

            raise RustyError(
                error="SQL+Jinja template render failed",
                details=str(err),
                help=f"""\
{line_help}
```text
{self._annotate_template(template)}
```
""",
            ) from err

    def _validate_context(self, context: dict[str, Any], template_type: str) -> None:
        if template_type == TemplateType.DDL:
            for key in context:
                if key not in {"ddl_command", "name", "add", "change", "remove"}:
                    raise RustyError(
                        error=f'unexpected context key "{key}"',
                        help="expected one of: ddl_command, name, add, change, remove",
                        note="these keys are referenced by your Jinja SQL template",
                    )
        elif template_type == TemplateType.STATE and "name" not in context:
            raise RustyError(
                error='missing required key: "name"',
                help='add "name" to the template context',
                note="these keys are referenced by your Jinja SQL template",
            )

    def _annotate_template(self, template: str) -> str:
        return "\n".join(
            f"{idx + 1:4} | {line}" for idx, line in enumerate(template.splitlines())
        )

    @overload
    @staticmethod
    def pretty_sql(sql: str, as_syntax: Literal[False] = False) -> str: ...

    @overload
    @staticmethod
    def pretty_sql(sql: str, as_syntax: Literal[True]) -> Syntax: ...

    @staticmethod
    def pretty_sql(sql: str, as_syntax: bool = False) -> str | Syntax:
        """Format SQL and return either a raw string or a Rich Syntax object.

        Default behavior returns a formatted string.
        """
        formatted_sql = sqlparse.format(
            sql,
            reindent=True,
            keyword_case="upper",
            strip_comments=True,
            strip_whitespace=True,
            use_semicolons=True,
            encoding="utf-8",
        )

        if not as_syntax:
            return formatted_sql

        return Syntax(
            formatted_sql,
            "sql",
            theme="monokai",
            line_numbers=True,
            indent_guides=False,
            padding=(0, 1),
            word_wrap=True,
        )

    @classmethod
    def print_sql(cls, sql: str) -> None:
        """Parse and pretty print SQL query to the terminal."""
        console = Console(force_terminal=True)
        sql_pretty_syntax = cls.pretty_sql(sql, as_syntax=True)
        console.print(Text())
        console.print(sql_pretty_syntax)
