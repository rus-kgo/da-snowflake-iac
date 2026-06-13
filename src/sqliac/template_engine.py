"""SQL template rendering using Jinja2."""

from __future__ import annotations

import sqlparse
from jinja2 import (
    Environment,
    TemplateError,
    StrictUndefined,
    TemplateSyntaxError,
    UndefinedError,
)
from typing import Any
from rich.console import Console
from rich.text import Text
from rich.markdown import Markdown
from sqlglot import transpile
from sqlglot.errors import ParseError, ErrorLevel

from sqliac.value_sanitizer import ValueSanitizer
from sqliac.errors import RustyError
from sqliac import TemplateType


class TemplateEngine:
    """Renders Jinja2 templates to formatted SQL statements."""

    def __init__(self):
        """Initialize the TemplateEngine."""
        self.sanitizer = ValueSanitizer()
        self.console = Console(force_terminal=True)

    def render(
        self,
        template: str,
        template_type: str,
        context: dict[str, Any] | None = None,
    ) -> str:
        """Render a Jinja2 template to SQL.

        Args:
            template(str): Jinja2 template string
            template_type(str): Type of the SQL template (DDL, State Query)
            context(dict[str, Any]): Dictionary of variables to substitute

        Returns:
            Rendered and formatted SQL string
        """
        env = Environment(undefined=StrictUndefined)

        sanitized_context = self.sanitizer.deep_clean(context)

        self._validate_context(sanitized_context, template_type)

        try:
            sql_template = env.from_string(template)
            sql = sql_template.render(**sanitized_context)
            return self._format_sql(sql)

        except (
            TemplateSyntaxError,
            UndefinedError,
            TemplateError,
            TypeError,
            AttributeError,
        ) as err:
            raise RustyError(
                error="SQL+Jinja template render failed",
                details=str(err),
                help=f"""\
{f"the error is at line {err}" if getattr(err, "lineno", None) else "check the your template"}
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

    def _format_sql(self, sql: str) -> str:
        """Format SQL for consistent style and readability."""
        return sqlparse.format(
            sql,
            reindent=True,
            keyword_case="upper",
            identifier_case="upper",
            use_space_around_operators=True,
            compact=True,
        )

    def print_sql(self, sql: str, provider: str = "postgres"):
        """Parse and pretty print SQL query."""
        try:
            result = transpile(
                sql,
                dialect=provider,
                identify=True,
                pretty=True,
                error_level=ErrorLevel.RAISE,
            )[0]
            sql = result
        except ParseError as err:
            raise RustyError(
                error=f"failed to parse `{provider}` SQL statement",
                details=str(err),
                help=f"""review your SQL statement below
```postgres
{sql}
```
""",
            ) from err

        else:
            self.console.print(Text())
            self.console.print(
                Markdown(f"""\
```postgres
{sql}
```
""")
            )
