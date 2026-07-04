import pytest

from sqliac.constants import TemplateType
from sqliac.errors import RustyError
from sqliac.template_engine import TemplateEngine


def test_render_sanitizes_context_and_applies_filters():
    engine = TemplateEngine()
    template = (
        "{{ ddl_command }} TABLE {{ name }} "
        "COMMENT = {{ 'comment' | pick(add, change) | sql_escape }}"
    )

    sql = engine.render(
        template=template,
        rsc_type="table",
        template_type=TemplateType.DDL,
        context={
            "ddl_command": "create",
            "name": "db.public.users",
            "add": {"comment": "Owner's table"},
        },
    )

    assert sql == "CREATE TABLE DB.PUBLIC.USERS COMMENT = 'Owner''s table'"


def test_render_rejects_unexpected_ddl_context_key():
    with pytest.raises(RustyError, match='unexpected context key "extra"'):
        TemplateEngine().render(
            template="{{ name }}",
            rsc_type="table",
            template_type=TemplateType.DDL,
            context={"name": "T", "extra": "value"},
        )


def test_render_requires_name_for_state_templates():
    with pytest.raises(RustyError, match='missing required key: "name"'):
        TemplateEngine().render(
            template="SELECT 1",
            rsc_type="table",
            template_type=TemplateType.STATE,
            context={},
        )


def test_render_wraps_jinja_errors_as_rusty_error():
    with pytest.raises(RustyError, match="SQL\\+Jinja template render failed"):
        TemplateEngine().render(
            template="SELECT {{ missing }}",
            rsc_type="table",
            template_type=TemplateType.STATE,
            context={"name": "T"},
        )


def test_pretty_sql_formats_keywords_and_strips_comments():
    sql = "select * from users -- comment\nwhere id=1"

    pretty = TemplateEngine.pretty_sql(sql)

    assert "SELECT" in pretty
    assert "FROM users" in pretty
    assert "-- comment" not in pretty
