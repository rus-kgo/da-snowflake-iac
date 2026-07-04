import pytest

from sqliac.errors import RustyError
from sqliac.execution_plan import ExecutionPlan


def test_build_execution_plan_creates_dependency_and_reverse_graphs():
    definitions = {
        "database": [{"name": "DB", "depends_on": {}}],
        "schema": [{"name": "DB.PUBLIC", "depends_on": {"database": ["DB"]}}],
        "table": [{"name": "DB.PUBLIC.USERS", "depends_on": {"schema": ["DB.PUBLIC"]}}],
    }

    plan = ExecutionPlan(definitions).build_execution_plan()

    assert plan.dependency_graph == {
        "database::DB": set(),
        "schema::DB.PUBLIC": {"database::DB"},
        "table::DB.PUBLIC.USERS": {"schema::DB.PUBLIC"},
    }
    assert plan.reverse_dependency_graph == {
        "database::DB": {"schema::DB.PUBLIC"},
        "schema::DB.PUBLIC": {"table::DB.PUBLIC.USERS"},
        "table::DB.PUBLIC.USERS": set(),
    }


def test_build_execution_plan_rejects_missing_dependency():
    definitions = {
        "schema": [{"name": "DB.PUBLIC", "depends_on": {"database": ["MISSING"]}}],
    }

    with pytest.raises(RustyError, match="invalid dependency reference"):
        ExecutionPlan(definitions).build_execution_plan()


def test_build_execution_plan_rejects_cycles():
    definitions = {
        "database": [{"name": "DB", "depends_on": {"schema": ["DB.PUBLIC"]}}],
        "schema": [{"name": "DB.PUBLIC", "depends_on": {"database": ["DB"]}}],
    }

    with pytest.raises(RustyError, match="circular dependency detected"):
        ExecutionPlan(definitions).build_execution_plan()


def test_generate_dot_graph_writes_dependency_file(tmp_path, monkeypatch):
    definitions = {
        "database": [{"name": "DB", "depends_on": {}}],
        "schema": [{"name": "DB.PUBLIC", "depends_on": {"database": ["DB"]}}],
    }
    monkeypatch.chdir(tmp_path)

    ExecutionPlan(definitions).generate_dot_graph()

    assert (tmp_path / "dependencies.dot").read_text() == "\n".join(
        [
            "digraph G {",
            "  rankdir=LR;",
            "  node [shape=ellipse, style=filled, fillcolor=lightgrey, fontname=Helvetica];",
            '"database::DB";',
            '"schema::DB.PUBLIC" -> "database::DB";',
            "}",
        ]
    )
