from unittest.mock import MagicMock

import pytest

from sqliac.constants import IacAction, RunMode
from sqliac.errors import RustyError
from sqliac.execution_plan import ExecutionPlanResult
from sqliac.scheduler import Scheduler


def test_scheduler_gates_failures():
    # Setup resources and definitions
    # database::MODELING is a dependency of schema::MODELING.MODELING_SCHEMA
    definitions = {
        "database": [{"name": "MODELING", "depends_on": {}}],
        "schema": [
            {
                "name": "MODELING.MODELING_SCHEMA",
                "depends_on": {"database": ["MODELING"]},
            }
        ],
    }

    # We want database::MODELING to fail during apply, and check that:
    # 1. schema::MODELING.MODELING_SCHEMA is skipped.
    # 2. Scheduler raises a RustyError detailing the failure and the skipped downstream task.

    provider_resources = {"database": MagicMock(), "schema": MagicMock()}

    # Mock resources templates
    provider_resources["database"].state_query = "SELECT ..."
    provider_resources["database"].ddl_context = {"name": "MODELING"}
    provider_resources["database"].ddl_template = "CREATE DATABASE {{ name }};"
    provider_resources["database"].ddl_command = {
        "create": "CREATE",
        "alter": "ALTER",
        "drop": "DROP",
    }

    provider_resources["schema"].state_query = "SELECT ..."
    provider_resources["schema"].ddl_context = {"name": "MODELING.MODELING_SCHEMA"}
    provider_resources["schema"].ddl_template = "CREATE SCHEMA {{ name }};"
    provider_resources["schema"].ddl_command = {
        "create": "CREATE",
        "alter": "ALTER",
        "drop": "DROP",
    }

    mock_conn = MagicMock()

    # Mocking execute on the adapter.
    # First query will be drift check for database, returns empty {} (so it needs CREATE)
    # Then execute_rendered_sql_template tries to run CREATE DATABASE, which we force to fail!
    # Let's write a side_effect for mock_conn.execute:
    def execute_side_effect(sql):
        if "INFORMATION_SCHEMA" in sql or "information_schema" in sql or "SHOW" in sql:
            # drift check: return empty to force CREATE
            return ()
        # DDL query execution: raise an error
        raise Exception("Database creation failed due to permission constraints")

    mock_conn.execute.side_effect = execute_side_effect

    scheduler = Scheduler(
        provider_resources=provider_resources,
        definitions=definitions,
        run_mode=RunMode.LIVE_RUN,
        iac_action=IacAction.APPLY,
        connection=mock_conn,
        threads=6,
    )

    # Build execution plan mock result
    plan = ExecutionPlanResult(
        dependency_graph={
            "database::MODELING": set(),
            "schema::MODELING.MODELING_SCHEMA": {"database::MODELING"},
        },
        reverse_dependency_graph={
            "database::MODELING": {"schema::MODELING.MODELING_SCHEMA"},
            "schema::MODELING.MODELING_SCHEMA": set(),
        },
    )

    # Running the scheduler should raise a RustyError due to the pipeline failure
    with pytest.raises(RustyError) as exc_info:
        scheduler.run(plan)

    err_msg = str(exc_info.value)

    # The error message should list the failed task and the skipped downstream task
    assert "database::MODELING" in err_msg
    assert "schema::MODELING.MODELING_SCHEMA" in err_msg
    assert "skipped" in err_msg.lower() or "skipped downstream tasks" in err_msg.lower()
