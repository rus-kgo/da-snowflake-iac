import pytest
from unittest.mock import MagicMock
from sqliac.drift import Drift
from sqliac.constants import DDLCommand, TemplateType
from sqliac.template_engine import TemplateEngine

def test_drift_comment_case_mismatch():
    # Setup
    mock_conn = MagicMock()
    # Mocking execution to return a state matching the definition but in mixed case
    mock_conn.execute.return_value = (
        '{"columns": [{"name": "COLUMN1", "type": "VARCHAR", "nullable": "NO", "comment": "Column 1 description"}]}',
    )

    drift = Drift(connection=mock_conn)

    definition = {
        "name": "MODELING.MODELING_SCHEMA.FIRST_TABLE",
        "columns": [
            {
                "name": "column1",
                "type": "varchar",
                "nullable": "NO",
                "comment": "Column 1 description",
            }
        ],
    }

    template = {
        "name": "MODELING.MODELING_SCHEMA.FIRST_TABLE",
        "columns": [
            {
                "name": "COLUMN1",
                "type": "VARCHAR",
                "nullable": "NO",
                "comment": "Column 1 description",
            }
        ],
    }

    # Execute drift check
    drift_context = drift.resource_state(
        resource_type="table",
        definition=definition,
        template=template,
        state_query="SELECT ...",
        name="MODELING.MODELING_SCHEMA.FIRST_TABLE",
    )

    # If they matched, DDLCommand should be NO_ACTION.
    # If they mismatched due to case, it will be ALTER.
    assert drift_context.ddl_command == DDLCommand.NO_ACTION, f"Expected NO_ACTION, got {drift_context.ddl_command} with diff: {drift_context.to_dict()}"

def test_table_alter_nullable_correct_ddl():
    # Setup state: column1 is currently nullable (nullable = YES)
    state = {
        "columns": [
            {
                "name": "COLUMN1",
                "type": "VARCHAR",
                "nullable": "YES",
                "comment": "Some comment",
            }
        ]
    }
    
    # Definition: user wants column1 to be NOT nullable (nullable = NO)
    definition = {
        "name": "MODELING.MODELING_SCHEMA.FIRST_TABLE",
        "columns": [
            {
                "name": "column1",
                "type": "varchar",
                "nullable": "NO",
                "comment": "Some comment",
            }
        ]
    }

    template = {
        "name": "MODELING.MODELING_SCHEMA.FIRST_TABLE",
        "columns": [
            {
                "name": "COLUMN1",
                "type": "VARCHAR",
                "nullable": "YES",
                "comment": "Some comment",
            }
        ]
    }

    mock_conn = MagicMock()
    drift = Drift(connection=mock_conn)
    
    # Normalize definitions
    rsc_def = drift._normalize_definition(definition)
    rsc_state = drift._normalize_state(state, rsc_def)
    
    # Check drift
    drift_context = drift._check_state_values(rsc_state, rsc_def, template)
    
    # Render ALTER DDL
    engine = TemplateEngine()
    
    # Read the template from active provider config (where we applied the fix)
    with open("src/sqliac/templates/provider/table/ddl_template.sql") as f:
        ddl_template_content = f.read()
        
    ddl_sql = engine.render(
        template=ddl_template_content,
        rsc_type="table",
        template_type=TemplateType.DDL,
        context=drift_context.to_dict(),
    )
    
    # Casing and whitespace normalized check
    rendered_statements = [s.strip() for s in ddl_sql.split(";") if s.strip()]
    assert any("SET NOT NULL" in stmt for stmt in rendered_statements), f"Expected 'SET NOT NULL' DDL, got: {ddl_sql}"
    assert not any("DROP NOT NULL" in stmt for stmt in rendered_statements), f"Should not drop NOT NULL: {ddl_sql}"
