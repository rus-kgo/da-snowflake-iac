import pytest

from sqliac.definitions_loader import DefinitionsLoader
from sqliac.errors import RustyError
from sqliac.providers_loader import ResourceConfig


def test_validate_definitions_dir_returns_only_known_toml_files(tmp_path, monkeypatch):
    definitions_dir = tmp_path / "definitions"
    definitions_dir.mkdir()
    table_file = definitions_dir / "table.toml"
    table_file.write_text("[[table]]\nname = 'T'\n")
    (definitions_dir / "README.md").write_text("ignored")

    monkeypatch.chdir(tmp_path)

    assert DefinitionsLoader._validate_definitions_dir(["table"]) == [table_file]


def test_validate_definitions_dir_rejects_unknown_resource_file(tmp_path, monkeypatch):
    definitions_dir = tmp_path / "definitions"
    definitions_dir.mkdir()
    (definitions_dir / "unknown.toml").write_text("[[unknown]]\nname = 'X'\n")

    monkeypatch.chdir(tmp_path)

    with pytest.raises(RustyError, match="invalid definition files"):
        DefinitionsLoader._validate_definitions_dir(["table"])


def test_load_files_reads_toml_definitions(tmp_path):
    file = tmp_path / "table.toml"
    file.write_text("[[table]]\nname = 'T'\n")

    assert DefinitionsLoader._load_files([file]) == {"table": [{"name": "T"}]}


def test_check_definition_keys_allows_reserved_keys_and_template_fields():
    template = {
        "name": "TABLE_NAME",
        "columns": [{"name": "COLUMN_NAME", "type": "VARCHAR"}],
        "depends_on": {},
    }
    definition = {
        "name": "DB.SCHEMA.TABLE",
        "wait_time": 1,
        "depends_on": {"schema": ["DB.SCHEMA"]},
        "columns": [{"name": "ID", "type": "NUMBER"}],
    }

    DefinitionsLoader._check_definition_keys("table", definition, template)


def test_check_definition_keys_reports_nested_invalid_key():
    template = {
        "name": "TABLE_NAME",
        "columns": [{"name": "COLUMN_NAME", "type": "VARCHAR"}],
        "depends_on": {},
    }
    definition = {
        "name": "DB.SCHEMA.TABLE",
        "columns": [{"name": "ID", "type": "NUMBER", "default": "0"}],
    }

    with pytest.raises(RustyError, match=r"columns\[0\]\.default"):
        DefinitionsLoader._check_definition_keys("table", definition, template)


def test_validate_definitions_accepts_resource_config_schema():
    provider_resources = {
        "table": ResourceConfig(
            ddl_command={"create": "CREATE", "alter": "ALTER", "drop": "DROP"},
            ddl_context={
                "name": "TABLE_NAME",
                "columns": [{"name": "COLUMN_NAME", "type": "VARCHAR"}],
                "depends_on": {},
            },
        )
    }
    definitions = {"table": [{"name": "T", "columns": [{"name": "ID", "type": "NUMBER"}]}]}

    DefinitionsLoader._validate_definitions(provider_resources, definitions)
