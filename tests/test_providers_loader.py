import pytest

from sqliac.errors import RustyError
from sqliac.providers_loader import ProviderLoader, ResourceConfig


def test_resource_config_requires_dict_fields():
    with pytest.raises(RustyError, match="ddl_command"):
        ResourceConfig(ddl_command="CREATE", ddl_context={"name": "T", "depends_on": {}})

    with pytest.raises(RustyError, match="ddl_context"):
        ResourceConfig(ddl_command={}, ddl_context="name = T")


def test_resource_config_requires_name_and_depends_on():
    with pytest.raises(RustyError, match="missing `name`"):
        ResourceConfig(ddl_command={}, ddl_context={"depends_on": {}}, resource_type="table")

    with pytest.raises(RustyError, match="missing `depends_on`"):
        ResourceConfig(ddl_command={}, ddl_context={"name": "T"}, resource_type="table")


def test_resource_config_rejects_unknown_ddl_command():
    with pytest.raises(RustyError, match="invalid DDL command"):
        ResourceConfig(
            ddl_command={"rename": "RENAME"},
            ddl_context={"name": "T", "depends_on": {}},
        )


def test_parse_single_resource_reads_templates_from_provider_dir(tmp_path, monkeypatch):
    resource_dir = tmp_path / ".sqliac" / "provider" / "table"
    resource_dir.mkdir(parents=True)
    (resource_dir / "ddl_template.sql").write_text("CREATE TABLE {{ name }};")
    (resource_dir / "state.sql").write_text("SELECT '{}';")
    config_file = tmp_path / ".sqliac" / "provider" / "config.toml"
    config_file.write_text("[snowflake.table]\n")
    monkeypatch.chdir(tmp_path)

    config = {
        "ddl_command": {"create": "CREATE", "alter": "ALTER", "drop": "DROP"},
        "ddl_context": {"name": "T", "depends_on": {}},
    }

    result = ProviderLoader._parse_single_resource("table", config)

    assert result.ddl_template == "CREATE TABLE {{ name }};"
    assert result.state_query == "SELECT '{}';"
    assert result.ddl_command == config["ddl_command"]
    assert result.ddl_context == config["ddl_context"]


def test_parse_single_resource_requires_template_files(tmp_path, monkeypatch):
    (tmp_path / ".sqliac" / "provider" / "table").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(RustyError, match="missing `table` DDL template"):
        ProviderLoader._parse_single_resource("table", {})
