import unittest
from unittest.mock import patch, MagicMock

from drift import Drift, CheckResult
from errors import DefinitionKeyError

from sqlalchemy import create_engine

class TestDrift(unittest.TestCase):

    def test_normalize_definition(self):
        """Clean the resources definitions."""
        expected = {
            "name": "EXAMPLE_TABLE",
            "database": "EXAMPLE_DATABASE",
            "schema": "EXAMPLE_SCHEMA",
            "owner": "SYSADMIN",
            "comment": "THIS IS A TABLE COMMENT\nTHIS IS THE NEXT LINE",
            "columns": [
                {
                    "name": "COLUMN1",
                    "type": "VARCHAR",
                    "nullable": True,
                    "default": 1.2,
                    "comment": "COLUMN 1 DESCRIPTION",
                },
                {
                    "name": "COLUMN2",
                    "type": "VARIANT",
                    "nullable": True,
                    "default": 0,
                    "comment": "COLUMN 1 DESCRIPTION",
                },
                {
                    "name": "COLUMN3",
                    "type": "STRING",
                    "nullable": False,
                    "default": -1,
                    "comment": "COLUMN 1 DESCRIPTION",
                },
                {
                    "name": "COLUMN4",
                    "type": "INT",
                    "nullable": False,
                    "default": 0,
                    "comment": "COLUMN 2 DESCRIPTION",
                },
            ],
        }

        possible_input = {
            "Name": "example_table",
            "DATABASE": " Example_Database",
            "schema": "EXAMPLE_SCHEMA ",
            " owner": " Sysadmin ",
            "comment": "THIS IS A TABLE COMMENT\nTHIS IS THE NEXT LINE",
            "columns": [
                {
                    "Name": "COLUMN1",
                    "type": "VARCHAR",
                    " nullable": "True",
                    "default": "1.2",
                    "COMMENT": "COLUMN 1 DESCRIPTION",
                },
                {
                    "name": "COLUMN2",
                    "type": "VARIANT",
                    "nullable": "TRUE",
                    "default": 0,
                    "comment": "COLUMN 1 DESCRIPTION",
                },
                {
                    "name": "COLUMN3",
                    "type": "string",
                    "nullable": "false",
                    "default": -1,
                    "comment": "COLUMN 1 DESCRIPTION",
                },
                {
                    "name": "COLUMN4",
                    "type": "Int",
                    "nullable": False,
                    "default": "0",
                    "comment": "COLUMN 2 DESCRIPTION",
                },
            ],
        }

        drift = Drift(
            conn=MagicMock(),
        )
        clean_output = drift._normalize_definition(definition=possible_input)

        assert expected == clean_output, \
        f"\nExpected: \n{expected}" \
        f"\nGot: \n{clean_output}"

    def test_state_query(self):
        """Test fetching of the resource state query as dictionary."""
        expected_output = {
            "name": "actors",
            "database": "main",
            "schema": "main",
            "owner": "sqlite",
            "columns": [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "nullable": 1,
                    "default": None,
                },
                {
                    "name": "name",
                    "type": "TEXT",
                    "nullable": 0,
                    "default": None,
                },
            ],
        }

        state_query = """
        WITH table_info AS (
            SELECT 
                m.name as table_name,
                m.sql as table_definition,
                json_group_array(
                    json_object(
                        'name', p.name,
                        'type', p.type,
                        'nullable', CASE WHEN p."notnull" = 0 THEN 1 ELSE 0 END,
                        'default', p."dflt_value"
                    )
                ) as columns
            FROM sqlite_master m
            LEFT JOIN pragma_table_info('actors') p
            WHERE m.type = 'table'
            AND m.name = 'actors'
            GROUP BY m.name
        )
        SELECT json_object(
            'name', table_name,
            'database', 'main',
            'schema', 'main',
            'owner', 'sqlite',
            'columns', json(columns)
        ) as table_metadata
        FROM table_info;
        """

        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.exec_driver_sql("""
                CREATE TABLE IF NOT EXISTS actors (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    )
            """)

            drift = Drift(
                conn=conn,
            )

            result = drift._fetch_state_query(query=state_query)

        assert expected_output == result, \
        f"\nExpected:\n{expected_output}" \
        f"\nGot:\n{result}"

    def test_check_keys(self):

        expected_state = {
            "name": "",
            "database": "",
            "schema": "",
            "owner": "",
            "comment": "",
            "columns": [
                {
                    "name": "MATCH_NAME",
                    "type": "",
                    "nullable": "",
                    "default": 0,
                    "comment": "",
                },
            ],
        }

        correct_state = {
            "name": "",
            "database": "",
            "schema": "",
            "owner": "",
            "comment": "",
            "columns": [
                {
                    "name": "",
                    "type": "",
                    "nullable": "",
                    "default": 0,
                    "comment": "",
                },
            ],
        }


        wrong_state = {
            "name": "",
            "database": "",
            "schema": "",
            "owner": "",
            "comment": "",
            "wrong_extra":"",
            "columns": [
                {
                    "name": "",
                    "type": "",
                    "nullable": "",
                    "comment": "",
                },
            ],
        }

        drift = Drift(
            conn=MagicMock(),
        )

        # Test mismatch scenario
        with self.assertRaises(DefinitionKeyError) as cm:
            drift._check_keys(
                name="table",
                definition=expected_state,
                state=wrong_state,
            )

        err = cm.exception
        err_msg = str(err)

        self.assertEqual(
            first=(
                "Invalid or missing definition variables for the resource: 'table'."
                "\nVariables: "
                "\n- columns::default"
                "\n- wrong_extra"
                ),
            second=err_msg,
            )

        # Test the matching scenario
        result = drift._check_keys(
            name="table",
            definition=expected_state,
            state=correct_state,

        )

        self.assertTrue(result.match)

    def test_check_values(self):

        definition = {
            "name": "",
            "database": "",
            "schema": "new_schema",
            "owner": "",
            "comment": {"msg":"new_msg"},
            "columns": [
                {
                    "name": "new_column",
                    "type": "int",
                    "nullable": "",
                    "default": 0,
                    "comment": "",
                },
                {
                    "name": "column",
                    "type": "int",
                    "nullable": "",
                    "default": 0,
                    "comment": "",
                },
            ],
        }

        match_state = {
            "name": "",
            "schema": "new_schema",
            "database": "",
            "owner": "",
            "comment": {"msg":"new_msg"},
            "columns": [
                {
                    "name": "new_column",
                    "type": "int",
                    "nullable": "",
                    "default": 0,
                    "comment": "",
                },
                {
                    "name": "column",
                    "type": "int",
                    "nullable": "",
                    "default": 0,
                    "comment": "",
                },
            ],
        }

        mismatch_state = {
            "name": "",
            "database": "",
            "schema": "old_schema",
            "owner": "",
            "comment": {"msg":"msg"},
            "columns": [
                {
                    "name": "column",
                    "type": "int",
                    "nullable": "",
                    "default": 0,
                    "comment": "",
                },
            ],
        }
        drift = Drift(conn=MagicMock())

        # Test when there is no drift
        result = drift._check_values(
            definition=definition,
            state=match_state,
        )

        self.assertTrue(result.match)

        # Test when there is drift
        result = drift._check_values(
            definition=definition,
            state=mismatch_state,
        )

        self.assertFalse(result.match)

        expected_output = {
            "schema": "new_schema",
            "comment": {"msg":"new_msg"},
            "columns": [
                {
                    "name": "new_column",
                    "type": "int",
                    "nullable": "",
                    "default": 0,
                    "comment": "",
                },
            ],
        }

        self.assertEqual(
            result.diff,
            expected_output,
        )


    def test_resource_state(self):
        """Compare state query to resource definition.

        Check the keys of the state query matches
        the keys of the resource definition.
        Check the values drift.
        """
        definition = {
            "name": "",
            "database": "",
            "schema": "",
            "owner": "",
            "comment": "",
            "columns": [
                {
                    "name": "MATCH_NAME",
                    "type": "",
                    "nullable": "",
                    "default": 0,
                    "comment": "",
                },
            ],
        }

        mismatch_values = {
            "columns": [
                {
                    "name": "column_name_change",
                    "type": "string",
                },
            ],
        }


        drift = Drift(
            conn=MagicMock(),
        )

        # Test when resource does not exists
        with patch(
            "drift.Drift._fetch_state_query",
            return_value=None,
            ):

            result = drift.resource_state(
                definition=definition,
                state_query="",
                name="table",
            )

            expected_output = {
                "iac_action": "create",
                "definition": definition,
            }

            assert expected_output == result, \
            f"\nExpected:\n{expected_output}" \
            f"\nGot:\n{result}"

        # Test when resource exists and there is a drift
        with (
            patch(
                "drift.Drift._fetch_state_query",
                ),
            patch(
                "drift.Drift._check_keys",
                ) as mock_keys_check,
            patch(
                "drift.Drift._check_values",
                return_value=CheckResult(match=False,diff=mismatch_values),
                ),
        ):
            result = drift.resource_state(
                definition=definition,
                state_query="",
                name="table",
            )

            mock_keys_check.assert_called_once()

            expected_output = {
                "iac_action": "alter",
                "definition": mismatch_values,
            }

            assert expected_output == result, \
            f"\nExpected:\n{expected_output}" \
            f"\nGot:\n{result}"

        # Test when resource exists and there is no drift

        with (
            patch(
                "drift.Drift._fetch_state_query",
                ),
            patch(
                "drift.Drift._check_keys",
                ) as mock_keys_check,
            patch(
                "drift.Drift._check_values",
                return_value=CheckResult(match=True),
                ),
        ):
            result = drift.resource_state(
                definition=definition,
                state_query="",
                name="table",
            )

            mock_keys_check.assert_called_once()

            expected_output = {
                "iac_action": "no-action",
                "definition": None,
            }

            assert expected_output == result, \
            f"\nExpected:\n{expected_output}" \
            f"\nGot:\n{result}"





if __name__ == "__main__":
    unittest.main()
