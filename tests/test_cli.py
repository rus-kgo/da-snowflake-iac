import argparse

import pytest

from sqliac.cli import _validate_inputs


def test_validate_inputs_uses_subparser_help_for_compile_errors(capsys):
    parser = argparse.ArgumentParser(prog="sqliac")
    subparsers = parser.add_subparsers(dest="command")
    compile_parser = subparsers.add_parser("compile")
    subparser_map = {"compile": compile_parser}

    args = argparse.Namespace(command="compile", template=None, operation=None)

    with pytest.raises(SystemExit):
        _validate_inputs(parser, args, subparser_map)

    captured = capsys.readouterr()
    assert "error: compile command requires the type of template" in captured.out
    assert "usage:" in captured.out
