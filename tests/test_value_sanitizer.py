import pytest

from sqliac.errors import RustyError
from sqliac.value_sanitizer import ValueSanitizer


def test_deep_clean_uppercases_values_but_preserves_case_sensitive_fields():
    sanitizer = ValueSanitizer()

    result = sanitizer.deep_clean(
        {
            "name": " db.public.users ",
            "comment": " Mixed Case Comment ",
            "columns": [{"name": "id", "type": "number"}],
        }
    )

    assert result == {
        "name": "DB.PUBLIC.USERS",
        "comment": "Mixed Case Comment",
        "columns": [{"name": "ID", "type": "NUMBER"}],
    }


def test_to_upper_string_handles_none():
    assert ValueSanitizer.to_upper_string(None) is None


def test_to_int_converts_numeric_strings():
    assert ValueSanitizer.to_int("42", source="wait_time") == 42


def test_to_int_rejects_non_numeric_values():
    with pytest.raises(RustyError, match="value must be convertible to an integer"):
        ValueSanitizer.to_int("not-a-number", source="wait_time")


def test_deep_clean_rejects_too_much_nesting():
    value = "leaf"
    for _ in range(ValueSanitizer.MAX_DEPTH + 1):
        value = {"nested": value}

    with pytest.raises(RustyError, match="nesting limit exceeded"):
        ValueSanitizer().deep_clean(value)
