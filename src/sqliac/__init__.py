"""sqliac - SQL Infrastructure as Code."""

__version__ = "0.1.0"
__author__ = "Ruslan Gonzalez Konstantinov"
__license__ = "MIT"
__prog_name__ = "sqliac"

from sqliac.constants import TemplateType, DDLCommand, IacAction, RunMode, Paths

__all__ = [
    # Version
    "__version__",
    "__author__",
    "__license__",
    "TemplateType",
    "DDLCommand",
    "IacAction",
    "RunMode",
    "Paths",
]
