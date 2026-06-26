"""Resource and definition configuration management."""

from __future__ import annotations

import tomllib
from importlib.resources import files
from pathlib import Path
from tomllib import TOMLDecodeError
from typing import TYPE_CHECKING, Any

from dictdiffer import diff

from sqliac.constants import Paths
from sqliac.errors import RustyError

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable

    from sqliac.providers_loader import ProviderConfig, ResourceConfig


def _resolve_path(path: Path):
    return path if path.is_absolute() else Path.cwd() / path


class DefinitionsLoader:
    """Manages the loading of resouce definitions."""

    @classmethod
    def load(
        cls, provider_config: ProviderConfig
    ) -> dict[str, list[dict[str, str | bool | dict[Any, Any] | None]]]:
        """Load all the definitions."""
        available_resources = list(provider_config.resources.keys())
        definition_files = cls._validate_definitions_dir(available_resources)
        definitions = cls._load_files(definition_files)
        cls._validate_definitions(provider_config.resources, definitions)

        return definitions

    @classmethod
    def _validate_definitions_dir(cls, available_resources: list[str]) -> list[Path]:
        target_dir = _resolve_path(Paths.DEFINITIONS_DIR)

        try:
            definition_files = target_dir.iterdir()
        except FileNotFoundError:
            raise RustyError(
                error="invalid definitions directory",
                file=str(target_dir),
                help=f"resources definitions must be in \
                `{str(target_dir)}` directory",
            ) from None
        else:
            toml_files = [
                f for f in definition_files if f.is_file() and f.suffix == ".toml"
            ]

            invalid_files = {
                f.stem for f in toml_files if f.stem not in available_resources
            }

            if invalid_files:
                invalid_files_list = "\n  - ".join(invalid_files)
                raise RustyError(
                    error="invalid definition files",
                    file=str(target_dir),
                    details=f"""invalid definitions:
    - {invalid_files_list if invalid_files_list else "None"}""",
                    help="only include definition of resources that are \
                    available in the provider config",
                ) from None

        return toml_files

    @classmethod
    def _load_files(cls, definition_files: list[Path]) -> dict[str, Any]:
        """Load resource definitions from TOML files."""
        definitions = {}

        for file in definition_files:
            try:
                with open(file, "rb") as f:
                    resource_definitions: dict[str, Any] = tomllib.load(f)
            except TOMLDecodeError as err:
                raise RustyError(
                    error="invalid TOML file",
                    file=str(file),
                    help="check TOML formatting in the definition file",
                ) from err

            except PermissionError:
                raise RustyError(
                    error="read permission denied",
                    file=str(file),
                    help="the file might be open by another application or missing read permissions",
                ) from None

            else:
                definitions.update(resource_definitions)

        return definitions

    @classmethod
    def _validate_definitions(
        cls,
        provider_resources: dict[str, ResourceConfig],
        all_definitions: dict[str, Any],
    ) -> None:
        for resource, definitions in all_definitions.items():
            definition_template = provider_resources[resource].ddl_context

            for item in definitions:
                cls._check_definition_keys(
                    resource_type=resource,
                    definition=item,
                    template=definition_template,
                )

    @staticmethod
    def _check_definition_keys(resource_type, definition, template) -> None:
        """Validate definition keys against template."""
        keys_check_result = list(diff(first=template, second=definition))

        invalid_keys = set()

        for action, path, details in keys_check_result:
            if path != "":
                continue

            if action == "add":
                for tpl in details:
                    key = tpl[0] if len(tpl) > 1 else None
                    invalid_keys.add(key)

        expected_keys_str = "\n  - ".join(set(template.keys()))

        if "name" not in definition:
            raise RustyError(
                error="mising `name` argument in the definition",
                file=f".\\{resource_type}.toml",
                help=f"""review arguments for each definition of the resource\n
expected arguments:\n  - {expected_keys_str}\n""",
            )
        if invalid_keys:
            invalid_keys_str = "\n  - ".join(invalid_keys)
            raise RustyError(
                error="invalid definition arguments",
                file=f".\\{resource_type}.toml",
                details=f"""`{definition["name"]}` definition\n
invalid arguments:\n  - {invalid_keys_str if invalid_keys_str else "None"}\n
expected arguments:\n  - {expected_keys_str}""",
                help="review arguments for each definition of the resource",
            )

    @classmethod
    def _copy_resources(cls, source_dir: Traversable, target_dir: Path):
        for item in source_dir.iterdir():
            dest = target_dir / item.name
            if item.is_file():
                dest.write_text(item.read_text())
            else:
                dest.mkdir(exist_ok=True)
                cls._copy_resources(item, dest)

    @classmethod
    def init_definitions(cls):
        """Create definitions scaffolding."""
        source_dir = files(Paths.TEMPLATES_ANCHOR) / Paths.DEFINITIONS_DIR

        target_dir = _resolve_path(Paths.DEFINITIONS_DIR)
        target_dir.mkdir(parents=True, exist_ok=True)

        cls._copy_resources(source_dir, target_dir)
