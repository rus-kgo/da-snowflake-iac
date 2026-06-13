"""Resource and definition configuration management."""

from __future__ import annotations

import os
import tomllib
from importlib.resources import files
from tomllib import TOMLDecodeError
from typing import Any, TYPE_CHECKING
from pathlib import Path
from dictdiffer import diff


from sqliac.errors import RustyError
from sqliac.constants import Paths

if TYPE_CHECKING:
    from sqliac.providers_loader import ProviderConfig, ResourceConfig
    from importlib.resources.abc import Traversable


class DefinitionsLoader:
    """Manages the loading of resouce definitions."""

    def __init__(self, directory: str) -> None:  # noqa: D107
        self.directory_path = self._validate_dir(directory)

    def load(
        self, provider_config: ProviderConfig
    ) -> dict[str, list[dict[str, str | bool | dict[Any, Any] | None]]]:
        """Load all the definitions."""
        available_resources = list(provider_config.resources.keys())

        definition_files = self._validate_definitions_dir(available_resources)

        definitions = self._load_files(definition_files)

        self._validate_definitions(provider_config.resources, definitions)

        return definitions

    @staticmethod
    def _validate_dir(directory: str) -> Path:
        directory_path = Path(directory)
        if not directory_path.exists():
            raise RustyError(
                error="definitions directory not found",
                file=directory,
                help=f"create the definitions directory: `.\\{Paths.DEFINITIONS_DIR}\\<resource type>.toml`",
            )
        if not directory_path.is_dir():
            raise RustyError(
                error="definitions path must be a directory",
                file=directory,
            )
        return directory_path

    def _load_files(self, definition_files: list[str]) -> dict[str, Any]:
        """Load resource definitions from TOML files."""
        definitions = {}

        # Load each definition file
        for file in definition_files:
            if not file.endswith(".toml"):
                continue

            file_path = os.path.join(self.directory_path, file)

            try:
                with open(file_path, "rb") as f:
                    resource_definitions: dict[str, Any] = tomllib.load(f)
            except TOMLDecodeError as err:
                raise RustyError(
                    error="invalid TOML file",
                    file=file,
                    help="check TOML formatting in the definition file",
                ) from err

            except PermissionError:
                raise RustyError(
                    error="read permission denied",
                    file=file,
                    help="the file might be open by another application or missing read permissions",
                ) from None

            else:
                definitions.update(resource_definitions)

        return definitions

    def _validate_definitions_dir(self, available_resources: list[str]) -> list[str]:
        definition_files = []
        try:
            definition_files: list[str] = os.listdir(self.directory_path)
        except FileNotFoundError:
            raise RustyError(
                error="invalid definitions directory",
                file=str(self.directory_path),
                help="""create resources definitions directory `.\\definitions\\<provider name>\\`""",
            ) from None

        except PermissionError:
            raise RustyError(
                error="permission denied reading definitions directory",
                file=str(self.directory_path),
                help="the file might be open by another application or missing permissions",
            ) from None

        else:
            toml_files = [f for f in definition_files if f.endswith(".toml")]
            invalid_resources = {
                f
                for f in toml_files
                if f.removesuffix(".toml") not in available_resources
            }

            if invalid_resources:
                invalid_resources_str = "\n  - ".join(invalid_resources)
                raise RustyError(
                    error="invalid definition or it's resource is not available",
                    file=str(self.directory_path),
                    details=f"""invalid definitions:
   - {invalid_resources_str if invalid_resources_str else "None"}""",
                    help="remove definition of resources that are not available for the provider",
                ) from None

        return definition_files

    def _validate_definitions(
        self,
        provider_resources: dict[str, ResourceConfig],
        all_definitions: dict[str, Any],
    ) -> None:
        for resource, definitions in all_definitions.items():
            definition_template = provider_resources[resource].ddl_context

            for item in definitions:
                self._check_definition_keys(
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
        source_dir = files(Paths.TEMPLATES_ANCHOR).joinpath(Paths.DEFINITIONS_DIR)

        target_dir = Path(Paths.DEFINITIONS_DIR)
        target_dir.mkdir(parents=True, exist_ok=True)

        cls._copy_resources(source_dir, target_dir)
