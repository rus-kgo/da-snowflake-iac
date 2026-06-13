"""Main entrance point of the pipeline."""

from __future__ import annotations

from pathlib import Path

from sqliac.adapters import AdapterFactory
from sqliac.execution_plan import ExecutionPlan
from sqliac.definitions_loader import DefinitionsLoader
from sqliac.providers_loader import ProviderLoader
from sqliac.scheduler import Scheduler
from sqliac.errors import RustyError
from sqliac.constants import Paths, IacAction, RunMode


def _resolve_path(path: str) -> str:
    path_obj = Path(path)
    working_dir = Path.cwd()
    return str(path_obj if path_obj.is_absolute() else working_dir / path_obj)


def run(iac_action: IacAction, run_mode: RunMode) -> None:
    """Entry point of the pipeline."""
    try:
        definitions_path = _resolve_path(Paths.DEFINITIONS_DIR)
        provider_path = _resolve_path(f"{Paths.CONFIG_DIR}\\{Paths.PROVIDER_DIR}")

        provider_config = ProviderLoader(provider_path).load()

        rsc_definitions = DefinitionsLoader(definitions_path).load(provider_config)

        execution_plan = ExecutionPlan(rsc_definitions).build_execution_plan()

        adapter = AdapterFactory.get_adapter(
            provider=provider_config.name,
        )

        with adapter as conn:
            scheduler = Scheduler(
                connection=conn,
                iac_action=iac_action,
                run_mode=run_mode,
                provider_resources=provider_config.resources,
                definitions=rsc_definitions,
            )

            scheduler.run(execution_plan=execution_plan)
    except RustyError as err:
        print(err)
