"""Main entrance point of the pipeline."""

from __future__ import annotations

from sqliac.adapters import AdapterFactory
from sqliac.constants import IacAction, RunMode
from sqliac.definitions_loader import DefinitionsLoader
from sqliac.errors import RustyError
from sqliac.execution_plan import ExecutionPlan
from sqliac.providers_loader import ProviderLoader
from sqliac.scheduler import Scheduler


def run(iac_action: IacAction, run_mode: RunMode) -> None:
    """Entry point of the pipeline."""
    try:
        provider_config = ProviderLoader.load()
        rsc_definitions = DefinitionsLoader.load(provider_config)
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
