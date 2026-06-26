"""Scheduler for executing database infrastructure tasks."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any

from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text

from sqliac import DDLCommand, IacAction, RunMode, TemplateType
from sqliac.drift import Drift
from sqliac.errors import RustyError
from sqliac.template_engine import TemplateEngine

if TYPE_CHECKING:
    from sqliac.adapters.base import BaseAdapter
    from sqliac.execution_plan import ExecutionPlanResult
    from sqliac.providers_loader import ResourceConfig

TASK_SEPARATOR = "::"
MAX_THREADS = 3


class Scheduler:
    """Orchestrates parallel execution of database infrastructure tasks."""

    def __init__(
        self,
        provider_resources: dict[str, ResourceConfig],
        definitions: dict[str, list[dict[str, Any]]],
        run_mode: str,
        iac_action: str,
        connection: BaseAdapter,
    ):
        """Initialize the scheduler."""
        self.conn = connection
        self.iac_action = iac_action
        self.run_mode = run_mode
        self.provider_resources = provider_resources
        self.definitions = definitions

        self.drift = Drift(connection=connection)
        self.template_engine = TemplateEngine()

        self.succeded_tasks: set[str] = set()

    def execute_rendered_sql_template(
        self,
        adapter: BaseAdapter,
        sql: str,
        depends_on: dict[str, dict[Any, Any]],
        wait_time: int | None = None,
    ) -> None:
        """Execute rendered SQL template using adapter."""
        message_lines = []

        sql_pretty_syntax = TemplateEngine.pretty_sql(sql=sql, as_syntax=True)
        if wait_time:
            message_lines.append(
                Text.assemble(
                    ("wait time: ", "cyan"),
                    (str(wait_time), ""),
                    (" s."),
                )
            )

        if depends_on:
            dep_lines = "\n".join(f"- {k}: {v}" for k, v in depends_on.items())
            message_lines.append(
                Text.assemble(("depends on:\n", "cyan"), (dep_lines, "")),
            )

        if self.run_mode != RunMode.DRY_RUN:
            adapter.execute(sql)

            color = "green"
            title = "[green]Live Run[/green]"

        else:
            color = "yellow"
            title = "[yellow]Dry Run[/yellow]"

        message_lines.append(Text())
        message_lines.append(Text("sql statement:", style=color))
        message_lines.append(sql_pretty_syntax)

        msg = Group(*message_lines)

        Console(force_terminal=True).print(
            Panel(
                msg,
                title=title,
                expand=False,
                border_style=color,
            ),
        )

        if self.run_mode != RunMode.DRY_RUN and wait_time:
            time.sleep(wait_time)

    def run_task(self, task: str) -> None:  # noqa: PLR0912
        """Execute a single task with comprehensive error handling."""
        rsc_type, rsc_name = task.split(TASK_SEPARATOR)

        for ddl_context in self.definitions[rsc_type]:
            if ddl_context["name"] != rsc_name:
                continue

            rsc_state_query = self.template_engine.render(
                template=self.provider_resources[rsc_type].state_query,
                template_type=TemplateType.STATE,
                context=ddl_context,
            )

            # Step 2: Check drift
            drift_ddl_context = self.drift.resource_state(
                definition=ddl_context,
                template=self.provider_resources[rsc_type].ddl_context,
                resource_type=rsc_type,
                state_query=rsc_state_query,
                name=rsc_name,
            )

            # Step 3: No action needed
            if drift_ddl_context.ddl_command == DDLCommand.NO_ACTION:
                msg = Text.assemble(
                    ("no changes detected in ", "cyan"),
                    (f"'{rsc_type}' "),
                    ("named as ", "cyan"),
                    (f"'{rsc_name}'"),
                )

                Console(force_terminal=True).print(
                    Panel(
                        msg,
                        title="[cyan]Relax[/cyan]",
                        border_style="cyan",
                        expand=False,
                    )
                )
                continue

            # Step 4: Handle create-or-update
            if self.iac_action == IacAction.APPLY:
                drift_ddl_context.ddl_command = self.provider_resources[
                    rsc_type
                ].ddl_command[drift_ddl_context.ddl_command.value]

                render_result = self.template_engine.render(
                    template=self.provider_resources[rsc_type].ddl_template,
                    template_type=TemplateType.DDL,
                    context=drift_ddl_context.to_dict(),
                )

                try:
                    self.execute_rendered_sql_template(
                        adapter=self.conn,
                        sql=render_result,
                        depends_on=ddl_context["depends_on"],
                        wait_time=ddl_context.get("wait_time", None),
                    )
                except RustyError as err:
                    err.print()
                except Exception as err:
                    raise RustyError(
                        error=f"failed to execute SQL query, task: {task}",
                        details=str(err),
                    ) from err
                else:
                    self.succeded_tasks.add(task)

            # Step 5: Handle destroy
            if self.iac_action == IacAction.DESTROY:
                render_result = self.template_engine.render(
                    template=self.provider_resources[rsc_type].ddl_template,
                    template_type=TemplateType.DDL,
                    context={
                        "name": rsc_name,
                        "ddl_command": self.provider_resources[rsc_type].ddl_command[
                            DDLCommand.DROP
                        ],
                    },
                )

                try:
                    self.execute_rendered_sql_template(
                        adapter=self.conn,
                        sql=render_result,
                        depends_on=ddl_context["depends_on"],
                        wait_time=ddl_context.get("wait_time", None),
                    )
                except Exception as err:
                    raise RustyError(
                        error="failed to execute SQL query.",
                        details=f"faild task: {task}",
                    ) from err
                else:
                    self.succeded_tasks.add(task)

    def run(self, execution_plan: ExecutionPlanResult) -> None:
        """Execute all scheduled tasks and collect errors."""
        completed = set()
        ready = {n for n, d in execution_plan.dependency_graph.items() if not d}
        futures = {}

        with ThreadPoolExecutor(
            max_workers=MAX_THREADS, thread_name_prefix=self.conn.dialect
        ) as executor:
            while ready or futures:
                # submit all ready tasks
                while ready:
                    task = ready.pop()
                    future = executor.submit(self.run_task, task)
                    futures[future] = task

                # wait for one task to finish
                for future in as_completed(futures):
                    task = futures.pop(future)
                    future.result()
                    completed.add(task)

                    # unlock downstream tasks
                    for child in execution_plan.reverse_dependency_graph[task]:
                        execution_plan.dependency_graph[child].remove(task)
                        if not execution_plan.dependency_graph[child]:
                            ready.add(child)
