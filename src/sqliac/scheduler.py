"""Scheduler for executing database infrastructure tasks."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pprint import pformat
from typing import TYPE_CHECKING, Any

from sqliac import DDLCommand, IacAction, RunMode, TemplateType
from sqliac.drift import Drift, DriftDDLContext
from sqliac.errors import RustyError
from sqliac.template_engine import TemplateEngine
from sqliac.utils import box_message

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
        wait_time: int | None = None,
    ) -> str:
        """Execute rendered SQL template using adapter."""

        exec_output = ""
        if self.run_mode != RunMode.DRY_RUN:
            exec_output = adapter.execute(sql)
            exec_output = (
                exec_output[0]
                if isinstance(exec_output, tuple) and len(exec_output) > 0
                else ""
            )

        if self.run_mode != RunMode.DRY_RUN and wait_time:
            time.sleep(wait_time)

        return exec_output

    def _print_execution_message(
        self,
        sql: str,
        exec_output: str,
        rsc_type: str,
        depends_on: dict[str, dict[Any, Any]],
        drift_ddl_context: DriftDDLContext,
        wait_time: int | None,
    ) -> None:
        """Prints the message lines and title for SQL execution output."""
        message_lines = []
        sql_pretty_syntax = TemplateEngine.pretty_sql(sql=sql)

        message_lines.append(f"resource: {rsc_type}")
        if wait_time:
            message_lines.append(f"wait time: {wait_time}")

        if depends_on:
            dep_lines = "\n".join(f"- {k}: {v}" for k, v in depends_on.items())
            message_lines.append(f"depends on:\n{dep_lines}")

        if not self.iac_action == IacAction.DESTROY:
            message_lines.append(
                f"scheduled changes:\n{pformat(drift_ddl_context.to_dict(), indent=2, expand=True, width=80, sort_dicts=False)}"
            )

        if sql:
            message_lines.append(
                f"\nstatement output: {exec_output}\n{box_message(title='SQL', message=sql_pretty_syntax)}"
            )

        title = "Dry Run" if self.run_mode == RunMode.DRY_RUN else "Live Run"
        print(box_message(title=title, message="\n".join(message_lines), width=100))

    def run_task(self, task: str) -> None:
        """Execute a single task with comprehensive error handling."""
        rsc_type, rsc_name = task.split(TASK_SEPARATOR)

        for ddl_context in self.definitions[rsc_type]:
            if ddl_context["name"] != rsc_name:
                continue

            rsc_state_query = self.template_engine.render(
                template=self.provider_resources[rsc_type].state_query,
                rsc_type=rsc_type,
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
            if (
                drift_ddl_context.ddl_command == DDLCommand.NO_ACTION
                and self.iac_action == IacAction.APPLY
            ):
                self._print_execution_message(
                    sql="",
                    exec_output="",
                    rsc_type=rsc_type,
                    depends_on=ddl_context["depends_on"],
                    wait_time=ddl_context.get("wait_time", None),
                    drift_ddl_context=drift_ddl_context,
                )
                continue

            # Step 4: Handle create-or-update
            if self.iac_action == IacAction.APPLY:
                drift_ddl_context.ddl_command = self.provider_resources[
                    rsc_type
                ].ddl_command[drift_ddl_context.ddl_command.value]

                render_result = self.template_engine.render(
                    template=self.provider_resources[rsc_type].ddl_template,
                    rsc_type=rsc_type,
                    template_type=TemplateType.DDL,
                    context=drift_ddl_context.to_dict(),
                )

                try:
                    exec_output = self.execute_rendered_sql_template(
                        adapter=self.conn,
                        sql=render_result,
                        wait_time=ddl_context.get("wait_time", None),
                    )
                    self._print_execution_message(
                        sql=render_result,
                        exec_output=exec_output,
                        rsc_type=rsc_type,
                        depends_on=ddl_context["depends_on"],
                        wait_time=ddl_context.get("wait_time", None),
                        drift_ddl_context=drift_ddl_context,
                    )
                except RustyError as err:
                    print(err)
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
                    rsc_type=rsc_type,
                    template_type=TemplateType.DDL,
                    context={
                        "name": rsc_name,
                        "ddl_command": self.provider_resources[rsc_type].ddl_command[
                            DDLCommand.DROP
                        ],
                    },
                )

                try:
                    exec_output = self.execute_rendered_sql_template(
                        adapter=self.conn,
                        sql=render_result,
                        wait_time=ddl_context.get("wait_time", None),
                    )
                    self._print_execution_message(
                        sql=render_result,
                        rsc_type=rsc_type,
                        exec_output=exec_output,
                        depends_on=ddl_context["depends_on"],
                        wait_time=ddl_context.get("wait_time", None),
                        drift_ddl_context=drift_ddl_context,
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
