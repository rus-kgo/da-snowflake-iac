"""Command line interface for sqliac."""

from __future__ import annotations

import argparse

from sqliac import (
    DDLCommand,
    IacAction,
    Paths,
    RunMode,
    TemplateType,
    __prog_name__,
    __version__,
)
from sqliac.adapters import AdapterFactory
from sqliac.definitions_loader import DefinitionsLoader
from sqliac.errors import RustyError
from sqliac.execution_plan import ExecutionPlan
from sqliac.main import run
from sqliac.providers_loader import ProviderConfig, ProviderLoader, ResourceConfig
from sqliac.template_engine import TemplateEngine


def _validate_inputs(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.command == "compile":
        if args.template == "ddl" and not args.operation:
            print("error: DDL compilation requires an operation\n")
            parser.print_help()
            exit()

        if args.template == "state" and args.operation:
            print("error: state compilation does not accept an operation\n")
            parser.print_help()
            exit()
        if not args.template and not args.operation:
            print("error: compile command requires the type of template\n")
            parser.print_help()
            exit()


def _build_drift_ddl_context(resource_config: ResourceConfig, command: str) -> dict:
    ddl_context = {
        "ddl_command": resource_config.ddl_command[command],
        "name": resource_config.ddl_context["name"],
        "add": {},
        "change": {},
        "remove": {},
    }

    if command == DDLCommand.CREATE:
        ddl_context.update({"add": resource_config.ddl_context})
    elif command == DDLCommand.ALTER:
        ddl_context.update({"add": resource_config.ddl_context})
        ddl_context.update({"change": resource_config.ddl_context})
        ddl_context.update({"remove": resource_config.ddl_context})

    return ddl_context


def _parse_resource_config(provider_config: ProviderConfig, resource_type: str):
    resource_config = provider_config.resources.get(resource_type)
    if not resource_config:
        raise RustyError(
            error=f"`{resource_type}` config is missing",
            file=str(Paths.PROVIDER_CONFIG_FILE),
        )
    if not resource_config.ddl_context:
        raise RustyError(
            error=f"the `{resource_type}` DDL context is empty",
            file=str(Paths.PROVIDER_CONFIG_FILE),
        )

    if not resource_config.ddl_template:
        raise RustyError(
            error=f"the `{resource_type}` DDL template SQL file is empty",
            file=str(Paths.ddl_template_file(resource_type)),
        )

    if not resource_config.state_query:
        raise RustyError(
            error=f"the `{resource_type}` state query SQL file is empty",
            file=str(Paths.state_file(resource_type)),
        )
    return resource_config


def _cmd_compile(args: argparse.Namespace) -> None:
    """Render a provider SQL template using its example definition."""
    template_engine = TemplateEngine()
    # TODO: the provider load does validation of all resources, not only the one
    # in the args, the command uv run sqliac compile --ddl create alert will fail
    # on stage not alert
    provider_config = ProviderLoader.load()

    resource_config = _parse_resource_config(provider_config, args.target)

    if args.template == TemplateType.DDL and not args.operation:
        for command in DDLCommand.executable():
            ddl_context = _build_drift_ddl_context(resource_config, command)

            sql = template_engine.render(
                resource_config.ddl_template, args.template, ddl_context
            )
            template_engine.print_sql(sql)

    elif (
        args.template == TemplateType.DDL and args.operation in DDLCommand.executable()
    ):
        ddl_context = _build_drift_ddl_context(resource_config, args.operation)
        sql = template_engine.render(
            resource_config.ddl_template, args.template, ddl_context
        )
        template_engine.print_sql(sql)

    elif args.template == TemplateType.STATE and not args.operation:
        sql = template_engine.render(
            resource_config.state_query, args.template, resource_config.ddl_context
        )
        template_engine.print_sql(sql)


def _cmd_list() -> None:
    """List available providers."""
    available_adapters = AdapterFactory().list_adapters()
    print(f"""available adapters:
-  {"\n-  ".join(available_adapters)}""")


def _cmd_init() -> None:
    """Create tool scafolding."""
    DefinitionsLoader.init_definitions()
    ProviderLoader.init_providers()


def _cmd_graph() -> None:
    """Print dependency graph from definitions."""
    provider_config = ProviderLoader.load()
    rsc_definitions = DefinitionsLoader.load(provider_config)
    ExecutionPlan(rsc_definitions).generate_dot_graph()


def _cmd_run(args: argparse.Namespace) -> None:
    run(iac_action=args.command, run_mode=args.run_mode)


def main() -> None:
    """Parse CLI inputs and execute commands."""
    parser = argparse.ArgumentParser(
        prog=__prog_name__,
        description="SQL Infrastructure as Code tool.",
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("list", help="print available adapters")
    subparsers.add_parser("graph", help="print resource dependency graph")
    subparsers.add_parser("init", help="project scafolding")

    apply_parser = subparsers.add_parser(
        IacAction.APPLY, help="executes the definitions"
    )
    apply_parser.add_argument(
        "--dry-run",
        dest="run_mode",
        action="store_const",
        const=RunMode.DRY_RUN,
        default=RunMode.LIVE_RUN,
    )

    destroy_parser = subparsers.add_parser(
        IacAction.DESTROY, help="destroy defined resources"
    )
    destroy_parser.add_argument(
        "--dry-run",
        dest="run_mode",
        action="store_const",
        const=RunMode.DRY_RUN,
        default=RunMode.LIVE_RUN,
    )

    compile_parser = subparsers.add_parser("compile", help="render a SQL template")

    query_type = compile_parser.add_mutually_exclusive_group()
    query_type.add_argument(
        "--state",
        dest="template",
        action="store_const",
        const="state",
        help="compile State SQL template",
    )
    query_type.add_argument(
        "--ddl",
        dest="template",
        action="store_const",
        const="ddl",
        help="compile DDL SQL template",
    )

    compile_parser.add_argument(
        dest="operation",
        nargs="?",
        help="DDL operation (create, alter or drop)",
        choices=["create", "alter", "drop"],
    )

    compile_parser.add_argument("target", help="<resource type> (e.g. database)")

    args = parser.parse_args()

    _validate_inputs(compile_parser, args)

    try:
        if args.command == "graph":
            _cmd_graph()
        elif args.command == "compile":
            _cmd_compile(args)
        elif args.command == "list":
            _cmd_list()
        elif args.command == "init":
            _cmd_init()
        elif args.command in (IacAction.APPLY, IacAction.DESTROY):
            _cmd_run(args)
        else:
            print(f"SQL IaC {__version__}")
    except RustyError as err:
        err.print()
