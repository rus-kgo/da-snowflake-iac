"""Command line interface for sqliac."""

from __future__ import annotations

import argparse
import logging
import re

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
from sqliac.utils import box_message


def _validate_inputs(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    subparser_map: dict[str, argparse.ArgumentParser] | None = None,
) -> None:
    active_subparser = None
    if subparser_map and args.command in subparser_map:
        active_subparser = subparser_map[args.command]

    if "threads" in args and (
        args.threads is None or not re.match(r"^[+-]?[1-9]+$", args.threads)
    ):
        print("error: threads value must be a positivie integer greater than 0")
        if active_subparser is not None:
            active_subparser.print_help()
        else:
            parser.print_help()
        exit(1)

    if args.command == "compile":
        if not args.template:
            print("error: compile command requires the type of template\n")
            if active_subparser is not None:
                active_subparser.print_help()
            else:
                parser.print_help()
            exit(1)
        if args.template == "ddl" and not args.operation:
            print("error: DDL compilation requires an operation\n")
            if active_subparser is not None:
                active_subparser.print_help()
            else:
                parser.print_help()
            exit(1)

        if args.template == "state" and args.operation:
            print("error: state compilation does not accept an operation\n")
            if active_subparser is not None:
                active_subparser.print_help()
            else:
                parser.print_help()
            exit(1)


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
    provider_config = ProviderLoader.load(args.target)

    resource_config = _parse_resource_config(provider_config, args.target)

    if args.template == TemplateType.DDL and args.operation in DDLCommand.executable():
        ddl_context = _build_drift_ddl_context(resource_config, args.operation)
        sql = template_engine.render(
            template=resource_config.ddl_template,
            rsc_type=args.target,
            template_type=args.template,
            context=ddl_context,
        )
        print(box_message(title="DDL SQL", message=template_engine.pretty_sql(sql)))

    elif args.template == TemplateType.STATE and not args.operation:
        sql = template_engine.render(
            template=resource_config.state_query,
            rsc_type=args.target,
            template_type=args.template,
            context=resource_config.ddl_context,
        )
        print(box_message(title="State SQL", message=template_engine.pretty_sql(sql)))


def _cmd_list() -> None:
    """List available providers."""
    available_adapters = AdapterFactory().list_adapters()
    print(f"""available adapters:
-  {"\n-  ".join(available_adapters)}""")


def _cmd_init() -> None:
    """Create tool scaffolding."""
    DefinitionsLoader.init_definitions()
    ProviderLoader.init_providers()


def _cmd_graph() -> None:
    """Print dependency graph from definitions."""
    provider_config = ProviderLoader.load()
    rsc_definitions = DefinitionsLoader.load(provider_config)
    ExecutionPlan(rsc_definitions).generate_dot_graph()


def _cmd_run(args: argparse.Namespace) -> None:
    run(iac_action=args.command, run_mode=args.run_mode, threads=int(args.threads))


def _setup_logging(log: str, log_out: str):
    """Configures global logging for the application."""
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    level = log.upper() if log else "CRITICAL"
    if log_out:
        level = log_out.upper() if log_out else "CRITICAL"
        file_handler = logging.FileHandler("sqliac.log", mode="a", encoding="utf-8")
        handlers.append(file_handler)

    logging.basicConfig(
        level=level,
        format="%(asctime)s|%(name)s|%(lineno)d|%(levelname)s: %(message)s",
        handlers=handlers,
        force=True,
    )


def main() -> None:
    """Parse CLI inputs and execute commands."""
    parser = argparse.ArgumentParser(
        prog=__prog_name__,
        description="SQL Infrastructure as Code tool.",
    )

    parser.add_argument(
        "--log",
        nargs="?",
        default="",
        help="enable logging.",
        choices=["info", "debug"],
    )
    parser.add_argument(
        "--log-out",
        nargs="?",
        default="",
        help="log output to file.",
        choices=["info", "debug"],
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("list", help="print available adapters")
    subparsers.add_parser(
        "graph", help="print avaliable resources definitions dependency graph"
    )
    subparsers.add_parser("init", help="project scaffolding")

    apply_parser = subparsers.add_parser(
        IacAction.APPLY, help="executes the definitions"
    )

    apply_parser.add_argument(
        "--threads",
        nargs="?",
        default="3",
        help="nubmer of threads",
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
        "--threads",
        nargs="?",
        default="3",
        help="nubmer of threads",
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

    subparser_map = {
        "list": subparsers.choices["list"],
        "graph": subparsers.choices["graph"],
        "init": subparsers.choices["init"],
        IacAction.APPLY: subparsers.choices[IacAction.APPLY],
        IacAction.DESTROY: subparsers.choices[IacAction.DESTROY],
        "compile": subparsers.choices["compile"],
    }

    _validate_inputs(parser, args, subparser_map)
    _setup_logging(args.log, args.log_out)

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
        print(err)
        exit(1)
    except Exception as err:
        logging.critical(f"unexpected system error: {err}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
