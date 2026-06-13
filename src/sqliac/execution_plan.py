"""Execution plan DAG (Directed Acyclic Graph) building."""

from __future__ import annotations

from collections import deque
from typing import Any
from dataclasses import dataclass, field
from pathlib import Path

from sqliac.errors import RustyError
from sqliac.constants import Paths


@dataclass
class ExecutionPlanResult:
    """Encapsulates the execution plan result."""

    dependency_graph: dict[str, set[str]] = field(default_factory=dict)
    reverse_dependency_graph: dict[str, set[str]] = field(default_factory=dict)


class ExecutionPlan:
    """Builds and validates execution plans for resources."""

    def __init__(self, rsc_definitions: dict[str, list[dict[str, Any]]]) -> None:  # noqa: D107
        self.rsc_definitions = rsc_definitions

    def build_execution_plan(self) -> ExecutionPlanResult:
        """Build an execution plan from resource definitions."""
        dependency_graph = self._build_dependency_graph()

        self._validate_dependencies(dependency_graph)

        reverse_dependency_graph = self._build_reverse_graph(dependency_graph)

        self._detect_cycles(dependency_graph, reverse_dependency_graph)

        return ExecutionPlanResult(
            dependency_graph=dependency_graph,
            reverse_dependency_graph=reverse_dependency_graph,
        )

    def _build_dependency_graph(self) -> dict[str, set[str]]:
        """Build the forward dependency graph."""
        dependency_graph = {}
        for resource_type, definitions in self.rsc_definitions.items():
            if not definitions:
                continue

            for resource_def in definitions:
                resource_id = f"{resource_type}::{resource_def['name']}"

                # Check that depends_on exists
                depends_on: dict[str, list[str]] = resource_def.get("depends_on", {})

                # Build set of dependency IDs
                dependencies = {
                    f"{dep_type}::{dep_name}"
                    for dep_type, dep_names in depends_on.items()
                    for dep_name in dep_names
                }

                dependency_graph[resource_id] = dependencies
        return dependency_graph

    def _validate_dependencies(self, dependency_graph: dict[str, set[str]]) -> None:
        """Validate that all declared dependencies exist."""
        # Collect all resource IDs that exist
        existing_resources = set(dependency_graph.keys())

        referenced_resources = set().union(*dependency_graph.values())

        # Find missing resources
        missing = referenced_resources - existing_resources

        if missing:
            blocks = []

            for item in missing:
                rsc, name = item.split("::")

                block = f"""\
            --> {rsc}.yml
            |
            1 | [[{rsc}]]
            2 | name = "{name}"
            |       ^^^ missing definition
            |
            """

                blocks.append(block)

            raise RustyError(
                error="invalid dependency reference",
                details="missing resources:\n\n" + "\n".join(blocks),
            )

    def _build_reverse_graph(
        self, dependency_graph: dict[str, set[str]]
    ) -> dict[str, set[str]]:
        """Build the reverse dependency graph (dependents)."""
        reverse_dependency_graph = {}
        for resource, dependencies in dependency_graph.items():
            # Ensure all resources exist in reverse graph
            reverse_dependency_graph.setdefault(resource, set())

            # For each dependency, add this resource as a dependent
            for dep in dependencies:
                reverse_dependency_graph.setdefault(dep, set()).add(resource)
        return reverse_dependency_graph

    def _detect_cycles(
        self,
        dependency_graph: dict[str, set[str]],
        reverse_graph: dict[str, set[str]],
    ) -> None:
        """Detect circular dependencies using Kahn's algorithm."""
        in_degree = {node: len(deps) for node, deps in dependency_graph.items()}

        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        processed_count = 0

        # Process nodes
        while queue:
            current = queue.popleft()
            processed_count += 1

            # Reduce dependency count of nodes that depend on current
            for dependent in reverse_graph.get(current, set()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # If we didn't process all nodes, there's a cycle
        if processed_count != len(in_degree):
            cycle_nodes = {node for node, degree in in_degree.items() if degree > 0}

            blocks = []

            for node in cycle_nodes:
                deps = dependency_graph[node] & cycle_nodes
                if not deps:
                    continue

                rsc, name = node.split("::")

                dep_lines = []
                line = 4

                for dep in deps:
                    drsc, dname = dep.split("::")
                    dep_lines.append(f" {line} | {drsc} = '{dname}'")
                    line += 1

                dep_str = "\n".join(dep_lines)

                block = f"""\
        --> {rsc}.yml
        |
        1 | [[{rsc}]]
        2 | name = "{name}"
        3 | [{rsc}.depends_on]
        {dep_str}
        |   ^^^^^^^^^^^^^ creates a cycle
        |
        """
                blocks.append(block)

            raise RustyError(
                error="circular dependency detected",
                details="\n".join(blocks),
                help="resources depend on each other in a cycle",
            )

    @staticmethod
    def _resolve_path(path: str) -> str:
        path_obj = Path(path)
        working_dir = Path.cwd()
        return str(path_obj if path_obj.is_absolute() else working_dir / path_obj)

    def generate_dot_graph(self) -> None:
        """Generate the dependencies graph and save as DOT text file."""
        plan = self.build_execution_plan()
        dot_lines = [
            "digraph G {",
            "  rankdir=LR;",
            "  node [shape=ellipse, style=filled, fillcolor=lightgrey, fontname=Helvetica];",
        ]

        for node, deps in sorted(plan.dependency_graph.items()):
            if deps:
                for dep in sorted(deps):
                    dot_lines.append(f'"{node}" -> "{dep}";')
            else:
                dot_lines.append(f'"{node}";')

        dot_lines.append("}")

        dot_graph_str = "\n".join(dot_lines)

        file_path = self._resolve_path(Paths.DEPENDENCIES_FILE)

        with open(file_path, "w", encoding="utf-8") as file:
            file.write(dot_graph_str)

        print(f"info: saved dependency graph to: '{file_path}'")
