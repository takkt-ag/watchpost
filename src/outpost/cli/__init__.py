# Copyright 2025 TAKKT Industrial & Packaging GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Iterable

from rich.live import Live

from outpost.executor import BlockingCheckExecutor, CheckExecutor

try:
    import click  # type: ignore
    from rich.console import Console
    from rich.table import Table
except ImportError as e:
    raise ImportError(
        "To use the Outpost CLI, you have to install outpost with the `cli` extra, e.g. by running: uv add outpost[cli]"
    ) from e

from ..app import Outpost
from ..result import CheckState, ExecutionResult
from .loader import find_app

STATE_STYLES = {
    CheckState.OK: "bold green",
    CheckState.WARN: "bold yellow",
    CheckState.CRIT: "bold red",
    CheckState.UNKNOWN: "bold magenta",
}


def display_results_table(results: Iterable[ExecutionResult]) -> None:
    """Displays a list of ExecutionResults in a rich Table."""

    console = Console()
    table = Table(title="Check Execution Results")
    table.add_column("State", justify="center", no_wrap=True)
    table.add_column("Environment", style="cyan")
    table.add_column("Service Name", style="bold")
    table.add_column("Summary", style="default", overflow="fold")

    with Live(table, console=console, vertical_overflow="visible"):
        for result in results:
            state_style = STATE_STYLES.get(result.check_state, "default")
            table.add_row(
                f"[{state_style}]{result.check_state.name}[/]",
                result.environment_name,
                result.service_name,
                result.summary,
            )


@click.group()  # type: ignore[misc]
@click.option(
    "--app",
    help="The Outpost application to load, in 'module:variable' format.",
)  # type: ignore[misc]
@click.pass_context  # type: ignore[misc]
def cli(
    ctx: click.Context,
    app: str | None = None,
) -> None:
    ctx.obj = find_app(app)


@cli.command()  # type: ignore[misc]
@click.pass_obj  # type: ignore[misc]
def list_checks(app: Outpost) -> None:
    for check in sorted(app.checks, key=lambda check: check.name):
        click.echo(f"{check.name}{check.signature}")


@cli.command()  # type: ignore[misc]
@click.option(
    "--asynchronous-check-execution/--synchronous-check-execution",
    is_flag=True,
    default=False,
    help=(
        "Whether to run checks asynchronously or synchronously (default). If "
        "you run them asynchronously, almost all checks will return in the "
        "`UNKNOWN` state informing you they are running asynchronously."
    ),
)  # type: ignore[misc]
@click.option(
    "--filter-prefix",
    default=None,
    help="Filter which checks to run by prefix against their name (as shown by list-checks)",
)  # type: ignore[misc]
@click.option(
    "--filter-contains",
    default=None,
    help="Filter which checks to run by substring against their name (as shown by list-checks)",
)  # type: ignore[misc]
@click.pass_obj  # type: ignore[misc]
def run_checks(
    app: Outpost,
    asynchronous_check_execution: bool = False,
    filter_prefix: str | None = None,
    filter_contains: str | None = None,
) -> None:
    custom_executor: CheckExecutor[list[ExecutionResult]] | None = None
    if not asynchronous_check_execution:
        custom_executor = BlockingCheckExecutor()

    def _run() -> Iterable[ExecutionResult]:
        for check in app.checks:
            if filter_prefix and not check.name.startswith(filter_prefix):
                continue
            if filter_contains and filter_contains not in check.name:
                continue

            yield from app.run_check(
                check,
                custom_executor=custom_executor,
            )

    display_results_table(_run())


def main() -> None:
    cli(auto_envvar_prefix="OUTPOST")


if __name__ == "__main__":
    main()
