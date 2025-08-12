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

from __future__ import annotations

import sys
from collections.abc import Generator
from contextlib import contextmanager
from typing import Annotated, Any, TypeVar, get_args, get_origin

from starlette.applications import Starlette
from starlette.types import Receive, Scope, Send

from . import http
from .check import Check
from .datasource import Datasource, DatasourceFactory, FromFactory
from .environment import Environment
from .executor import CheckExecutor
from .globals import _cv
from .result import CheckState, ExecutionResult

_D = TypeVar("_D", bound=Datasource)
_DF = TypeVar("_DF", bound=DatasourceFactory)


class Outpost:
    def __init__(
        self,
        *,
        checks: list[Check],
        outpost_environment: Environment,
        version: str = "unknown",
        max_workers: int | None = None,
    ):
        self.checks = checks
        self.outpost_environment = outpost_environment
        self.version = version
        self.executor = CheckExecutor(max_workers=max_workers)

        self._datasource_definitions: dict[type[Datasource], dict[str, Any]] = {}
        self._datasource_factories: dict[type, DatasourceFactory] = {}
        self._instantiated_datasources: dict[
            type[Datasource] | tuple[type[DatasourceFactory], int, int], Datasource
        ] = {}

        self._starlette = Starlette(
            routes=http.routes,
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        with self.app_context():
            return await self._starlette(scope, receive, send)

    @contextmanager
    def app_context(self) -> Generator[Outpost]:
        _token = _cv.set(self)
        try:
            yield self
        finally:
            _cv.reset(_token)

    def register_datasource(
        self,
        datasource_type: type[_D],
        **kwargs: dict[str, Any],
    ) -> None:
        self._datasource_definitions[datasource_type] = kwargs

    def register_datasource_factory(self, factory_type: type[_DF]) -> None:
        self._datasource_factories[factory_type] = factory_type()

    def _generate_checkmk_agent_output(self) -> Generator[bytes]:
        yield b"<<<check_mk>>>\n"
        yield b"Version: outpost-"
        yield self.version.encode("utf-8")
        yield b"\n"
        yield b"AgentOS: outpost\n"

    def _generate_synthetic_result_outputs(self) -> Generator[bytes]:
        def run_checks() -> ExecutionResult:
            details = "Check functions:\n- "
            details += "\n- ".join(check.name for check in self.checks)

            return ExecutionResult(
                piggyback_host="",
                service_name="Run checks",
                service_labels={},
                environment_name=self.outpost_environment.name,
                check_state=CheckState.OK,
                summary=f"Ran {len(self.checks)} checks",
                details=details,
            )

        execution_results = [
            run_checks(),
        ]

        for execution_result in execution_results:
            yield from execution_result.generate_checkmk_output()

    def _resolve_datasource(self, datasource_type: type[_D]) -> Datasource:
        if instantiated_datasource := self._instantiated_datasources.get(
            datasource_type
        ):
            return instantiated_datasource

        datasource_kwargs = self._datasource_definitions.get(datasource_type)
        if datasource_kwargs is None:
            raise ValueError(f"No datasource definition for {datasource_type}")

        datasource = datasource_type(**datasource_kwargs)
        self._instantiated_datasources[datasource_type] = datasource
        return datasource

    def _resolve_datasource_from_factory(self, from_factory: FromFactory) -> Datasource:
        if instantiated_datasource := self._instantiated_datasources.get(
            from_factory.cache_key
        ):
            return instantiated_datasource

        if datasource_factory := self._datasource_factories.get(
            from_factory.factory_type
        ):
            datasource = datasource_factory.new(
                *from_factory.args,
                **from_factory.kwargs,
            )
            self._instantiated_datasources[from_factory.cache_key] = datasource
            return datasource

        raise ValueError(
            f"No datasource factory for {from_factory.factory_type}. "
            f"Make sure you have registered the factory using register_datasource_factory({from_factory.factory_type.__name__}) "
            f"before running checks."
        )

    def _resolve_datasources(self, check: Check) -> dict[str, Datasource]:
        datasources = {}
        for parameter in check.signature.parameters.values():
            if isinstance(parameter.annotation, FromFactory):
                datasources[parameter.name] = self._resolve_datasource_from_factory(
                    parameter.annotation
                )
                continue

            if get_origin(parameter.annotation) is Annotated:
                type_key, *args = get_args(parameter.annotation)
                annotation_class = args[0]

                if isinstance(annotation_class, FromFactory):
                    datasources[parameter.name] = self._resolve_datasource_from_factory(
                        annotation_class
                    )
                    continue

                raise ValueError(
                    f"Unsupported annotation {parameter.annotation}. "
                    f"When using Annotated, the second argument must be an instance of FromFactory. "
                    f"Example: Annotated[YourDatasourceType, FromFactory(YourFactoryType, 'arg1', arg2=value)]"
                )

            if issubclass(parameter.annotation, Datasource):
                datasources[parameter.name] = self._resolve_datasource(
                    parameter.annotation
                )
                continue

        return datasources

    def run_checks(self) -> Generator[bytes]:
        yield from self._generate_checkmk_agent_output()

        for check in self.checks:
            datasources = self._resolve_datasources(check)
            execution_results = check.run(datasources=datasources)
            for execution_result in execution_results:
                yield from execution_result.generate_checkmk_output()

        yield from self._generate_synthetic_result_outputs()

    def run_checks_once(self) -> None:
        with self.app_context():
            for check in self.checks:
                datasources = self._resolve_datasources(check)
                execution_results = check.run(datasources=datasources)
                for execution_result in execution_results:
                    for chunk in execution_result.generate_checkmk_output():
                        sys.stdout.buffer.write(chunk)
