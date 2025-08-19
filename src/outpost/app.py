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

import logging
import sys
import traceback
from collections.abc import Generator
from contextlib import contextmanager
from types import ModuleType
from typing import Annotated, Any, TypeVar, assert_never, get_args, get_origin

from starlette.applications import Starlette
from starlette.types import Receive, Scope, Send

from . import http
from .cache import InMemoryStorage, Storage
from .check import Check, CheckCache
from .datasource import (
    Datasource,
    DatasourceFactory,
    DatasourceUnavailable,
    FromFactory,
)
from .discover_checks import discover_checks
from .environment import Environment
from .executor import CheckExecutor
from .globals import _cv
from .hostname import HostnameInput, resolve_hostname, to_strategy
from .result import CheckState, ExecutionResult
from .scheduling_strategy import (
    DetectImpossibleCombinationStrategy,
    InvalidCheckConfiguration,
    SchedulingDecision,
    SchedulingStrategy,
)

logger = logging.getLogger(f"{__package__}.{__name__}")

_D = TypeVar("_D", bound=Datasource)
_DF = TypeVar("_DF", bound=DatasourceFactory)


class Outpost:
    def __init__(
        self,
        *,
        checks: list[Check | ModuleType],
        execution_environment: Environment,
        version: str = "unknown",
        max_workers: int | None = None,
        executor: CheckExecutor[list[ExecutionResult]] | None = None,
        check_cache_storage: Storage | None = None,
        default_scheduling_strategies: list[SchedulingStrategy] | None = None,
        hostname: HostnameInput | None = None,
        hostname_strict: bool = False,
    ):
        self.checks: list[Check] = []
        for check_or_module in checks:
            if isinstance(check_or_module, ModuleType):
                self.checks.extend(
                    discover_checks(
                        module=check_or_module,
                        recursive=True,
                        raise_on_import_error=True,
                    )
                )
            else:
                self.checks.append(check_or_module)

        self.execution_environment = execution_environment
        self.version = version
        self.hostname_strategy = to_strategy(hostname)
        self._hostname_strict = hostname_strict
        if executor:
            self.executor = executor
        else:
            self.executor = CheckExecutor(max_workers=max_workers)

        if default_scheduling_strategies:
            self.default_scheduling_strategies = default_scheduling_strategies
        else:
            self.default_scheduling_strategies = [
                DetectImpossibleCombinationStrategy(),
            ]

        self._check_cache = CheckCache(
            storage=check_cache_storage or InMemoryStorage(),
        )

        self._datasource_definitions: dict[type[Datasource], dict[str, Any]] = {}
        self._datasource_factories: dict[type, DatasourceFactory] = {}
        self._instantiated_datasources: dict[
            type[Datasource] | tuple[type[DatasourceFactory], int, int], Datasource
        ] = {}

        self._resolved_datasources: dict[Check, dict[str, Datasource]] = {}
        self._resolved_strategies: dict[Check, list[SchedulingStrategy]] = {}

        self._starlette = Starlette(
            routes=http.routes,
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        with self.app_context():
            return await self._starlette(scope, receive, send)

    @contextmanager
    def app_context(self) -> Generator[Outpost]:
        try:
            _cv.get()
            yield self
        except LookupError:
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
        if datasource_type.scheduling_strategies is Ellipsis:
            logger.warning(
                "The provided datasource '%s' has no scheduling strategies defined. Please make sure to either define them or explicitly set scheduling_strategies=().",
                datasource_type.__name__,
            )
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
                environment_name=self.execution_environment.name,
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

            if getattr(datasource, "scheduling_strategies", ...) is Ellipsis:
                if from_factory.factory_type.scheduling_strategies:
                    datasource.scheduling_strategies = (
                        from_factory.factory_type.scheduling_strategies
                    )
                elif from_factory.factory_type.scheduling_strategies is Ellipsis:
                    logger.warning(
                        "The factory-created datasource has no scheduling strategies defined. Please make sure that either your factory or the datasource created by your factory has them defined or explicitly set to scheduling_strategies=(). Datasource=%s, Factory=%s",
                        datasource,
                        from_factory.factory_type,
                    )

            self._instantiated_datasources[from_factory.cache_key] = datasource
            return datasource

        raise ValueError(
            f"No datasource factory for {from_factory.factory_type}. "
            f"Make sure you have registered the factory using register_datasource_factory({from_factory.factory_type.__name__}) "
            f"before running checks."
        )

    def _resolve_datasources(self, check: Check) -> dict[str, Datasource]:
        if resolved_datasources := self._resolved_datasources.get(check):
            return resolved_datasources

        datasources = {}
        for name, parameter in check.type_hints.items():
            if get_origin(parameter) is Annotated:
                type_key, *args = get_args(parameter)
                annotation_class = args[0]

                if isinstance(annotation_class, FromFactory):
                    datasources[name] = self._resolve_datasource_from_factory(
                        annotation_class
                    )
                    continue

                raise ValueError(
                    f"Unsupported annotation {parameter}. "
                    f"When using Annotated, the second argument must be an instance of FromFactory. "
                    f"Example: Annotated[YourDatasourceType, FromFactory(YourFactoryType, 'arg1', arg2=value)]"
                )

            if issubclass(parameter, Datasource):
                datasources[name] = self._resolve_datasource(parameter)
                continue

        self._resolved_datasources[check] = datasources
        return datasources

    def _resolve_scheduling_strategies(self, check: Check) -> list[SchedulingStrategy]:
        if resolved_strategies := self._resolved_strategies.get(check):
            return resolved_strategies

        strategies = []

        if check.scheduling_strategies:
            strategies.extend(check.scheduling_strategies)

        for datasource in self._resolve_datasources(check).values():
            if (
                datasource.scheduling_strategies
                and datasource.scheduling_strategies is not Ellipsis
            ):
                strategies.extend(datasource.scheduling_strategies)

        strategies.extend(self.default_scheduling_strategies)

        self._resolved_strategies[check] = strategies
        return strategies

    def _resolve_check_scheduling_decision(
        self,
        check: Check,
        environment: Environment,
    ) -> SchedulingDecision:
        strategies = self._resolve_scheduling_strategies(check)

        final_decision = SchedulingDecision.SCHEDULE
        for strategy in strategies:
            decision = strategy.schedule(
                check=check,
                current_execution_environment=self.execution_environment,
                target_environment=environment,
            )
            if decision > final_decision:
                final_decision = decision

        return final_decision

    def _verify_check_scheduling(self) -> None:
        exceptions = []
        with self.app_context():
            for check in self.checks:
                for target_environment in check.environments:
                    # We ignore the return value, we only care if .schedule
                    # throws an InvalidCheckConfiguration exception.
                    try:
                        self._resolve_check_scheduling_decision(
                            check, target_environment
                        )
                    except InvalidCheckConfiguration as e:
                        exceptions.append(e)

        if exceptions:
            raise ExceptionGroup(
                "One or more checks are not well-configured", exceptions
            )

    def _run_check(
        self,
        check: Check,
        environment: Environment,
        datasources: dict[str, Datasource],
        *,
        custom_executor: CheckExecutor[list[ExecutionResult]] | None = None,
    ) -> list[ExecutionResult] | None:
        executor = custom_executor or self.executor

        piggyback_host = resolve_hostname(
            outpost=self,
            environment=environment,
            check=check,
            result=None,
            strict=self._hostname_strict,
        )

        scheduling_decision = self._resolve_check_scheduling_decision(
            check,
            environment,
        )
        check_results_cache_entry = self._check_cache.get_check_results_cache_entry(
            check=check,
            environment=environment,
            return_expired=True,
        )

        match scheduling_decision:
            case SchedulingDecision.SCHEDULE:
                # Fall through to the logic below.
                pass
            case SchedulingDecision.SKIP:
                if not check_results_cache_entry:
                    return [
                        ExecutionResult(
                            piggyback_host=piggyback_host,
                            service_name=check.service_name,
                            service_labels=check.service_labels,
                            environment_name=environment.name,
                            check_state=CheckState.UNKNOWN,
                            summary="Check is temporarily unschedulable and no prior results are available",
                            check_definition=check.invocation_information,
                        )
                    ]
                return check_results_cache_entry.value
            case SchedulingDecision.DONT_SCHEDULE:
                return None
            case _:
                assert_never(scheduling_decision)  # type: ignore[type-assertion-failure]

        executor_key = (check.name, environment.name)
        should_update_cache = (
            check.cache_for is None
            or check_results_cache_entry is None
            or check_results_cache_entry.is_expired()
        )
        can_reuse_results = (
            check_results_cache_entry is not None
            and not check_results_cache_entry.is_expired()
        )

        if should_update_cache or not can_reuse_results:
            executor.submit(
                key=executor_key,
                func=check.run_async if check.is_async else check.run_sync,
                resubmit=check.cache_for is None,
                outpost=self,
                environment=environment,
                datasources=datasources,
            )

        if can_reuse_results:
            return check_results_cache_entry.value  # type: ignore[union-attr]

        try:
            maybe_execution_results = executor.result(key=executor_key)

            # If the check is still running asynchronously but we did have a set
            # of results cached, we do want to fall back to this cache while it
            # is still available. This ensures that checks that are marked
            # `cache_for=None` that do have a cached result in a persistent
            # cache (if used) are not ignored. It also makes sure that any check
            # that has a `cache_for` specified does not return "check is running
            # asynchronously" in the short time period where the cache has
            # expired and the check was just submitted.
            if not maybe_execution_results and check_results_cache_entry:
                return check_results_cache_entry.value
        except DatasourceUnavailable as e:
            additional_details = f"\n\n{e!s}\n" + "".join(traceback.format_exception(e))
            if check_results_cache_entry and check_results_cache_entry.value:
                for result in check_results_cache_entry.value:
                    if result.details:
                        result.details += additional_details
                    else:
                        result.details = additional_details
                return check_results_cache_entry.value

            return [
                ExecutionResult(
                    piggyback_host=piggyback_host,
                    service_name=check.service_name,
                    service_labels=check.service_labels,
                    environment_name=environment.name,
                    check_state=CheckState.UNKNOWN,
                    summary=str(e),
                    details=additional_details,
                    check_definition=check.invocation_information,
                )
            ]
        except Exception as e:
            return [
                ExecutionResult(
                    piggyback_host=piggyback_host,
                    service_name=check.service_name,
                    service_labels=check.service_labels,
                    environment_name=environment.name,
                    check_state=CheckState.CRIT,
                    summary=str(e),
                    details="".join(traceback.format_exception(e)),
                    check_definition=check.invocation_information,
                )
            ]

        if not maybe_execution_results:
            return [
                ExecutionResult(
                    piggyback_host=piggyback_host,
                    service_name=check.service_name,
                    service_labels=check.service_labels,
                    environment_name=environment.name,
                    check_state=CheckState.UNKNOWN,
                    summary="Check is running asynchronously and first results are not available yet",
                    check_definition=check.invocation_information,
                )
            ]

        self._check_cache.store_check_results(
            check=check,
            environment=environment,
            results=maybe_execution_results,
        )
        return maybe_execution_results

    def run_check(
        self,
        check: Check,
        *,
        custom_executor: CheckExecutor[list[ExecutionResult]] | None = None,
    ) -> Generator[ExecutionResult]:
        with self.app_context():
            datasources = self._resolve_datasources(check)
            for environment in check.environments:
                execution_results = self._run_check(
                    check=check,
                    environment=environment,
                    datasources=datasources,
                    custom_executor=custom_executor,
                )

                if not execution_results:
                    continue
                yield from execution_results

    def run_checks(self) -> Generator[bytes]:
        with self.app_context():
            yield from self._generate_checkmk_agent_output()

            for check in self.checks:
                for execution_result in self.run_check(check):
                    yield from execution_result.generate_checkmk_output()

            yield from self._generate_synthetic_result_outputs()

    def run_checks_once(self) -> None:
        with self.app_context():
            for chunk in self.run_checks():
                sys.stdout.buffer.write(chunk)
