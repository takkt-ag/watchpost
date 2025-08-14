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

import contextlib
import hashlib
import inspect
import io
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, TypeVar

from .cache import Cache, CacheEntry, Storage
from .datasource import Datasource
from .environment import Environment
from .result import (
    CheckResult,
    ExecutionResult,
    OngoingCheckResult,
    normalize_check_function_result,
)
from .scheduling_strategy import SchedulingStrategy
from .utils import (
    InvocationInformation,
    get_invocation_information,
    normalize_to_timedelta,
)

CheckFunctionResult = (
    CheckResult
    | OngoingCheckResult
    | list[CheckResult | OngoingCheckResult]
    | Generator[CheckResult | OngoingCheckResult]
)

_E = Environment
_D = TypeVar("_D", bound=Datasource)
_R = CheckFunctionResult

CheckFunction = (
    Callable[[_D], _R]
    | Callable[[_D, _D], _R]
    | Callable[[_D, _D, _D], _R]
    | Callable[[_D, _D, _D, _D], _R]
    | Callable[[_D, _D, _D, _D, _D], _R]
    | Callable[[_D, _D, _D, _D, _D, _D], _R]
    | Callable[[_D, _D, _D, _D, _D, _D, _D], _R]
    | Callable[[_D, _D, _D, _D, _D, _D, _D, _D], _R]
    | Callable[[_D, _D, _D, _D, _D, _D, _D, _D, _D], _R]
    | Callable[[_E, _D], _R]
    | Callable[[_E, _D], _R]
    | Callable[[_E, _D, _D], _R]
    | Callable[[_E, _D, _D, _D], _R]
    | Callable[[_E, _D, _D, _D, _D], _R]
    | Callable[[_E, _D, _D, _D, _D, _D], _R]
    | Callable[[_E, _D, _D, _D, _D, _D, _D], _R]
    | Callable[[_E, _D, _D, _D, _D, _D, _D, _D], _R]
    | Callable[[_E, _D, _D, _D, _D, _D, _D, _D, _D], _R]
    | Callable[[_E, _D, _D, _D, _D, _D, _D, _D, _D, _D], _R]
)


@dataclass(frozen=True)
class Check:
    check_function: CheckFunction
    service_name: str
    service_labels: dict[str, Any]
    environments: list[Environment]
    cache_for: timedelta | None
    invocation_information: InvocationInformation | None = None
    scheduling_strategies: list[SchedulingStrategy] | None = None

    def __hash__(self) -> int:
        return hash(
            (
                self.check_function,
                self.service_name,
                tuple(self.service_labels.items()),
                self.cache_for,
                self.invocation_information,
                self.scheduling_strategies,
            )
        )

    @property
    def name(self) -> str:
        return f"{self.check_function.__module__}.{self.check_function.__qualname__}"

    @property
    def signature(self) -> inspect.Signature:
        return self._check_function_signature  # type: ignore[attr-defined]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_check_function_signature",
            inspect.signature(self.check_function),
        )

    def __call__(self, *args, **kwargs) -> CheckFunctionResult:  # type: ignore[no-untyped-def]
        return self.check_function(*args, **kwargs)

    def run(
        self,
        *,
        environment: Environment,
        datasources: dict[str, Datasource],
    ) -> list[ExecutionResult]:
        kwargs: dict[str, Environment | Datasource] = {
            **datasources,
        }

        collected_results = []
        if "environment" in self._check_function_signature.parameters:  # type: ignore[attr-defined]
            kwargs["environment"] = environment

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            initial_result = self.check_function(**kwargs)  # type: ignore[call-arg]
        normalized_results = normalize_check_function_result(
            initial_result,
            stdout,
            stderr,
        )

        for result in normalized_results:
            collected_results.append(
                ExecutionResult(
                    piggyback_host=self.generate_hostname(environment),
                    service_name=self.service_name,
                    service_labels=self.service_labels,
                    environment_name=environment.name,
                    check_state=result.check_state,
                    summary=result.summary,
                    details=result.details,
                    metrics=result.metrics,
                    check_definition=self.invocation_information,
                )
            )

        return collected_results

    def generate_hostname(self, environment: Environment) -> str:
        return f"{self.service_name}-{environment.name}-NOTIMPLEMENTEDYET"


def check(
    *,
    name: str,
    service_labels: dict[str, Any],
    environments: list[Environment],
    cache_for: timedelta | str | None,
) -> Callable[[CheckFunction], Check]:
    check_definition = get_invocation_information()

    def decorator(func: CheckFunction) -> Check:
        return Check(
            check_function=func,
            service_name=name,
            service_labels=service_labels,
            environments=environments,
            cache_for=normalize_to_timedelta(cache_for),
            invocation_information=check_definition,
        )

    return decorator


class CheckCache:
    def __init__(self, storage: Storage):
        self._cache = Cache(storage)

    @staticmethod
    def _hash_datasources(datasources: dict[str, Datasource]) -> str:
        return hashlib.sha256(
            str({k: repr(v) for k, v in sorted(datasources.items())}).encode()
        ).hexdigest()

    @staticmethod
    def _generate_check_cache_key(
        check: Check,
        environment: Environment,
    ) -> str:
        return f"{check.name}:{environment.name}"

    def get_check_results_cache_entry(
        self,
        check: Check,
        environment: Environment,
        return_expired: bool = False,
    ) -> CacheEntry[list[ExecutionResult]] | None:
        return self._cache.get(
            self._generate_check_cache_key(check, environment),
            return_expired=return_expired,
        )

    def store_check_results(
        self,
        check: Check,
        environment: Environment,
        results: list[ExecutionResult],
    ) -> None:
        """
        Stores the results of a check execution for caching purposes.
        """
        if not check.cache_for:
            return

        self._cache.store(
            self._generate_check_cache_key(check, environment),
            results,
            ttl=check.cache_for,
        )
