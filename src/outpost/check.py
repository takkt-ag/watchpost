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
import inspect
import io
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Any, TypeVar

from .datasource import Datasource
from .environment import Environment
from .result import (
    CheckResult,
    ExecutionResult,
    OngoingCheckResult,
    normalize_check_function_result,
)
from .utils import InvocationInformation, get_invocation_information

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


@dataclass
class Check:
    check_function: CheckFunction
    service_name: str
    service_labels: dict[str, Any]
    environments: list[Environment]
    datasources: list[type[Datasource]]
    invocation_information: InvocationInformation | None = None

    def __post_init__(self):
        self._check_function_signature = inspect.signature(self.check_function)

    def __call__(self, *args, **kwargs):
        return self.check_function(*args, **kwargs)

    def run(self) -> list[ExecutionResult]:
        kwargs: dict[str, Environment | Datasource] = {
            datasource.argument_name: datasource.instance
            for datasource in self.datasources
        }

        collected_results = []
        for environment in self.environments:
            if "environment" in self._check_function_signature.parameters:
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
    datasources: list[type[Datasource]],
) -> Callable[[CheckFunction], Check]:
    check_definition = get_invocation_information()

    def decorator(func: CheckFunction) -> Check:
        return Check(
            check_function=func,
            service_name=name,
            service_labels=service_labels,
            environments=environments,
            datasources=datasources,
            invocation_information=check_definition,
        )

    return decorator
