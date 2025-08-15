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

import base64
import io
import json
import traceback
from collections.abc import Callable, Generator
from dataclasses import dataclass
from enum import Enum
from types import GeneratorType
from typing import TYPE_CHECKING, Any, cast

from .utils import InvocationInformation

if TYPE_CHECKING:
    from .check import CheckFunctionResult

Details = str | dict | Exception


def normalize_details(details: Details | None) -> str | None:
    if details is None:
        return None

    if isinstance(details, Exception):
        return "".join(traceback.format_exception(details))

    if isinstance(details, dict):
        return "\n".join(f"{key}: {value}" for key, value in details.items())

    if isinstance(details, str) and details.strip() == "":
        return None

    return details


@dataclass
class Thresholds:
    warning: int | float
    critical: int | float

    def to_json_compatible_dict(self) -> dict[str, int | float]:
        return {
            "warning": self.warning,
            "critical": self.critical,
        }


@dataclass
class Metric:
    name: str
    value: int | float
    levels: Thresholds | None = None
    boundaries: Thresholds | None = None

    def to_json_compatible_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "name": self.sanitized_name,
            "value": self.value,
        }
        if self.levels is not None:
            result["levels"] = self.levels.to_json_compatible_dict()
        if self.boundaries is not None:
            result["boundaries"] = self.boundaries.to_json_compatible_dict()
        return result

    @property
    def sanitized_name(self) -> str:
        return (
            self.name.replace(" ", "_")
            .replace(":", "_")
            .replace("/", "_")
            .replace("\\", "_")
        )


class CheckState(Enum):
    OK = 0
    WARN = 1
    CRIT = 2
    UNKNOWN = 3

    def __lt__(self, other: CheckState) -> bool:
        return self.value < other.value

    @property
    def check_function(self) -> Callable[..., CheckResult]:
        match self:
            case CheckState.OK:
                return ok
            case CheckState.WARN:
                return warn
            case CheckState.CRIT:
                return crit
            case CheckState.UNKNOWN:
                return unknown
        raise ValueError("Unknown check state")


@dataclass(init=False)
class CheckResult:
    check_state: CheckState
    summary: str
    details: str | None = None
    name_suffix: str | None = None
    metrics: list[Metric] | None = None
    hostname: str | None = None

    def __init__(
        self,
        check_state: CheckState,
        summary: str,
        details: Details | None = None,
        name_suffix: str | None = None,
        metrics: list[Metric] | None = None,
        hostname: str | None = None,
    ):
        self.check_state = check_state
        self.summary = summary
        self.details = normalize_details(details)
        self.name_suffix = name_suffix
        self.metrics = metrics
        self.hostname = hostname


def ok(
    summary: str,
    details: Details | None = None,
    name_suffix: str | None = None,
    metrics: list[Metric] | None = None,
    alternative_hostname: str | None = None,
) -> CheckResult:
    return CheckResult(
        check_state=CheckState.OK,
        summary=summary,
        details=details,
        name_suffix=name_suffix,
        metrics=metrics,
        hostname=alternative_hostname,
    )


def warn(
    summary: str,
    details: Details | None = None,
    name_suffix: str | None = None,
    metrics: list[Metric] | None = None,
    alternative_hostname: str | None = None,
) -> CheckResult:
    return CheckResult(
        check_state=CheckState.WARN,
        summary=summary,
        details=details,
        name_suffix=name_suffix,
        metrics=metrics,
        hostname=alternative_hostname,
    )


def crit(
    summary: str,
    details: Details | None = None,
    name_suffix: str | None = None,
    metrics: list[Metric] | None = None,
    alternative_hostname: str | None = None,
) -> CheckResult:
    return CheckResult(
        check_state=CheckState.CRIT,
        summary=summary,
        details=details,
        name_suffix=name_suffix,
        metrics=metrics,
        hostname=alternative_hostname,
    )


def unknown(
    summary: str,
    details: Details | None = None,
    name_suffix: str | None = None,
    metrics: list[Metric] | None = None,
    alternative_hostname: str | None = None,
) -> CheckResult:
    return CheckResult(
        check_state=CheckState.UNKNOWN,
        summary=summary,
        details=details,
        name_suffix=name_suffix,
        metrics=metrics,
        hostname=alternative_hostname,
    )


class OngoingCheckResult:
    @dataclass
    class Partial:
        check_state: CheckState
        summary: str
        details: Details | None

        def __str__(self) -> str:
            if self.details:
                return f"{self.summary}:\n{normalize_details(self.details)}\n"
            return f"{self.summary}\n"

    def __init__(
        self,
        ok_summary: str,
        fail_summary: str,
        base_details: Details | None = None,
        name_suffix: str | None = None,
        metrics: list[Metric] | None = None,
        alternative_hostname: str | None = None,
    ):
        self.ok_summary = ok_summary
        self.fail_summary = fail_summary
        self.base_details = base_details
        self.name_suffix = name_suffix
        self.metrics = metrics
        self.alternative_hostname = alternative_hostname
        self.results: list[OngoingCheckResult.Partial] = []

    @property
    def check_state(self) -> CheckState:
        if not self.results:
            return CheckState.OK

        # CRIT is a special case in that it is the worst state, even though numerically
        # UNKNOWN is higher.
        if any(result.check_state == CheckState.CRIT for result in self.results):
            return CheckState.CRIT

        return max(result.check_state for result in self.results)

    def add_check_result(self, check_result: CheckResult) -> None:
        self.results.append(
            OngoingCheckResult.Partial(
                check_state=check_result.check_state,
                summary=check_result.summary,
                details=check_result.details,
            )
        )

    def ok(self, summary: str, details: Details | None = None) -> None:
        self.results.append(OngoingCheckResult.Partial(CheckState.OK, summary, details))

    def warn(self, summary: str, details: Details | None = None) -> None:
        self.results.append(
            OngoingCheckResult.Partial(CheckState.WARN, summary, details)
        )

    def crit(self, summary: str, details: Details | None = None) -> None:
        self.results.append(
            OngoingCheckResult.Partial(CheckState.CRIT, summary, details)
        )

    def unknown(self, summary: str, details: Details | None = None) -> None:
        self.results.append(
            OngoingCheckResult.Partial(CheckState.UNKNOWN, summary, details)
        )

    def to_check_result(self) -> CheckResult:
        maximum_state = self.check_state

        details = None
        # If any details were provided, we create a compound string of them.
        # Otherwise, we'll keep the details None.
        if self.base_details is not None or len(self.results) > 0:
            details = normalize_details(self.base_details) if self.base_details else ""
            if self.results:
                result_details = "\n".join(str(result) for result in self.results)
                details = f"{details}\n\n{result_details}".strip()

        return maximum_state.check_function(
            summary=(
                self.ok_summary if maximum_state == CheckState.OK else self.fail_summary
            ),
            details=details,
            name_suffix=self.name_suffix,
            metrics=self.metrics,
            alternative_hostname=self.alternative_hostname,
        )


def build_result(
    ok_summary: str,
    fail_summary: str,
    base_details: Details | None = None,
    name_suffix: str | None = None,
    metrics: list[Metric] | None = None,
    alternative_hostname: str | None = None,
) -> OngoingCheckResult:
    return OngoingCheckResult(
        ok_summary=ok_summary,
        fail_summary=fail_summary,
        base_details=base_details,
        name_suffix=name_suffix,
        metrics=metrics,
        alternative_hostname=alternative_hostname,
    )


def normalize_check_function_result(
    check_function_result: CheckFunctionResult,
    stdout: io.StringIO,
    stderr: io.StringIO,
) -> list[CheckResult]:
    maybe_ongoing_check_results: list[CheckResult | OngoingCheckResult]
    if isinstance(check_function_result, GeneratorType):
        maybe_ongoing_check_results = list(check_function_result)
    elif isinstance(check_function_result, list):
        maybe_ongoing_check_results = check_function_result
    else:
        maybe_ongoing_check_results = [
            cast(
                CheckResult | OngoingCheckResult,
                check_function_result,
            )
        ]

    check_results: list[CheckResult] = [
        (result.to_check_result() if isinstance(result, OngoingCheckResult) else result)
        for result in maybe_ongoing_check_results
        if result is not None
    ]

    stdfd_details = ""
    if stdout.tell() > 0:
        stdfd_details += f"\n\n<STDOUT>\n{stdout.getvalue()}\n</STDOUT>"
    if stderr.tell() > 0:
        stdfd_details += f"\n\n<STDERR>\n{stderr.getvalue()}\n</STDERR>"
    stdfd_details = stdfd_details.strip()

    if stdfd_details:
        for check_result in check_results:
            if check_result.details is None:
                check_result.details = ""
            check_result.details += stdfd_details

    if len(check_results) == 0:
        return [
            CheckResult(
                check_state=CheckState.UNKNOWN,
                summary="CHECK CODE ERROR: no results returned",
                details=(
                    "The outpost check function returned no results. Please verify "
                    "the code of this check if there is a circumstance in which it "
                    "does not return any results, and remediate this."
                ),
            )
        ]

    return check_results


@dataclass
class ExecutionResult:
    piggyback_host: str
    service_name: str
    service_labels: dict[str, str]
    environment_name: str
    check_state: CheckState
    summary: str
    details: str | None = None
    metrics: list[Metric] | None = None
    check_definition: InvocationInformation | None = None

    def generate_checkmk_output(self) -> Generator[bytes]:
        yield b"<<<<"
        yield self.piggyback_host.encode("utf-8")
        yield b">>>>\n"
        yield b"<<<outpost>>>\n"

        result: dict[str, Any] = {
            "service_name": self.service_name,
            "service_labels": self.service_labels,
            "environment": self.environment_name,
            "check_state": self.check_state.name,
            "summary": self.summary,
            "metrics": [
                metric.to_json_compatible_dict()
                for metric in (self.metrics if self.metrics else [])
            ],
        }

        details = self.details
        if self.check_definition:
            result["check_definition"] = {
                "relative_path": self.check_definition.relative_path,
                "line_number": self.check_definition.line_number,
            }
        result["details"] = details

        yield base64.b64encode(json.dumps(result).encode("utf-8"))
        yield b"\n"
        yield b"<<<<>>>>\n"
