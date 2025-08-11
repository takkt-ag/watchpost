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

from starlette.applications import Starlette
from starlette.types import Receive, Scope, Send

from . import http
from .check import Check
from .executor import CheckExecutor
from .globals import _cv
from .result import CheckState, ExecutionResult


class Outpost:
    def __init__(
        self,
        *,
        checks: list[Check],
        version: str = "unknown",
        max_workers: int | None = None,
    ):
        self.checks = checks
        self.version = version
        self.executor = CheckExecutor(max_workers=max_workers)
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
                environment_name="NOTIMPLEMENTEDYET",
                check_state=CheckState.OK,
                summary=f"Ran {len(self.checks)} checks",
                details=details,
            )

        execution_results = [
            run_checks(),
        ]

        for execution_result in execution_results:
            yield from execution_result.generate_checkmk_output()

    def run_checks(self) -> Generator[bytes]:
        yield from self._generate_checkmk_agent_output()

        for check in self.checks:
            execution_results = check.run()
            for execution_result in execution_results:
                yield from execution_result.generate_checkmk_output()

        yield from self._generate_synthetic_result_outputs()

    def run_checks_once(self) -> None:
        with self.app_context():
            for check in self.checks:
                execution_results = check.run()
                for execution_result in execution_results:
                    for chunk in execution_result.generate_checkmk_output():
                        sys.stdout.buffer.write(chunk)
