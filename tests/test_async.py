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

import asyncio
from unittest.mock import patch

from outpost.app import Outpost
from outpost.check import Check, check
from outpost.datasource import Datasource
from outpost.environment import Environment
from outpost.executor import BlockingCheckExecutor
from outpost.result import CheckResult, ExecutionResult, ok, warn

from .utils import decode_checkmk_output

TEST_ENVIRONMENT = Environment("test-env-async")


class DummyDatasource(Datasource):
    pass


def test_run_sync_and_run_async_parity():
    """Ensure that run_sync and run_async produce equivalent ExecutionResults.

    This protects against future divergence between the two implementations.
    """

    # Sync check function
    def sync_func(_: DummyDatasource) -> list[CheckResult]:
        print("hello from sync")  # noqa: T201
        # Two results to also exercise name_suffix handling and aggregation
        return [
            ok("all good", name_suffix="one"),
            warn("be careful", name_suffix="two"),
        ]

    # Async check function mirroring the sync behavior
    async def async_func(_: DummyDatasource) -> list[CheckResult]:
        # a tiny await to make it a real coroutine
        await asyncio.sleep(0)
        print("hello from sync")  # noqa: T201
        return [
            ok("all good", name_suffix="one"),
            warn("be careful", name_suffix="two"),
        ]

    sync_check = Check(
        check_function=sync_func,
        service_name="parity-check",
        service_labels={"k": "v"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    async_check = Check(
        check_function=async_func,
        service_name="parity-check",
        service_labels={"k": "v"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )

    app = Outpost(
        checks=[sync_check, async_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(DummyDatasource)

    with app.app_context():
        # Run sync
        sync_results = sync_check.run_sync(
            outpost=app,
            environment=TEST_ENVIRONMENT,
            datasources={"_": DummyDatasource()},
        )

        # Run async
        async_results = asyncio.run(
            async_check.run_async(
                outpost=app,
                environment=TEST_ENVIRONMENT,
                datasources={"_": DummyDatasource()},
            )
        )

    # Helper to strip fields that may legitimately differ (e.g., check_definition line numbers)
    def normalize(results: list[ExecutionResult]):
        norm: list[dict] = []
        for r in results:
            norm.append(
                {
                    "piggyback_host": r.piggyback_host,
                    "service_name": r.service_name,
                    "service_labels": r.service_labels,
                    "environment_name": r.environment_name,
                    "check_state": r.check_state.name,
                    "summary": r.summary,
                    "details": r.details,
                    "metrics": [m.to_json_compatible_dict() for m in (r.metrics or [])],
                    # Exclude check_definition; line numbers/paths differ per function
                }
            )
        return norm

    assert normalize(sync_results) == normalize(async_results)


def test_app_runs_async_check_and_emits_output():
    """Integration: Outpost should run an async check and produce Checkmk output."""

    @check(
        name="async-check",
        service_labels={"type": "async"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    async def my_async_check(_: DummyDatasource):
        print("from async check")  # noqa: T201
        await asyncio.sleep(0)
        return ok("works")

    app = Outpost(
        checks=[my_async_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(DummyDatasource)

    # Capture stdout writes of the Checkmk output
    with patch("sys.stdout.buffer.write") as mock_write:
        app.run_checks_once()
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)
        results = decode_checkmk_output(all_data)

    # Should include both our check and the synthetic "Run checks"
    assert any(r["service_name"] == "async-check" for r in results)
    our = next(r for r in results if r["service_name"] == "async-check")
    assert our["check_state"] == "OK"
    assert our["summary"] == "works"
