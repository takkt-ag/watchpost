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

from concurrent.futures import wait

from outpost.app import Outpost
from outpost.check import check
from outpost.environment import Environment
from outpost.executor import CheckExecutor
from outpost.result import ok

from .utils import decode_checkmk_output, with_event


def _collect_output(app: Outpost) -> bytes:
    return b"".join(app.run_checks())


def test_run_checks_returns_placeholder_until_result_is_ready():
    # Arrange: environment and a check function that waits on an Event
    env = Environment("env-nonblocking")
    outpost_env = Environment("outpost-env")
    with (
        CheckExecutor(max_workers=1) as executor,
        with_event() as event,
    ):

        @check(
            name="nonblocking-service",
            service_labels={"test": "true"},
            environments=[env],
            cache_for=None,  # ensure resubmit behavior each run
        )
        def my_check() -> object:
            event.wait()
            return ok("All good")

        app = Outpost(
            checks=[my_check],
            execution_environment=outpost_env,
            executor=executor,
            version="test",
        )

        # Act 1: First run while the event is not set
        output1 = _collect_output(app)
        results1 = decode_checkmk_output(output1)

        # Assert 1: We should see the placeholder UNKNOWN for our service
        service_results1 = [
            r for r in results1 if r["service_name"] == "nonblocking-service"
        ]
        assert len(service_results1) == 1
        assert service_results1[0]["environment"] == env.name
        assert service_results1[0]["check_state"] == "UNKNOWN"
        assert (
            service_results1[0]["summary"]
            == "Check is running asynchronously and first results are not available yet"
        )

        # The synthetic 'Run checks' result should also be present
        assert any(r["service_name"] == "Run checks" for r in results1)

        # Act 2: Second run without setting the event yet should still yield UNKNOWN
        output2 = _collect_output(app)
        results2 = decode_checkmk_output(output2)
        service_results2 = [
            r for r in results2 if r["service_name"] == "nonblocking-service"
        ]
        assert len(service_results2) == 1
        assert service_results2[0]["check_state"] == "UNKNOWN"
        assert any(r["service_name"] == "Run checks" for r in results2)


def test_run_checks_returns_final_result_after_event_is_set():
    # Arrange: environment and a check function that waits on an Event
    env = Environment("env-nonblocking")
    outpost_env = Environment("outpost-env")

    with (
        CheckExecutor(max_workers=1) as executor,
        with_event() as event,
    ):

        @check(
            name="nonblocking-service",
            service_labels={"test": "true"},
            environments=[env],
            cache_for=None,  # ensure resubmit behavior each run
        )
        def my_check() -> object:
            event.wait()
            return ok("All good")

        app = Outpost(
            checks=[my_check],
            execution_environment=outpost_env,
            executor=executor,
            version="test",
        )

        # Act 1: First run while the event is not set -> expect UNKNOWN placeholder
        output1 = _collect_output(app)
        results1 = decode_checkmk_output(output1)
        service_results1 = [
            r for r in results1 if r["service_name"] == "nonblocking-service"
        ]
        assert len(service_results1) == 1
        assert service_results1[0]["check_state"] == "UNKNOWN"

        # Signal the check can complete and wait for the first submitted future to finish
        event.set()
        key = (my_check.name, env.name)
        key_state = executor._state.get(key)
        assert key_state, "future should be present for failing check"
        assert key_state.active_futures, "future should be present for failing check"
        wait(executor._state[key].active_futures, return_when="ALL_COMPLETED")

        # Act 2: Second run -> expect OK from finished result
        output2 = _collect_output(app)
        results2 = decode_checkmk_output(output2)

        service_results2 = [
            r for r in results2 if r["service_name"] == "nonblocking-service"
        ]
        assert len(service_results2) == 1
        assert service_results2[0]["environment"] == env.name
        assert service_results2[0]["check_state"] == "OK"
        assert service_results2[0]["summary"] == "All good"

        # The synthetic 'Run checks' result should also be present
        assert any(r["service_name"] == "Run checks" for r in results2)


def test_executor_errored_integration_nonblocking():
    env = Environment("env-nonblocking")
    outpost_env = Environment("outpost-env")

    with (
        CheckExecutor(max_workers=1) as executor,
        with_event() as event,
    ):

        @check(
            name="failing-service",
            service_labels={"test": "true"},
            environments=[env],
            cache_for="1m",
        )
        def failing_check() -> object:
            event.wait()
            raise ValueError("boom")

        app = Outpost(
            checks=[failing_check],
            execution_environment=outpost_env,
            executor=executor,
            version="test",
        )

        # First run: ensures submission and returns placeholder UNKNOWN
        output1 = b"".join(app.run_checks())
        results1 = decode_checkmk_output(output1)
        sr1 = [r for r in results1 if r["service_name"] == "failing-service"]
        assert len(sr1) == 1 and sr1[0]["check_state"] == "UNKNOWN"

        # Let the check complete with an error and wait for its future
        key = (failing_check.name, env.name)
        event.set()
        key_state = executor._state.get(key)
        assert key_state, "future should be present for failing check"
        assert key_state.active_futures, "future should be present for failing check"
        wait(executor._state[key].active_futures, return_when="ALL_COMPLETED")

        # Before pickup: errored() should report the error with a key string
        errs = executor.errored()
        assert len(errs) == 1
        err_key, err_msg = next(iter(errs.items()))
        assert failing_check.name in err_key
        assert env.name in err_key
        assert err_msg == "boom"

        # Next run should attempt to pick up the errored future and create a CRIT result
        output2 = b"".join(app.run_checks())
        results2 = decode_checkmk_output(output2)
        sr2 = [r for r in results2 if r["service_name"] == "failing-service"]
        assert len(sr2) == 1
        assert sr2[0]["check_state"] == "CRIT"
        assert sr2[0]["summary"] == "boom"
        assert "ValueError: boom" in sr2[0]["details"]

        # After pickup, errored() must be cleared
        assert executor.errored() == {}
