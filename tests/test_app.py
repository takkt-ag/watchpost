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

from datetime import timedelta
from typing import override
from unittest.mock import MagicMock, patch

import pytest

from outpost.app import Outpost
from outpost.check import Check, check
from outpost.datasource import Datasource, DatasourceUnavailable
from outpost.environment import Environment
from outpost.globals import current_app
from outpost.result import CheckState, ExecutionResult, ok

from .utils import BlockingCheckExecutor, decode_checkmk_output

TEST_ENVIRONMENT = Environment("test-env")


class TestDatasource(Datasource):
    pass


def test_outpost_initialization():
    """Test that an Outpost object can be properly initialized."""
    # Create a mock check
    mock_check = MagicMock(spec=Check)

    # Initialize the Outpost object
    app = Outpost(
        checks=[mock_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Verify the Outpost object was initialized correctly
    assert app.checks == [mock_check]


def test_app_context():
    """Test that the app_context method properly sets and resets the context variable."""
    # Create an Outpost instance
    app = Outpost(
        checks=[],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Before entering the context, current_app should raise an error
    with pytest.raises(RuntimeError, match="Outpost application is not available"):
        _ = current_app.__name__  # type: ignore[unresolved-attribute]

    # Within the context, current_app should be the app instance
    with app.app_context():
        assert current_app._get_current_object() is app  # type: ignore[unresolved-attribute]

    # After exiting the context, current_app should raise an error again
    with pytest.raises(RuntimeError, match="Outpost application is not available"):
        _ = current_app.__name__  # type: ignore[unresolved-attribute]


def test_app_context_exception_handling():
    """Test that the app_context method properly handles exceptions."""
    # Create an Outpost instance
    app = Outpost(
        checks=[],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Test that the context is properly reset even if an exception occurs
    try:
        with app.app_context():
            assert current_app._get_current_object() is app  # type: ignore[unresolved-attribute]
            raise ValueError("Test exception")
    except ValueError:
        pass

    # After the exception, current_app should raise an error
    with pytest.raises(RuntimeError, match="Outpost application is not available"):
        _ = current_app.__name__  # type: ignore[unresolved-attribute]


def test_run_checks_once():
    """Test that the run_checks_once method runs all checks and outputs the results."""
    # Create a mock check that returns a known ExecutionResult
    mock_check = MagicMock(spec=Check)
    mock_check.name = "Test Check"
    mock_check.environments = [TEST_ENVIRONMENT]
    mock_check.cache_for = None
    execution_result = ExecutionResult(
        piggyback_host="test-host",
        service_name="test-service",
        service_labels={"env": "test"},
        environment_name="test-env",
        check_state=CheckState.OK,
        summary="Test summary",
    )
    mock_check.run.return_value = [execution_result]

    # Initialize the Outpost object
    app = Outpost(
        checks=[mock_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Mock sys.stdout.buffer.write to capture the output
    with patch("sys.stdout.buffer.write") as mock_write:
        # Run the checks
        app.run_checks_once()

        # Verify that the check was run
        mock_check.run.assert_called_once()

        # Verify that sys.stdout.buffer.write was called with the expected data
        assert mock_write.call_count > 0

        # Collect all the data written to stdout
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)

        # Decode the base64 data using the utility function
        json_data = decode_checkmk_output(all_data)[0]

        # Verify the decoded data contains the expected values
        assert json_data["service_name"] == "test-service"
        assert json_data["environment"] == "test-env"
        assert json_data["check_state"] == "OK"
        assert json_data["summary"] == "Test summary"


def test_run_checks_once_with_multiple_checks():
    """Test that the run_checks_once method runs multiple checks."""
    # Create two mock checks
    mock_check1 = MagicMock(spec=Check)
    mock_check1.name = "Test Check 1"
    mock_check1.environments = [TEST_ENVIRONMENT]
    mock_check1.cache_for = None
    execution_result1 = ExecutionResult(
        piggyback_host="test-host-1",
        service_name="test-service-1",
        service_labels={"env": "test"},
        environment_name="test-env",
        check_state=CheckState.OK,
        summary="Test summary 1",
    )
    mock_check1.run.return_value = [execution_result1]

    mock_check2 = MagicMock(spec=Check)
    mock_check2.name = "Test Check 2"
    mock_check2.environments = [TEST_ENVIRONMENT]
    mock_check2.cache_for = None
    execution_result2 = ExecutionResult(
        piggyback_host="test-host-2",
        service_name="test-service-2",
        service_labels={"env": "test"},
        environment_name="test-env",
        check_state=CheckState.WARN,
        summary="Test summary 2",
    )
    mock_check2.run.return_value = [execution_result2]

    # Initialize the Outpost object with both checks
    app = Outpost(
        checks=[mock_check1, mock_check2],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Mock sys.stdout.buffer.write to capture the output
    with patch("sys.stdout.buffer.write") as mock_write:
        # Run the checks
        app.run_checks_once()

        # Verify that both checks were run
        mock_check1.run.assert_called_once()
        mock_check2.run.assert_called_once()

        # Collect all the data written to stdout
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)

        # Verify the basic structure
        assert b"test-host-1" in all_data
        assert b"test-host-2" in all_data

        # Decode the base64 data using the utility function
        json_data_list = decode_checkmk_output(all_data)

        # Verify we found three results (our two and the one default outpost check)
        assert len(json_data_list) == 3

        # Verify the first result
        assert any(
            data["service_name"] == "test-service-1"
            and data["environment"] == "test-env"
            and data["check_state"] == "OK"
            and data["summary"] == "Test summary 1"
            for data in json_data_list
        )

        # Verify the second result
        assert any(
            data["service_name"] == "test-service-2"
            and data["environment"] == "test-env"
            and data["check_state"] == "WARN"
            and data["summary"] == "Test summary 2"
            for data in json_data_list
        )


def test_run_checks_once_with_real_check():
    """Test that the run_checks_once method works with a real Check object."""

    # Create a simple check function
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    # Create a real Check object
    check = Check(
        check_function=check_func,
        service_name="test-service",
        service_labels={"env": "test"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )

    # Initialize the Outpost object
    app = Outpost(
        checks=[check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(TestDatasource)

    # Mock sys.stdout.buffer.write to capture the output
    with patch("sys.stdout.buffer.write") as mock_write:
        # Run the checks
        app.run_checks_once()

        # Collect all the data written to stdout
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)

        # Verify the basic structure
        expected_host = "test-service-test-env-NOTIMPLEMENTEDYET"
        assert expected_host.encode() in all_data

        # Decode the base64 data using the utility function
        json_data = decode_checkmk_output(all_data)[0]

        # Verify the decoded data contains the expected values
        assert json_data["service_name"] == "test-service"
        assert json_data["environment"] == "test-env"
        assert json_data["check_state"] == "OK"
        assert json_data["summary"] == "Test passed"


def test_ensure_current_app_is_set_in_check():
    @check(
        name="Current app is set in check",
        service_labels={"test": "true"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource

        from outpost.globals import current_app

        return ok(repr(current_app))

    app = Outpost(
        checks=[check_func],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(TestDatasource)

    with app.app_context():
        raw_output = b"".join(app.run_checks())
        decoded_output = decode_checkmk_output(raw_output)
        for item in decoded_output:
            item.pop("check_definition", None)

    assert sorted(decoded_output, key=lambda result: result["service_name"]) == sorted(
        [
            {
                "check_state": "OK",
                "details": None,
                "environment": "test-env",
                "metrics": [],
                "service_labels": {"test": "true"},
                "service_name": "Current app is set in check",
                "summary": repr(app),
            },
            {
                "check_state": "OK",
                "details": "Check functions:\n- tests.test_app.test_ensure_current_app_is_set_in_check.<locals>.check_func",
                "environment": "test-env",
                "metrics": [],
                "service_labels": {},
                "service_name": "Run checks",
                "summary": "Ran 1 checks",
            },
        ],
        key=lambda result: result["service_name"],
    )


def test_run_checks_skip_without_prior_results_returns_unknown():
    from outpost.scheduling_strategy import SchedulingDecision, SchedulingStrategy

    class AlwaysSkipStrategy(SchedulingStrategy):
        @override
        def schedule(self, check, current_execution_environment, target_environment):
            return SchedulingDecision.SKIP

    @check(
        name="Skip without prior",
        service_labels={"test": "true"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def my_check():
        raise AssertionError("Should not be executed when SKIP without prior results")

    app = Outpost(
        checks=[my_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
        default_scheduling_strategies=[AlwaysSkipStrategy()],
    )

    with patch("sys.stdout.buffer.write") as mock_write:
        app.run_checks_once()
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)
        results = decode_checkmk_output(all_data)
        result = next(r for r in results if r["service_name"] == "Skip without prior")
        assert result["check_state"] == "UNKNOWN"
        assert (
            result["summary"]
            == "Check is temporarily unschedulable and no prior results are available"
        )


def test_run_checks_skip_with_prior_results_reuses_cache():
    from outpost.scheduling_strategy import SchedulingDecision, SchedulingStrategy

    class AlwaysSkipStrategy(SchedulingStrategy):
        @override
        def schedule(self, check, current_execution_environment, target_environment):
            return SchedulingDecision.SKIP

    @check(
        name="Skip with prior",
        service_labels={"test": "true"},
        environments=[TEST_ENVIRONMENT],
        cache_for="5s",
    )
    def my_check():
        raise AssertionError("Should not be executed when cache exists and SKIP")

    app = Outpost(
        checks=[my_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
        default_scheduling_strategies=[AlwaysSkipStrategy()],
    )

    # Prime the cache with a prior OK result
    execution_result = ExecutionResult(
        piggyback_host="pre",
        service_name="Skip with prior",
        service_labels={"test": "true"},
        environment_name=TEST_ENVIRONMENT.name,
        check_state=CheckState.OK,
        summary="Cached result",
    )
    app._check_cache.store_check_results(my_check, TEST_ENVIRONMENT, [execution_result])

    with patch("sys.stdout.buffer.write") as mock_write:
        app.run_checks_once()
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)
        results = decode_checkmk_output(all_data)
        result = next(r for r in results if r["service_name"] == "Skip with prior")
        assert result["check_state"] == "OK"
        assert result["summary"] == "Cached result"


def test_run_checks_reuses_cached_results_under_schedule():
    call_count = {"n": 0}

    @check(
        name="Cached schedule",
        service_labels={"test": "true"},
        environments=[TEST_ENVIRONMENT],
        cache_for="10s",
    )
    def my_check():
        call_count["n"] += 1
        return ok(f"Run {call_count['n']}")

    app = Outpost(
        checks=[my_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # First run: executes the check and stores the result in cache
    with patch("sys.stdout.buffer.write") as mock_write1:
        app.run_checks_once()
        all_data1 = b"".join(
            call_args[0][0] for call_args in mock_write1.call_args_list
        )
        results1 = decode_checkmk_output(all_data1)
        res1 = next(r for r in results1 if r["service_name"] == "Cached schedule")
        assert res1["summary"] == "Run 1"
        assert call_count["n"] == 1

    # Second run: should reuse cached results, not execute the check again
    with patch("sys.stdout.buffer.write") as mock_write2:
        app.run_checks_once()
        all_data2 = b"".join(
            call_args[0][0] for call_args in mock_write2.call_args_list
        )
        results2 = decode_checkmk_output(all_data2)
        res2 = next(r for r in results2 if r["service_name"] == "Cached schedule")
        assert res2["summary"] == "Run 1"
        assert call_count["n"] == 1


def test_run_checks_dont_schedule_produces_no_results():
    from outpost.scheduling_strategy import SchedulingDecision, SchedulingStrategy

    class AlwaysDontScheduleStrategy(SchedulingStrategy):
        @override
        def schedule(self, check, current_execution_environment, target_environment):
            return SchedulingDecision.DONT_SCHEDULE

    @check(
        name="Dont schedule",
        service_labels={"test": "true"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def my_check():
        raise AssertionError("Should not be executed when DONT_SCHEDULE")

    app = Outpost(
        checks=[my_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
        default_scheduling_strategies=[AlwaysDontScheduleStrategy()],
    )

    with patch("sys.stdout.buffer.write") as mock_write:
        app.run_checks_once()
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)
        results = decode_checkmk_output(all_data)

        # There should only be the synthetic "Run checks" result
        assert len(results) == 1
        assert results[0]["service_name"] == "Run checks"
        # And definitely no result for our check
        assert not any(r["service_name"] == "Dont schedule" for r in results)


def test_datasource_unavailable_without_cache_returns_unknown():
    @check(
        name="DS Unavailable",
        service_labels={"test": "true"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def failing_check():
        raise DatasourceUnavailable("temporary outage")

    app = Outpost(
        checks=[failing_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    with patch("sys.stdout.buffer.write") as mock_write:
        app.run_checks_once()
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)
        results = decode_checkmk_output(all_data)

    result = next(r for r in results if r["service_name"] == "DS Unavailable")
    assert result["check_state"] == "UNKNOWN"
    assert result["summary"] == "temporary outage"
    assert result["details"] is not None
    assert "temporary outage" in result["details"]
    assert "DatasourceUnavailable" in result["details"]
    # Synthetic result is present
    assert any(r["service_name"] == "Run checks" for r in results)


def test_datasource_unavailable_with_expired_cache_returns_enriched_cached_result():
    calls = {"n": 0}

    @check(
        name="DS Unavailable Cached",
        service_labels={"test": "true"},
        environments=[TEST_ENVIRONMENT],
        cache_for=timedelta(microseconds=1),
    )
    def flaky_check():
        calls["n"] += 1
        if calls["n"] == 1:
            return ok("Cached ok")
        raise DatasourceUnavailable("backend down")

    app = Outpost(
        checks=[flaky_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # First run: stores OK in cache
    with patch("sys.stdout.buffer.write") as mock_write1:
        app.run_checks_once()
        all_data1 = b"".join(
            call_args[0][0] for call_args in mock_write1.call_args_list
        )
        results1 = decode_checkmk_output(all_data1)
        res1 = next(r for r in results1 if r["service_name"] == "DS Unavailable Cached")
        assert res1["check_state"] == "OK"
        assert res1["summary"] == "Cached ok"

    # Second run: raises DatasourceUnavailable, should reuse cached result with enriched details
    with patch("sys.stdout.buffer.write") as mock_write2:
        app.run_checks_once()
        all_data2 = b"".join(
            call_args[0][0] for call_args in mock_write2.call_args_list
        )
        results2 = decode_checkmk_output(all_data2)
        res2 = next(r for r in results2 if r["service_name"] == "DS Unavailable Cached")

    assert res2["check_state"] == "OK"
    assert res2["summary"] == "Cached ok"
    # details should be added (was None on first run) and include the exception info
    assert res2["details"] is not None
    assert "backend down" in res2["details"]
    assert "DatasourceUnavailable" in res2["details"]
