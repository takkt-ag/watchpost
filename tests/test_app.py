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

import base64
import json
from unittest.mock import MagicMock, patch

import pytest

from outpost.app import Outpost
from outpost.check import Check
from outpost.datasource import Datasource
from outpost.environment import Environment
from outpost.globals import current_app
from outpost.result import CheckState, ExecutionResult, ok

TEST_ENVIRONMENT = Environment("test-env")


class TestDatasource(Datasource):
    argument_name = "test_datasource"


def test_outpost_initialization():
    """Test that an Outpost object can be properly initialized."""
    # Create a mock check
    mock_check = MagicMock(spec=Check)

    # Initialize the Outpost object
    app = Outpost(
        checks=[mock_check],
        outpost_environment=TEST_ENVIRONMENT,
    )

    # Verify the Outpost object was initialized correctly
    assert app.checks == [mock_check]


def test_app_context():
    """Test that the app_context method properly sets and resets the context variable."""
    # Create an Outpost instance
    app = Outpost(
        checks=[],
        outpost_environment=TEST_ENVIRONMENT,
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
        outpost_environment=TEST_ENVIRONMENT,
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
        outpost_environment=TEST_ENVIRONMENT,
    )

    # Mock sys.stdout.buffer.write to capture the output
    with patch("sys.stdout.buffer.write") as mock_write:
        # Run the checks
        app.run_checks_once()

        # Verify that the check was run
        mock_check.run.assert_called_once()

        # Verify that sys.stdout.buffer.write was called with the expected data
        assert mock_write.call_count > 0

        # The first call should be the header
        assert mock_write.call_args_list[0][0][0] == b"<<<<"

        # The second call should be the piggyback host
        assert mock_write.call_args_list[1][0][0] == b"test-host"

        # Collect all the data written to stdout
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)

        # Find the base64 encoded part
        import re

        base64_pattern = re.compile(rb"<<<outpost>>>\n(.*?)\n<<<<>>>>", re.DOTALL)
        match = base64_pattern.search(all_data)
        assert match is not None

        # Decode the base64 data
        base64_data = match.group(1)
        json_data = json.loads(base64.b64decode(base64_data))

        # Verify the decoded data contains the expected values
        assert json_data["service_name"] == "test-service"
        assert json_data["environment"] == "test-env"
        assert json_data["check_state"] == "OK"
        assert json_data["summary"] == "Test summary"


def test_run_checks_once_with_multiple_checks():
    """Test that the run_checks_once method runs multiple checks."""
    # Create two mock checks
    mock_check1 = MagicMock(spec=Check)
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
        outpost_environment=TEST_ENVIRONMENT,
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

        # Find the base64 encoded parts
        import re

        base64_pattern = re.compile(rb"<<<outpost>>>\n(.*?)\n<<<<>>>>", re.DOTALL)
        matches = base64_pattern.finditer(all_data)

        # Decode and verify each match
        json_data_list = []
        for match in matches:
            base64_data = match.group(1)
            json_data = json.loads(base64.b64decode(base64_data))
            json_data_list.append(json_data)

        # Verify we found two results
        assert len(json_data_list) == 2

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

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Create a real Check object
    check = Check(
        check_function=check_func,
        service_name="test-service",
        service_labels={"env": "test"},
        datasources=[TestDatasource],
        environments=[TEST_ENVIRONMENT],
    )

    # Initialize the Outpost object
    app = Outpost(
        checks=[check],
        outpost_environment=TEST_ENVIRONMENT,
    )

    # Mock sys.stdout.buffer.write to capture the output
    with patch("sys.stdout.buffer.write") as mock_write:
        # Run the checks
        app.run_checks_once()

        # Collect all the data written to stdout
        all_data = b"".join(call_args[0][0] for call_args in mock_write.call_args_list)

        # Verify the basic structure
        expected_host = "test-service-test-env-NOTIMPLEMENTEDYET"
        assert expected_host.encode() in all_data

        # Find the base64 encoded part
        import re

        base64_pattern = re.compile(rb"<<<outpost>>>\n(.*?)\n<<<<>>>>", re.DOTALL)
        match = base64_pattern.search(all_data)
        assert match is not None

        # Decode the base64 data
        base64_data = match.group(1)
        json_data = json.loads(base64.b64decode(base64_data))

        # Verify the decoded data contains the expected values
        assert json_data["service_name"] == "test-service"
        assert json_data["environment"] == "test-env"
        assert json_data["check_state"] == "OK"
        assert json_data["summary"] == "Test passed"
