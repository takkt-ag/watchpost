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

from outpost.check import Check, check
from outpost.datasource import Datasource
from outpost.environment import Environment
from outpost.result import CheckState, crit, ok, unknown, warn
from outpost.utils import InvocationInformation


class TestDatasource(Datasource):
    argument_name = "test_datasource"


class AnotherTestDatasource(Datasource):
    argument_name = "another_datasource"


def test_check_initialization():
    """Test that a Check object can be properly initialized."""

    # Create a simple check function
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    # Initialize the Check object
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )

    # Verify the Check object was initialized correctly
    assert check.check_function == check_func
    assert check.service_name == "test_service"
    assert check.service_labels == {"env": "test"}
    assert len(check.environments) == 1
    assert check.environments[0].name == "test_env"
    assert check.datasources == [TestDatasource]
    assert check.invocation_information is None


def test_check_with_invocation_information():
    """Test that a Check object can be initialized with invocation information."""

    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    invocation_info = InvocationInformation(relative_path="test/path", line_number=42)

    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
        invocation_information=invocation_info,
    )

    assert check.invocation_information
    assert check.invocation_information == invocation_info
    assert check.invocation_information.relative_path == "test/path"
    assert check.invocation_information.line_number == 42


def test_generate_hostname():
    """Test the generate_hostname method."""

    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={},
        environments=[],
        datasources=[],
    )

    env = Environment("test_env")
    hostname = check.generate_hostname(env)

    # Verify the hostname format
    assert hostname == "test_service-test_env-NOTIMPLEMENTEDYET"


def test_run_with_ok_result():
    """Test the run method with a check function that returns an OK result."""

    # Create a check function that returns an OK result
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Everything is fine")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Initialize the Check object
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )

    # Run the check
    results = check.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "Everything is fine"
    assert results[0].service_name == "test_service"
    assert results[0].environment_name == "test_env"
    assert results[0].service_labels == {"env": "test"}
    assert results[0].piggyback_host == "test_service-test_env-NOTIMPLEMENTEDYET"


def test_run_with_critical_result():
    """Test the run method with a check function that returns a CRIT result."""

    # Create a check function that returns a CRIT result
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return crit("Something is wrong")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Initialize the Check object
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )

    # Run the check
    results = check.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.CRIT
    assert results[0].summary == "Something is wrong"


def test_run_with_warning_result():
    """Test the run method with a check function that returns a WARN result."""

    # Create a check function that returns a WARN result
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return warn("Something might be wrong")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Initialize the Check object
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )

    # Run the check
    results = check.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.WARN
    assert results[0].summary == "Something might be wrong"


def test_run_with_unknown_result():
    """Test the run method with a check function that returns an UNKNOWN result."""

    # Create a check function that returns an UNKNOWN result
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return unknown("Status is unknown")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Initialize the Check object
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )

    # Run the check
    results = check.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.UNKNOWN
    assert results[0].summary == "Status is unknown"


def test_run_with_multiple_environments():
    """Test the run method with multiple environments."""

    # Create a check function
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Checked environment")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Initialize the Check object with multiple environments
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("env1"), Environment("env2"), Environment("env3")],
        datasources=[TestDatasource],
    )

    # Run the check
    results = check.run()

    # Verify the results
    assert len(results) == 3
    assert results[0].environment_name == "env1"
    assert results[1].environment_name == "env2"
    assert results[2].environment_name == "env3"


def test_run_with_multiple_datasources():
    """Test the run method with multiple datasources."""

    # Create a check function that uses multiple datasources
    def check_func(
        test_datasource: TestDatasource,
        another_datasource: AnotherTestDatasource,
    ):
        _ = test_datasource
        _ = another_datasource
        return ok("Multiple datasources used")

    # Set up the datasources
    TestDatasource.instance = TestDatasource()
    AnotherTestDatasource.instance = AnotherTestDatasource()

    # Initialize the Check object with multiple datasources
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource, AnotherTestDatasource],
    )

    # Run the check
    results = check.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "Multiple datasources used"


def test_run_with_environment_parameter():
    """Test the run method with a check function that takes an environment parameter."""

    # Create a check function that takes an environment parameter
    def check_func(environment: Environment, test_datasource: TestDatasource):
        _ = test_datasource
        return ok(f"Checked environment: {environment.name}")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Initialize the Check object
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )

    # Run the check
    results = check.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "Checked environment: test_env"


def test_run_captures_stdout_stderr():
    """Test that the run method captures stdout and stderr."""

    # Create a check function that prints to stdout and stderr
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        print("This is printed to stdout")  # noqa: T201
        import sys

        print("This is printed to stderr", file=sys.stderr)  # noqa: T201
        return ok("Check completed")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Initialize the Check object
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )

    results = check.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "Check completed"
    assert (
        results[0].details
        == "<STDOUT>\nThis is printed to stdout\n\n</STDOUT>\n\n<STDERR>\nThis is printed to stderr\n\n</STDERR>"
    )


def test_run_with_list_of_results():
    """Test the run method with a check function that returns a list of results."""

    # Create a check function that returns a list of results
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        return [
            ok("First check passed"),
            warn("Second check has a warning"),
            crit("Third check failed"),
        ]

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Initialize the Check object
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )

    # Run the check
    results = check.run()

    # Verify the results
    assert len(results) == 3
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "First check passed"
    assert results[1].check_state == CheckState.WARN
    assert results[1].summary == "Second check has a warning"
    assert results[2].check_state == CheckState.CRIT
    assert results[2].summary == "Third check failed"


def test_run_with_generator_of_results():
    """Test the run method with a check function that returns a generator of results."""

    # Create a check function that returns a generator of results
    def check_func(test_datasource: TestDatasource):
        _ = test_datasource
        yield ok("First check passed")
        yield warn("Second check has a warning")
        yield crit("Third check failed")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Initialize the Check object
    check = Check(
        check_function=check_func,
        service_name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )

    # Run the check
    results = check.run()

    # Verify the results
    assert len(results) == 3
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "First check passed"
    assert results[1].check_state == CheckState.WARN
    assert results[1].summary == "Second check has a warning"
    assert results[2].check_state == CheckState.CRIT
    assert results[2].summary == "Third check failed"


# Tests for the check decorator


def test_check_decorator_returns_check_instance():
    """Test that the check decorator returns a Check instance."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    # Verify that the decorated function is a Check instance
    assert isinstance(decorated_func, Check)
    assert decorated_func.service_name == "test_service"
    assert decorated_func.service_labels == {"env": "test"}
    assert len(decorated_func.environments) == 1
    assert decorated_func.environments[0].name == "test_env"
    assert decorated_func.datasources == [TestDatasource]


def test_decorated_function_can_be_called_directly():
    """Test that a decorated function can be called directly."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Call the decorated function directly
    result = decorated_func(TestDatasource.instance)

    # Verify the result
    assert result.check_state == CheckState.OK
    assert result.summary == "Test passed"


def test_decorated_function_run_method():
    """Test the run method of a decorated function."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Run the check
    results = decorated_func.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "Test passed"
    assert results[0].service_name == "test_service"
    assert results[0].environment_name == "test_env"
    assert results[0].service_labels == {"env": "test"}
    assert results[0].piggyback_host == "test_service-test_env-NOTIMPLEMENTEDYET"


def test_decorated_function_with_different_result_types():
    """Test decorated functions with different result types."""

    # Test with OK result
    @check(
        name="ok_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def ok_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Everything is fine")

    # Test with WARN result
    @check(
        name="warn_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def warn_func(test_datasource: TestDatasource):
        _ = test_datasource
        return warn("Something might be wrong")

    # Test with CRIT result
    @check(
        name="crit_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def crit_func(test_datasource: TestDatasource):
        _ = test_datasource
        return crit("Something is wrong")

    # Test with UNKNOWN result
    @check(
        name="unknown_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def unknown_func(test_datasource: TestDatasource):
        _ = test_datasource
        return unknown("Status is unknown")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Run the checks
    ok_results = ok_func.run()
    warn_results = warn_func.run()
    crit_results = crit_func.run()
    unknown_results = unknown_func.run()

    # Verify the OK results
    assert len(ok_results) == 1
    assert ok_results[0].check_state == CheckState.OK
    assert ok_results[0].summary == "Everything is fine"

    # Verify the WARN results
    assert len(warn_results) == 1
    assert warn_results[0].check_state == CheckState.WARN
    assert warn_results[0].summary == "Something might be wrong"

    # Verify the CRIT results
    assert len(crit_results) == 1
    assert crit_results[0].check_state == CheckState.CRIT
    assert crit_results[0].summary == "Something is wrong"

    # Verify the UNKNOWN results
    assert len(unknown_results) == 1
    assert unknown_results[0].check_state == CheckState.UNKNOWN
    assert unknown_results[0].summary == "Status is unknown"


def test_decorated_function_with_multiple_environments():
    """Test a decorated function with multiple environments."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("env1"), Environment("env2"), Environment("env3")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Checked environment")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Run the check
    results = decorated_func.run()

    # Verify the results
    assert len(results) == 3
    assert results[0].environment_name == "env1"
    assert results[1].environment_name == "env2"
    assert results[2].environment_name == "env3"


def test_decorated_function_with_multiple_datasources():
    """Test a decorated function with multiple datasources."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource, AnotherTestDatasource],
    )
    def decorated_func(
        test_datasource: TestDatasource,
        another_datasource: AnotherTestDatasource,
    ):
        _ = test_datasource
        _ = another_datasource
        return ok("Multiple datasources used")

    # Set up the datasources
    TestDatasource.instance = TestDatasource()
    AnotherTestDatasource.instance = AnotherTestDatasource()

    # Run the check
    results = decorated_func.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "Multiple datasources used"


def test_decorated_function_with_environment_parameter():
    """Test a decorated function with an environment parameter."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(environment: Environment, test_datasource: TestDatasource):
        _ = test_datasource
        return ok(f"Checked environment: {environment.name}")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Run the check
    results = decorated_func.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "Checked environment: test_env"


def test_decorated_function_captures_stdout_stderr():
    """Test that a decorated function captures stdout and stderr."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        print("This is printed to stdout")  # noqa: T201
        import sys

        print("This is printed to stderr", file=sys.stderr)  # noqa: T201
        return ok("Check completed")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Run the check
    results = decorated_func.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "Check completed"
    assert (
        results[0].details
        == "<STDOUT>\nThis is printed to stdout\n\n</STDOUT>\n\n<STDERR>\nThis is printed to stderr\n\n</STDERR>"
    )


def test_decorated_function_with_list_of_results():
    """Test a decorated function that returns a list of results."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        return [
            ok("First check passed"),
            warn("Second check has a warning"),
            crit("Third check failed"),
        ]

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Run the check
    results = decorated_func.run()

    # Verify the results
    assert len(results) == 3
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "First check passed"
    assert results[1].check_state == CheckState.WARN
    assert results[1].summary == "Second check has a warning"
    assert results[2].check_state == CheckState.CRIT
    assert results[2].summary == "Third check failed"


def test_decorated_function_with_generator_of_results():
    """Test a decorated function that returns a generator of results."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        yield ok("First check passed")
        yield warn("Second check has a warning")
        yield crit("Third check failed")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Run the check
    results = decorated_func.run()

    # Verify the results
    assert len(results) == 3
    assert results[0].check_state == CheckState.OK
    assert results[0].summary == "First check passed"
    assert results[1].check_state == CheckState.WARN
    assert results[1].summary == "Second check has a warning"
    assert results[2].check_state == CheckState.CRIT
    assert results[2].summary == "Third check failed"


def test_check_decorator_captures_invocation_information():
    """Test that the check decorator captures invocation information."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    # Verify that the decorated function has invocation information
    assert decorated_func.invocation_information is not None
    assert isinstance(decorated_func.invocation_information, InvocationInformation)
    assert decorated_func.invocation_information.relative_path.endswith(
        "tests/test_check.py"
    )
    # The line number should be the line where the decorator is applied
    assert decorated_func.invocation_information.line_number > 0


def test_check_decorator_passes_invocation_information_to_execution_result():
    """Test that the invocation information from the check decorator is passed to the ExecutionResult."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Run the check
    results = decorated_func.run()

    # Verify the results
    assert len(results) == 1
    assert results[0].check_definition is not None
    assert results[0].check_definition == decorated_func.invocation_information
    assert results[0].check_definition.relative_path.endswith("tests/test_check.py")
    assert results[0].check_definition.line_number > 0


def test_check_decorator_invocation_information_in_checkmk_output():
    """Test that the invocation information from the check decorator is included in the CheckMK output."""

    @check(
        name="test_service",
        service_labels={"env": "test"},
        environments=[Environment("test_env")],
        datasources=[TestDatasource],
    )
    def decorated_func(test_datasource: TestDatasource):
        _ = test_datasource
        return ok("Test passed")

    # Set up the datasource
    TestDatasource.instance = TestDatasource()

    # Run the check
    results = decorated_func.run()

    # Generate the CheckMK output
    output_generator = results[0].generate_checkmk_output()
    output_bytes = b"".join(list(output_generator))
    output_str = output_bytes.decode("utf-8")

    # Extract and decode the base64 encoded JSON
    import base64
    import json
    import re

    # Find the base64 encoded part between the outpost markers
    match = re.search(r"<<<outpost>>>\n(.*?)\n<<<<", output_str, re.DOTALL)
    assert match is not None, "Could not find base64 encoded data in output"

    base64_data = match.group(1)
    decoded_data = base64.b64decode(base64_data).decode("utf-8")
    json_data = json.loads(decoded_data)

    # Verify the output contains the check definition
    assert "check_definition" in json_data
    assert "relative_path" in json_data["check_definition"]
    assert "line_number" in json_data["check_definition"]
    assert json_data["check_definition"]["relative_path"].endswith(
        "tests/test_check.py"
    )
