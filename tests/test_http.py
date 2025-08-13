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

from unittest.mock import patch

from starlette.testclient import TestClient

from outpost import Datasource
from outpost.app import Outpost
from outpost.check import check
from outpost.environment import Environment
from outpost.executor import CheckExecutor
from outpost.http import routes
from outpost.result import ok

from .utils import BlockingCheckExecutor, decode_checkmk_output

TEST_ENVIRONMENT = Environment("test-env")


def test_healthcheck():
    app = Outpost(
        checks=[],
        outpost_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    client = TestClient(app)
    response = client.get("/healthcheck")

    assert response.status_code == 204
    assert response.content == b""


def test_executor_statistics():
    """Test that the executor statistics endpoint returns the expected statistics."""
    app = Outpost(
        checks=[],
        outpost_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    mock_statistics = CheckExecutor.Statistics(
        total=10,
        completed=5,
        errored=2,
        running=3,
        awaiting_pickup=7,
    )

    with patch.object(app.executor, "statistics", return_value=mock_statistics):
        client = TestClient(app)
        response = client.get("/executor/statistics")

        assert response.status_code == 200
        assert response.json() == {
            "total": 10,
            "completed": 5,
            "errored": 2,
            "running": 3,
            "awaiting_pickup": 7,
        }


def test_executor_errored():
    """Test that the executor errored endpoint returns the expected error information."""
    # Create a mock check
    app = Outpost(
        checks=[],
        outpost_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    mock_errored = {
        "check1": "Error message 1",
        "check2": "Error message 2",
    }

    with patch.object(app.executor, "errored", return_value=mock_errored):
        client = TestClient(app)
        response = client.get("/executor/errored")

        assert response.status_code == 200
        assert response.json() == {
            "check1": "Error message 1",
            "check2": "Error message 2",
        }


def test_root():
    """Test that the root endpoint returns a streaming response with check results."""
    app = Outpost(
        checks=[],
        outpost_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    expected_output = [
        b"<<<check_mk>>>\n",
        b"Version: outpost-unknown\n",
        b"AgentOS: outpost\n",
    ]

    with patch.object(app, "run_checks", return_value=expected_output):
        client = TestClient(app)
        response = client.get("/")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
        assert response.content == b"".join(expected_output)


def test_root_with_real_check():
    """Test that the root endpoint returns actual check results."""

    @check(
        name="simple-check",
        service_labels={"test": "true"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def simple_check():
        return ok("Simple check passed")

    app = Outpost(
        checks=[simple_check],
        outpost_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")

    assert b"<<<check_mk>>>" in response.content
    assert b"Version: outpost-unknown" in response.content
    assert b"AgentOS: outpost" in response.content

    assert b"<<<outpost>>>" in response.content
    assert b"simple-check" in response.content

    checkmk_output = decode_checkmk_output(response.content)
    for item in checkmk_output:
        item.pop("check_definition", None)

    assert sorted(checkmk_output, key=lambda result: result["service_name"]) == sorted(
        [
            {
                "service_name": "simple-check",
                "service_labels": {"test": "true"},
                "environment": "test-env",
                "check_state": "OK",
                "summary": "Simple check passed",
                "metrics": [],
                "details": None,
            },
            {
                "service_name": "Run checks",
                "service_labels": {},
                "environment": "test-env",
                "check_state": "OK",
                "summary": "Ran 1 checks",
                "metrics": [],
                "details": "Check functions:\n- tests.test_http.test_root_with_real_check.<locals>.simple_check",
            },
        ],
        key=lambda result: result["service_name"],
    )


def test_root_with_real_check_and_datasource():
    class TestDatasource(Datasource):
        pass

    @check(
        name="simple-check",
        service_labels={"test": "true"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def simple_check(test_datasource: TestDatasource):
        return ok(f"Simple check passed, got {type(test_datasource)}")

    app = Outpost(
        checks=[simple_check],
        outpost_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(TestDatasource)
    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")

    assert b"<<<check_mk>>>" in response.content
    assert b"Version: outpost-unknown" in response.content
    assert b"AgentOS: outpost" in response.content

    assert b"<<<outpost>>>" in response.content
    assert b"simple-check" in response.content

    checkmk_output = decode_checkmk_output(response.content)
    for item in checkmk_output:
        item.pop("check_definition", None)

    assert sorted(checkmk_output, key=lambda result: result["service_name"]) == sorted(
        [
            {
                "service_name": "simple-check",
                "service_labels": {"test": "true"},
                "environment": "test-env",
                "check_state": "OK",
                "summary": "Simple check passed, got <class 'tests.test_http.test_root_with_real_check_and_datasource.<locals>.TestDatasource'>",
                "metrics": [],
                "details": None,
            },
            {
                "service_name": "Run checks",
                "service_labels": {},
                "environment": "test-env",
                "check_state": "OK",
                "summary": "Ran 1 checks",
                "metrics": [],
                "details": "Check functions:\n- tests.test_http.test_root_with_real_check_and_datasource.<locals>.simple_check",
            },
        ],
        key=lambda result: result["service_name"],
    )


def test_routes_configuration():
    """Test that the routes are configured correctly."""
    # Verify that the routes list contains the expected routes
    route_paths = [route.path for route in routes]
    assert "/healthcheck" in route_paths
    assert "/executor/statistics" in route_paths
    assert "/executor/errored" in route_paths
    assert "/" in route_paths

    # Verify that the routes have the expected endpoints
    route_endpoints = {route.path: route.endpoint.__name__ for route in routes}
    assert route_endpoints["/healthcheck"] == "healthcheck"
    assert route_endpoints["/executor/statistics"] == "executor_statistics"
    assert route_endpoints["/executor/errored"] == "executor_errored"
    assert route_endpoints["/"] == "root"
