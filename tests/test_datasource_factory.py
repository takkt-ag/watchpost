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

import logging
from typing import Annotated, cast

import pytest

from outpost.app import Outpost
from outpost.check import Check, check
from outpost.datasource import Datasource, DatasourceFactory, FromFactory
from outpost.environment import Environment
from outpost.executor import BlockingCheckExecutor
from outpost.result import CheckResult, ok
from outpost.scheduling_strategy import (
    MustRunInGivenExecutionEnvironmentStrategy,
    SchedulingDecision,
)
from tests.utils import decode_checkmk_output

# Define test environment
TEST_ENVIRONMENT = Environment("test-env")


# Define test datasources and factories
class TestDatasource(Datasource):
    __test__ = False


class ConfigurableTestDatasource(Datasource):
    def __init__(self, config_value: str):
        self.config_value = config_value


class TestFactory(DatasourceFactory):
    __test__ = False

    def new(self, service: str) -> Datasource:
        return ConfigurableTestDatasource(f"factory-created-{service}")


class ParameterizedTestFactory(DatasourceFactory):
    def __init__(self):
        self.prefix = "default-prefix"

    def new(self, service: str, region: str = "default-region") -> Datasource:
        return ConfigurableTestDatasource(f"{self.prefix}-{service}-{region}")


def test_register_datasource_factory() -> None:
    """Test that a datasource factory can be registered with an Outpost instance."""
    # Create an Outpost instance
    app = Outpost(
        checks=[],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register a factory
    app.register_datasource_factory(TestFactory)

    # Verify the factory was registered
    assert TestFactory in app._datasource_factories
    assert isinstance(app._datasource_factories[TestFactory], TestFactory)


def test_resolve_datasource_from_factory() -> None:
    """Test that a datasource can be resolved from a factory."""
    # Create an Outpost instance
    app = Outpost(
        checks=[],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register a factory
    app.register_datasource_factory(TestFactory)

    # Create a FromFactory instance
    from_factory = FromFactory(TestFactory, "test-service")

    # Resolve the datasource
    datasource = app._resolve_datasource_from_factory(from_factory)

    # Verify the datasource was resolved correctly
    assert isinstance(datasource, ConfigurableTestDatasource)
    assert datasource.config_value == "factory-created-test-service"


def test_resolve_datasource_from_factory_caching() -> None:
    """Test that resolved datasources are cached."""
    # Create an Outpost instance
    app = Outpost(
        checks=[],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register a factory
    app.register_datasource_factory(TestFactory)

    # Create a FromFactory instance
    from_factory = FromFactory(TestFactory, "test-service")

    # Resolve the datasource twice
    datasource1 = app._resolve_datasource_from_factory(from_factory)
    datasource2 = app._resolve_datasource_from_factory(from_factory)

    # Verify that the same instance was returned both times
    assert datasource1 is datasource2


def test_resolve_datasource_from_factory_with_different_args() -> None:
    """Test that different args produce different datasources."""
    # Create an Outpost instance
    app = Outpost(
        checks=[],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register a factory
    app.register_datasource_factory(TestFactory)

    # Create two FromFactory instances with different args
    from_factory1 = FromFactory(TestFactory, "service1")
    from_factory2 = FromFactory(TestFactory, "service2")

    # Resolve the datasources
    datasource1: ConfigurableTestDatasource = cast(
        ConfigurableTestDatasource,
        app._resolve_datasource_from_factory(from_factory1),
    )
    datasource2: ConfigurableTestDatasource = cast(
        ConfigurableTestDatasource,
        app._resolve_datasource_from_factory(from_factory2),
    )

    # Verify that different instances were returned
    assert datasource1 is not datasource2
    assert datasource1.config_value == "factory-created-service1"
    assert datasource2.config_value == "factory-created-service2"


def test_resolve_datasource_from_factory_with_kwargs() -> None:
    """Test that a datasource can be resolved from a factory with kwargs."""
    # Create an Outpost instance
    app = Outpost(
        checks=[],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register a factory
    app.register_datasource_factory(ParameterizedTestFactory)

    # Create a FromFactory instance with kwargs
    from_factory = FromFactory(
        ParameterizedTestFactory, "test-service", region="us-west-2"
    )

    # Resolve the datasource
    datasource = app._resolve_datasource_from_factory(from_factory)

    # Verify the datasource was resolved correctly
    assert isinstance(datasource, ConfigurableTestDatasource)
    assert datasource.config_value == "default-prefix-test-service-us-west-2"


def test_factory_not_registered() -> None:
    """Test that an error is raised when trying to resolve a datasource from an unregistered factory."""
    # Create an Outpost instance
    app = Outpost(
        checks=[],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Create a FromFactory instance
    from_factory = FromFactory(TestFactory, "test-service")

    # Attempt to resolve the datasource
    with pytest.raises(ValueError, match="No datasource factory for"):
        app._resolve_datasource_from_factory(from_factory)


def test_check_with_factory_datasource() -> None:
    """Test that a check can use a datasource from a factory."""

    # Create a check function that uses a factory datasource
    @check(
        name="test-service",
        service_labels={"env": "test"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def test_check(
        datasource: Annotated[
            ConfigurableTestDatasource, FromFactory(TestFactory, "test-service")
        ],
    ) -> CheckResult:
        return ok(f"Datasource config: {datasource.config_value}")

    # Create an Outpost instance with the check
    app = Outpost(
        checks=[test_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register the factory
    app.register_datasource_factory(TestFactory)

    # Run the check
    results = test_check.run_sync(
        outpost=app,
        datasources={
            "datasource": app._resolve_datasource_from_factory(
                FromFactory(TestFactory, "test-service")
            ),
        },
        environment=TEST_ENVIRONMENT,
    )

    # Verify the results
    assert len(results) == 1
    assert results[0].summary == "Datasource config: factory-created-test-service"


def test_check_with_multiple_factory_datasources() -> None:
    """Test that a check can use multiple datasources from factories."""

    # Create a check function that uses multiple factory datasources
    @check(
        name="test-service",
        service_labels={"env": "test"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def test_check(
        datasource1: Annotated[
            ConfigurableTestDatasource, FromFactory(TestFactory, "service1")
        ],
        datasource2: Annotated[
            ConfigurableTestDatasource, FromFactory(TestFactory, "service2")
        ],
    ) -> CheckResult:
        return ok(
            f"Datasource1: {datasource1.config_value}, Datasource2: {datasource2.config_value}"
        )

    # Create an Outpost instance with the check
    app = Outpost(
        checks=[test_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register the factory
    app.register_datasource_factory(TestFactory)

    # Run the check
    results = test_check.run_sync(
        outpost=app,
        datasources={
            "datasource1": app._resolve_datasource_from_factory(
                FromFactory(TestFactory, "service1")
            ),
            "datasource2": app._resolve_datasource_from_factory(
                FromFactory(TestFactory, "service2")
            ),
        },
        environment=TEST_ENVIRONMENT,
    )

    # Verify the results
    assert len(results) == 1
    assert (
        results[0].summary
        == "Datasource1: factory-created-service1, Datasource2: factory-created-service2"
    )


def test_check_with_mixed_datasources() -> None:
    """Test that a check can use both regular datasources and factory datasources."""

    # Create a check function that uses both types of datasources
    @check(
        name="test-service",
        service_labels={"env": "test"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def test_check(
        regular: TestDatasource,
        factory: Annotated[
            ConfigurableTestDatasource, FromFactory(TestFactory, "test-service")
        ],
    ) -> CheckResult:
        return ok(f"Regular: {type(regular).__name__}, Factory: {factory.config_value}")

    # Create an Outpost instance with the check
    app = Outpost(
        checks=[test_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register the datasource and factory
    app.register_datasource(TestDatasource)
    app.register_datasource_factory(TestFactory)

    # Run the check
    results = test_check.run_sync(
        outpost=app,
        datasources={
            "regular": TestDatasource(),
            "factory": app._resolve_datasource_from_factory(
                FromFactory(TestFactory, "test-service")
            ),
        },
        environment=TEST_ENVIRONMENT,
    )

    # Verify the results
    assert len(results) == 1
    assert (
        results[0].summary
        == "Regular: TestDatasource, Factory: factory-created-test-service"
    )


def test_outpost_resolve_datasources_with_factory() -> None:
    """Test that Outpost._resolve_datasources correctly handles factory datasources."""

    # Create a check function that uses a factory datasource
    def check_func(
        datasource: Annotated[
            ConfigurableTestDatasource, FromFactory(TestFactory, "test-service")
        ],
    ) -> CheckResult:
        return ok(f"Datasource config: {datasource.config_value}")

    # Create a Check object
    check_obj = Check(
        check_function=check_func,
        service_name="test-service",
        service_labels={"env": "test"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )

    # Create an Outpost instance
    app = Outpost(
        checks=[check_obj],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register the factory
    app.register_datasource_factory(TestFactory)

    # Resolve the datasources
    datasources = app._resolve_datasources(check_obj)

    # Verify the datasources
    assert "datasource" in datasources
    assert isinstance(datasources["datasource"], ConfigurableTestDatasource)
    assert datasources["datasource"].config_value == "factory-created-test-service"


def test_outpost_run_checks_with_factory() -> None:
    """Test that Outpost.run_checks correctly handles factory datasources."""

    # Create a check function that uses a factory datasource
    @check(
        name="test-service",
        service_labels={"env": "test"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )
    def test_check(
        datasource: Annotated[
            ConfigurableTestDatasource, FromFactory(TestFactory, "test-service")
        ],
    ) -> CheckResult:
        return ok(f"Datasource config: {datasource.config_value}")

    # Create an Outpost instance with the check
    app = Outpost(
        checks=[test_check],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Register the factory
    app.register_datasource_factory(TestFactory)

    # Run the checks
    output = list(app.run_checks())

    # Verify that output was generated
    assert len(output) > 0
    assert isinstance(output[0], bytes)

    decoded_output = decode_checkmk_output(b"".join(output))
    for item in decoded_output:
        item.pop("check_definition", None)

    assert sorted(decoded_output, key=lambda result: result["service_name"]) == sorted(
        [
            {
                "check_state": "OK",
                "details": None,
                "environment": "test-env",
                "metrics": [],
                "service_labels": {"env": "test"},
                "service_name": "test-service",
                "summary": "Datasource config: factory-created-test-service",
            },
            {
                "check_state": "OK",
                "details": "Check functions:\n- tests.test_datasource_factory.test_outpost_run_checks_with_factory.<locals>.test_check",
                "environment": "test-env",
                "metrics": [],
                "service_labels": {},
                "service_name": "Run checks",
                "summary": "Ran 1 checks",
            },
        ],
        key=lambda result: result["service_name"],
    )


def test_fromfactory_cache_key() -> None:
    """Test that FromFactory.cache_key is correctly generated."""
    # Create two FromFactory instances with the same args
    from_factory1 = FromFactory(TestFactory, "test-service")
    from_factory2 = FromFactory(TestFactory, "test-service")

    # Verify that they have the same cache key
    assert from_factory1.cache_key == from_factory2.cache_key

    # Create two FromFactory instances with different args
    from_factory3 = FromFactory(TestFactory, "service1")
    from_factory4 = FromFactory(TestFactory, "service2")

    # Verify that they have different cache keys
    assert from_factory3.cache_key != from_factory4.cache_key

    # Create two FromFactory instances with the same args but different kwargs
    from_factory5 = FromFactory(
        ParameterizedTestFactory, "test-service", region="us-west-1"
    )
    from_factory6 = FromFactory(
        ParameterizedTestFactory, "test-service", region="us-west-2"
    )

    # Verify that they have different cache keys
    assert from_factory5.cache_key != from_factory6.cache_key


def test_annotated_with_non_fromfactory() -> None:
    """Test that using Annotated with something other than FromFactory raises a ValueError."""

    # Create a check function that uses Annotated with something other than FromFactory
    def check_func(
        _datasource: Annotated[ConfigurableTestDatasource, "not-a-fromfactory"],
    ) -> CheckResult:
        return ok("Test passed")

    # Create a Check object
    check_obj = Check(
        check_function=check_func,
        service_name="test-service",
        service_labels={"env": "test"},
        environments=[TEST_ENVIRONMENT],
        cache_for=None,
    )

    # Create an Outpost instance
    app = Outpost(
        checks=[check_obj],
        execution_environment=TEST_ENVIRONMENT,
        executor=BlockingCheckExecutor(),
    )

    # Attempt to resolve the datasources, which should raise a ValueError
    with pytest.raises(ValueError, match="Unsupported annotation"):
        app._resolve_datasources(check_obj)


def test_factory_datasource_prefers_own_strategies_over_factory() -> None:
    A = Environment("A")
    B = Environment("B")

    class OwnStrategyDatasource(Datasource):
        scheduling_strategies = (MustRunInGivenExecutionEnvironmentStrategy(A),)

    class FactoryWithDifferentStrategy(DatasourceFactory):
        # Factory provides a conflicting execution environment (B), but the
        # datasource created by the factory defines its own strategy (A), which
        # must take precedence.
        scheduling_strategies = (MustRunInGivenExecutionEnvironmentStrategy(B),)

        def new(self, *_args, **_kwargs) -> Datasource:
            return OwnStrategyDatasource()

    @check(
        name="factory-prefers-ds-own",
        service_labels={"test": "true"},
        environments=[A],
        cache_for=None,
    )
    def my_check(_ds: Annotated[Datasource, FromFactory(FactoryWithDifferentStrategy)]):
        from outpost.result import ok

        return ok("unused")

    from outpost.app import Outpost

    app = Outpost(
        checks=[my_check],
        execution_environment=A,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource_factory(FactoryWithDifferentStrategy)

    with app.app_context():
        # The decision should be based on the datasource's own strategy (A),
        # not the factory's (B).
        decision = app._resolve_check_scheduling_decision(my_check, A)
        assert decision == SchedulingDecision.SCHEDULE

        # Verify that the resolved strategies include the A constraint and not B.
        strategies = app._resolve_scheduling_strategies(my_check)
        a_present = False
        b_present = False
        for s in strategies:
            if isinstance(s, MustRunInGivenExecutionEnvironmentStrategy):
                if (
                    A in s.supported_execution_environments
                    and len(s.supported_execution_environments) == 1
                ):
                    a_present = True
                if (
                    B in s.supported_execution_environments
                    and len(s.supported_execution_environments) == 1
                ):
                    b_present = True
        assert a_present
        assert not b_present


def test_factory_strategies_applied_when_datasource_has_no_strategies() -> None:
    A = Environment("A")
    B = Environment("B")

    class EllipsisDatasource(Datasource):
        # No explicit strategies -> Ellipsis from base class
        pass

    class FactoryWithStrategies(DatasourceFactory):
        scheduling_strategies = (MustRunInGivenExecutionEnvironmentStrategy(B),)

        def new(self, *_args, **_kwargs) -> Datasource:
            return EllipsisDatasource()

    @check(
        name="factory-strategies-applied",
        service_labels={"test": "true"},
        environments=[B],
        cache_for=None,
    )
    def my_check(_ds: Annotated[Datasource, FromFactory(FactoryWithStrategies)]):
        from outpost.result import ok

        return ok("unused")

    from outpost.app import Outpost

    # Executing from B should be allowed due to factory strategies
    app_b = Outpost(
        checks=[my_check],
        execution_environment=B,
        executor=BlockingCheckExecutor(),
    )
    app_b.register_datasource_factory(FactoryWithStrategies)

    with app_b.app_context():
        decision_b = app_b._resolve_check_scheduling_decision(my_check, B)
        assert decision_b == SchedulingDecision.SCHEDULE

    # Executing from A should not be allowed due to factory strategies pinning to B
    app_a = Outpost(
        checks=[my_check],
        execution_environment=A,
        executor=BlockingCheckExecutor(),
    )
    app_a.register_datasource_factory(FactoryWithStrategies)

    with app_a.app_context():
        decision_a = app_a._resolve_check_scheduling_decision(my_check, B)
        assert decision_a == SchedulingDecision.DONT_SCHEDULE


def test_factory_and_datasource_without_strategies_logs_no_warning(caplog) -> None:
    ENV = Environment("env")

    class NoStrategyDatasource(Datasource):
        # No explicit strategies -> Ellipsis from base class
        pass

    class FactoryWithoutStrategies(DatasourceFactory):
        # Explicitly define no strategies
        scheduling_strategies = ()

        def new(self, *_args, **_kwargs) -> Datasource:
            return NoStrategyDatasource()

    @check(
        name="warning-when-no-strategies",
        service_labels={"test": "true"},
        environments=[ENV],
        cache_for=None,
    )
    def my_check(_ds: Annotated[Datasource, FromFactory(FactoryWithoutStrategies)]):
        from outpost.result import ok

        return ok("unused")

    from outpost.app import Outpost

    app = Outpost(
        checks=[my_check],
        execution_environment=ENV,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource_factory(FactoryWithoutStrategies)

    # Capture warnings emitted during datasource resolution
    caplog.set_level(logging.WARNING)

    with app.app_context():
        # Trigger resolution of strategies which resolves the datasource as well
        _ = app._resolve_scheduling_strategies(my_check)

    warnings = [rec for rec in caplog.records if rec.levelno == logging.WARNING]
    assert all(
        "The factory-created datasource has no scheduling strategies defined"
        not in rec.message
        for rec in warnings
    )
