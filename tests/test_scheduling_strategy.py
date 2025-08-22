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

from typing import override

import pytest

from watchpost.app import Watchpost
from watchpost.check import check
from watchpost.datasource import Datasource
from watchpost.environment import Environment
from watchpost.executor import BlockingCheckExecutor
from watchpost.result import CheckResult
from watchpost.scheduling_strategy import (
    DetectImpossibleCombinationStrategy,
    InvalidCheckConfiguration,
    MustRunAgainstGivenTargetEnvironmentStrategy,
    MustRunInGivenExecutionEnvironmentStrategy,
    MustRunInTargetEnvironmentStrategy,
    SchedulingDecision,
    SchedulingStrategy,
)

Monitoring = Environment("Monitoring")
Preprod = Environment("Preprod")


class LogSystem(Datasource):
    scheduling_strategies = (
        MustRunInGivenExecutionEnvironmentStrategy(Monitoring),
        MustRunAgainstGivenTargetEnvironmentStrategy(Monitoring, Preprod),
    )


class ProductService(Datasource):
    scheduling_strategies = (
        MustRunInTargetEnvironmentStrategy(),
        MustRunAgainstGivenTargetEnvironmentStrategy(Preprod),
    )


def test_invalid_combination():
    @check(
        name="Invalid combination",
        service_labels={"test": "true"},
        environments=[Monitoring],
        cache_for=None,
    )
    def invalid_combination(
        log_system: LogSystem,
        product_service: ProductService,
    ) -> CheckResult:
        raise ValueError(
            f"This check should never run! {log_system=}, {product_service=}"
        )

    app = Watchpost(
        checks=[invalid_combination],
        execution_environment=Monitoring,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(LogSystem)
    app.register_datasource(ProductService)
    with pytest.raises(ExceptionGroup) as exc_info:
        app.verify_check_scheduling()

    assert isinstance(exc_info.value, ExceptionGroup)
    exception_group: ExceptionGroup = exc_info.value
    assert len(exception_group.exceptions) == 1
    icc_exception = exception_group.exceptions[0]
    assert isinstance(icc_exception, InvalidCheckConfiguration)
    assert icc_exception.check == invalid_combination
    assert "Target-environment constraints conflict" in icc_exception.reason


def test_decision_must_run_in_current_execution_environment():
    @check(
        name="Product check",
        service_labels={"test": "true"},
        environments=[Preprod],
        cache_for=None,
    )
    def product_check(_ds: ProductService) -> CheckResult:
        raise AssertionError("Should not be executed in this decision test")

    # Executing from Preprod against Preprod: allowed
    app1 = Watchpost(
        checks=[product_check],
        execution_environment=Preprod,
        executor=BlockingCheckExecutor(),
    )
    app1.register_datasource(ProductService)
    with app1.app_context():
        decision1 = app1._resolve_check_scheduling_decision(product_check, Preprod)
    assert decision1 == SchedulingDecision.SCHEDULE

    # Executing from Monitoring against Preprod: not allowed
    app2 = Watchpost(
        checks=[product_check],
        execution_environment=Monitoring,
        executor=BlockingCheckExecutor(),
    )
    app2.register_datasource(ProductService)
    with app2.app_context():
        decision2 = app2._resolve_check_scheduling_decision(product_check, Preprod)
    assert decision2 == SchedulingDecision.DONT_SCHEDULE


def test_decision_must_run_in_given_execution_environment():
    class MonitoringOnlyDatasource(Datasource):
        scheduling_strategies = (
            MustRunInGivenExecutionEnvironmentStrategy(Monitoring),
            MustRunAgainstGivenTargetEnvironmentStrategy(Monitoring),
        )

    @check(
        name="Monitoring-only",
        service_labels={"test": "true"},
        environments=[Monitoring],
        cache_for=None,
    )
    def monitoring_check(_ds: MonitoringOnlyDatasource) -> CheckResult:
        raise AssertionError("Should not be executed in this decision test")

    # Executing from Preprod against Monitoring: not allowed
    app1 = Watchpost(
        checks=[monitoring_check],
        execution_environment=Preprod,
        executor=BlockingCheckExecutor(),
    )
    app1.register_datasource(MonitoringOnlyDatasource)
    with app1.app_context():
        decision1 = app1._resolve_check_scheduling_decision(
            monitoring_check, Monitoring
        )
    assert decision1 == SchedulingDecision.DONT_SCHEDULE

    # Executing from Monitoring against Monitoring: allowed
    app2 = Watchpost(
        checks=[monitoring_check],
        execution_environment=Monitoring,
        executor=BlockingCheckExecutor(),
    )
    app2.register_datasource(MonitoringOnlyDatasource)
    with app2.app_context():
        decision2 = app2._resolve_check_scheduling_decision(
            monitoring_check, Monitoring
        )
    assert decision2 == SchedulingDecision.SCHEDULE


def test_invalid_disjoint_execution_environments():
    class MustRunExecMonitoring(Datasource):
        scheduling_strategies = (
            MustRunInGivenExecutionEnvironmentStrategy(Monitoring),
        )

    class MustRunExecPreprod(Datasource):
        scheduling_strategies = (MustRunInGivenExecutionEnvironmentStrategy(Preprod),)

    @check(
        name="Disjoint exec envs",
        service_labels={"test": "true"},
        environments=[Monitoring],
        cache_for=None,
    )
    def disjoint_exec_envs(
        _ds1: MustRunExecMonitoring,
        _ds2: MustRunExecPreprod,
    ) -> CheckResult:
        raise AssertionError("Should not be executed in this verification test")

    app = Watchpost(
        checks=[disjoint_exec_envs],
        execution_environment=Monitoring,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(MustRunExecMonitoring)
    app.register_datasource(MustRunExecPreprod)

    with pytest.raises(ExceptionGroup) as exc_info:
        app.verify_check_scheduling()

    assert isinstance(exc_info.value, ExceptionGroup)
    exception_group: ExceptionGroup = exc_info.value
    assert len(exception_group.exceptions) == 1
    icc_exception = exception_group.exceptions[0]
    assert isinstance(icc_exception, InvalidCheckConfiguration)
    assert "Conflicting execution-environment constraints" in icc_exception.reason


def test_skip_decision_is_selected_over_schedule():
    class AlwaysSkipStrategy(SchedulingStrategy):
        @override
        def schedule(self, check, current_execution_environment, target_environment):
            return SchedulingDecision.SKIP

    class UnstableDatasource(Datasource):
        scheduling_strategies = (
            AlwaysSkipStrategy(),
            MustRunAgainstGivenTargetEnvironmentStrategy(Monitoring),
        )

    @check(
        name="Skip preferred",
        service_labels={"test": "true"},
        environments=[Monitoring],
        cache_for=None,
    )
    def skip_check(_ds: UnstableDatasource) -> CheckResult:
        raise AssertionError("Should not be executed in this decision test")

    app = Watchpost(
        checks=[skip_check],
        execution_environment=Monitoring,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(UnstableDatasource)

    with app.app_context():
        decision = app._resolve_check_scheduling_decision(skip_check, Monitoring)
    assert decision == SchedulingDecision.SKIP


def test_aggregation_app_and_datasource_strategies():
    class NeedsPreprodExec(Datasource):
        scheduling_strategies = (MustRunInGivenExecutionEnvironmentStrategy(Preprod),)

    @check(
        name="Aggregated",
        service_labels={"test": "true"},
        environments=[Monitoring],
        cache_for=None,
    )
    def aggregated_check(_ds: NeedsPreprodExec) -> CheckResult:
        raise AssertionError("Should not be executed in this decision test")

    # App-level strategy constrains target env to Monitoring
    app1 = Watchpost(
        checks=[aggregated_check],
        execution_environment=Monitoring,
        executor=BlockingCheckExecutor(),
        default_scheduling_strategies=[
            MustRunAgainstGivenTargetEnvironmentStrategy(Monitoring),
        ],
    )
    app1.register_datasource(NeedsPreprodExec)

    # Even though target env matches (Monitoring), execution env (Monitoring) doesn't
    # satisfy datasource requirement (Preprod), thus DONT_SCHEDULE dominates
    with app1.app_context():
        decision1 = app1._resolve_check_scheduling_decision(
            aggregated_check, Monitoring
        )
    assert decision1 == SchedulingDecision.DONT_SCHEDULE

    # If we change execution environment to Preprod, both constraints are satisfied
    app2 = Watchpost(
        checks=[aggregated_check],
        execution_environment=Preprod,
        executor=BlockingCheckExecutor(),
        default_scheduling_strategies=[
            MustRunAgainstGivenTargetEnvironmentStrategy(Monitoring),
            DetectImpossibleCombinationStrategy(),
        ],
    )
    app2.register_datasource(NeedsPreprodExec)
    with app2.app_context():
        decision2 = app2._resolve_check_scheduling_decision(
            aggregated_check, Monitoring
        )
    assert decision2 == SchedulingDecision.SCHEDULE


def test_target_env_subset_allowed():
    class WideTargetDatasource(Datasource):
        scheduling_strategies = (
            MustRunInGivenExecutionEnvironmentStrategy(Preprod),
            MustRunAgainstGivenTargetEnvironmentStrategy(Monitoring, Preprod),
        )

    @check(
        name="Subset target",
        service_labels={"test": "true"},
        environments=[Preprod],
        cache_for=None,
    )
    def subset_target_check(_ds: WideTargetDatasource) -> CheckResult:
        raise AssertionError("Should not be executed in this verification test")

    app = Watchpost(
        checks=[subset_target_check],
        execution_environment=Preprod,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(WideTargetDatasource)

    # With strategies supporting a superset of target environments, this should be valid
    app.verify_check_scheduling()


def test_invalid_exec_and_target_envs_without_intersection_but_current_required():
    class ExecMonitoring(Datasource):
        scheduling_strategies = (
            MustRunInGivenExecutionEnvironmentStrategy(Monitoring),
        )

    class TargetPreprodAndCurrent(Datasource):
        scheduling_strategies = (
            MustRunInTargetEnvironmentStrategy(),
            MustRunAgainstGivenTargetEnvironmentStrategy(Preprod),
        )

    @check(
        name="Exec/Target mismatch with current requirement",
        service_labels={"test": "true"},
        environments=[Preprod],
        cache_for=None,
    )
    def impossible_combo(
        _ds1: ExecMonitoring,
        _ds2: TargetPreprodAndCurrent,
    ) -> CheckResult:
        raise AssertionError("Should not be executed in this verification test")

    app = Watchpost(
        checks=[impossible_combo],
        execution_environment=Monitoring,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(ExecMonitoring)
    app.register_datasource(TargetPreprodAndCurrent)

    # This should trigger the branch where both overlapping execution and target sets
    # exist, MustRunInCurrentExecutionEnvironmentStrategy is present, but they don't
    # intersect, thus raising InvalidCheckConfiguration with the specific reason.
    with pytest.raises(ExceptionGroup) as exc_info:
        app.verify_check_scheduling()

    assert isinstance(exc_info.value, ExceptionGroup)
    exception_group: ExceptionGroup = exc_info.value
    assert len(exception_group.exceptions) == 1
    icc_exception = exception_group.exceptions[0]
    assert isinstance(icc_exception, InvalidCheckConfiguration)
    assert "Current=Target requirement cannot be satisfied" in icc_exception.reason
