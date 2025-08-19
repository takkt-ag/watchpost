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

from enum import IntEnum
from functools import reduce
from typing import TYPE_CHECKING, Protocol, TypeVar, override

if TYPE_CHECKING:
    from .check import Check
    from .environment import Environment


from .globals import current_app


class InvalidCheckConfiguration(Exception):
    """
    Should be raised when a check's configuration is contradictory or otherwise
    invalid and cannot be scheduled anywhere until corrected.

    This can happen if, for example, two datasources with conflicting scheduling
    requirements are used in a single check.
    """

    def __init__(self, check: Check, reason: str, cause: Exception | None = None):
        self.check = check
        self.reason = reason
        self.cause = cause
        super().__init__(
            f"Invalid check configuration: {reason}\n\nAffected check: {check}"
        )


class SchedulingDecision(IntEnum):
    """
    Scheduling decision returned by a SchedulingStrategy.

    The value communicates what the executor should do with a given check in the
    current situation. Use this to express whether a check should be run now,
    temporarily skipped (reusing a previously cached result if available), or
    never be scheduled on this node.

    If you determine that the check's configuration is invalid and can never be
    scheduled until fixed, raise `InvalidCheckConfiguration` instead!

    Notes on semantics:
    - SCHEDULE: The check is eligible to run now.
    - SKIP: Conditions are temporarily unfavorable; the last known result will
      be reused if available. The strategy expects that conditions might change
      later (e.g., outside a maintenance window, or when a datasource becomes
      reachable again). If no cached result is available, the check will be
      marked as UNKNOWN.
    - DONT_SCHEDULE: This node/environment should never run the check (e.g., the
      check is pinned to another environment). Different from SKIP in that this
      is a permanent decision for the current environment, not reporting on the
      check at all.
    """

    SCHEDULE = 0
    """
    The check is eligible to run now.
    """

    SKIP = 1
    """
    The check should be skipped temporarily and any prior results, if available,
    should be reused.
    
    This usually means that the conditions are temporarily unfavorable; the last
    known result will be reused if available. The strategy expects that
    conditions might change later (e.g., outside a maintenance window, or when a
    datasource becomes reachable again). If no cached result is available, the
    check will be marked as UNKNOWN.
    """

    DONT_SCHEDULE = 2
    """
    The check should not run at all.
    
    This node/environment should never run the check (e.g., the check is pinned
    to another environment). Different from SKIP in that this is a permanent
    decision for the current environment, not reporting on the check at all.
    
    This primarily applies for checks that have to run from a certain
    environment, and the current environment is not suitable.
    """


class SchedulingStrategy(Protocol):
    """
    Protocol for pluggable scheduling logic.

    A scheduling strategy decides if and how a check should be executed in the
    current context. Implementations can consider the check definition, the
    environment that is attempting to execute the check (current execution
    environment), and the target environment the check is meant to observe.

    Typical concerns handled by a strategy include:
    - Aligning check execution with datasource availability windows
    - Respecting environment pinning (e.g., only run from a specific location)
    - Handling temporary outages by skipping while reusing previous results
    - Detecting invalid configurations that can never be scheduled
    """

    def schedule(
        self,
        check: Check,
        current_execution_environment: Environment,
        target_environment: Environment,
    ) -> SchedulingDecision:
        """
        Decide whether and how the given check should be executed.

        Parameters:
            check:
                The check definition whose execution is being considered.
            current_execution_environment:
                The environment that the check would execute within.
            target_environment:
                The environment that the check is intended to observe or act
                upon.

        Returns:
            A SchedulingDecision indicating whether to run the check now
            (SCHEDULE), skip temporarily and possibly reuse previous results
            (SKIP), or avoid running it from this environment entirely
            (DONT_SCHEDULE).

        Raises:
            InvalidCheckConfiguration:
                The check's configuration is contradictory or otherwise invalid
                and cannot be scheduled anywhere until corrected.
        """
        ...


class MustRunInGivenExecutionEnvironmentStrategy(SchedulingStrategy):
    def __init__(self, *environments: Environment):
        self.supported_execution_environments = set(environments)

    @override
    def schedule(
        self,
        check: Check,
        current_execution_environment: Environment,
        target_environment: Environment,
    ) -> SchedulingDecision:
        if current_execution_environment in self.supported_execution_environments:
            return SchedulingDecision.SCHEDULE
        return SchedulingDecision.DONT_SCHEDULE


class MustRunInTargetEnvironmentStrategy(SchedulingStrategy):
    @override
    def schedule(
        self,
        check: Check,
        current_execution_environment: Environment,
        target_environment: Environment,
    ) -> SchedulingDecision:
        if current_execution_environment == target_environment:
            return SchedulingDecision.SCHEDULE
        return SchedulingDecision.DONT_SCHEDULE


class MustRunAgainstGivenTargetEnvironmentStrategy(SchedulingStrategy):
    def __init__(self, *environments: Environment):
        self.supported_target_environments = set(environments)

    @override
    def schedule(
        self,
        check: Check,
        current_execution_environment: Environment,
        target_environment: Environment,
    ) -> SchedulingDecision:
        if target_environment in self.supported_target_environments:
            return SchedulingDecision.SCHEDULE
        return SchedulingDecision.DONT_SCHEDULE


_S = TypeVar("_S", bound=SchedulingStrategy)


class DetectImpossibleCombinationStrategy(SchedulingStrategy):
    @staticmethod
    def _filter_strategies(
        strategies: list[SchedulingStrategy],
        strategy_type: type[_S],
    ) -> list[_S]:
        return [
            strategy for strategy in strategies if isinstance(strategy, strategy_type)
        ]

    @override
    def schedule(
        self,
        check: Check,
        current_execution_environment: Environment,
        target_environment: Environment,
    ) -> SchedulingDecision:
        strategies = current_app._resolve_scheduling_strategies(check)

        must_run_in_given_execution_environment_strategies = self._filter_strategies(
            strategies,
            MustRunInGivenExecutionEnvironmentStrategy,
        )
        must_run_against_given_target_environment_strategies = self._filter_strategies(
            strategies,
            MustRunAgainstGivenTargetEnvironmentStrategy,
        )
        must_run_in_current_execution_environment = bool(
            self._filter_strategies(
                strategies,
                MustRunInTargetEnvironmentStrategy,
            )
        )

        # Verify that execution-environment constraints across all strategies are compatible.
        #
        # We aggregate all MustRunInGivenExecutionEnvironmentStrategy constraints and
        # require their intersection to be non-empty. If it's empty, there is no single
        # environment from which the check could legally run.
        # Example: DS A requires Monitoring, DS B requires Preprod -> impossible.
        overlapping_execution_environments: set[Environment] | None = None
        if must_run_in_given_execution_environment_strategies:
            overlapping_execution_environments = reduce(
                set.intersection,
                (
                    strategy.supported_execution_environments
                    for strategy in must_run_in_given_execution_environment_strategies
                ),
            )
            if not overlapping_execution_environments:
                raise InvalidCheckConfiguration(
                    check,
                    "Conflicting execution-environment constraints: no common execution environment across MustRunInGivenExecutionEnvironment strategies (e.g., one datasource requires 'Monitoring' while another requires 'Preprod').",
                )

        # Verify that target-environment constraints match the check's declared targets.
        #
        # We aggregate all MustRunAgainstGivenTargetEnvironmentStrategy constraints and
        # compare their intersection to the set of environments declared in the @check
        # decorator. They must be identical; otherwise the check declares to target
        # environments that not all strategies support.
        # Example: Check targets [Monitoring, Preprod] but one datasource only allows [Preprod].
        overlapping_target_environments: set[Environment] | None = None
        if must_run_against_given_target_environment_strategies:
            overlapping_target_environments = reduce(
                set.intersection,
                (
                    strategy.supported_target_environments
                    for strategy in must_run_against_given_target_environment_strategies
                ),
            )
            if not overlapping_target_environments.issuperset(set(check.environments)):
                raise InvalidCheckConfiguration(
                    check,
                    "Target-environment constraints conflict with the check's declared environments: the @check(..., environments=[...]) set must be a subset of the intersection across MustRunAgainstGivenTargetEnvironment strategies.",
                )

        # If the check must run in the current execution environment, then the set of
        # allowed execution environments and the set of allowed target environments must
        # overlap — because current_execution_environment == target_environment at runtime.
        # If there's no overlap, the check can never be scheduled anywhere.
        # Example: execution must be Monitoring, target must be Preprod -> impossible.
        if must_run_in_current_execution_environment:
            if overlapping_execution_environments and overlapping_target_environments:
                if not overlapping_execution_environments.intersection(
                    overlapping_target_environments
                ):
                    raise InvalidCheckConfiguration(
                        check,
                        "Current=Target requirement cannot be satisfied: allowed execution environments and allowed target environments have no overlap (e.g., execution must be 'Monitoring' while target must be 'Preprod').",
                    )

        return SchedulingDecision.SCHEDULE
