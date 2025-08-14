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

import pytest

from outpost.app import Outpost
from outpost.check import check
from outpost.datasource import Datasource
from outpost.environment import Environment
from outpost.result import CheckResult
from outpost.scheduling_strategy import (
    InvalidCheckConfiguration,
    MustRunAgainstGivenTargetEnvironmentStrategy,
    MustRunInCurrentExecutionEnvironmentStrategy,
    MustRunInGivenExecutionEnvironmentStrategy,
)

from .utils import BlockingCheckExecutor

Monitoring = Environment("Monitoring")
Preprod = Environment("Preprod")


class LogSystem(Datasource):
    scheduling_strategies = (
        MustRunInGivenExecutionEnvironmentStrategy(Monitoring),
        MustRunAgainstGivenTargetEnvironmentStrategy(Monitoring, Preprod),
    )


class ProductService(Datasource):
    scheduling_strategies = (
        MustRunInCurrentExecutionEnvironmentStrategy(),
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

    app = Outpost(
        checks=[invalid_combination],
        execution_environment=Monitoring,
        executor=BlockingCheckExecutor(),
    )
    app.register_datasource(LogSystem)
    app.register_datasource(ProductService)
    with pytest.raises(ExceptionGroup) as exc_info:
        app._verify_check_scheduling()

    assert isinstance(exc_info.value, ExceptionGroup)
    exception_group: ExceptionGroup = exc_info.value
    assert len(exception_group.exceptions) == 1
    icc_exception = exception_group.exceptions[0]
    assert isinstance(icc_exception, InvalidCheckConfiguration)
    assert icc_exception.check == invalid_combination
    assert (
        icc_exception.reason
        == "The target environments are not supported by any of the given strategies."
    )
