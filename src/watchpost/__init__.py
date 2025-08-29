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

from .app import Watchpost
from .cache import Cache, ChainedStorage, DiskStorage, InMemoryStorage, RedisStorage
from .check import CheckFunctionResult, check
from .datasource import (
    Datasource,
    DatasourceFactory,
    DatasourceUnavailable,
    FromFactory,
)
from .environment import Environment
from .globals import current_app
from .result import (
    CheckResult,
    Metric,
    Thresholds,
    build_result,
    crit,
    ok,
    unknown,
    warn,
)
from .scheduling_strategy import (
    MustRunAgainstGivenTargetEnvironmentStrategy,
    MustRunInGivenExecutionEnvironmentStrategy,
    MustRunInTargetEnvironmentStrategy,
)

__all__ = [
    "Cache",
    "ChainedStorage",
    "CheckFunctionResult",
    "CheckResult",
    "Datasource",
    "DatasourceFactory",
    "DatasourceUnavailable",
    "DiskStorage",
    "Environment",
    "FromFactory",
    "InMemoryStorage",
    "Metric",
    "MustRunAgainstGivenTargetEnvironmentStrategy",
    "MustRunInGivenExecutionEnvironmentStrategy",
    "MustRunInTargetEnvironmentStrategy",
    "RedisStorage",
    "Thresholds",
    "Watchpost",
    "build_result",
    "check",
    "crit",
    "current_app",
    "ok",
    "unknown",
    "warn",
]
