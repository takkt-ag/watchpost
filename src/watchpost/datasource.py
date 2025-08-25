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

from abc import ABC
from collections.abc import Callable
from types import EllipsisType
from typing import Any, ClassVar, Protocol, overload

from .scheduling_strategy import SchedulingStrategy


class DatasourceUnavailable(Exception):
    pass


class Datasource(ABC):
    scheduling_strategies: tuple[SchedulingStrategy, ...] | EllipsisType | None = ...


class DatasourceFactory(Protocol):
    new: ClassVar[Callable[..., Datasource]]
    scheduling_strategies: tuple[SchedulingStrategy, ...] | EllipsisType | None = ...


class FromFactory:
    @overload
    def __init__(
        self,
        factory: type[DatasourceFactory],
        *args: Any,
        **kwargs: Any,
    ): ...

    @overload
    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ): ...

    def __init__(
        self,
        factory: type[DatasourceFactory] | Any = None,
        *args: Any,
        **kwargs: Any,
    ):
        self.factory_type: type[DatasourceFactory] | None
        if factory is None or isinstance(factory, type):
            self.factory_type = factory
            self.args = args
        else:
            self.factory_type = None
            self.args = (factory, *args)

        self.kwargs = kwargs

    def cache_key(
        self,
        type_key: type[DatasourceFactory] | None,
    ) -> tuple[type[DatasourceFactory] | None, int, int]:
        return (
            self.factory_type or type_key,
            hash(frozenset(self.args)),
            hash(frozenset(self.kwargs.items())),
        )
