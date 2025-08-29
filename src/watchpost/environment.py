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

from collections.abc import Hashable
from typing import Any

from .hostname import HostnameInput, to_strategy


class Environment:
    def __init__(
        self,
        name: str,
        *,
        hostname: HostnameInput | None = None,
        **metadata: Hashable,
    ):
        self.name = name
        self.hostname_strategy = to_strategy(hostname)
        self.metadata = metadata

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Environment):
            return (
                self.name == other.name
                and self.hostname_strategy == other.hostname_strategy
                and self.metadata == other.metadata
            )
        return False

    def __hash__(self) -> int:
        return hash(
            (
                self.name,
                self.hostname_strategy,
                frozenset(self.metadata.items()),
            )
        )


class EnvironmentRegistry:
    def __init__(self) -> None:
        self._environments: dict[str, Environment] = {}

    def __getitem__(self, name: str) -> Environment:
        return self._environments[name]

    def __contains__(self, name: str) -> bool:
        return name in self._environments

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self._environments.values())

    def __len__(self) -> int:
        return len(self._environments)

    def get(self, name: str, default: Environment | None = None) -> Environment | None:
        return self._environments.get(name, default)

    def new(
        self,
        name: str,
        *,
        hostname: HostnameInput | None = None,
        **metadata: Hashable,
    ) -> Environment:
        environment = Environment(
            name,
            hostname=hostname,
            **metadata,
        )
        self.add(environment)
        return environment

    def add(self, environment: Environment) -> None:
        self._environments[environment.name] = environment
