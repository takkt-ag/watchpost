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

from typing import Any

from .hostname import HostnameInput, to_strategy


class Environment:
    def __init__(
        self,
        name: str,
        *,
        hostname: HostnameInput | None = None,
    ):
        self.name = name
        self.hostname_strategy = to_strategy(hostname)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Environment):
            return (
                self.name == other.name
                and self.hostname_strategy == other.hostname_strategy
            )
        return False

    def __hash__(self) -> int:
        return hash(
            (
                self.name,
                self.hostname_strategy,
            )
        )
