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


class DatasourceUnavailable(Exception):
    pass


class Datasource(ABC):
    argument_name: str
    instance: Datasource
    initialize_on_startup: bool = True

    @classmethod
    def available_datasources(cls) -> set[type[Datasource]]:
        subclasses = set()
        queue = [cls]
        while queue:
            class_ = queue.pop()
            for subclass in class_.__subclasses__():
                subclasses.add(subclass)
                queue.append(subclass)

        return subclasses
