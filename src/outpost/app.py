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

from collections.abc import Callable, Generator
from contextlib import contextmanager

from .globals import _cv


class Outpost:
    def __init__(
        self,
        *,
        checks: list[Callable],
    ):
        self._checks = checks

    @contextmanager
    def app_context(self) -> Generator[Outpost]:
        _token = _cv.set(self)
        try:
            yield self
        finally:
            _cv.reset(_token)

    def run_checks_once(self):
        with self.app_context():
            for check in self._checks:
                check()
