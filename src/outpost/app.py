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

import sys
from collections.abc import Generator
from contextlib import contextmanager

from .check import Check
from .globals import _cv


class Outpost:
    def __init__(
        self,
        *,
        checks: list[Check],
    ):
        self._checks = checks

    @contextmanager
    def app_context(self) -> Generator[Outpost]:
        _token = _cv.set(self)
        try:
            yield self
        finally:
            _cv.reset(_token)

    def run_checks_once(self) -> None:
        with self.app_context():
            for check in self._checks:
                execution_results = check.run()
                for execution_result in execution_results:
                    for chunk in execution_result.generate_checkmk_output():
                        sys.stdout.buffer.write(chunk)
