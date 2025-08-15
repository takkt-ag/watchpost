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

from unittest.mock import MagicMock

from outpost.check import Check
from outpost.environment import Environment
from outpost.result import build_result, ok


def _mk_outpost_mock() -> MagicMock:
    outpost = MagicMock()
    # Ensure non-strict so default fallback hostname is used and does not interfere
    outpost.hostname_strategy = None
    outpost._hostname_strict = False
    return outpost


def test_service_name_is_appended_with_name_suffix_for_simple_result():
    def check_func():
        return ok("OK", name_suffix=":db")

    check = Check(
        check_function=check_func,
        service_name="svc",
        service_labels={},
        environments=[Environment("env")],
        cache_for=None,
    )

    env = Environment("env")
    results = check.run(outpost=_mk_outpost_mock(), environment=env, datasources={})
    assert len(results) == 1
    assert results[0].service_name == "svc:db"


def test_service_name_is_appended_with_name_suffix_for_ongoing_result():
    def check_func():
        # No partial results; defaults to OK with provided name_suffix
        return build_result("OK", "FAIL", name_suffix=":part")

    check = Check(
        check_function=check_func,
        service_name="svc",
        service_labels={},
        environments=[Environment("env")],
        cache_for=None,
    )

    env = Environment("env")
    results = check.run(outpost=_mk_outpost_mock(), environment=env, datasources={})
    assert len(results) == 1
    assert results[0].service_name == "svc:part"


def test_multiple_results_have_individual_suffixes():
    def check_func():
        return [
            ok("a", name_suffix=":a"),
            ok("b", name_suffix=":b"),
        ]

    check = Check(
        check_function=check_func,
        service_name="svc",
        service_labels={},
        environments=[Environment("env")],
        cache_for=None,
    )

    env = Environment("env")
    results = check.run(outpost=_mk_outpost_mock(), environment=env, datasources={})
    assert len(results) == 2
    service_names = sorted(r.service_name for r in results)
    assert service_names == ["svc:a", "svc:b"]
