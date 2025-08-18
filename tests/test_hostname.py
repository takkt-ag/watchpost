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

from typing import override
from unittest.mock import MagicMock

import pytest

from outpost.app import Outpost
from outpost.check import check
from outpost.datasource import Datasource
from outpost.environment import Environment
from outpost.hostname import (
    CompositeStrategy,
    FunctionStrategy,
    HostnameContext,
    HostnameResolutionError,
    HostnameStrategy,
    StaticHostnameStrategy,
    TemplateStrategy,
    ValidatingStrategy,
    resolve_hostname,
    to_strategy,
)
from outpost.result import ok, warn

from .utils import BlockingCheckExecutor


class TestDatasource(Datasource):
    pass


def _mk_outpost(*, hostname: str | None = None, strict: bool = False) -> Outpost:
    return Outpost(
        checks=[],
        execution_environment=Environment("exec-env"),
        executor=BlockingCheckExecutor(),
        hostname=hostname,
        hostname_strict=strict,
    )


def test_precedence_result_overrides_check_env_outpost():
    env = Environment("prod", hostname="env-{environment.name}")

    @check(
        name="svc",
        service_labels={},
        environments=[env],
        cache_for=None,
        hostname="check-host",
    )
    def my_check(test: TestDatasource):
        _ = test
        return ok("x", alternative_hostname="result-host")

    app = _mk_outpost(hostname="op-{service_name}")

    results = my_check.run_sync(
        outpost=app,
        environment=env,
        datasources={"test": TestDatasource()},
    )

    assert results[0].piggyback_host == "result-host"


def test_precedence_check_over_env_and_outpost():
    env = Environment("prod", hostname="env-{environment.name}")

    @check(
        name="svc",
        service_labels={},
        environments=[env],
        cache_for=None,
        hostname="check-host",
    )
    def my_check(test: TestDatasource):
        _ = test
        return ok("x")

    app = _mk_outpost(hostname="op-{service_name}")
    results = my_check.run_sync(
        outpost=app,
        environment=env,
        datasources={"test": TestDatasource()},
    )

    assert results[0].piggyback_host == "check-host"


def test_precedence_env_over_outpost():
    env = Environment("prod", hostname="env-{environment.name}")

    @check(
        name="svc",
        service_labels={},
        environments=[env],
        cache_for=None,
    )
    def my_check(test: TestDatasource):
        _ = test
        return ok("x")

    app = _mk_outpost(hostname="op-{service_name}")
    results = my_check.run_sync(
        outpost=app,
        environment=env,
        datasources={"test": TestDatasource()},
    )

    assert results[0].piggyback_host == "env-prod"


def test_outpost_level_strategy_used_when_no_others():
    env = Environment("prod")

    @check(
        name="svc",
        service_labels={},
        environments=[env],
        cache_for=None,
    )
    def my_check(test: TestDatasource):
        _ = test
        return ok("x")

    app = _mk_outpost(hostname="{service_name}-{environment.name}")
    results = my_check.run_sync(
        outpost=app,
        environment=env,
        datasources={"test": TestDatasource()},
    )

    assert results[0].piggyback_host == "svc-prod"


def test_template_fields_available():
    env = Environment("prod")

    @check(
        name="svc",
        service_labels={"team": "x"},
        environments=[env],
        cache_for=None,
        hostname="{service_name}-{environment.name}",
    )
    def my_check(test: TestDatasource):
        _ = test
        return ok("x")

    app = _mk_outpost()
    results = my_check.run_sync(
        outpost=app,
        environment=env,
        datasources={"test": TestDatasource()},
    )

    assert results[0].piggyback_host == "svc-prod"


def test_multi_result_per_result_overrides():
    env = Environment("e1")

    @check(
        name="svc",
        service_labels={},
        environments=[env],
        cache_for=None,
    )
    def my_check(test: TestDatasource):
        _ = test
        return [
            warn("w1", alternative_hostname="h1"),
            warn("w2", alternative_hostname="h2"),
        ]

    app = _mk_outpost()
    results = my_check.run_sync(
        outpost=app,
        environment=env,
        datasources={"test": TestDatasource()},
    )

    hosts = [r.piggyback_host for r in results]
    assert hosts == ["h1", "h2"]


def test_validation_and_strict_mode_raises_when_unresolved():
    env = Environment("prod")

    # Strategy always returns an invalid hostname
    class AlwaysInvalid(HostnameStrategy):
        @override
        def resolve(self, ctx: HostnameContext) -> str | None:
            return "invalid_host!"  # invalid char '!'

    @check(
        name="svc",
        service_labels={},
        environments=[env],
        cache_for=None,
        hostname=ValidatingStrategy(AlwaysInvalid()),
    )
    def my_check(test: TestDatasource):
        _ = test
        return ok("x")

    app = _mk_outpost(strict=True)

    with pytest.raises(HostnameResolutionError):
        _ = my_check.run_sync(
            outpost=app,
            environment=env,
            datasources={"test": TestDatasource()},
        )


def test_non_strict_falls_back_when_unresolved():
    env = Environment("prod")

    class NoneStrategy(HostnameStrategy):
        @override
        def resolve(self, ctx: HostnameContext) -> str | None:
            return None

    @check(
        name="svc",
        service_labels={},
        environments=[env],
        cache_for=None,
        hostname=NoneStrategy(),
    )
    def my_check(test: TestDatasource):
        _ = test
        return ok("x")

    app = _mk_outpost(strict=False)
    results = my_check.run_sync(
        outpost=app,
        environment=env,
        datasources={"test": TestDatasource()},
    )

    # Default non-strict fallback
    assert results[0].piggyback_host == "svc-prod"


def test_exception_in_strategy_is_wrapped():
    env = Environment("prod")

    class Exploding(HostnameStrategy):
        @override
        def resolve(self, ctx: HostnameContext) -> str | None:
            raise RuntimeError("boom")

    @check(
        name="svc",
        service_labels={},
        environments=[env],
        cache_for=None,
        hostname=Exploding(),
    )
    def my_check(test: TestDatasource):
        _ = test
        return ok("x")

    app = _mk_outpost(strict=False)
    with pytest.raises(HostnameResolutionError) as ei:
        _ = my_check.run_sync(
            outpost=app,
            environment=env,
            datasources={"test": TestDatasource()},
        )

    assert "check level" in str(ei.value)


def test_static_hostname_strategy_returns_value():
    fake_check = MagicMock()
    fake_env = Environment("e1")
    ctx = HostnameContext(check=fake_check, environment=fake_env)

    strat = StaticHostnameStrategy("static-host")
    assert strat.resolve(ctx) == "static-host"


def test_function_strategy_uses_callable():
    fake_check = MagicMock()
    fake_check.service_name = "svc"
    fake_check.service_labels = {"team": "x"}
    fake_env = Environment("e1")
    ctx = HostnameContext(check=fake_check, environment=fake_env)

    def fn(c: HostnameContext) -> str:
        return f"{c.service_name}-{c.environment.name}"

    strat = FunctionStrategy(fn)
    assert strat.resolve(ctx) == "svc-e1"


def test_to_strategy_with_callable_and_invalid_input():
    fake_check = MagicMock()
    fake_check.service_name = "svc"
    fake_check.service_labels = {}
    fake_env = Environment("env")
    ctx = HostnameContext(check=fake_check, environment=fake_env)

    # Callable -> FunctionStrategy behavior
    s = to_strategy(lambda c: f"{c.service_name}-{c.environment.name}")
    assert s is not None
    assert s.resolve(ctx) == "svc-env"

    # Invalid input type -> TypeError
    with pytest.raises(TypeError):
        _ = to_strategy(123)  # type: ignore[arg-type]


def test_template_strategy_format_failure_returns_none():
    fake_check = MagicMock()
    fake_env = Environment("e1")
    ctx = HostnameContext(check=fake_check, environment=fake_env)

    # Refers to a missing key; should return None
    tpl = TemplateStrategy("{nonexistent}")
    assert tpl.resolve(ctx) is None


def test_composite_strategy_resolution_order():
    fake_check = MagicMock()
    fake_check.service_name = "svc"
    fake_env = Environment("e1")
    ctx = HostnameContext(check=fake_check, environment=fake_env)

    none_first = FunctionStrategy(lambda _: None)
    second = StaticHostnameStrategy("second")
    comp = CompositeStrategy(none_first, second)
    assert comp.resolve(ctx) == "second"

    first = StaticHostnameStrategy("first")
    comp2 = CompositeStrategy(first, second)
    assert comp2.resolve(ctx) == "first"


def test_resolve_hostname_uses_final_fallback_when_strict_and_none_other():
    env = Environment("prod")

    @check(
        name="svc",
        service_labels={},
        environments=[env],
        cache_for=None,
    )
    def my_check():
        return ok("x")

    app = Outpost(
        checks=[],
        execution_environment=Environment("exec-env"),
        executor=BlockingCheckExecutor(),
        hostname_strict=True,
    )

    result = ok("x")
    ctx_check = my_check  # type: ignore[assignment]
    # Ensure check has no hostname strategy
    assert ctx_check.hostname_strategy is None

    fb = StaticHostnameStrategy("fallback-host")
    resolved = resolve_hostname(
        outpost=app,
        environment=env,
        check=ctx_check,
        result=result,
        strict=True,
        final_fallback=fb,
    )
    assert resolved == "fallback-host"
