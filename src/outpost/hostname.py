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

import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:  # pragma: no cover - only for type checking
    from .app import Outpost
    from .check import Check
    from .environment import Environment
    from .result import CheckResult


@dataclass(frozen=True)
class HostnameContext:
    check: Check
    environment: Environment
    result: CheckResult | None = None

    # Convenience properties for templates/functions
    @property
    def service_name(self) -> str:
        return self.check.service_name

    @property
    def service_labels(self) -> dict[str, Any]:
        return self.check.service_labels


class HostnameStrategy(Protocol):
    def resolve(self, ctx: HostnameContext) -> str | None: ...


class StaticHostnameStrategy:
    def __init__(self, hostname: str):
        self.hostname = hostname

    def resolve(self, ctx: HostnameContext) -> str | None:  # noqa: ARG002 - ctx unused
        return self.hostname


class FunctionStrategy:
    def __init__(self, fn: Callable[[HostnameContext], str | None]):
        self.fn = fn

    def resolve(self, ctx: HostnameContext) -> str | None:
        return self.fn(ctx)


class TemplateStrategy:
    def __init__(self, template: str):
        self.template = template

    def resolve(self, ctx: HostnameContext) -> str | None:
        # Provide both object-level and convenience keys
        try:
            return self.template.format(**asdict(ctx))
        except Exception:
            # KeyError/AttributeError or formatting issues -> no decision
            return None


class CompositeStrategy:
    def __init__(self, *strategies: HostnameStrategy):
        self.strategies = strategies

    def resolve(self, ctx: HostnameContext) -> str | None:
        for s in self.strategies:
            val = s.resolve(ctx)
            if val:
                return val
        return None


RFC1123 = re.compile(r"^(?=.{1,253}$)(?!-)[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*$")


class ValidatingStrategy:
    def __init__(self, inner: HostnameStrategy, *, regex: re.Pattern[str] = RFC1123):
        self.inner = inner
        self.regex = regex

    def resolve(self, ctx: HostnameContext) -> str | None:
        val = self.inner.resolve(ctx)
        if val is None:
            return None
        return val if self.regex.match(val) else None


HostnameInput = str | Callable[[HostnameContext], str | None] | HostnameStrategy


class HostnameResolutionError(Exception):
    pass


def to_strategy(value: HostnameInput | None) -> HostnameStrategy | None:
    if value is None:
        return None
    if isinstance(value, str):
        return TemplateStrategy(value)
    if hasattr(value, "resolve"):
        # Assume it matches the protocol
        return value
    # Validate callable input explicitly
    if callable(value):
        return FunctionStrategy(value)
    raise TypeError(
        f"Unsupported hostname input type: {type(value)!r}. Expected str, callable, or HostnameStrategy."
    )


def resolve_hostname(
    *,
    outpost: Outpost,
    environment: Environment,
    check: Check,
    result: CheckResult,
    strict: bool = True,
    final_fallback: HostnameStrategy | None = None,
) -> str:
    # 1) Per-result override
    try:
        if result.hostname:
            return result.hostname
    except Exception:
        pass

    ctx = HostnameContext(check=check, environment=environment, result=result)

    # 2) Check-level strategy
    if check.hostname_strategy:
        try:
            val = check.hostname_strategy.resolve(ctx)
            if val:
                return val
        except Exception as e:
            raise HostnameResolutionError(
                f"Hostname strategy failed at check level for {check.service_name}/{environment.name}: {e}"
            ) from e

    # 3) Environment-level strategy
    if environment.hostname_strategy:
        try:
            val = environment.hostname_strategy.resolve(ctx)
            if val:
                return val
        except Exception as e:
            raise HostnameResolutionError(
                f"Hostname strategy failed at environment level for {check.service_name}/{environment.name}: {e}"
            ) from e

    # 4) Outpost-level strategy
    if outpost.hostname_strategy:
        try:
            val = outpost.hostname_strategy.resolve(ctx)
            if val:
                return val
        except Exception as e:
            raise HostnameResolutionError(
                f"Hostname strategy failed at outpost level for {check.service_name}/{environment.name}: {e}"
            ) from e

    # 5) Final fallback or error
    if final_fallback:
        val = final_fallback.resolve(ctx)
        if val:
            return val

    if strict:
        raise HostnameResolutionError(
            f"No hostname could be resolved for check={check.service_name} env={environment.name}"
        )

    # Non-strict default fallback
    default_fallback = TemplateStrategy("{service_name}-{environment.name}")
    val = default_fallback.resolve(ctx)
    return val or f"{check.service_name}-{environment.name}"
