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
import unicodedata
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Protocol, override

if TYPE_CHECKING:
    from .app import Outpost
    from .check import Check
    from .environment import Environment
    from .result import CheckResult


@dataclass(frozen=True)
class HostnameContext:
    check: Check
    environment: Environment
    service_name: str
    service_labels: dict[str, Any]
    result: CheckResult | None = None

    @classmethod
    def new(
        cls,
        *,
        check: Check,
        environment: Environment,
        result: CheckResult | None = None,
        service_name: str | None = None,
        service_labels: dict[str, Any] | None = None,
    ) -> HostnameContext:
        return cls(
            check=check,
            environment=environment,
            result=result,
            service_name=service_name or check.service_name,
            service_labels=service_labels or check.service_labels,
        )


class HostnameStrategy(Protocol):
    def resolve(self, ctx: HostnameContext) -> str | None: ...


class StaticHostnameStrategy(HostnameStrategy):
    def __init__(self, hostname: str):
        self.hostname = hostname

    @override
    def resolve(self, ctx: HostnameContext) -> str | None:
        return self.hostname


class FunctionStrategy(HostnameStrategy):
    def __init__(self, fn: Callable[[HostnameContext], str | None]):
        self.fn = fn

    @override
    def resolve(self, ctx: HostnameContext) -> str | None:
        return self.fn(ctx)


class TemplateStrategy(HostnameStrategy):
    def __init__(self, template: str):
        self.template = template

    @override
    def resolve(self, ctx: HostnameContext) -> str | None:
        return self.template.format(**asdict(ctx))


class CompositeStrategy(HostnameStrategy):
    def __init__(self, *strategies: HostnameStrategy):
        self.strategies = strategies

    @override
    def resolve(self, ctx: HostnameContext) -> str | None:
        for s in self.strategies:
            val = s.resolve(ctx)
            if val:
                return val
        return None


LABEL_MAX = 63
HOSTNAME_MAX = 253


def is_rfc1123_hostname(value: str) -> bool:
    if not value or len(value) > HOSTNAME_MAX:
        return False

    # Only ASCII letters, digits, hyphens and dots overall
    for ch in value:
        o = ord(ch)
        if not (97 <= o <= 122 or 48 <= o <= 57 or ch in {".", "-"} or 65 <= o <= 90):
            return False

    # Check labels
    parts = value.split(".")
    for label in parts:
        if not (1 <= len(label) <= LABEL_MAX):
            return False
        # Must start/end with alnum
        if not (label[0].isalnum() and label[-1].isalnum()):
            return False
        # Only alnum or hyphen in label
        for ch in label:
            if not (ch.isalnum() or ch == "-"):
                return False
    return True


_invalid_char_re = re.compile(r"[^a-z0-9.-]+")  # after lowercasing
_multi_dot_re = re.compile(r"\.+")
_multi_dash_re = re.compile(r"-+")


def coerce_to_rfc1123(value: str) -> str:
    if not value:
        raise ValueError("Cannot coerce empty hostname")

    # Normalize Unicode to ASCII (drop unsupported)
    norm = unicodedata.normalize("NFKD", value)
    s = norm.encode("ascii", "ignore").decode("ascii").lower()

    # Replace invalid chars, normalize repeated separators
    s = _invalid_char_re.sub("-", s)
    s = _multi_dot_re.sub(".", s)
    s = _multi_dash_re.sub("-", s)

    # Clean labels
    labels: list[str] = []
    for raw in s.split("."):
        label = raw.strip("-")  # cannot start/end with hyphen
        if not label:
            continue
        if len(label) > LABEL_MAX:
            label = label[:LABEL_MAX]
        labels.append(label)

    if not labels:
        raise ValueError(f"Cannot coerce hostname {value!r} to RFC1123")

    # Truncate overall length at label boundaries
    out: list[str] = []
    total = 0
    for i, label in enumerate(labels):
        extra = len(label) + (1 if i > 0 else 0)
        if total + extra > HOSTNAME_MAX:
            break
        out.append(label)
        total += extra

    if not out:
        raise ValueError(f"Cannot coerce hostname {value!r} to RFC1123")

    return ".".join(out)


class CoercingStrategy(HostnameStrategy):
    def __init__(self, inner: HostnameStrategy):
        self.inner = inner

    @override
    def resolve(self, ctx: HostnameContext) -> str | None:
        val = self.inner.resolve(ctx)
        return None if val is None else coerce_to_rfc1123(val)


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
    if callable(value):
        return FunctionStrategy(value)

    raise TypeError(
        f"Unsupported hostname input type: {type(value)!r}. Expected str, callable, or HostnameStrategy."
    )


def resolve_hostname(
    *,
    outpost: Outpost,
    check: Check,
    environment: Environment,
    result: CheckResult | None,
    fallback_to_default_hostname_generation: bool = True,
    coerce_into_valid_hostname: bool = True,
) -> str:
    ctx = HostnameContext.new(
        check=check,
        environment=environment,
        result=result,
    )

    # Determine candidate in precedence order
    candidate: str | None = None

    # 1) Per-result override
    if result and result.hostname:
        candidate = result.hostname
    else:
        # 2) Check-level strategy
        if check.hostname_strategy:
            try:
                val = check.hostname_strategy.resolve(ctx)
                if isinstance(val, str) and val:
                    candidate = val
            except Exception as e:
                raise HostnameResolutionError(
                    f"Hostname strategy failed at check level for {check.service_name}/{environment.name}: {e}"
                ) from e

        # 3) Environment-level strategy
        if candidate is None and environment.hostname_strategy:
            try:
                val = environment.hostname_strategy.resolve(ctx)
                if isinstance(val, str) and val:
                    candidate = val
            except Exception as e:
                raise HostnameResolutionError(
                    f"Hostname strategy failed at environment level for {check.service_name}/{environment.name}: {e}"
                ) from e

        # 4) Outpost-level strategy
        if candidate is None and outpost.hostname_strategy:
            try:
                val = outpost.hostname_strategy.resolve(ctx)
                if isinstance(val, str) and val:
                    candidate = val
            except Exception as e:
                raise HostnameResolutionError(
                    f"Hostname strategy failed at outpost level for {check.service_name}/{environment.name}: {e}"
                ) from e

    if candidate is None and fallback_to_default_hostname_generation:
        candidate = f"{check.service_name}-{environment.name}"

    if candidate is None:
        raise HostnameResolutionError(
            f"No hostname could be resolved for check={check.service_name} env={environment.name}"
        )

    if is_rfc1123_hostname(candidate):
        return candidate

    if not coerce_into_valid_hostname:
        raise HostnameResolutionError(
            f"Resolved hostname is not RFC1123-compatible: {candidate!r} for {check.service_name}/{environment.name}"
        )

    try:
        coerced_candidate = coerce_to_rfc1123(candidate)
        return coerced_candidate
    except ValueError as e:
        raise HostnameResolutionError(
            f"Cannot coerce hostname {candidate!r} to RFC1123: {e}"
        ) from e
