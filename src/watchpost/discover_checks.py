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

"""
Utilities for discovering checks defined in modules and packages.

This module scans Python modules for global `Check` instances and, when
requested, traverses packages to find checks across submodules. It offers
predicates to include or exclude modules, an optional per-check filter, and
configurable error handling for import failures.
"""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Callable, Iterable
from types import ModuleType

from .check import Check

ModulePredicate = Callable[[ModuleType], bool]
CheckPredicate = Callable[[Check, ModuleType, str], bool]


class DiscoveryError(Exception):
    """
    Raised when a submodule import fails during discovery and strict error
    handling is enabled.

    This exception is raised only when `raise_on_import_error=True` is passed to
    `discover_checks()` and a submodule cannot be imported while traversing a
    package.
    """


def discover_checks(
    module: ModuleType | str,
    *,
    recursive: bool = True,
    include_module: ModulePredicate | None = None,
    exclude_module: ModulePredicate | None = None,
    check_filter: CheckPredicate | None = None,
    raise_on_import_error: bool = False,
) -> list[Check]:
    """
    Discover `Check` instances defined as global names in a module or package.

    This function scans the given module for global `Check` objects and, when
    `recursive` is enabled, traverses subpackages to discover checks across the
    package tree. It supports module-level include/exclude predicates, a
    per-check filter, and optional strict error handling for imports.

    Parameters:
        module:
            The root module object or dotted module name to scan for checks.
        recursive:
            Indicates whether to traverse subpackages when the module is a
            package.
        include_module:
            Predicate receiving the imported module. Return True to include the
            module in scanning; return False to skip it. Use this to constrain
            traversal (for example, skip test modules or expensive imports).
        exclude_module:
            Predicate receiving the imported module. If it returns True, the
            module is skipped. This is applied after `include_module`.
        check_filter:
            Predicate receiving `(check, module, name)` that decides whether a
            discovered `Check` should be included. Return True to keep it.
        raise_on_import_error:
            If True, raise a `DiscoveryError` when a submodule import fails
            while traversing packages. If False (default), such modules are
            skipped.

    Returns:
        A list of discovered `Check` instances. If the same `Check` object is
        re-exported from multiple modules, it appears only once.

    Raises:
        DiscoveryError:
            If a submodule import fails and `raise_on_import_error` is True.

    Notes:
        Checks are discovered by scanning module globals and selecting objects
        that are instances of `Check`. Identity de-duplicates duplicate objects
        to avoid listing the same `Check` multiple times when re-exported.
    """
    if isinstance(module, str):
        root_module = importlib.import_module(module)
    else:
        root_module = module

    discovered_checks: list[Check] = []
    seen_check_ids: set[int] = set()

    def should_visit(m: ModuleType) -> bool:
        if include_module and not include_module(m):
            return False
        if exclude_module and exclude_module(m):
            return False
        return True

    def scan_module_for_checks(mod: ModuleType) -> None:
        if not should_visit(mod):
            return
        for name, value in vars(mod).items():
            if isinstance(value, Check):
                if check_filter and not check_filter(value, mod, name):
                    continue
                if id(value) not in seen_check_ids:
                    seen_check_ids.add(id(value))
                    discovered_checks.append(value)

    def walk_package(m: ModuleType) -> Iterable[ModuleType]:
        # Only packages expose __path__
        path = getattr(m, "__path__", None)
        if path is None:
            return
        for _, name, _ in pkgutil.walk_packages(
            path,
            prefix=m.__name__ + ".",
        ):
            try:
                sub_module = importlib.import_module(name)
            except Exception as e:  # pragma: no cover (behavior controlled by flag)
                if raise_on_import_error:
                    raise DiscoveryError(f"Failed to import {name}") from e
                else:
                    continue
            yield sub_module

    scan_module_for_checks(root_module)
    if recursive:
        for sub_module in walk_package(root_module):
            scan_module_for_checks(sub_module)
    return discovered_checks
