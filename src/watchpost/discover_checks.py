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

import importlib
import pkgutil
from collections.abc import Callable, Iterable
from types import ModuleType

from .check import Check

ModulePredicate = Callable[[ModuleType], bool]
CheckPredicate = Callable[[Check, ModuleType, str], bool]


class DiscoveryError(Exception):
    pass


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
    Discover Check instances defined as global names in the given module or package.

    - If `module` is a string, it is imported via importlib.import_module.
    - If `recursive` and the module is a package, discover submodules via pkgutil.walk_packages.
    - `include_module` and `exclude_module` receive the imported module object and
      can be used to constrain traversal (e.g., skip tests or heavy modules).
    - `check_filter` receives (check, module, name) and can exclude specific checks.
    - If `raise_on_import_error` is False (default), modules that cannot be imported
      are skipped; otherwise, the import error is raised.
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
