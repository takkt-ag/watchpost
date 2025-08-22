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

import importlib
import uuid
from pathlib import Path

import pytest

from watchpost.discover_checks import DiscoveryError, discover_checks


@pytest.fixture()
def temp_pkg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Create a temporary package structure with multiple modules and checks.

    Layout:
    temp_pkg/
      __init__.py            # re-exports check from a.sub
      a/
        __init__.py
        sub.py               # defines check_a_sub
      b/
        __init__.py
        mod.py               # defines check_b_mod
      bad/
        __init__.py
        mod.py               # raises Exception upon import
    """

    # Adding a random value to the package name ensures that the packages
    # created do not overlap with any other tests, given the modification of the
    # syspath below.
    random_id = uuid.uuid4().hex
    pkg = tmp_path / f"temp_pkg_{random_id}"
    (pkg / "a").mkdir(parents=True)
    (pkg / "b").mkdir(parents=True)
    (pkg / "bad").mkdir(parents=True)

    # Common helper for modules that define a Check
    check_module_src = (
        "from watchpost.check import check\n"
        "from watchpost.environment import Environment\n"
        "env = Environment('e')\n"
        "@check(name='svc_{tag}', service_labels={{}}, environments=[env], cache_for=None)\n"
        "def {name}():\n"
        "    return []\n"
    )

    # a/sub.py defines check_a_sub
    (pkg / "a" / "__init__.py").write_text("")
    (pkg / "a" / "sub.py").write_text(
        check_module_src.format(tag="a_sub", name="check_a_sub")
    )

    # b/mod.py defines check_b_mod
    (pkg / "b" / "__init__.py").write_text("")
    (pkg / "b" / "mod.py").write_text(
        check_module_src.format(tag="b_mod", name="check_b_mod")
    )

    # bad/mod.py raises during import
    (pkg / "bad" / "__init__.py").write_text("")
    (pkg / "bad" / "mod.py").write_text("raise RuntimeError('boom')\n")

    # Package __init__ re-exports the a.sub.check_a_sub (to test deduplication)
    (pkg / "__init__.py").write_text(
        "from .a.sub import check_a_sub as reexported_check\n"
    )

    # Make tmp package importable
    monkeypatch.syspath_prepend(str(tmp_path))

    # Import and return the package module for convenience
    mod = importlib.import_module(f"temp_pkg_{random_id}")
    return mod


def test_basic_discovery_from_module_object(temp_pkg):
    checks = discover_checks(temp_pkg, recursive=False)
    # Should find only the reexported check in the root module when not recursive
    names = sorted(c.service_name for c in checks)
    assert names == ["svc_a_sub"]


def test_recursive_discovery_across_subpackages_and_dedup(temp_pkg):
    checks = discover_checks(temp_pkg, recursive=True)
    # Expect to find a.sub.check_a_sub and b.mod.check_b_mod, but not duplicate from re-export
    names = sorted(c.service_name for c in checks)
    assert names == ["svc_a_sub", "svc_b_mod"]


def test_include_module_predicate_limits_traversal(temp_pkg):
    # Only include modules whose dotted name contains '.b.' (root excluded to avoid re-export)
    checks = discover_checks(
        temp_pkg,
        recursive=True,
        include_module=lambda m: ".b." in m.__name__,
    )
    names = sorted(c.service_name for c in checks)
    assert names == ["svc_b_mod"]


def test_exclude_module_predicate_skips_modules(temp_pkg):
    # Exclude modules under the 'a' package; note the root module still re-exports a_sub
    checks = discover_checks(
        temp_pkg,
        recursive=True,
        exclude_module=lambda m: m.__name__.startswith("temp_pkg.a"),
    )
    names = sorted(c.service_name for c in checks)
    assert names == ["svc_a_sub", "svc_b_mod"]


def test_check_filter_can_filter_specific_checks(temp_pkg):
    # Only keep checks whose service_name ends with 'a_sub'
    checks = discover_checks(
        temp_pkg,
        recursive=True,
        check_filter=lambda check, _module, _name: check.service_name.endswith("a_sub"),
    )
    names = sorted(c.service_name for c in checks)
    assert names == ["svc_a_sub"]


def test_raise_on_import_error_false_skips_bad_modules(temp_pkg):
    # With default raise_on_import_error=False, the bad module is skipped
    checks = discover_checks(temp_pkg, recursive=True)
    names = sorted(c.service_name for c in checks)
    assert names == ["svc_a_sub", "svc_b_mod"]


def test_raise_on_import_error_true_raises(temp_pkg):
    with pytest.raises(DiscoveryError) as exc:
        discover_checks(temp_pkg, recursive=True, raise_on_import_error=True)
    assert "Failed to import" in str(exc.value)


def test_accepts_module_as_string_and_object(temp_pkg):
    # As string
    checks_str = discover_checks(temp_pkg.__name__, recursive=False)
    # As module object
    checks_mod = discover_checks(temp_pkg, recursive=False)
    assert {c.service_name for c in checks_str} == {c.service_name for c in checks_mod}
