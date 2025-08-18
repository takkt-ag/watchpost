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
import os
import sys

from ..app import Outpost


class AppNotFound(Exception):
    """Custom exception for when the app cannot be found."""


def find_app(app_str: str | None) -> Outpost:
    """
    Finds and loads the Outpost app instance.

    The search order is:
    1. The `app_str` argument if provided (e.g., 'my_module:app').
    2. Convention: look for an `Outpost` instance named `app` in `outpost.py`,
       then `app.py`, then `main.py` in the current directory.
    """
    if app_str:
        return _load_from_string(app_str)

    return _load_from_convention()


def _load_from_string(app_str: str) -> Outpost:
    """Loads an app from a string like 'module:variable'."""
    if ":" not in app_str:
        raise AppNotFound(
            f"Invalid app string '{app_str}'. Expected format 'module:variable'."
        )

    module_str, app_instance_str = app_str.split(":", 1)

    # Add current working directory to path to allow local imports
    sys.path.insert(0, os.getcwd())
    try:
        module = importlib.import_module(module_str)
        app = getattr(module, app_instance_str)
    except (ModuleNotFoundError, AttributeError) as e:
        raise AppNotFound(f"Could not import app '{app_str}'. Error: {e}")
    finally:
        sys.path.pop(0)

    if not isinstance(app, Outpost):
        raise AppNotFound(
            f"The object '{app_instance_str}' in '{module_str}' is not an Outpost instance."
        )
    return app


def _load_from_convention() -> Outpost:
    """Tries to find the app by convention."""
    for filename in ("outpost.py", "app.py", "main.py"):
        if os.path.exists(filename):
            module_name = filename[:-3]
            try:
                # Load from a file path
                return _load_from_string(f"{module_name}:app")
            except AppNotFound:
                continue
    raise AppNotFound(
        "Could not find an Outpost app. Either provide the app location with "
        "--app <module:instance> or the OUTPOST_APP environment variable."
    )
