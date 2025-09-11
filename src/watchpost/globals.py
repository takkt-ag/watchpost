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
Global state for Watchpost and a proxy to the active application instance.

This module exposes `current_app`, a lightweight `LocalProxy` that resolves to
the running `Watchpost` instance. It uses a `ContextVar`, so access is bound to
the current async/task context rather than a process-wide global. This makes it
safe to use from request handlers, check execution, and any code that runs under
`Watchpost.app_context()`.

Notes:

- `Watchpost.app_context()` sets and resets the context variable around
  operations that need access to the active application. The ASGI integration
  also enters this context automatically for incoming requests.
- Accessing `current_app` outside an active context raises a `RuntimeError` with
  a helpful message. Ensure your code runs inside `Watchpost.app_context()` or
  via the ASGI app when using `current_app`.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING, cast

from .vendored.local_proxy import LocalProxy

if TYPE_CHECKING:
    from .app import Watchpost

_no_app_message = """\
Watchpost application is not available.

Are you interacting with '{local}' in the context of the running Watchpost application?
"""

_cv: ContextVar = ContextVar("watchpost_context")
current_app: Watchpost = cast(
    "Watchpost",
    LocalProxy(
        local=_cv,
        unbound_message=_no_app_message.format(local="current_app"),
    ),
)
