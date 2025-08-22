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
