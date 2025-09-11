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
Starlette HTTP routes for Watchpost.

This module exposes operational endpoints and the streaming root endpoint:

- `/healthcheck` returns 204 for liveness checks.
- `/executor/statistics` returns executor statistics as JSON.
- `/executor/errored` returns a list of errored checks as JSON.
- `/` streams Checkmk-compatible output from running checks.
"""

from dataclasses import asdict

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from .globals import current_app


async def healthcheck(_request: Request) -> Response:
    return Response(status_code=204)


async def executor_statistics(_request: Request) -> JSONResponse:
    return JSONResponse(asdict(current_app.executor.statistics()))


async def executor_errored(_request: Request) -> JSONResponse:
    return JSONResponse(current_app.executor.errored())


async def root(_request: Request) -> StreamingResponse:
    return StreamingResponse(
        current_app.run_checks(),
        media_type="text/plain",
    )


routes = [
    Route("/healthcheck", endpoint=healthcheck),
    Route("/executor/statistics", endpoint=executor_statistics),
    Route("/executor/errored", endpoint=executor_errored),
    Route("/", endpoint=root),
]
