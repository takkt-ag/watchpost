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

import base64
import json
import re
from collections.abc import Generator, Hashable
from concurrent.futures import wait
from contextlib import contextmanager
from threading import Event
from typing import Any

from outpost.executor import CheckExecutor


class BlockingCheckExecutor[T](CheckExecutor[T]):
    def __init__(self, max_workers: int | None = 1):
        super().__init__(max_workers)

    def result(self, key: Hashable) -> T | None:
        wait(self._state[key].active_futures, return_when="ALL_COMPLETED")
        return super().result(key)


def decode_checkmk_output(output: str | bytes) -> list[dict[str, Any]]:
    """
    Decode base64 encoded JSON data from Checkmk output.

    This utility function extracts and decodes the base64 encoded JSON data
    that is contained between the outpost markers in Checkmk output.

    Args:
        output: The Checkmk output as a string or bytes

    Returns:
        If there is only one result, returns a dictionary with the decoded JSON data.
        If there are multiple results, returns a list of dictionaries.

    Raises:
        ValueError: If no base64 encoded data is found in the output
    """
    # Convert bytes to string if necessary
    if isinstance(output, bytes):
        output_str = output.decode("utf-8")
    else:
        output_str = output

    # Find all base64 encoded parts between the outpost markers
    pattern = re.compile(r"<<<outpost>>>\n(.*?)\n<<<<", re.DOTALL)
    matches = pattern.finditer(output_str)

    # Decode each match
    results = []
    for match in matches:
        base64_data = match.group(1)
        decoded_data = base64.b64decode(base64_data).decode("utf-8")
        json_data = json.loads(decoded_data)
        results.append(json_data)

    if not results:
        raise ValueError("No base64 encoded data found in Checkmk output")

    return results


@contextmanager
def with_event() -> Generator[Event]:
    event = Event()
    try:
        yield event
    finally:
        event.set()
