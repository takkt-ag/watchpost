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

import logging
from collections import defaultdict, deque
from collections.abc import Callable, Hashable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(f"{__package__}.{__name__}")


class CheckExecutor:
    @dataclass
    class Statistics:
        total: int
        completed: int
        errored: int
        running: int
        awaiting_pickup: int

    def __init__(
        self,
        max_workers: int | None = None,
    ):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.futures: dict[Hashable, deque[Future]] = defaultdict(deque)
        self.finished_futures: set[Future] = set()
        self.keys: dict[Future, Hashable] = {}
        self.results: dict[Future, Any] = {}
        self.errors: dict[Future, Exception] = {}

    def shutdown(self, wait: bool = False) -> None:
        self.executor.shutdown(wait=wait)

    def submit[**P, R](  # type: ignore[valid-type]
        self,
        key: Hashable,
        func: Callable[P, R],
        *args: P.args,
        resubmit: bool = False,
        **kwargs: P.kwargs,
    ) -> Future:
        if not resubmit and (futures := self.futures[key]):
            # One or more jobs for this key are already running. We don't want to start
            # another one, so we return the first existing future.
            return futures[0]

        logger.debug("Submitting future for key %s", key)
        future = self.executor.submit(func, *args, **kwargs)
        self.futures[key].append(future)
        self.keys[future] = key
        future.add_done_callback(self._done_callback)
        return future

    def _done_callback(self, future: Future) -> None:
        key = self.keys.get(future, "<unknown future>")
        try:
            self.results[future] = future.result()
            logger.debug("Future %s completed successfully", key)
        except Exception as e:
            self.errors[future] = e
            logger.debug("Future %s failed: %s", key, e)
        finally:
            self.finished_futures.add(future)

    def result(self, key: Hashable) -> Any:
        try:
            future = self.futures[key].popleft()
        except IndexError as e:
            raise KeyError(key) from e

        # NOTE: we are not using `future.done()` here, because there is some
        #       miniscule amount of time between when the future is done and
        #       when `self._done_callback` has actually populated
        #       `self.results` or `self.errors`. If we check for `done` here
        #       right in that little time window, we might not get either the
        #       result or the error.
        #       Instead, `self._done_callback` will let us know once the
        #       future's result/error is _actually_ ready to consume through
        #       `self.finished_futures`.
        if future not in self.finished_futures:
            self.futures[key].appendleft(future)
            return None
        self.finished_futures.remove(future)

        try:
            return self.results[future]
        except KeyError:
            raise self.errors[future] from None
        finally:
            if future in self.keys:
                del self.keys[future]
            if future in self.results:
                del self.results[future]
            if future in self.errors:
                del self.errors[future]

    def statistics(self) -> CheckExecutor.Statistics:
        total = sum(len(futures) for futures in self.futures.values())
        completed = len(self.results)
        errored = len(self.errors)
        running = total - completed - errored
        awaiting_pickup = completed + errored

        return CheckExecutor.Statistics(
            total=total,
            completed=completed,
            errored=errored,
            running=running,
            awaiting_pickup=awaiting_pickup,
        )

    def errored(self) -> dict[str, str]:
        result = {}
        for future, exception in self.errors.items():
            try:
                key = str(self.keys[future])
            except KeyError:
                key = "<unknown future>"
            result[key] = str(exception)

        return result
