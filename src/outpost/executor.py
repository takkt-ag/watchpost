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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger(f"{__package__}.{__name__}")


class CheckExecutor[T]:
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
        self.futures: dict[Hashable, list[Future]] = defaultdict(list)
        self.finished_futures: dict[Hashable, deque[Future]] = defaultdict(deque)
        self.keys: dict[Future, Hashable] = {}
        self.results: dict[Future, T] = {}
        self.errors: dict[Future, Exception] = {}

    def __enter__(self) -> CheckExecutor[T]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.shutdown(wait=True)

    def shutdown(self, wait: bool = False) -> None:
        self.executor.shutdown(wait=wait)

    def submit[**P](  # type: ignore[valid-type]
        self,
        key: Hashable,
        func: Callable[P, T],
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
            self.finished_futures[key].append(future)

    def result(self, key: Hashable) -> T | None:
        try:
            finished_future = self.finished_futures[key].popleft()
        except IndexError:
            if not self.futures[key]:
                # No future for the key has been submitted at all.
                raise KeyError(key) from None

            # No future for the key has finished yet, returning no result.
            return None

        try:
            return self.results[finished_future]
        except KeyError:
            raise self.errors[finished_future] from None
        finally:
            if finished_future in self.keys:
                del self.keys[finished_future]
            if finished_future in self.results:
                del self.results[finished_future]
            if finished_future in self.errors:
                del self.errors[finished_future]
            try:
                self.futures[key].remove(finished_future)
            except ValueError:
                pass

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
