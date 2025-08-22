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

import asyncio
import inspect
import logging
import threading
from collections import deque
from collections.abc import Awaitable, Callable, Hashable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast, override

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger(f"{__package__}.{__name__}")


class AsyncioLoopThread(threading.Thread):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.loop: asyncio.AbstractEventLoop | None = None
        self.loop_started = threading.Event()

    def run(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.loop_started.set()
        self.loop.run_forever()

    def stop(self) -> None:
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)


@dataclass
class _KeyState[T]:
    active_futures: list[Future[T]] = field(default_factory=list)
    finished_futures: deque[Future[T]] = field(default_factory=deque)


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
        self._state: dict[Hashable, _KeyState[T]] = {}
        self._asyncio_loop_thread: AsyncioLoopThread | None = None

    @property
    def asyncio_loop(self) -> asyncio.AbstractEventLoop:
        if not self._asyncio_loop_thread:
            self._asyncio_loop_thread = AsyncioLoopThread(daemon=True)
            self._asyncio_loop_thread.start()
            self._asyncio_loop_thread.loop_started.wait()

        return cast(asyncio.AbstractEventLoop, self._asyncio_loop_thread.loop)

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
        if self._asyncio_loop_thread:
            self._asyncio_loop_thread.stop()
        self.executor.shutdown(wait=wait)

    def submit[**P](  # type: ignore[valid-type]
        self,
        key: Hashable,
        func: Callable[P, T | Awaitable[T]],
        *args: P.args,
        resubmit: bool = False,
        **kwargs: P.kwargs,
    ) -> Future:
        key_state = self._state.setdefault(key, _KeyState())

        if not resubmit and key_state.active_futures:
            # One or more jobs for this key are already running. We don't want
            # to start another one, so we return the first existing future.
            return key_state.active_futures[0]

        logger.debug("Submitting future for key %s", key)
        if inspect.iscoroutinefunction(func) or inspect.iscoroutinefunction(func):
            future = asyncio.run_coroutine_threadsafe(
                func(*args, **kwargs),  # type: ignore[invalid-argument-type]
                self.asyncio_loop,
            )
        else:
            future = self.executor.submit(func, *args, **kwargs)
        key_state.active_futures.append(future)
        future.add_done_callback(lambda future: self._done_callback(key, future))
        return future

    def _done_callback(self, key: Hashable, future: Future[T]) -> None:
        if key_state := self._state.get(key):
            logger.debug("Future %s completed successfully", key)
            key_state.finished_futures.append(future)
        else:
            logger.warning("Future %s completed after state cleanup", key)

    def result(self, key: Hashable) -> T | None:
        key_state = self._state.get(key)

        if not key_state or not key_state.finished_futures:
            if not key_state or not key_state.active_futures:
                # No future for the key has been submitted at all.
                raise KeyError(key) from None

            # No future for the key has finished yet, returning no result.
            return None

        finished_future = key_state.finished_futures.popleft()
        try:
            key_state.active_futures.remove(finished_future)
        except ValueError:
            pass

        if not key_state.active_futures:
            assert len(key_state.finished_futures) == 0
            del self._state[key]

        return finished_future.result()

    def statistics(self) -> CheckExecutor.Statistics:
        total = 0
        completed = 0
        errored = 0

        for key_state in self._state.values():
            total += len(key_state.active_futures)
            for finished_future in key_state.finished_futures:
                assert finished_future.done()
                if finished_future.exception():
                    errored += 1
                else:
                    completed += 1

        awaiting_pickup = completed + errored
        running = total - awaiting_pickup

        return CheckExecutor.Statistics(
            total=total,
            completed=completed,
            errored=errored,
            running=running,
            awaiting_pickup=awaiting_pickup,
        )

    def errored(self) -> dict[str, str]:
        errors = {}
        for key, key_state in self._state.items():
            for future in key_state.finished_futures:
                if (exception := future.exception()) is not None:
                    errors[str(key)] = str(exception)

        return errors


class BlockingCheckExecutor[T](CheckExecutor[T]):
    """
    A BlockingCheckExecutor class for executing checks with synchronous behavior.

    This class extends the CheckExecutor class and allows for blocking execution
    of tasks until all associated futures are completed. It can be used in
    scenarios where synchronization of task execution results is required, such
    as tests or in the CLI.

    PLEASE DO NOT USE THIS EXECUTOR UNLESS YOU KNOW WHAT YOU ARE DOING! All
    regular uses of Watchpost should make use of the default `CheckExecutor`
    class.
    """

    def __init__(
        self,
        max_workers: int | None = 1,
    ):
        super().__init__(max_workers)

    @override
    def result(
        self,
        key: Hashable,
    ) -> T | None:
        wait(self._state[key].active_futures, return_when="ALL_COMPLETED")
        return super().result(key)
