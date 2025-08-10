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

import time
from dataclasses import asdict

import pytest

from outpost.executor import CheckExecutor


def job(*args, **kwargs):
    return {
        "args": args,
        "kwargs": kwargs,
    }


def slow_job(delay: float, *args, **kwargs):
    time.sleep(delay)
    return {
        "delay": delay,
        "args": args,
        "kwargs": kwargs,
    }


def test_empty_statistics():
    executor = CheckExecutor(max_workers=1)
    assert asdict(executor.statistics()) == {
        "total": 0,
        "completed": 0,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 0,
    }


def test_cant_get_result_of_unknown_job():
    executor = CheckExecutor(max_workers=1)

    with pytest.raises(KeyError):
        executor.result("job")


def test_submitting_and_getting_a_job():
    executor = CheckExecutor(max_workers=1)

    future = executor.submit("key", job, "arg", kwarg="kwarg")
    future.result()  # Wait for the job to complete

    assert future.done() is True
    assert asdict(executor.statistics()) == {
        "total": 1,
        "completed": 1,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 1,
    }

    assert executor.result("key") == {
        "args": ("arg",),
        "kwargs": {"kwarg": "kwarg"},
    }
    assert asdict(executor.statistics()) == {
        "total": 0,
        "completed": 0,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 0,
    }


def test_throwing_job():
    executor = CheckExecutor(max_workers=1)

    future = executor.submit("key", lambda: 1 / 0)

    # Wait for the job to complete
    with pytest.raises(ZeroDivisionError):
        future.result()

    assert future.done() is True
    assert asdict(executor.statistics()) == {
        "total": 1,
        "completed": 0,
        "errored": 1,
        "running": 0,
        "awaiting_pickup": 1,
    }

    with pytest.raises(ZeroDivisionError):
        executor.result("key")

    assert asdict(executor.statistics()) == {
        "total": 0,
        "completed": 0,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 0,
    }


def test_slower_job():
    executor = CheckExecutor(max_workers=1)

    future = executor.submit("key", slow_job, delay=0.1)
    assert future.done() is False
    assert asdict(executor.statistics()) == {
        "total": 1,
        "completed": 0,
        "errored": 0,
        "running": 1,
        "awaiting_pickup": 0,
    }
    assert executor.result("key") is None

    future.result()  # Wait for the job to complete
    assert asdict(executor.statistics()) == {
        "total": 1,
        "completed": 1,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 1,
    }

    assert executor.result("key") == {
        "delay": 0.1,
        "args": (),
        "kwargs": {},
    }
    assert asdict(executor.statistics()) == {
        "total": 0,
        "completed": 0,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 0,
    }


def test_resubmit_disabled():
    executor = CheckExecutor(max_workers=1)

    future1 = executor.submit("key", job, "arg1", kwarg="kwarg1")
    future2 = executor.submit("key", job, "arg2", kwarg="kwarg2", resubmit=False)

    assert future1 is future2
    future1.result()  # Wait for the job to complete

    assert future1.done() is True
    assert asdict(executor.statistics()) == {
        "total": 1,
        "completed": 1,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 1,
    }

    assert executor.result("key") == {
        "args": ("arg1",),
        "kwargs": {"kwarg": "kwarg1"},
    }


def test_resubmit_enabled():
    executor = CheckExecutor(max_workers=1)

    future1 = executor.submit("key", job, "arg1", kwarg="kwarg1")
    future2 = executor.submit("key", job, "arg2", kwarg="kwarg2", resubmit=True)

    assert future1 is not future2
    # Wait for the jobs to complete
    future1.result()
    future2.result()

    assert future1.done() is True
    assert asdict(executor.statistics()) == {
        "total": 2,
        "completed": 2,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 2,
    }

    assert executor.result("key") == {
        "args": ("arg1",),
        "kwargs": {"kwarg": "kwarg1"},
    }
    assert asdict(executor.statistics()) == {
        "total": 1,
        "completed": 1,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 1,
    }
    assert executor.result("key") == {
        "args": ("arg2",),
        "kwargs": {"kwarg": "kwarg2"},
    }
    assert asdict(executor.statistics()) == {
        "total": 0,
        "completed": 0,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 0,
    }


def test_resubmit_order_retained():
    executor = CheckExecutor(max_workers=1)

    future1 = executor.submit("key", slow_job, delay=0.2)
    future2 = executor.submit("key", slow_job, delay=0.1, resubmit=True)

    assert future1 is not future2
    # Wait for the jobs to complete
    future1.result()
    future2.result()

    assert future1.done() is True
    assert asdict(executor.statistics()) == {
        "total": 2,
        "completed": 2,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 2,
    }

    assert executor.result("key") == {
        "delay": 0.2,
        "args": (),
        "kwargs": {},
    }
    assert asdict(executor.statistics()) == {
        "total": 1,
        "completed": 1,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 1,
    }
    assert executor.result("key") == {
        "delay": 0.1,
        "args": (),
        "kwargs": {},
    }
    assert asdict(executor.statistics()) == {
        "total": 0,
        "completed": 0,
        "errored": 0,
        "running": 0,
        "awaiting_pickup": 0,
    }
