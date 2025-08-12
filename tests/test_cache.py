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

from datetime import UTC, datetime, timedelta
from tempfile import TemporaryDirectory
from typing import cast

import pytest

from outpost.cache import Cache, CacheKey, DiskStorage, InMemoryStorage


class TestDiskStorage:
    def test_directory_handling(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            assert disk_storage.directory.exists()
            assert disk_storage.directory.is_dir()

    def test_unknown_key(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)
            assert cache.get("key") is None

    def test_unknown_key_with_default(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)
            default = "default"
            cache_entry = cache.get("key", default=default)
            assert cache_entry is not None
            assert cache_entry.cache_key.key == "key"
            assert cache_entry.cache_key.package == __package__
            assert cache_entry.value == default
            assert cache_entry.added_at is None
            assert cache_entry.ttl is None

    def test_cache_key_uniqueness(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)

            cache.store("key", "value")
            cache.store("key", "updated-value")
            cache_entry = cache.get("key")

            assert cache_entry is not None
            assert cache_entry.cache_key.key == "key"
            assert cache_entry.cache_key.package == __package__
            assert cache_entry.value == "updated-value"
            assert cache_entry.added_at is not None
            assert cache_entry.ttl is None
            assert cache_entry.is_expired() is False

    def test_basic_store_and_get(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)

            cache.store("key", "value")
            cache_entry = cache.get("key")

            assert cache_entry is not None
            assert cache_entry.cache_key.key == "key"
            assert cache_entry.cache_key.package == __package__
            assert cache_entry.value == "value"
            assert cache_entry.added_at is not None
            assert cache_entry.ttl is None
            assert cache_entry.is_expired() is False

    def test_store_with_package(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)

            cache.store("key", "value", package="test")
            cache_entry = cache.get("key", package="test")

            assert cache_entry is not None
            assert cache_entry.cache_key.key == "key"
            assert cache_entry.cache_key.package == "test"
            assert cache_entry.value == "value"
            assert cache_entry.added_at is not None
            assert cache_entry.ttl is None
            assert cache_entry.is_expired() is False

    def test_ttl_works(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)

            cache.store("key", "value", ttl=timedelta(seconds=-1))
            cache_entry_path = disk_storage._get_file_path(
                CacheKey(key="key", package=cast(str, __package__))
            )

            assert cache_entry_path.exists()
            assert cache.get("key") is None
            assert not cache_entry_path.exists()

    def test_return_expired(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)

            cache.store("key", "value", ttl=timedelta(seconds=-1))
            cache_entry_path = disk_storage._get_file_path(
                CacheKey(key="key", package=cast(str, __package__))
            )

            assert cache_entry_path.exists()
            cache_entry = cache.get("key", return_expired=True)
            assert cache_entry is not None
            assert cache_entry.cache_key.key == "key"
            assert cache_entry.cache_key.package == __package__
            assert cache_entry.value == "value"
            assert cache_entry.added_at is not None
            assert cache_entry.ttl == timedelta(seconds=-1)
            assert cache_entry.is_expired() is True
            assert not cache_entry_path.exists()

    def test_memoize_with_key(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)

            @cache.memoize(key="memoize-key")
            def memoized_function(a):
                return a

            assert memoized_function(1) == 1
            cache_key = CacheKey(
                key="memoize-key",
                package=cast(str, __package__),
            )
            cache_entry_path = disk_storage._get_file_path(cache_key)
            assert cache_entry_path.exists()
            # memoize currently does not cache by the arguments provided, so providing
            # something else will still return the prior value while the cache is active
            assert memoized_function(2) == 1

            cache_entry = disk_storage.get(cache_key)
            assert cache_entry is not None
            disk_storage._remove_cache_entry_on_disk(cache_entry)
            assert memoized_function(2) == 2

    def test_memoize_with_key_generator(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)

            def key_generator(*args, **_kwargs) -> str:
                return str(args[0])

            @cache.memoize(key_generator=key_generator)
            def memoized_function(_a):
                return datetime.now(tz=UTC)

            call_1 = memoized_function(1)
            assert isinstance(call_1, datetime)
            cache_key = CacheKey(
                key="1",
                package=cast(str, __package__),
            )
            cache_entry_path = disk_storage._get_file_path(cache_key)
            assert cache_entry_path.exists()

            # Repeat calls return the same result
            assert memoized_function(1) == call_1
            # Calls with a different argument return a different result
            call_2 = memoized_function(2)
            assert isinstance(call_2, datetime)
            assert call_2 > call_1

    def test_memoize_without_key_or_key_generator(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)

            with pytest.raises(
                ValueError, match="Either key or key_generator must be provided."
            ):

                @cache.memoize()
                def memoized_function(a):
                    return a

    def test_memoize_with_both_key_and_key_generator(self):
        with TemporaryDirectory() as tmpdir:
            disk_storage = DiskStorage(tmpdir)
            cache = Cache(disk_storage)

            def key_generator(*_args, **_kwargs) -> str:
                return "something"

            with pytest.raises(
                ValueError, match="Only one of key or key_generator can be provided."
            ):

                @cache.memoize(key="something", key_generator=key_generator)
                def memoized_function(a):
                    return a


class TestInMemoryStorage:
    def test_unknown_key(self):
        in_memory_storage = InMemoryStorage()
        cache = Cache(in_memory_storage)
        assert cache.get("key") is None

    def test_unknown_key_with_default(self):
        in_memory_storage = InMemoryStorage()
        cache = Cache(in_memory_storage)
        default = "default"
        cache_entry = cache.get("key", default=default)
        assert cache_entry is not None
        assert cache_entry.cache_key.key == "key"
        assert cache_entry.cache_key.package == __package__
        assert cache_entry.value == default
        assert cache_entry.added_at is None
        assert cache_entry.ttl is None

    def test_cache_key_uniqueness(self):
        in_memory_storage = InMemoryStorage()
        cache = Cache(in_memory_storage)

        cache.store("key", "value")
        cache.store("key", "updated-value")
        cache_entry = cache.get("key")

        assert cache_entry is not None
        assert cache_entry.cache_key.key == "key"
        assert cache_entry.cache_key.package == __package__
        assert cache_entry.value == "updated-value"
        assert cache_entry.added_at is not None
        assert cache_entry.ttl is None
        assert cache_entry.is_expired() is False

    def test_basic_store_and_get(self):
        in_memory_storage = InMemoryStorage()
        cache = Cache(in_memory_storage)

        cache.store("key", "value")
        cache_entry = cache.get("key")

        assert cache_entry is not None
        assert cache_entry.cache_key.key == "key"
        assert cache_entry.cache_key.package == __package__
        assert cache_entry.value == "value"
        assert cache_entry.added_at is not None
        assert cache_entry.ttl is None
        assert cache_entry.is_expired() is False

    def test_store_with_package(self):
        in_memory_storage = InMemoryStorage()
        cache = Cache(in_memory_storage)

        cache.store("key", "value", package="test")
        cache_entry = cache.get("key", package="test")

        assert cache_entry is not None
        assert cache_entry.cache_key.key == "key"
        assert cache_entry.cache_key.package == "test"
        assert cache_entry.value == "value"
        assert cache_entry.added_at is not None
        assert cache_entry.ttl is None
        assert cache_entry.is_expired() is False

    def test_ttl_works(self):
        in_memory_storage = InMemoryStorage()
        cache = Cache(in_memory_storage)

        cache.store("key", "value", ttl=timedelta(seconds=-1))
        cache_key = CacheKey(key="key", package=cast(str, __package__))
        assert cache_key in in_memory_storage.cache
        assert cache.get("key") is None
        assert cache_key not in in_memory_storage.cache

    def test_return_expired(self):
        in_memory_storage = InMemoryStorage()
        cache = Cache(in_memory_storage)

        cache.store("key", "value", ttl=timedelta(seconds=-1))
        cache_key = CacheKey(key="key", package=cast(str, __package__))

        assert cache_key in in_memory_storage.cache
        cache_entry = cache.get("key", return_expired=True)
        assert cache_entry is not None
        assert cache_entry.cache_key.key == "key"
        assert cache_entry.cache_key.package == __package__
        assert cache_entry.value == "value"
        assert cache_entry.added_at is not None
        assert cache_entry.ttl == timedelta(seconds=-1)
        assert cache_entry.is_expired() is True
        assert cache_key not in in_memory_storage.cache

    def test_memoize(self):
        in_memory_storage = InMemoryStorage()
        cache = Cache(in_memory_storage)

        @cache.memoize(key="memoize-key")
        def memoized_function(a):
            return a

        assert memoized_function(1) == 1
        cache_key = CacheKey(
            key="memoize-key",
            package=cast(str, __package__),
        )
        assert cache_key in in_memory_storage.cache
        # memoize currently does not cache by the arguments provided, so providing
        # something else will still return the prior value while the cache is active
        assert memoized_function(2) == 1

        del in_memory_storage.cache[cache_key]
        assert memoized_function(2) == 2
