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

import functools
import hashlib
import inspect
import pickle
from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar, cast

if TYPE_CHECKING:
    from redis import Redis

T = TypeVar("T")


def get_caller_package() -> str:
    current_frame = inspect.currentframe()
    assert current_frame
    assert current_frame.f_back
    assert current_frame.f_back.f_back
    caller_frame = current_frame.f_back.f_back
    caller_module = inspect.getmodule(caller_frame)
    assert caller_module is not None
    package_name = (
        caller_module.__package__
        if caller_module.__package__
        else caller_module.__name__
    )
    return package_name


@dataclass
class CacheKey:
    key: Hashable
    package: str

    def __hash__(self) -> int:
        return hash((self.key, self.package))


@dataclass
class CacheEntry[T]:
    # Increment the version if you change the serialization format, i.e. if the types,
    # meaning of values, or other aspects of this type are modified in a way that is not
    # backwards-compatible.
    VERSION = 1

    cache_key: CacheKey
    value: T

    added_at: datetime | None
    ttl: timedelta | None

    def is_expired(self) -> bool:
        if not self.added_at:
            return False
        return self.ttl is not None and datetime.now(tz=UTC) - self.added_at > self.ttl


class Storage(ABC):
    @abstractmethod
    def get(
        self,
        cache_key: CacheKey,
        return_expired: bool = False,
    ) -> CacheEntry[T] | None:
        """
        :param cache_key: The key to retrieve the value for.
        :param return_expired: Whether to return a key that has expired. The value will
                               be returned at most once.
        :return: A cache entry of the value from the cache if it was found, otherwise
                 `None`.
        """

    @abstractmethod
    def store(
        self,
        entry: CacheEntry,
    ) -> None:
        """
        :param entry: The cache entry to store
        """


class ChainedStorage(Storage):
    """
    A storage implementation that chains multiple storage backends together.

    On retrieval, it tries each storage in order until it finds a hit. On
    storage, it stores the value in all storage backends.
    """

    def __init__(self, storages: list[Storage]):
        """
        Initialize a chained storage with multiple storage backends.

        :param storages: A list of storage backends to chain together. The order
        determines the lookup order during retrieval, with the first hit being
        returned.
        """
        if not storages:
            raise ValueError("At least one storage must be provided")
        self.storages = storages

    def get(
        self,
        cache_key: CacheKey,
        return_expired: bool = False,
    ) -> CacheEntry[T] | None:
        for storage in self.storages:
            cache_entry: CacheEntry[T] | None = storage.get(
                cache_key,
                return_expired=return_expired,
            )
            if cache_entry:
                # Found in this storage, propagate to earlier storages
                self._propagate_to_earlier_storages(cache_entry, storage)
                return cache_entry
        return None

    def _propagate_to_earlier_storages(
        self,
        cache_entry: CacheEntry,
        found_in_storage: Storage,
    ) -> None:
        """
        Propagate a cache entry to all storages that come before the one where
        it was found. This ensures that future lookups will find the entry in
        the prioritized storages.
        """
        for storage in self.storages:
            if storage is found_in_storage:
                # Stop once we reach the storage where the entry was found
                break
            storage.store(cache_entry)

    def store(
        self,
        entry: CacheEntry,
    ) -> None:
        for storage in self.storages:
            storage.store(entry)


class InMemoryStorage(Storage):
    def __init__(self) -> None:
        self.cache: dict[CacheKey, CacheEntry] = {}

    def get(
        self,
        cache_key: CacheKey,
        return_expired: bool = False,
    ) -> CacheEntry[T] | None:
        cache_entry: CacheEntry[T] | None = self.cache.get(cache_key)
        if not cache_entry:
            return None

        if cache_entry.is_expired():
            del self.cache[cache_key]
            if return_expired:
                return cache_entry
            return None

        return cache_entry

    def store(
        self,
        entry: CacheEntry,
    ) -> None:
        self.cache[entry.cache_key] = entry


class DiskStorage(Storage):
    def __init__(self, directory: str):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, cache_key: CacheKey) -> Path:
        key_hash = hashlib.sha256(
            str((cache_key.package, cache_key.key)).encode()
        ).hexdigest()
        prefix = key_hash[:2]
        return self.directory / f"v{CacheEntry.VERSION}" / prefix / key_hash

    @staticmethod
    def _remove_empty_directories(file_path: Path) -> None:
        try:
            file_path.parent.rmdir()
            file_path.parent.parent.rmdir()
        except OSError:
            pass

    def _remove_cache_entry_on_disk(self, cache_entry: CacheEntry) -> None:
        file_path = self._get_file_path(cache_entry.cache_key)
        file_path.unlink()
        self._remove_empty_directories(file_path)

    def get(
        self,
        cache_key: CacheKey,
        return_expired: bool = False,
    ) -> CacheEntry[T] | None:
        file_path = self._get_file_path(cache_key)
        if not file_path.exists():
            return None

        with file_path.open("rb") as file:
            cache_entry: CacheEntry[T] = pickle.load(file)

        if cache_entry.is_expired():
            self._remove_cache_entry_on_disk(cache_entry)
            if return_expired:
                return cache_entry
            return None

        return cache_entry

    def store(
        self,
        entry: CacheEntry,
    ) -> None:
        file_path = self._get_file_path(entry.cache_key)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("wb") as file:
            pickle.dump(entry, file)


class RedisStorage(Storage):
    def __init__(
        self,
        redis_client: Redis,
        *,
        use_redis_ttl: bool = True,
        redis_key_infix: str | None = None,
    ):
        """
        Initialize a Redis-based storage for cache entries.

        :param redis_client: An instantiated Redis client
        :param use_redis_ttl: Whether to use the Redis TTL for cache entries. If
        `True`, expired entries will never be returned, i.e.
        `get(..., return_expired=True)` will always return `None` if the entry
        has expired.
        :param redis_key_infix: An optional infix to use for Redis keys to
        ensure the keys don't collide between multiple caches or outposts.
        """
        self.redis = redis_client
        self._use_redis_ttl = use_redis_ttl
        self._redis_key_infix = redis_key_infix

    def _get_redis_key(self, cache_key: CacheKey) -> str:
        """
        Generate a Redis key from a CacheKey.

        :param cache_key: The cache key to generate a Redis key for
        :return: A string key for use with Redis
        """
        key_hash = hashlib.sha256(
            str((cache_key.package, cache_key.key)).encode()
        ).hexdigest()

        infix = ""
        if self._redis_key_infix:
            infix = f"{self._redis_key_infix}:"

        return f"outpost:cache:{infix}v{CacheEntry.VERSION}:{key_hash}"

    def get(
        self,
        cache_key: CacheKey,
        return_expired: bool = False,
    ) -> CacheEntry[T] | None:
        """
        Retrieve a cache entry from Redis.

        :param cache_key: The key to retrieve the value for
        :param return_expired: Whether to return a key that has expired
        :return: A cache entry if found, otherwise None
        """
        redis_key = self._get_redis_key(cache_key)
        data: Any = self.redis.get(redis_key)

        if data is None:
            return None

        cache_entry: CacheEntry[T] = pickle.loads(data)

        if cache_entry.is_expired():
            self.redis.delete(redis_key)
            if return_expired:
                return cache_entry
            return None

        return cache_entry

    def store(
        self,
        entry: CacheEntry,
    ) -> None:
        """
        Store a cache entry in Redis.

        :param entry: The cache entry to store
        """
        redis_key = self._get_redis_key(entry.cache_key)
        data = pickle.dumps(entry)

        if self._use_redis_ttl and entry.ttl is not None and entry.added_at is not None:
            expiry_seconds = int(entry.ttl.total_seconds())
            if expiry_seconds > 0:
                self.redis.setex(redis_key, expiry_seconds, data)
            else:
                # If the entry is already expired, ensure we don't hold a
                # potentially old version in Redis anymore.
                self.redis.delete(redis_key)
        else:
            self.redis.set(redis_key, data)


class Cache:
    def __init__(self, storage: Storage):
        self.storage = storage

    def get(
        self,
        key: Hashable,
        default: T | None = None,
        *,
        package: str | None = None,
        return_expired: bool = False,
    ) -> CacheEntry[T] | None:
        """
        Retrieve a value from the cache.

        :param key: The key to retrieve the value for. This key must be hashable. The
                    key is unique within the package that you are invoking the cache
                    from.
        :param default: The value to return if the key is not found in the cache.
        :param package: The package that the key is unique within. If not provided, the
        :param return_expired: If True, return the cache entry even if it is expired.
        :return: A cache entry of the value from the cache if it was found. If the key
                 was not found in the cache, `None` is returned if no default value
                 was provided, otherwise a cache entry with the default value.
        """

        default_cache_entry = None
        if default is not None:
            default_cache_entry = CacheEntry(
                cache_key=CacheKey(
                    key=key,
                    package=package if package else get_caller_package(),
                ),
                value=cast(T, default),
                added_at=None,
                ttl=None,
            )

        cache_key = CacheKey(
            key=key,
            package=package if package else get_caller_package(),
        )

        cache_entry: CacheEntry[T] | None = self.storage.get(
            cache_key,
            return_expired=return_expired,
        )
        if cache_entry:
            return cache_entry
        return default_cache_entry

    def store(
        self,
        key: Hashable,
        value: T,
        *,
        package: str | None = None,
        ttl: timedelta | None = None,
    ) -> CacheEntry[T]:
        """
        Store a value in the cache.

        :param key: The key to store the value under. This key must be hashable. The key
                    is unique within the package that you are invoking the cache from
                    (i.e. the same key can be used by multiple checks across different
                    packages without conflicts).
        :param value: The value to store in the cache.
        :param package: The package that the key is unique within. If not provided, the
                        package of the caller is used.
        :param ttl: The time-to-live for the cache entry. If not provided, the entry
                    will never expire.
        :return: The cache entry that was stored.
        """

        cache_entry = CacheEntry(
            cache_key=CacheKey(
                key=key,
                package=package if package else get_caller_package(),
            ),
            value=value,
            added_at=datetime.now(tz=UTC),
            ttl=ttl,
        )

        self.storage.store(cache_entry)

        return cache_entry

    def memoize[R, **P](
        self,
        *,
        key: Hashable | None = None,
        key_generator: Callable[P, Hashable] | None = None,
        package: str | None = None,
        return_expired: bool = False,
        ttl: timedelta | None = None,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:  # pylint: disable=too-many-arguments
        if key and key_generator:
            raise ValueError("Only one of key or key_generator can be provided.")
        if not key and not key_generator:
            raise ValueError("Either key or key_generator must be provided.")

        package = package or get_caller_package()

        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            @functools.wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                cache_key = key
                if key_generator:
                    cache_key = key_generator(*args, **kwargs)

                cache_entry: CacheEntry[R] | None = self.get(
                    key=cache_key,
                    package=package,
                    return_expired=return_expired,
                )
                if cache_entry:
                    return cache_entry.value

                value = func(*args, **kwargs)
                self.store(
                    key=cache_key,
                    value=value,
                    package=package,
                    ttl=ttl,
                )
                return value

            return wrapper

        return decorator
