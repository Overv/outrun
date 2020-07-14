"""Data structures used by multiple file system caching and prefetching components."""

from __future__ import annotations

import collections
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import threading
from typing import Any, Dict, Iterator, Optional

import lz4.frame

from outrun.filesystem.common import Attributes


@dataclass
class Metadata:
    """
    Container of metadata related to accessing file system entries.

    Used to store all metadata required to answer calls like access() and readlink(). It
    may alternatively contain the I/O error that should be returned upon these calls.
    """

    attr: Optional[Attributes] = None
    link: Optional[str] = None
    error: Optional[Exception] = None


@dataclass
class FileContents:
    """
    Container for the full contents of a file.

    File contents are compressed to reduce bandwidth usage, which speeds up file access
    under the assumption that the entire file is read. LZ4 was chosen because it's fast
    enough to deliver a good trade-off between bandwidth reduction and
    compression/decompression latency.
    """

    compressed_data: bytes
    checksum: str
    size: int

    @staticmethod
    def from_data(data: bytes) -> FileContents:
        """Wrap raw file data into a FileContents object."""
        return FileContents(
            compressed_data=lz4.frame.compress(data),
            checksum=hashlib.sha256(data).hexdigest(),
            size=len(data),
        )

    @property
    def data(self) -> bytes:
        """Retrieve and decompress the original file data."""
        return lz4.frame.decompress(self.compressed_data)


@dataclass
class PrefetchEntry:
    """
    Container for a prefetched file system entry.

    Used by the caching and prefetching RPC service to push additional entries back to
    the remote machine.
    """

    path: str
    metadata: Metadata
    contents: Optional[FileContents]


class LockIndex:
    """
    Collection of mutexes to lock critical sections by arbitrary values.

    Its use case is to lock critical sections based on unpredictable input values, like
    arbitrary file descriptors. Locks are automatically garbage collected when no longer
    in use (no threads in the critical section and none waiting to enter).
    """

    def __init__(self) -> None:
        """Instantiate a LockIndex."""
        self._global_lock = threading.Lock()

        self._locks: Dict[Any, threading.Lock] = collections.defaultdict(threading.Lock)
        self._lock_users: Dict[Any, int] = collections.defaultdict(int)

    @contextmanager
    def lock(self, key: Any, blocking=True) -> Iterator[bool]:
        """Lock a critical section based on the specified key."""
        # Retrieve lock and increment user count
        with self._global_lock:
            self._lock_users[key] += 1
            lock = self._locks[key]

        acquired = lock.acquire(blocking)

        try:
            yield acquired
        finally:
            if acquired:
                lock.release()

            # Decrement user count and delete lock if there are none left
            with self._global_lock:
                self._lock_users[key] -= 1

                if self._lock_users[key] == 0:
                    del self._lock_users[key]
                    del self._locks[key]

    @property
    def lock_count(self):
        """Return the number of locks currently in use."""
        with self._global_lock:
            return len(self._locks)
