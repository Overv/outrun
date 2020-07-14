"""
Module that extends the FUSE file system to use caching for file metadata and contents.

It is designed to be used in conjunction with caching.LocalFileSystemService.
"""

from __future__ import annotations

import errno
import os
from typing import Callable, Dict, Optional

import outrun.filesystem as filesystem
from outrun.filesystem.caching.cache import RemoteCache
import outrun.rpc as rpc


class RemoteCachedFileSystem(filesystem.RemoteFileSystem):
    """
    Extension of RemoteFileSystem that selectively caches file system operations.

    Caching and prefetching are only used for directories that are considered safe to
    cache. A directory is safe to cache if it contains files that change only relatively
    infrequently, meaning that they can be assumed to be read-only during an outrun
    session.

    Good examples are /usr/lib and /usr/bin that contain large application dependencies
    like executables and shared libraries, and will generally only change as a result of
    system/package updates.

    Some calls like readdir() are not cached at all because they are infrequently used
    in practice.
    """

    def __init__(
        self,
        client: rpc.Client,
        mount_callback: Optional[Callable],
        cache: RemoteCache,
    ) -> None:
        """Instantiate cached file system with its cache."""
        super().__init__(client, mount_callback)

        self._cache = cache

    def destroy(self) -> None:
        """Persist the cache to disk as part of unmounting the file system."""
        self._cache.save()

    def getattr(self, path: str, fh: Optional[int]) -> Dict:
        """Retrieve (cached) file system entry attributes."""
        if not self._cache.is_cacheable(path):
            return super().getattr(path, fh)

        return self._cache.get_metadata(path).attr.__dict__

    def readlink(self, path: str) -> str:
        """Read the (cached) path referenced by a symlink."""
        if not self._cache.is_cacheable(path):
            return super().readlink(path)

        link = self._cache.get_metadata(path).link

        # Path does not point to a symlink
        if not link:
            raise OSError(errno.EINVAL)

        return link

    def open(self, path, flags) -> int:
        """
        Open a (cached) file for reading or writing.

        Cached files can only be opened for reading.
        """
        if not self._cache.is_cacheable(path):
            return super().open(path, flags)

        return self._cache.open_contents(path, flags)

    def read(self, path: str, fh: int, offset: int, size: int) -> bytes:
        """Read a chunk of bytes from a (cached) file."""
        if not self._cache.is_cacheable(path):
            return super().read(path, fh, offset, size)

        return os.pread(fh, size, offset)

    def release(self, path: str, fh: int) -> None:
        """Close a (cached) file."""
        if not self._cache.is_cacheable(path):
            super().release(path, fh)
            return

        os.close(fh)

    def flush(self, path: str, fh: int) -> None:
        """
        Flush changes to a file to disk.

        This is a no-op if the file descriptor refers to a cached file. (It is still
        called by FUSE for read-only files in an attempt to update metadata.)
        """
        if not self._cache.is_cacheable(path):
            super().flush(path, fh)
