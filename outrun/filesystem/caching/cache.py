"""Module that implements the cache and prefetch logic for the caching file system."""

from __future__ import annotations

from contextlib import contextmanager
import dataclasses
from dataclasses import dataclass, field
import os
import stat
import time
from typing import (
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
)
import uuid

import fasteners

from outrun.filesystem.caching.common import (
    FileContents,
    LockIndex,
    Metadata,
    PrefetchEntry,
)
from outrun.logger import log
import outrun.rpc as rpc


@dataclass
class ContentsBlob:
    """
    Information about cached file contents.

    Storage points to the file where the cached contents are stored.

    The size is stored for LRU cache cleaning purposes.

    The checksum is used to check with the local machine if the contents of the file
    have changed since it was cached.

    Lastly, "dirty" is set if the contents have potentially changed and the checksum
    should be checked on its next read. It is generally set if the file metadata (e.g.
    its size or modification timestamp) have changed.
    """

    storage: str
    size: int
    checksum: str
    dirty: bool = False


@dataclass
class CacheEntry:
    """
    Cached metadata and possibly contents for a file system entry on a specific machine.

    The path uses the format "machine-id:/dir/subdir/file" which allows files to be
    cached separately per local machine. This is useful if two machines have a different
    /usr/lib/libpthread.a, for example.

    Metadata contains the last known metadata and should be refreshed every time a new
    outrun session starts.

    Last access is the timestamp for when the cache entry was last retrieved and is used
    for LRU cleaning.

    The last updated timestamp is reset every time the cache entry (metadata or
    contents) have changed. It is used to resolve merge conflicts between different
    versions of the cache e.g. when the in-memory cache is merged with the disk cache.

    Contents references optionally cached file contents.
    """

    path: str
    meta: Metadata

    last_access: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)

    contents: Optional[ContentsBlob] = None

    def newer_than(self, other: Optional[CacheEntry] = None) -> bool:
        """Check if this cache entry is newer than the other one."""
        return other is None or self.last_update > other.last_update


class RemoteCache:
    """
    Class that handles all of the file system caching and prefetching logic.

    The cache is designed around the idea that bandwidth is cheap and latency is very
    expensive. This means that we'd much rather transfer a little bit too much data in
    bulk rather than using many calls to pull in the specific data that's really needed.

    In practice this translates to decisions like the following:

    * Metadata freshness is checked with a bulk request after the cache is loaded.
    * Contents of cached files are always completely transferred upfront.
    * Metadata always includes both os.lstat() and os.readlink() results because it is
    very likely for a symlink to be read upon access.

    The cache refresh logic is written to assume that files will never change during an
    outrun session and cached files are exposed as read-only. This is because it's
    designed to be used to cache (large) dependencies for running programs, like those
    in /usr/lib and /usr/bin. These files are only expected to change as a result of
    system updates.

    When the cache is initialized it will try to load itself an index on the disk. If
    this index exists then all stored metadata entries are checked against the local
    machine to see if any have changed since the last session. Any changed metadata is
    returned and written to the cache. If metadata has changed for a cache entry that
    also had its contents cached then the contents are marked as dirty and will have
    the checksum compared with the local machine the next time they are requested.

    Once a session has finished, the disk cache is locked and reread to check if any
    other sessions have updated it in the meanwhile. All of these changes are merged
    with the in-memory cache and update timestamps are used to resolve conflicts. After
    that the cache is LRU cleaned and finally saved to the disk.

    All CacheEntry objects are serialized to a file as JSON. Cached contents are written
    to files in the disk cache directory. Any files that are no longer referenced by the
    index (either due to LRU, merges or corruption) are automatically garbage collected.
    """

    # Directories that are allowed to be cached, generally because they contain common
    # dependencies and can be considered read-only during the session.
    DEFAULT_CACHEABLE_PATHS = (
        "/bin",
        "/sbin",
        "/lib",
        "/lib32",
        "/lib64",
        "/etc",
        "/opt",
        "/usr",
    )

    def __init__(
        self,
        base_path: str,
        machine_id: str,
        client: rpc.Client,
        prefetch: bool,
        max_entries: int,
        max_size: int,
        cacheable_paths: Iterable[str] = DEFAULT_CACHEABLE_PATHS,
    ):
        """Instantiate cache management for the specified local machine."""
        self._base_path = base_path
        self._machine_id = machine_id
        self._client = client
        self._prefetch = prefetch
        self._max_entries = max_entries
        self._max_size = max_size
        self._cacheable_paths = cacheable_paths

        self._entries: Dict[str, CacheEntry] = {}
        self._entry_locks = LockIndex()

        # Initialize cache storage
        self._encoding = rpc.Encoding(CacheEntry)

        os.makedirs(self._cache_contents_path, exist_ok=True)

        # Set up prefetching paths
        self._client.set_prefetchable_paths(self._cacheable_paths)

    def count(self):
        """Return the number of cached entries."""
        return len(self._entries)

    def size(self):
        """Return the total number of bytes of contents being cached."""
        return sum([e.contents.size for e in self._entries.values() if e.contents])

    def is_cacheable(self, path: str) -> bool:
        """Return whether the specified path is cachable (and prefetchable)."""
        return any(os.path.commonpath([path, p]) == p for p in self._cacheable_paths)

    def get_metadata(self, path: str) -> Metadata:
        """Retrieve cached metadata for the file system entry at the given path."""
        with self._lock_entry(path) as entry:  # type: CacheEntry
            meta = entry.meta

            if meta.error:
                raise meta.error
            elif not meta.attr:
                raise ValueError("entry is missing attributes")

            # Strip all write permissions from cached entries
            return dataclasses.replace(meta, attr=meta.attr.as_readonly())

    def open_contents(self, path: str, flags: int) -> int:
        """Open a file descriptor for the cached contents of the specified file."""
        with self._lock_entry(path) as entry:  # type: CacheEntry
            if entry.contents is None or entry.contents.dirty:
                entry.contents = self._update_contents(entry)

            while True:
                # Handle cases where the cached contents file has disappeared from disk
                try:
                    return os.open(entry.contents.storage, flags)
                except FileNotFoundError:
                    entry.contents = self._update_contents(entry, force=True)

    def _update_contents(self, entry: CacheEntry, force: bool = False) -> ContentsBlob:
        """
        Update the cached contents of a file forcefully or if it has changed.

        The file contents are updated if the checksum has changed.

        Caching file contents is a trade-off between bandwidth and latency. We could
        cache contents in blocks to avoid having to download an entire shared library
        if only a part of its functionality is used, for example, but in practice this
        would massively increase the total latency of read operations compared to just
        downloading the entire file upfront. In addition to that it's also much easier
        to take advantage of compression when transferring the entire file.
        """
        contents: FileContents

        if not force and entry.contents:
            contents = self._client.readfile_conditional(
                entry.path, entry.contents.checksum
            )

            # File contents have not changed, so nothing to do
            if not contents:
                entry.contents.dirty = False
                return entry.contents
        else:
            if self._prefetch:
                contents, prefetches = self._client.readfile_prefetch(entry.path)
                self._try_store_prefetches(entry.path, prefetches)
            else:
                contents = self._client.readfile(entry.path)

        return self._save_contents(contents)

    def _save_contents(self, contents: FileContents) -> ContentsBlob:
        """Save cached contents to a file on disk."""
        storage = os.path.join(self._cache_contents_path, uuid.uuid4().hex)

        with open(storage, "wb") as f:
            f.write(contents.data)

        return ContentsBlob(
            storage=storage, size=contents.size, checksum=contents.checksum,
        )

    @contextmanager
    def _lock_entry(self, path: str) -> Generator[CacheEntry, None, None]:
        """
        Acquire exclusive access to the cache entry at the specified path.

        The cache entry is automatically initialized if it didn't exist yet, along with
        any prefetched related entries.
        """
        key = self._entry_key(path)

        with self._entry_locks.lock(key):
            if key not in self._entries:
                if self._prefetch:
                    metadata, prefetches = self._client.get_metadata_prefetch(path)

                    self._entries[key] = CacheEntry(path=path, meta=metadata)
                    self._try_store_prefetches(path, prefetches)
                else:
                    self._entries[key] = CacheEntry(
                        path=path, meta=self._client.get_metadata(path)
                    )
            else:
                self._entries[key].last_access = time.time()

            yield self._entries[key]

    def _try_store_prefetches(self, path: str, prefetches: List[PrefetchEntry]) -> None:
        """
        Create cache entries for prefetched entries if possible.

        Prefetched entries are saved on a best-effort basis depending on whether a lock
        can be acquired for the cache entry. This is to prevent deadlocks from occurring
        when multiple threads are trying to save overlapping prefetched data.

        There is one exception to this rule and that's where we're prefetching data for
        the file that originally triggered the prefetching. In that case it should
        already be locked in the calling function and we can ignore the inability to
        acquire a lock.
        """
        for prefetch in prefetches:
            key = self._entry_key(prefetch.path)

            with self._entry_locks.lock(key, False) as acquired:
                if not acquired and prefetch.path != path:
                    continue

                if prefetch.contents:
                    log.debug(f"storing prefetched contents for {prefetch.path}")
                else:
                    log.debug(f"storing prefetched metadata for {prefetch.path}")

                # Create cache entry for prefetch if it doesn't exist yet and assign
                # special timestamp to indicate that it has not been accessed yet.
                if key not in self._entries:
                    self._entries[key] = CacheEntry(
                        path=prefetch.path, meta=prefetch.metadata, last_access=0
                    )

                entry = self._entries[key]

                # If prefetch contains contents and we don't have cached contents yet,
                # then save them.
                if prefetch.contents and (not entry.contents or entry.contents.dirty):
                    entry.contents = self._save_contents(prefetch.contents)

    def _entry_key(self, path: str) -> str:
        """Determine the full cache key for a path on a specific local machine."""
        return f"{self._machine_id}:{path}"

    def load(self) -> None:
        """Initialize the in-memory cache from the disk cache."""
        with fasteners.InterProcessLock(self._cache_lock_path):
            self._entries = self._read_disk_entries()

    def save(self, merge_disk_cache=True) -> None:
        """
        Update the disk cache from the in-memory cache.

        If there is already a disk cache then it is read first to merge any new entries
        into the in-memory cache. This handles the case where a different outrun session
        has written new cache entries to disk in the background. Conflicts are handled
        by keeping the most recently updated entry.

        The LRU cleanup runs after this merge has completed and deletes entries and
        cached contents until the cache is below the specified limits again.

        Files with cached contents that are no longer referenced by the cache afterwards
        are deleted from the disk.
        """
        with fasteners.InterProcessLock(self._cache_lock_path):
            if merge_disk_cache:
                # Load latest cache entries in disk cache
                disk_entries = {}

                try:
                    disk_entries = self._read_disk_entries()
                except FileNotFoundError:
                    log.debug("no disk cache to merge with")
                except Exception as e:
                    log.error(f"not merging with existing disk cache: {e}")

                # Merge them with in-memory cache
                for key, disk_entry in disk_entries.items():
                    if disk_entry.newer_than(self._entries.get(key)):
                        self._entries[key] = disk_entry

            # LRU pass
            self._lru_cleanup()

            # Delete cached contents that are no longer referenced
            self._garbage_collect_blobs()

            with open(self._cache_index_path, "w") as f:
                self._encoding.dump_json(self._entries, f)

    @property
    def _cache_index_path(self) -> str:
        """Return the path to the disk cache index."""
        return os.path.join(self._base_path, "index.json")

    @property
    def _cache_lock_path(self) -> str:
        """Return the path to the disk cache index lock file."""
        return os.path.join(self._base_path, "index.lock")

    @property
    def _cache_contents_path(self) -> str:
        """Return the path to the contents cache directory."""
        return os.path.join(self._base_path, "contents")

    def _read_disk_entries(self) -> Dict[str, CacheEntry]:
        """Deserialize cache entries from the disk cache index."""
        with open(self._cache_index_path, "r") as f:
            return self._encoding.load_json(f)

    def _lru_cleanup(self) -> None:
        """
        Delete cache entries and cached contents until they're below limits.

        The entries that have been last accessed the longest time ago are deleted first
        until the total number of entries is below the max entries limit again. If the
        total size of the cached contents is still too high after this then some of the
        cached contents for the remaining cache entries are also deleted.
        """
        oldest_entries: List[Tuple[str, CacheEntry]] = list(self._entries.items())
        oldest_entries.sort(key=lambda tuple: tuple[1].last_access)

        entry_count = len(oldest_entries)
        contents_size = sum([e.contents.size for _, e in oldest_entries if e.contents])

        for key, entry in oldest_entries:
            if contents_size > self._max_size and entry.contents:
                contents_size -= entry.contents.size
                entry.contents = None

            if entry_count > self._max_entries:
                entry_count -= 1
                del self._entries[key]

    def _garbage_collect_blobs(self) -> None:
        """Delete cached contents blobs on disk that are no longer referenced."""
        orphans = {
            os.path.join(self._cache_contents_path, fn)
            for fn in os.listdir(self._cache_contents_path)
        }

        for entry in self._entries.values():
            # Only persisted files need to be cleaned up
            if entry.contents and isinstance(entry.contents.storage, str):
                try:
                    orphans.remove(entry.contents.storage)
                except KeyError:
                    # This may happen if a persisted file has disappeared from the disk
                    pass

        for path in orphans:
            try:
                os.remove(path)
            except FileNotFoundError:
                # Race condition where blob has already been removed
                pass

    def sync(self) -> None:
        """
        Synchronize the cache with the current state of the local machine.

        This is implemented by sending all cached metadata back to the local machine and
        having it compare the entries with the current metadata on disk. Any entries
        that have meaningfully changed (everything aside from last access timestamp) are
        returned and updated. Additionally, the local machine is informed of the
        contents in the remote cache to ensure that there are no superfluous prefetches.

        This sounds inefficient, but in practice this is much faster than checking the
        freshness of cache entries one-by-one upon first access because it avoids the
        latency overhead.
        """
        cached_metadata = {
            entry.path: entry.meta
            for key, entry in self._entries.items()
            if key.startswith(self._machine_id)
        }

        if len(cached_metadata) == 0:
            return

        changed_metadata: Dict[str, Metadata]
        changed_metadata = self._client.get_changed_metadata(cached_metadata)

        for path, new_metadata in changed_metadata.items():
            log.debug(f"updating metadata cache for {path}")

            with self._lock_entry(path) as entry:  # type: CacheEntry
                entry.meta = new_metadata
                entry.last_update = time.time()

                if entry.contents:
                    # Delete cached contents if entry is no longer an existent file
                    if entry.meta.error:
                        entry.contents = None
                    elif entry.meta.attr and not stat.S_ISREG(entry.meta.attr.st_mode):
                        entry.contents = None
                    else:
                        entry.contents.dirty = True

        # In addition to syncing metadata, also mark content as having been cached
        if self._prefetch:
            self._client.mark_previously_fetched_contents(
                [
                    entry.path
                    for entry in self._entries.values()
                    if entry.contents and not entry.contents.dirty
                ]
            )
