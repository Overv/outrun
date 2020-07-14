"""
Module with an RPC service that facilitates file system caching and prefetching.

It is designed to be used in conjunction with caching.RemoteCache.
"""

import collections
from dataclasses import replace
import hashlib
import os
import stat
import threading
from typing import Any, Dict, List, Optional, Set, Tuple

import outrun.constants as constants
from outrun.filesystem.caching.cache import FileContents, Metadata, PrefetchEntry
import outrun.filesystem.caching.prefetching as prefetching
from outrun.filesystem.common import Attributes
from outrun.logger import log


class LocalCacheService:
    """RPC service that provides bulk I/O calls for caching and prefetching."""

    def __init__(self):
        """Instantiate new caching and prefetching RPC service."""
        super().__init__()

        self._fetched_lock = threading.Lock()
        self._fetched_metadata: Set[str] = set()
        self._fetched_contents: Set[str] = set()

        self._prefetchable_paths: Optional[List[str]] = None

    def get_metadata(self, path: str) -> Metadata:
        """Retrieve all metadata of a file system entry, or the resulting I/O error."""
        metadata = Metadata()

        try:
            metadata.attr = Attributes.from_stat(os.lstat(path))

            if stat.S_ISLNK(metadata.attr.st_mode):
                metadata.link = os.readlink(path)
        except Exception as e:
            metadata.error = e

        with self._fetched_lock:
            self._fetched_metadata.add(path)

        return metadata

    @staticmethod
    def _significant_meta(meta: Metadata) -> Any:
        """
        Turn metadata into a derivative to check if it has changed significantly.

        Significance means that it warrants sending the change to the remote machine to
        update its cache. For example, a changed file access time is not very important
        in the grand scheme of things.
        """
        significant_attribs = (
            replace(meta.attr, st_atime_ns=None) if meta.attr else None
        )
        comparable_error = (type(meta.error), meta.error.args) if meta.error else None

        return (significant_attribs, comparable_error, meta.link)

    def get_changed_metadata(
        self, cached_metadata: Dict[str, Metadata]
    ) -> Dict[str, Metadata]:
        """Retrieve file system metadata that has changed since it was last cached."""
        changed_metadata = {}

        for path, metadata in cached_metadata.items():
            new_metadata = self.get_metadata(path)

            if self._significant_meta(new_metadata) != self._significant_meta(metadata):
                changed_metadata[path] = new_metadata

        return changed_metadata

    def readfile(self, path: str) -> FileContents:
        """Read the contents of the specified file."""
        with open(path, "rb") as f:
            data = f.read()

        with self._fetched_lock:
            self._fetched_contents.add(path)

        return FileContents.from_data(data)

    def readfile_conditional(self, path: str, checksum: str) -> Optional[FileContents]:
        """Read the contents of the specified file if the checksum has changed."""
        new_contents = self.readfile(path)

        if checksum != new_contents.checksum:
            return new_contents
        else:
            return None

    @staticmethod
    def get_app_specific_machine_id() -> str:
        """Derive a unique machine/installation identifier."""
        with open("/etc/machine-id", "rb") as f:
            confidential_id = f.read().strip()

        # http://man7.org/linux/man-pages/man5/machine-id.5.html
        # Based on sd_id128_get_machine_app_specific implementation
        return hashlib.sha256(confidential_id + constants.APP_ID).hexdigest()[:32]

    def mark_previously_fetched_contents(self, paths: List[str]) -> None:
        """
        Mark contents of specified paths as having already been fetched.

        This is used by the remote to indicate that the specified files should not be
        prefetched as their contents are already in the remote cache. Previously fetched
        metadata is already marked separately by get_changed_metadata().
        """
        with self._fetched_lock:
            self._fetched_contents.update(paths)

    def set_prefetchable_paths(self, paths: Optional[List[str]]) -> None:
        """
        Set the base paths that may be prefetched.

        If this function is not called or paths is set to None, then all paths may be
        prefetched.
        """
        self._prefetchable_paths = paths

    def _is_prefetchable(self, path: str) -> bool:
        """Return whether the specified path is prefetchable."""
        if self._prefetchable_paths is None:
            return True
        else:
            return any(
                os.path.commonpath([path, p]) == p for p in self._prefetchable_paths
            )

    def get_metadata_prefetch(self, path: str) -> Tuple[Metadata, List[PrefetchEntry]]:
        """Retrieve metadata of an entry and prefetch related data."""
        base = self.get_metadata(path)

        try:
            suggestions = prefetching.file_access(path)
            return base, self._resolve_prefetches(suggestions)
        except Exception as e:
            # Avoid complete I/O failure if prefetching breaks
            log.warning(f"prefetching for get_metadata({path}) failed: {e}")
            return base, []

    def readfile_prefetch(self, path: str) -> Tuple[FileContents, List[PrefetchEntry]]:
        """Retrieve file contents and prefetch related data."""
        base = self.readfile(path)

        try:
            suggestions = prefetching.file_read(path)
            return base, self._resolve_prefetches(suggestions)
        except Exception as e:
            # Avoid complete I/O failure if prefetching breaks
            log.warning(f"prefetching for readfile({path}) failed: {e}")
            return base, []

    def _resolve_prefetches(
        self, suggestions: List[prefetching.PrefetchSuggestion]
    ) -> List[PrefetchEntry]:
        # Group prefetch suggestions by path
        suggestions_by_path = collections.defaultdict(list)

        for suggestion in suggestions:
            suggestions_by_path[suggestion.path].append(suggestion)

        # Resolve suggestions into actual prefetches
        prefetches: List[PrefetchEntry] = []

        for path, path_suggestions in suggestions_by_path.items():
            # Don't prefetch things outside the prefetchable paths
            if not self._is_prefetchable(path):
                continue

            prefetch_contents = any(s.contents for s in path_suggestions)

            # Don't prefetch contents that have already been fetched, or metadata that
            # has already been fetched.
            with self._fetched_lock:
                if prefetch_contents and path in self._fetched_contents:
                    continue
                elif not prefetch_contents and path in self._fetched_metadata:
                    continue

            entry = PrefetchEntry(
                path=path, metadata=self.get_metadata(path), contents=None
            )

            # Try to prefetch contents if requested and available
            if prefetch_contents and not entry.metadata.error:
                try:
                    entry.contents = self.readfile(path)
                except Exception as e:
                    log.warning(f"failed to prefetch contents of {path}: {e}")

            prefetches.append(entry)

        return prefetches
