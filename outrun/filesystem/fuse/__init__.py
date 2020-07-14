"""
Module with high-level bindings for FUSE 3.x.

Outrun comes with its own FUSE bindings because existing bindings didn't meet its needs:

* pyfuse only supports FUSE 2.x, so it can't support features like write-back caching.
* pyfuse3 has an interface that abstracts the high-level API too much, which makes it
needlessly difficult to implement a passthrough file system.
"""

import ctypes
from dataclasses import dataclass
import errno
import os
import sys
import threading
import time
import traceback
from typing import Callable, List, Optional, Tuple, Type

from outrun.logger import log
from .fuse import (
    FUSE_ARGS_INIT,
    FUSE_CAP_WRITEBACK_CACHE,
    fuse_config_p,
    fuse_conn_info_p,
    fuse_file_info_p,
    fuse_fill_dir_t,
    fuse_main_real,
    fuse_operations,
    fuse_opt_add_arg,
    fuse_opt_parse,
    fuse_opt_proc_t,
    stat,
    stat_p,
    statvfs_t_p,
    timespec_p,
    UTIME_NOW,
    UTIME_OMIT,
)


@dataclass
class FuseConfig:
    """
    FUSE options and capabilities to enable.

    See the FUSE documentation about mount options and connection capabilities for more
    information:

    * https://man7.org/linux/man-pages/man8/mount.fuse.8.html
    * https://libfuse.github.io/doxygen/fuse__common_8h.html
    """

    default_permissions: bool = True
    auto_unmount: bool = True

    use_ino: bool = False

    kernel_cache: bool = False
    auto_cache: bool = False
    writeback_cache: bool = False


class Operations:
    """
    Base class for a FUSE file system.

    File systems should inherit this class and implement all of the file system
    functions they wish to support.

    The implementation should expect functions to be invoked simultaneously from an
    arbitrary number of threads. For example, read() should be reentrant for a single
    file handle by either using locks or avoiding stateful operations like a separate
    seek and read.

    Functions can return errors by raising the built-in OSError exception with the errno
    set. That means that exceptions from functions like os.stat work out of the box. If
    exceptions are raised manually then care must be taken to ensure that they have the
    errno set:

        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT))

    The passthrough example for libfuse shows what kind of behaviour is expected from
    each of these functions:

        https://github.com/libfuse/libfuse/blob/master/example/passthrough_fh.c
    """

    def init(self) -> None:
        """Initialize data after the file system has been mounted."""

    def destroy(self) -> None:
        """Clean up after the file system has been unmounted."""

    def getattr(self, path: str, fh: Optional[int]) -> dict:
        """
        Retrieve the attributes of a file system entry.

        The function should return a dict with st_* keys that correspond to stat data.
        """
        raise NotImplementedError()

    def readlink(self, path: str) -> str:
        """Read the target of a symlink."""
        raise NotImplementedError()

    def readdir(self, path: str) -> List[str]:
        """List the contents of a directory."""
        raise NotImplementedError()

    def mknod(self, path: str, mode: int, rdev: int) -> None:
        """Create a special or ordinary file."""
        raise NotImplementedError()

    def mkdir(self, path: str, mode: int) -> None:
        """Create a directory."""
        raise NotImplementedError()

    def symlink(self, path: str, target: str) -> None:
        """Create a symlink pointing to the target."""
        raise NotImplementedError()

    def unlink(self, path: str) -> None:
        """Remove a file system entry."""
        raise NotImplementedError()

    def rmdir(self, path: str) -> None:
        """Remove an empty directory."""
        raise NotImplementedError()

    def rename(self, old: str, new: str) -> None:
        """Rename a file system entry."""
        raise NotImplementedError()

    def link(self, path: str, target: str) -> None:
        """Create a hard link for a file."""
        raise NotImplementedError()

    def chmod(self, path: str, fh: Optional[int], mode: int) -> None:
        """Change the mode of a file system entry."""
        raise NotImplementedError()

    def chown(self, path: str, fh: Optional[int], uid: int, gid: int) -> None:
        """Change the ownership of a file system entry."""
        raise NotImplementedError()

    def truncate(self, path: str, fh: Optional[int], size: int) -> None:
        """Truncate a file (that may not exist yet)."""
        raise NotImplementedError()

    def utimens(self, path: str, fh: Optional[int], times: Tuple[int, int]) -> None:
        """
        Change the modification and access time of a file system entry.

        Times are specified in nanoseconds.
        """
        raise NotImplementedError()

    def open(self, path: str, flags: int) -> int:
        """Open a file."""
        raise NotImplementedError()

    def create(self, path: str, flags: int, mode: int) -> int:
        """Create a file."""
        raise NotImplementedError()

    def read(self, path: str, fh: int, offset: int, size: int) -> bytes:
        """Read from a file."""
        raise NotImplementedError()

    def write(self, path: str, fh: int, offset: int, data: bytes) -> int:
        """Write to a file."""
        raise NotImplementedError()

    def statfs(self, path: str) -> dict:
        """
        Retrieve information about the file system.

        The function should return a dict with f_* keys that correspond to statvfs data.
        """
        raise NotImplementedError()

    def release(self, path: str, fh: int) -> None:
        """Close a file handle."""
        raise NotImplementedError()

    def flush(self, path: str, fh: int) -> None:
        """
        Flush a file's attributes and contents.

        Called after every close of an open file.
        """

    def fsync(self, path: str, fh: int, datasync: bool) -> None:
        """Synchronize a file's attributes or contents."""

    def lseek(self, path: str, fh: int, offset: int, whence: int) -> int:
        """
        Seek within a file.

        This function is used for sparse files where whence can be SEEK_DATA or
        SEEK_HOLE.
        """
        raise NotImplementedError()


class FUSE:
    """
    File system wrapper class that handles the FUSE connection.

    This class starts the FUSE main loop and serves as the layer between the C callbacks
    and the Operations interface.
    """

    def __init__(self, operations: Operations, config: FuseConfig):
        """Specify the operations and configuration for FUSE."""

        self._operations = operations
        self._config = config

    def mount(self, name: str, mount_path: str) -> int:
        """Mount the FUSE file system at the specified path with a given name."""

        # File system name and mount path arguments for FUSE
        argv = (ctypes.POINTER(ctypes.c_char) * 2)()
        argv[:] = [
            ctypes.create_string_buffer(name.encode(errors="surrogateescape")),
            ctypes.create_string_buffer(mount_path.encode(errors="surrogateescape")),
        ]
        args = FUSE_ARGS_INIT(2, argv)

        fuse_opt_parse(
            ctypes.pointer(args), None, None, ctypes.cast(None, fuse_opt_proc_t)
        )

        # Additional options
        if self._config.auto_unmount:
            fuse_opt_add_arg(ctypes.pointer(args), b"-oauto_unmount")

        if self._config.default_permissions:
            fuse_opt_add_arg(ctypes.pointer(args), b"-odefault_permissions")

        fuse_opt_add_arg(ctypes.pointer(args), b"-f")

        # Operation callbacks
        operations = fuse_operations()

        operations.init = self._wrap_operation("init", self._op_init)
        operations.destroy = self._wrap_operation("destroy", self._op_destroy)
        operations.getattr = self._wrap_operation("getattr", self._op_getattr)
        operations.readlink = self._wrap_operation("readlink", self._op_readlink)
        operations.readdir = self._wrap_operation("readdir", self._op_readdir)
        operations.mknod = self._wrap_operation("mknod", self._op_mknod)
        operations.mkdir = self._wrap_operation("mkdir", self._op_mkdir)
        operations.symlink = self._wrap_operation("symlink", self._op_symlink)
        operations.unlink = self._wrap_operation("unlink", self._op_unlink)
        operations.rmdir = self._wrap_operation("rmdir", self._op_rmdir)
        operations.rename = self._wrap_operation("rename", self._op_rename)
        operations.link = self._wrap_operation("link", self._op_link)
        operations.chmod = self._wrap_operation("chmod", self._op_chmod)
        operations.chown = self._wrap_operation("chown", self._op_chown)
        operations.truncate = self._wrap_operation("truncate", self._op_truncate)
        operations.utimens = self._wrap_operation("utimens", self._op_utimens)
        operations.open = self._wrap_operation("open", self._op_open)
        operations.create = self._wrap_operation("create", self._op_create)
        operations.read = self._wrap_operation("read", self._op_read)
        operations.write = self._wrap_operation("write", self._op_write)
        operations.statfs = self._wrap_operation("statfs", self._op_statfs)
        operations.release = self._wrap_operation("release", self._op_release)
        operations.flush = self._wrap_operation("flush", self._op_flush)
        operations.fsync = self._wrap_operation("fsync", self._op_fsync)
        operations.lseek = self._wrap_operation("lseek", self._op_lseek)

        # FUSE main loop
        return fuse_main_real(
            args.argc,
            args.argv,
            ctypes.pointer(operations),
            ctypes.sizeof(operations),
            None,
        )

    def _wrap_operation(self, name: str, fn: Callable) -> Callable:
        """Wrap an operation callback to capture self and handle exceptions."""

        def wrapper(*args, **kwargs):
            # Support coverage.py within FUSE threads.
            if hasattr(threading, "_trace_hook"):
                sys.settrace(getattr(threading, "_trace_hook"))

            try:
                res = fn(*args, **kwargs)

                if res is None:
                    res = 0

                return res
            except OSError as e:
                # FUSE expects an error to be returned as negative errno.
                if e.errno:
                    return -e.errno
                else:
                    return -errno.EIO
            except NotImplementedError:
                log.debug(f"fuse::{name}() not implemented!")

                return -errno.ENOSYS
            except Exception:
                log.warning(f"fuse::{name}() raised an unexpected exception:")
                log.warning(traceback.format_exc())

                return -errno.EIO

        return self._typeof(fuse_operations, name)(wrapper)

    @staticmethod
    def _typeof(struct: ctypes.Structure, field: str) -> Type:
        """Return the type of a field in a ctypes Structure."""

        for name, t in getattr(struct, "_fields_"):
            if name == field:
                return t

        raise ValueError(f"cannot determine type of nonexistent field {field}")

    def _op_init(self, conn: fuse_conn_info_p, config: fuse_config_p) -> None:
        """
        Handle fuse_operations.init.

        Sets up the FUSE config and connection options.
        """

        if self._config.writeback_cache:
            conn.contents.want |= FUSE_CAP_WRITEBACK_CACHE

        # Don't request capabilities that the kernel doesn't support.
        conn.contents.want &= conn.contents.capable

        config.contents.auto_cache = 1 if self._config.auto_cache else 0
        config.contents.kernel_cache = 1 if self._config.kernel_cache else 0
        config.contents.use_ino = 1 if self._config.use_ino else 0

        self._operations.init()

    def _op_destroy(self, _private_data: ctypes.c_void_p) -> None:
        """Handle fuse_operations.destroy."""
        self._operations.destroy()

    def _op_getattr(self, path: bytes, stbuf: stat_p, fi: fuse_file_info_p) -> None:
        """Handle fuse_operations.getattr."""
        stat_values = self._operations.getattr(
            path.decode(), fi.contents.fh if fi else None
        )

        ctypes.memset(stbuf, 0, ctypes.sizeof(stat))

        for key, value in stat_values.items():
            if hasattr(stbuf.contents, key):
                setattr(stbuf.contents, key, value)
            elif key in ("st_atime_ns", "st_mtime_ns", "st_ctime_ns"):
                timespec = {
                    "st_atime_ns": stbuf.contents.st_atim,
                    "st_mtime_ns": stbuf.contents.st_mtim,
                    "st_ctime_ns": stbuf.contents.st_ctim,
                }[key]

                timespec.tv_sec, timespec.tv_nsec = divmod(int(value), 10 ** 9)

    def _op_readlink(
        self, path: bytes, buf: ctypes.POINTER(ctypes.c_char), size: int
    ) -> None:
        """Handle fuse_operations.readlink."""
        link = self._operations.readlink(path.decode())

        # Size includes space for the null terminator
        truncated_link = link.encode()[: size - 1]
        link_buffer = ctypes.create_string_buffer(truncated_link)

        ctypes.memmove(buf, link_buffer, len(link_buffer))

    def _op_readdir(
        self,
        path: bytes,
        buf: ctypes.c_void_p,
        filler: fuse_fill_dir_t,
        _offset: int,
        _fi: fuse_file_info_p,
        _flags: int,
    ) -> None:
        """
        Handle fuse_operations.readdir.

        Offsets and FUSE_FILL_DIR_PLUS are currently not supported.
        """
        entries = self._operations.readdir(path.decode())

        for entry in entries:
            if filler(buf, entry.encode(), None, 0, 0):
                break

    def _op_mknod(self, path: bytes, mode: int, rdev: int) -> None:
        """Handle fuse_operations.mknod."""
        self._operations.mknod(path.decode(), mode, rdev)

    def _op_mkdir(self, path: bytes, mode: int) -> None:
        """Handle fuse_operations.mkdir."""
        self._operations.mkdir(path.decode(), mode)

    def _op_unlink(self, path: bytes) -> None:
        """Handle fuse_operations.unlink."""
        self._operations.unlink(path.decode())

    def _op_rmdir(self, path: bytes) -> None:
        """Handle fuse_operations.rmdir."""
        self._operations.rmdir(path.decode())

    def _op_symlink(self, target: bytes, path: bytes) -> None:
        """Handle fuse_operations.symlink."""
        self._operations.symlink(path.decode(), target.decode())

    def _op_rename(self, old: bytes, new: bytes, flags: int) -> None:
        """
        Handle fuse_operations.rename.

        There is currently no support for renameat2 flags.
        """
        if flags:
            raise OSError(errno.EINVAL, os.strerror(errno.EINVAL))

        self._operations.rename(old.decode(), new.decode())

    def _op_link(self, target: bytes, path: bytes) -> None:
        """Handle fuse_operations.link."""
        self._operations.link(path.decode(), target.decode())

    def _op_chmod(self, path: bytes, mode: int, fi: fuse_file_info_p) -> None:
        """Handle fuse_operations.chmod."""
        self._operations.chmod(path.decode(), fi.contents.fh if fi else None, mode)

    def _op_chown(self, path: bytes, uid: int, gid: int, fi: fuse_file_info_p) -> None:
        """Handle fuse_operations.chown."""
        self._operations.chown(path.decode(), fi.contents.fh if fi else None, uid, gid)

    def _op_truncate(self, path: bytes, size: int, fi: fuse_file_info_p) -> None:
        """Handle fuse_operations.truncate."""
        self._operations.truncate(path.decode(), fi.contents.fh if fi else None, size)

    def _op_utimens(self, path: bytes, ts: timespec_p, fi: fuse_file_info_p) -> None:
        """
        Handle fuse_operations.utimens.

        Special values like UTIME_OMIT and UTIME_NOW are automatically handled here and
        don't need to be taken care of by the file system implementation.
        """
        times = [0, 0]

        if ts:
            attr = self._operations.getattr(
                path.decode(), fi.contents.fh if fi else None
            )
            fields = ["st_atime_ns", "st_mtime_ns"]

            for i in range(2):
                if ts[i].tv_nsec == UTIME_OMIT:
                    # Preserve existing timestamp by retrieving it and passing it along
                    times[i] = attr.get(fields[i], time.time_ns())
                elif ts[i].tv_nsec == UTIME_NOW:
                    times[i] = time.time_ns()
                else:
                    times[i] = ts[i].tv_sec * 10 ** 9 + ts[i].tv_nsec
        else:
            times = [time.time_ns(), time.time_ns()]

        self._operations.utimens(
            path.decode(), fi.contents.fh if fi else None, tuple(times)
        )

    def _op_create(self, path: bytes, mode: int, fi: fuse_file_info_p) -> None:
        """Handle fuse_operations.create."""
        fi.contents.fh = self._operations.create(path.decode(), fi.contents.flags, mode)

    def _op_open(self, path: bytes, fi: fuse_file_info_p) -> None:
        """Handle fuse_operations.open."""
        fi.contents.fh = self._operations.open(path.decode(), fi.contents.flags)

    def _op_read(
        self,
        path: bytes,
        buf: ctypes.POINTER(ctypes.c_char_p),
        size: int,
        offset: int,
        fi: fuse_file_info_p,
    ) -> int:
        """Handle fuse_operations.read."""
        data = self._operations.read(path.decode(), fi.contents.fh, offset, size)
        actual_size = len(data)

        assert actual_size <= size

        ctypes.memmove(buf, data, actual_size)

        return actual_size

    def _op_write(
        self,
        path: bytes,
        buf: ctypes.POINTER(ctypes.c_char),
        size: int,
        offset: int,
        fi: fuse_file_info_p,
    ) -> int:
        """Handle fuse_operations.write."""
        data = ctypes.string_at(buf, size)
        return self._operations.write(path.decode(), fi.contents.fh, offset, data)

    def _op_statfs(self, path: bytes, stbuf: statvfs_t_p) -> None:
        """Handle fuse_operations.statfs."""
        stat_values = self._operations.statfs(path.decode())

        ctypes.memset(stbuf, 0, ctypes.sizeof(statvfs_t_p))

        for key, value in stat_values.items():
            if hasattr(stbuf.contents, key):
                setattr(stbuf.contents, key, value)

    def _op_release(self, path: bytes, fi: fuse_file_info_p) -> None:
        """Handle fuse_operations.release."""
        self._operations.release(path.decode(), fi.contents.fh)

    def _op_flush(self, path: bytes, fi: fuse_file_info_p) -> None:
        """Handle fuse_operations.flush."""
        self._operations.flush(path.decode(), fi.contents.fh)

    def _op_fsync(self, path: bytes, datasync: int, fi: fuse_file_info_p) -> None:
        """Handle fuse_operations.fsync."""
        self._operations.fsync(path.decode(), fi.contents.fh, datasync != 0)

    def _op_lseek(
        self, path: bytes, offset: int, whence: int, fi: fuse_file_info_p
    ) -> int:
        """Handle fuse_operations.lseek."""
        return self._operations.lseek(path.decode(), fi.contents.fh, offset, whence)
