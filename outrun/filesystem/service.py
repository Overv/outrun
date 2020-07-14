"""Module that exposes local file system calls as an RPC service."""

import os
import os.path
import stat
from typing import List, Optional

from outrun.filesystem.common import Attributes


class LocalFileSystemService:
    """RPC service that exposes local file system operations."""

    #
    # File operations
    #

    @staticmethod
    def open(path: str, flags: int) -> int:
        return os.open(path, flags)

    @staticmethod
    def create(path: str, flags: int, mode: int) -> int:
        return os.open(path, flags, mode)

    @staticmethod
    def read(fh: int, offset: int, size: int) -> bytes:
        return os.pread(fh, size, offset)

    @staticmethod
    def write(fh: int, offset: int, data: bytes) -> int:
        return os.pwrite(fh, data, offset)

    @staticmethod
    def lseek(fh: int, offset: int, whence: int) -> int:
        return os.lseek(fh, offset, whence)

    @staticmethod
    def fsync(fh: int, datasync: bool) -> None:
        if datasync:
            os.fdatasync(fh)
        else:
            os.fsync(fh)

    @staticmethod
    def flush(fh: int) -> None:
        os.close(os.dup(fh))

    @staticmethod
    def truncate(path: str, fh: Optional[int], size: int) -> None:
        if fh and os.truncate in os.supports_fd:
            os.truncate(fh, size)
        else:
            os.truncate(path, size)

    @staticmethod
    def release(fh: int) -> None:
        os.close(fh)

    #
    # Metadata access
    #

    @staticmethod
    def readdir(path: str) -> List[str]:
        return [".", ".."] + os.listdir(path)

    @staticmethod
    def readlink(path: str) -> str:
        return os.readlink(path)

    @staticmethod
    def getattr(path: str, fh: Optional[int]) -> Attributes:
        if fh and os.stat in os.supports_fd:
            st = os.stat(fh)
        else:
            if os.stat in os.supports_follow_symlinks:
                st = os.stat(path, follow_symlinks=False)
            else:
                st = os.stat(path)

        return Attributes.from_stat(st)

    #
    # Metadata modification
    #

    @staticmethod
    def chmod(path: str, fh: Optional[int], mode: int) -> None:
        if fh and os.chmod in os.supports_fd:
            os.chmod(fh, mode)
        else:
            if os.chmod in os.supports_follow_symlinks:
                os.chmod(path, mode, follow_symlinks=False)
            else:
                os.chmod(path, mode)

    @staticmethod
    def chown(path: str, fh: Optional[int], uid: int, gid: int) -> None:
        if fh and os.chown in os.supports_fd:
            os.chown(fh, uid, gid)
        else:
            if os.chown in os.supports_follow_symlinks:
                os.chown(path, uid, gid, follow_symlinks=False)
            else:
                os.chown(path, uid, gid)

    @staticmethod
    def utimens(path: str, fh: Optional[int], times: List[int]) -> None:
        # times turns into a list due to the RPC encoding
        times_tpl = (times[0], times[1])

        if fh and os.utime in os.supports_fd:
            os.utime(fh, ns=times_tpl)
        else:
            if os.utime in os.supports_follow_symlinks:
                os.utime(path, ns=times_tpl, follow_symlinks=False)
            else:
                os.utime(path, ns=times_tpl)

    #
    # File system structure
    #

    @staticmethod
    def link(path: str, target: str) -> None:
        os.link(target, path)

    @staticmethod
    def symlink(path: str, target: str) -> None:
        os.symlink(target, path)

    @staticmethod
    def mkdir(path: str, mode: int) -> None:
        os.mkdir(path, mode)

    @staticmethod
    def mknod(path: str, mode: int, rdev: int) -> None:
        if stat.S_ISFIFO(mode):
            os.mkfifo(path, mode)
        else:
            os.mknod(path, mode, rdev)

    @staticmethod
    def rename(old: str, new: str) -> None:
        os.rename(old, new)

    @staticmethod
    def unlink(path: str) -> None:
        os.unlink(path)

    @staticmethod
    def rmdir(path: str) -> None:
        os.rmdir(path)

    #
    # Miscellaneous
    #

    @staticmethod
    def statfs(path: str) -> dict:
        st = os.statvfs(path)
        return {name: getattr(st, name) for name in dir(st) if name.startswith("f_")}
