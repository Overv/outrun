"""Module that contains the remote file system that simply forwards all calls."""

from typing import Callable, List, Optional, Tuple

from outrun.filesystem.fuse import Operations
import outrun.rpc as rpc


class RemoteFileSystem(Operations):
    """Class that implements a FUSE network file system that runs on RPC calls."""

    def __init__(self, client: rpc.Client, mount_callback: Optional[Callable]):
        """Instantiate file system with RPC client."""
        self._client = client
        self._mount_callback = mount_callback

    def init(self) -> None:
        """File system has been successfully mounted by FUSE."""
        if self._mount_callback is not None:
            self._mount_callback()

    #
    # File operations
    #

    def open(self, path: str, flags: int) -> int:
        return self._client.open(path, flags)

    def create(self, path: str, flags: int, mode: int) -> int:
        return self._client.create(path, flags, mode)

    def read(self, path: str, fh: int, offset: int, size: int) -> bytes:
        return self._client.read(fh, offset, size)

    def write(self, path: str, fh: int, offset: int, data: bytes) -> int:
        return self._client.write(fh, offset, data)

    def lseek(self, path: str, fh: int, offset: int, whence: int) -> int:
        return self._client.lseek(fh, offset, whence)

    def fsync(self, path: str, fh: int, datasync: bool) -> None:
        self._client.fsync(fh, datasync)

    def flush(self, path: str, fh: int) -> None:
        self._client.flush(fh)

    def truncate(self, path: str, fh: Optional[int], size: int) -> None:
        self._client.truncate(path, fh, size)

    def release(self, path: str, fh: int) -> None:
        self._client.release(fh)

    #
    # Metadata access
    #

    def readdir(self, path: str) -> List[str]:
        return self._client.readdir(path)

    def readlink(self, path: str) -> str:
        return self._client.readlink(path)

    def getattr(self, path: str, fh: Optional[int]) -> dict:
        return self._client.getattr(path, fh).__dict__

    #
    # Metadata modification
    #

    def chmod(self, path: str, fh: Optional[int], mode: int) -> None:
        self._client.chmod(path, fh, mode)

    def chown(self, path: str, fh: Optional[int], uid: int, gid: int) -> None:
        self._client.chown(path, fh, uid, gid)

    def utimens(self, path: str, fh: Optional[int], times: Tuple[int, int]) -> None:
        self._client.utimens(path, fh, times)

    #
    # File system structure
    #

    def link(self, path: str, target: str) -> None:
        self._client.link(path, target)

    def symlink(self, path: str, target: str) -> None:
        self._client.symlink(path, target)

    def mkdir(self, path: str, mode: int) -> None:
        self._client.mkdir(path, mode)

    def mknod(self, path: str, mode: int, rdev: int) -> None:
        self._client.mknod(path, mode, rdev)

    def rename(self, old: str, new: str) -> None:
        self._client.rename(old, new)

    def unlink(self, path: str) -> None:
        self._client.unlink(path)

    def rmdir(self, path: str) -> None:
        self._client.rmdir(path)

    #
    # Miscellaneous
    #

    def statfs(self, path: str) -> dict:
        return self._client.statfs(path)
