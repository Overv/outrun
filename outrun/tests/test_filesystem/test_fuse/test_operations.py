from contextlib import contextmanager
import errno
import os
from unittest import mock
import subprocess
import stat
import threading
import time
from typing import List, Optional, Tuple

import pytest

from outrun.filesystem.fuse import FuseConfig, FUSE, Operations


class LoopbackFS(Operations):
    def getattr(self, path: str, fh: Optional[int]) -> dict:
        if fh and os.stat in os.supports_fd:
            st = os.stat(fh)
        else:
            if os.stat in os.supports_follow_symlinks:
                st = os.stat(path, follow_symlinks=False)
            else:
                st = os.stat(path)

        return {k: getattr(st, k) for k in dir(st) if k.startswith("st_")}

    def readlink(self, path: str) -> str:
        return os.readlink(path)

    def readdir(self, path: str) -> List[str]:
        return [".", ".."] + os.listdir(path)

    def mknod(self, path: str, mode: int, rdev: int) -> None:
        if stat.S_ISFIFO(mode):
            os.mkfifo(path, mode)
        else:
            os.mknod(path, mode, rdev)

    def mkdir(self, path: str, mode: int) -> None:
        os.mkdir(path, mode)

    def symlink(self, path: str, target: str) -> None:
        os.symlink(target, path)

    def unlink(self, path: str) -> None:
        os.unlink(path)

    def rmdir(self, path: str) -> None:
        os.rmdir(path)

    def rename(self, old: str, new: str) -> None:
        os.rename(old, new)

    def link(self, path: str, target: str) -> None:
        os.link(target, path)

    def chmod(self, path: str, fh: Optional[int], mode: int) -> None:
        if fh and os.chmod in os.supports_fd:
            os.chmod(fh, mode)
        else:
            if os.chmod in os.supports_follow_symlinks:
                os.chmod(path, mode, follow_symlinks=False)
            else:
                os.chmod(path, mode)

    def chown(self, path: str, fh: Optional[int], uid: int, gid: int) -> None:
        if fh and os.chown in os.supports_fd:
            os.chown(fh, uid, gid)
        else:
            if os.chown in os.supports_follow_symlinks:
                os.chown(path, uid, gid, follow_symlinks=False)
            else:
                os.chown(path, uid, gid)

    def truncate(self, path: str, fh: Optional[int], size: int) -> None:
        if fh and os.truncate in os.supports_fd:
            os.truncate(fh, size)
        else:
            os.truncate(path, size)

    def utimens(self, path: str, fh: Optional[int], times: Tuple[int, int]) -> None:
        if fh and os.utime in os.supports_fd:
            os.utime(fh, ns=times)
        else:
            if os.utime in os.supports_follow_symlinks:
                os.utime(path, ns=times, follow_symlinks=False)
            else:
                os.utime(path, ns=times)

    def open(self, path: str, flags: int) -> int:
        return os.open(path, flags)

    def create(self, path: str, flags: int, mode: int) -> int:
        return os.open(path, flags, mode)

    def read(self, path: str, fh: int, offset: int, size: int) -> bytes:
        return os.pread(fh, size, offset)

    def write(self, path: str, fh: int, offset: int, data: bytes) -> int:
        return os.pwrite(fh, data, offset)

    def statfs(self, path: str) -> dict:
        st = os.statvfs(path)
        return {k: getattr(st, k) for k in dir(st) if k.startswith("f_")}

    def release(self, path: str, fh: int) -> None:
        os.close(fh)

    def flush(self, path: str, fh: int) -> None:
        os.close(os.dup(fh))

    def fsync(self, path: str, fh: int, datasync: bool) -> None:
        if datasync:
            os.fdatasync(fh)
        else:
            os.fsync(fh)

    def lseek(self, path: str, fh: int, offset: int, whence: int) -> int:
        return os.lseek(fh, offset, whence)


@contextmanager
def mount_fs(mount_path, fs, config):
    def do_mount():
        fuse = FUSE(fs, config)
        fuse.mount("test", str(mount_path))

    t = threading.Thread(target=do_mount)
    t.start()

    while True:
        # Command will fail until mount has completed.
        try:
            subprocess.check_call(["mountpoint", mount_path])
        except subprocess.CalledProcessError:
            pass
        else:
            break

        if not t.is_alive():
            raise RuntimeError("file system mount failed")

    try:
        yield
    finally:
        subprocess.check_output(["fusermount", "-u", str(mount_path)])

        # Wait for unmount to really finish
        while True:
            try:
                subprocess.check_call(["mountpoint", mount_path])
            except Exception:
                break


@pytest.fixture
def loopback_fs():
    return mock.Mock(wraps=LoopbackFS())


@pytest.fixture
def loopback_fs_root(tmp_path):
    mount_path = tmp_path / "mount"
    root_path = tmp_path / "root"

    os.makedirs(mount_path)
    os.makedirs(root_path)

    config = FuseConfig()
    config.writeback_cache = True

    with mount_fs(mount_path, LoopbackFS(), config):
        yield mount_path / root_path.relative_to("/")


@pytest.mark.fuse
def test_init(loopback_fs, tmp_path):
    with mount_fs(tmp_path, loopback_fs, FuseConfig()):
        loopback_fs.init.assert_called_once()


@pytest.mark.fuse
def test_destroy(loopback_fs, tmp_path):
    with mount_fs(tmp_path, loopback_fs, FuseConfig()):
        pass

    loopback_fs.destroy.assert_called_once()


@pytest.mark.fuse
def test_os_error(loopback_fs, tmp_path):
    loopback_fs.readdir.side_effect = OSError(
        errno.EADDRINUSE, os.strerror(errno.EADDRINUSE)
    )

    with mount_fs(tmp_path, loopback_fs, FuseConfig()):
        with pytest.raises(OSError) as e:
            os.listdir(tmp_path)

        assert e.value.errno == errno.EADDRINUSE


@pytest.mark.fuse
def test_unimplemented(loopback_fs, tmp_path):
    loopback_fs.readdir.side_effect = NotImplementedError()

    with mount_fs(tmp_path, loopback_fs, FuseConfig()):
        with pytest.raises(OSError) as e:
            os.listdir(tmp_path)

        assert e.value.errno == errno.ENOSYS


@pytest.mark.fuse
def test_unknown_exception(loopback_fs, tmp_path):
    loopback_fs.readdir.side_effect = RuntimeError()

    with mount_fs(tmp_path, loopback_fs, FuseConfig()):
        with pytest.raises(OSError) as e:
            os.listdir(tmp_path)

        assert e.value.errno == errno.EIO


@pytest.mark.fuse
def test_getattr(loopback_fs_root):
    (loopback_fs_root / "dir").mkdir()
    (loopback_fs_root / "file").touch()

    with pytest.raises(FileNotFoundError):
        os.lstat(loopback_fs_root / "nonexistent")

    (loopback_fs_root / "dir").is_dir()
    (loopback_fs_root / "file").is_file()


@pytest.mark.fuse
def test_getattr_fd(loopback_fs_root):
    fd = os.open(loopback_fs_root / "file", os.O_RDWR | os.O_CREAT)

    try:
        os.unlink(loopback_fs_root / "file")

        os.write(fd, b"abc")
        os.fsync(fd)

        assert os.fstat(fd).st_size == 3

        with pytest.raises(FileNotFoundError):
            os.lstat(loopback_fs_root / "file")
    finally:
        os.close(fd)


@pytest.mark.fuse
def test_readlink(loopback_fs_root):
    os.symlink("nonexistent", loopback_fs_root / "link")
    assert os.readlink(loopback_fs_root / "link") == "nonexistent"


@pytest.mark.fuse
def test_readdir(loopback_fs_root):
    assert os.listdir(loopback_fs_root) == []

    (loopback_fs_root / "foo").touch()
    (loopback_fs_root / "bar").mkdir()

    assert set(os.listdir(loopback_fs_root)) == set(["foo", "bar"])


@pytest.mark.fuse
def test_mknod(loopback_fs_root):
    os.mkfifo(loopback_fs_root / "foo", 0o600)
    assert (loopback_fs_root / "foo").is_fifo()


@pytest.mark.fuse
def test_mkdir(loopback_fs_root):
    (loopback_fs_root / "dir").mkdir()

    assert (loopback_fs_root / "dir").is_dir()

    with pytest.raises(FileExistsError):
        (loopback_fs_root / "dir").mkdir()


@pytest.mark.fuse
def test_symlink(loopback_fs_root):
    (loopback_fs_root / "link").symlink_to("nonexistent")
    assert os.readlink(loopback_fs_root / "link") == "nonexistent"


@pytest.mark.fuse
def test_unlink(loopback_fs_root):
    with pytest.raises(FileNotFoundError):
        (loopback_fs_root / "file").unlink()

    (loopback_fs_root / "file").touch()
    (loopback_fs_root / "file").unlink()


@pytest.mark.fuse
def test_rmdir(loopback_fs_root):
    (loopback_fs_root / "file").touch()

    with pytest.raises(NotADirectoryError):
        (loopback_fs_root / "file").rmdir()

    (loopback_fs_root / "dir").mkdir()
    (loopback_fs_root / "dir").rmdir()


@pytest.mark.fuse
def test_rename(loopback_fs_root):
    (loopback_fs_root / "file").touch()
    (loopback_fs_root / "file").rename(loopback_fs_root / "file2")
    assert (loopback_fs_root / "file2").is_file()


@pytest.mark.fuse
def test_link(loopback_fs_root):
    (loopback_fs_root / "file").touch()
    (loopback_fs_root / "file").link_to(loopback_fs_root / "link")

    with pytest.raises(FileNotFoundError):
        (loopback_fs_root / "nonexistent").link_to(loopback_fs_root / "link")


@pytest.mark.fuse
def test_chmod(loopback_fs_root):
    (loopback_fs_root / "file").touch()
    (loopback_fs_root / "file").chmod(0o600)


@pytest.mark.fuse
def test_chown(loopback_fs_root):
    (loopback_fs_root / "file").touch()
    os.chown(loopback_fs_root / "file", os.getuid(), os.getgid())


@pytest.mark.fuse
def test_truncate(loopback_fs_root):
    (loopback_fs_root / "file").touch()
    os.truncate(loopback_fs_root / "file", 123)
    assert (loopback_fs_root / "file").stat().st_size == 123


@pytest.mark.fuse
def test_utimens(loopback_fs_root):
    (loopback_fs_root / "file").touch()

    os.utime(loopback_fs_root / "file", ns=(123, 456))
    assert (loopback_fs_root / "file").stat().st_atime_ns == 123
    assert (loopback_fs_root / "file").stat().st_mtime_ns == 456

    (loopback_fs_root / "file").touch()

    assert (loopback_fs_root / "file").stat().st_mtime >= time.time() - 1.0


@pytest.mark.fuse
def test_read_write(loopback_fs_root):
    (loopback_fs_root / "file").write_bytes(b"abcdef")

    fd = os.open(loopback_fs_root / "file", os.O_RDWR)

    try:
        os.pwrite(fd, b"xxx", 2)
        assert os.pread(fd, 3, 1) == b"bxx"
        os.fsync(fd)
    finally:
        os.close(fd)


@pytest.mark.fuse
def test_statfs(loopback_fs_root):
    subprocess.check_call(["df", loopback_fs_root])


@pytest.mark.fuse
def test_lseek(loopback_fs_root):
    if not hasattr(os, "SEEK_DATA"):
        return

    fd = os.open(loopback_fs_root / "file", os.O_WRONLY | os.O_CREAT)
    try:
        os.pwrite(fd, b"abc", 1024 * 1024)
    finally:
        os.close(fd)

    # Note that the file must be reopened like this for lseek to work when the writeback
    # cache is enabled.

    fd = os.open(loopback_fs_root / "file", os.O_RDONLY)
    try:
        assert os.lseek(fd, 0, os.SEEK_DATA) > 0
        assert os.lseek(fd, 0, os.SEEK_HOLE) == 0
    finally:
        os.close(fd)
