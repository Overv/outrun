import os
import stat

import pytest

from outrun.filesystem.service import LocalFileSystemService


@pytest.fixture
def fs():
    return LocalFileSystemService()


def test_open_modes(fs, tmp_path):
    with pytest.raises(FileNotFoundError):
        fs.open(str(tmp_path / "write.txt"), os.O_WRONLY)

    fh = fs.open(str(tmp_path / "write.txt"), os.O_CREAT | os.O_WRONLY)

    try:
        with pytest.raises(OSError):
            fs.read(fh, 0, 2)
    finally:
        fs.release(fh)


def test_file_reads(fs, tmp_path):
    with open(tmp_path / "read.txt", "wb") as f:
        f.write(b"abcdef")

    fh = fs.open(str(tmp_path / "read.txt"), os.O_RDONLY)

    try:
        assert fs.read(fh, 0, 2) == b"ab"
        assert fs.read(fh, 0, 3) == b"abc"
        assert fs.read(fh, 1, 2) == b"bc"
        assert fs.read(fh, 0, 1024) == b"abcdef"
    finally:
        fs.release(fh)


def test_file_writes(fs, tmp_path):
    fh = fs.create(
        str(tmp_path / "write.txt"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o777
    )

    fs.write(fh, 1, b"bc")
    fs.write(fh, 0, b"a")

    fs.release(fh)

    with open(tmp_path / "write.txt", "rb") as f:
        assert f.read() == b"abc"


def test_file_truncate(fs, tmp_path):
    with open(tmp_path / "small", "wb"):
        pass

    with open(tmp_path / "large", "wb"):
        pass

    fs.truncate(str(tmp_path / "small"), None, 10)
    fs.truncate(str(tmp_path / "large"), None, 100)

    assert os.lstat(tmp_path / "small").st_size == 10
    assert os.lstat(tmp_path / "large").st_size == 100


def test_lseek(fs, tmp_path):
    if not hasattr(os, "SEEK_DATA"):
        return

    fd = fs.open(tmp_path / "file", os.O_RDWR | os.O_CREAT)
    try:
        fs.write(fd, 1024 * 1024, b"abc")
        assert fs.lseek(fd, 0, os.SEEK_DATA) > 0
        assert fs.lseek(fd, 0, os.SEEK_HOLE) == 0
    finally:
        fs.release(fd)


def test_readdir(fs, tmp_path):
    assert sorted(fs.readdir(str(tmp_path))) == sorted([".", ".."])

    with open(tmp_path / "a", "wb"):
        pass

    os.link(tmp_path / "a", tmp_path / "b")

    os.makedirs(tmp_path / "c" / "d")

    assert sorted(fs.readdir(str(tmp_path))) == sorted([".", "..", "a", "b", "c"])


def test_readlink(fs, tmp_path):
    os.symlink(tmp_path / "a", tmp_path / "b")

    assert fs.readlink(str(tmp_path / "b")) == str(tmp_path / "a")


def test_getattr(fs, tmp_path):
    with pytest.raises(FileNotFoundError):
        fs.getattr(str(tmp_path / "link"), None)

    with open(tmp_path / "file", "wb"):
        pass

    os.symlink(tmp_path / "file", tmp_path / "link")

    assert os.path.islink(tmp_path / "link")
    assert os.path.isfile(tmp_path / "file")

    assert fs.getattr(__file__, None).st_mtime == os.stat(__file__).st_mtime


def test_chmod(fs, tmp_path):
    with open(tmp_path / "file", "wb"):
        pass

    fs.chmod(str(tmp_path / "file"), None, 0o000)

    assert stat.S_IMODE(os.lstat(tmp_path / "file").st_mode) == 0o000

    fs.chmod(str(tmp_path / "file"), None, 0o777)

    assert stat.S_IMODE(os.lstat(tmp_path / "file").st_mode) == 0o777


def test_utimens_explicit(fs, tmp_path):
    with open(tmp_path / "file", "wb"):
        pass

    fs.utimens(str(tmp_path / "file"), None, [1, 2])

    st = os.stat(tmp_path / "file")
    assert st.st_atime_ns == 1
    assert st.st_mtime_ns == 2


def test_link(fs, tmp_path):
    with open(tmp_path / "file", "wb"):
        pass

    with pytest.raises(FileNotFoundError):
        fs.link(str(tmp_path / "link"), str(tmp_path / "nonexistent"))

    fs.link(str(tmp_path / "link"), str(tmp_path / "file"))

    assert os.path.isfile(tmp_path / "link")


def test_symlink(fs, tmp_path):
    fs.symlink(str(tmp_path / "link"), str(tmp_path / "nonexistent"))

    assert os.path.islink(tmp_path / "link")
    assert os.readlink(tmp_path / "link") == str(tmp_path / "nonexistent")


def test_mkdir(fs, tmp_path):
    fs.mkdir(str(tmp_path / "dir"), 0o777)

    with pytest.raises(FileExistsError):
        fs.mkdir(str(tmp_path / "dir"), 0o777)

    assert os.path.isdir(tmp_path / "dir")


def test_rename(fs, tmp_path):
    with open(tmp_path / "file", "wb") as f:
        f.write(b"abc")

    st = os.lstat(tmp_path / "file")

    fs.rename(str(tmp_path / "file"), str(tmp_path / "file2"))

    assert not os.path.exists(tmp_path / "file")
    assert os.path.isfile(tmp_path / "file2")

    with open(tmp_path / "file2", "rb") as f:
        assert f.read() == b"abc"

    assert os.lstat(tmp_path / "file2").st_mtime == st.st_mtime


def test_unlink(fs, tmp_path):
    with pytest.raises(FileNotFoundError):
        fs.unlink(str(tmp_path / "nonexistent"))

    os.makedirs(tmp_path / "dir")

    with pytest.raises(IsADirectoryError):
        fs.unlink(str(tmp_path / "dir"))

    with open(tmp_path / "file", "wb"):
        pass

    fs.unlink(str(tmp_path / "file"))
    assert not os.path.exists(tmp_path / "file")


def test_rmdir(fs, tmp_path):
    with pytest.raises(FileNotFoundError):
        fs.rmdir(str(tmp_path / "nonexistent"))

    with open(tmp_path / "file", "wb"):
        pass

    with pytest.raises(NotADirectoryError):
        fs.rmdir(str(tmp_path / "file"))

    os.makedirs(tmp_path / "dir")
    fs.rmdir(str(tmp_path / "dir"))
    assert not os.path.exists(tmp_path / "dir")


def test_statfs(fs):
    statvfs = fs.statfs("/")

    assert isinstance(statvfs, dict)
    assert "f_blocks" in statvfs
