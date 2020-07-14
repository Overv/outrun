import os
from unittest import mock

from outrun.filesystem.filesystem import RemoteFileSystem
from outrun.filesystem.common import Attributes


def test_mount_callback():
    callback = mock.Mock()

    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, callback)

    assert not callback.called
    fs.init()
    assert callback.called


def test_open():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    mock_client.open.return_value = 9
    assert fs.open("a/b/c", 123) == 9
    mock_client.open.assert_called_with("a/b/c", 123)


def test_create():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    mock_client.create.return_value = 9
    assert fs.create("a/b/c", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 567) == 9
    mock_client.create.assert_called_with(
        "a/b/c", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 567
    )


def test_read():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    mock_client.read.return_value = b"abc"
    fs.read("file", 1234, 5678, 9)
    mock_client.read.assert_called_with(1234, 5678, 9)


def test_write():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    mock_client.write.return_value = 3
    assert fs.write("file", b"abc", 123, 456) == 3
    mock_client.write.assert_called_with(b"abc", 123, 456)


def test_lseek():
    if not hasattr(os, "SEEK_DATA"):
        return

    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    mock_client.lseek.return_value = 3
    assert fs.lseek("file", 123, 456, os.SEEK_DATA) == 3
    mock_client.lseek.assert_called_with(123, 456, os.SEEK_DATA)


def test_fsync():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.fsync("path", 123, False)
    mock_client.fsync.assert_called_with(123, False)
    fs.fsync("path", 123, True)
    mock_client.fsync.assert_called_with(123, True)


def test_flush():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.flush("path", 1337)
    mock_client.flush.assert_called_with(1337)


def test_truncate():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.truncate("path", 123, 456)
    mock_client.truncate.assert_called_with("path", 123, 456)


def test_release():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.release("path", 123)
    mock_client.release.assert_called_with(123)


def test_readdir():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    mock_client.readdir.return_value = [".", "..", "foo", "bar"]
    assert fs.readdir("dir") == [".", "..", "foo", "bar"]
    mock_client.readdir.assert_called_with("dir")


def test_readlink():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    mock_client.readlink.return_value = "foo"
    assert fs.readlink("link") == "foo"
    mock_client.readlink.assert_called_with("link")


def test_getattr():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    attribs = Attributes.from_stat(os.lstat(__file__))
    mock_client.getattr.return_value = attribs
    assert fs.getattr("foo", 123) == attribs.__dict__
    mock_client.getattr.assert_called_with("foo", 123)


def test_chmod():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.chmod("path", 123, 456)
    mock_client.chmod.assert_called_with("path", 123, 456)


def test_chown():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.chown("path", 123, 456, 789)
    mock_client.chown.assert_called_with("path", 123, 456, 789)


def test_utimens():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    # RPC serialization turns the tuple into a list, but we're not testing that here
    fs.utimens("path", 123, (1, 2))
    mock_client.utimens.assert_called_with("path", 123, (1, 2))


def test_link():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.link("target", "source")
    mock_client.link.assert_called_with("target", "source")


def test_symlink():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.symlink("target", "source")
    mock_client.symlink.assert_called_with("target", "source")


def test_mkdir():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.mkdir("path", 123)
    mock_client.mkdir.assert_called_with("path", 123)


def test_mknod():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.mknod("path", 123, 456)
    mock_client.mknod.assert_called_with("path", 123, 456)


def test_rename():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.rename("old", "new")
    mock_client.rename.assert_called_with("old", "new")


def test_unlink():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.unlink("path")
    mock_client.unlink.assert_called_with("path")


def test_rmdir():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    fs.rmdir("path")
    mock_client.rmdir.assert_called_with("path")


def test_statfs():
    mock_client = mock.Mock()
    fs = RemoteFileSystem(mock_client, None)

    stfs = {"a": 1, "b": 2, "c": 3}

    mock_client.statfs.return_value = stfs
    assert fs.statfs("/") == stfs
    mock_client.statfs.assert_called_with("/")
