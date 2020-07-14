import errno
import os
import stat
from unittest import mock

import pytest

from outrun.filesystem.service import LocalFileSystemService
from outrun.filesystem.caching.service import LocalCacheService
from outrun.filesystem.caching.filesystem import RemoteCachedFileSystem
from outrun.filesystem.caching.cache import RemoteCache


def create_cache(tmp_path, **override_args):
    base_args = dict(
        base_path=str(tmp_path / "cache"),
        machine_id="machine",
        client=LocalCacheService(),
        prefetch=False,
        max_entries=1024,
        max_size=1024 * 1024,
        cacheable_paths=["/"],
    )

    final_args = {**base_args, **override_args}

    return RemoteCache(**final_args)


def create_remote_file_system(tmp_path, **override_args):
    base_args = dict(
        client=LocalFileSystemService(),
        mount_callback=None,
        cache=create_cache(tmp_path),
    )

    final_args = {**base_args, **override_args}

    return RemoteCachedFileSystem(**final_args)


def test_cacheable_paths(tmp_path):
    (tmp_path / "cache").mkdir()

    (tmp_path / "cached").mkdir()
    (tmp_path / "notcached").mkdir()

    mock_client = mock.Mock()
    mock_client.get_metadata.return_value = LocalCacheService().get_metadata("/")

    fs = create_remote_file_system(
        tmp_path,
        client=mock_client,
        cache=create_cache(
            tmp_path, client=mock_client, cacheable_paths=[str(tmp_path / "cached")]
        ),
    )

    fs.getattr(str(tmp_path / "cached" / "a"), None)
    fs.getattr(str(tmp_path / "cached" / "b"), None)
    fs.getattr(str(tmp_path / "cached" / "a"), None)

    assert mock_client.get_metadata.call_count == 2

    # Should not even be retrieved through cache
    fs.getattr(str(tmp_path / "notcached" / "a"), 123)
    fs.getattr(str(tmp_path / "notcached" / "b"), 456)
    fs.getattr(str(tmp_path / "notcached" / "a"), 789)

    assert mock_client.get_metadata.call_count == 2


def test_cached_readlink(tmp_path):
    (tmp_path / "cache").mkdir()
    (tmp_path / "cached").mkdir()

    os.symlink("a", tmp_path / "cached/b")

    mock_client = mock.Mock()
    mock_client.get_metadata.side_effect = LocalCacheService().get_metadata

    fs = create_remote_file_system(
        tmp_path,
        client=mock_client,
        cache=create_cache(
            tmp_path, client=mock_client, cacheable_paths=[str(tmp_path / "cached")]
        ),
    )

    with pytest.raises(FileNotFoundError):
        fs.getattr(str(tmp_path / "cached" / "a"), None)

    with pytest.raises(FileNotFoundError):
        fs.readlink(str(tmp_path / "cached" / "a"))

    assert mock_client.get_metadata.call_count == 1

    with pytest.raises(OSError) as e:
        fs.readlink(str(tmp_path / "cached"))

    assert e.value.args == (errno.EINVAL,)

    assert fs.readlink(str(tmp_path / "cached/b")) == "a"


def test_uncached_readlink(tmp_path):
    (tmp_path / "cache").mkdir()
    os.symlink("bar", tmp_path / "foo")

    fs = create_remote_file_system(
        tmp_path, cache=create_cache(tmp_path, cacheable_paths=[]),
    )

    assert fs.readlink(str(tmp_path / "foo")) == "bar"


def test_cached_read(tmp_path):
    (tmp_path / "cache").mkdir()
    (tmp_path / "file").write_text("abcd")

    fs = create_remote_file_system(tmp_path)

    fd = fs.open(str(tmp_path / "file"), os.O_RDONLY)

    try:
        assert fs.read(str(tmp_path / "file"), fd, 1, 2) == b"bc"
        assert fs.read(str(tmp_path / "file"), fd, 0, 2) == b"ab"
    finally:
        fs.release(str(tmp_path / "file"), fd)


def test_uncached_read(tmp_path):
    (tmp_path / "cache").mkdir()
    (tmp_path / "file").write_text("abcd")

    fs = create_remote_file_system(
        tmp_path, cache=create_cache(tmp_path, cacheable_paths=[]),
    )

    fd = fs.open(str(tmp_path / "file"), os.O_RDONLY)

    try:
        assert fs.read(str(tmp_path / "file"), fd, 1, 2) == b"bc"
        assert fs.read(str(tmp_path / "file"), fd, 0, 2) == b"ab"
    finally:
        fs.release(str(tmp_path / "file"), fd)


def test_cache_not_writable(tmp_path):
    (tmp_path / "cache").mkdir()
    (tmp_path / "file").write_text("abcd")

    fs = create_remote_file_system(
        tmp_path, cache=create_cache(tmp_path, cacheable_paths=[]),
    )

    assert (
        fs.getattr(str(tmp_path / "file"), None)["st_mode"]
        & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
        != 0
    )

    fs = create_remote_file_system(tmp_path)

    assert (
        fs.getattr(str(tmp_path / "file"), None)["st_mode"]
        & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
        == 0
    )


def test_cache_flush_file(tmp_path):
    (tmp_path / "cache").mkdir()

    fs = create_remote_file_system(tmp_path)

    # Ensure that flush is a no-op for cached files
    fs.flush(str(tmp_path / "file"), 0)
