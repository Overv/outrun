import contextlib
import dataclasses
import os
import time
from unittest import mock

from outrun.filesystem.caching.service import LocalCacheService
from outrun.filesystem.caching.cache import CacheEntry, RemoteCache


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


def test_cache_entry_newer_than():
    entry_a = CacheEntry("a", LocalCacheService().get_metadata("/"))
    entry_b = CacheEntry("b", LocalCacheService().get_metadata("/"))

    assert entry_b.newer_than(entry_a)

    entry_a.last_update = time.time()

    assert not entry_b.newer_than(entry_a)


def test_concurrent_cache_get_metadata(tmp_path):
    meta = LocalCacheService().get_metadata("/")
    meta = dataclasses.replace(meta, attr=meta.attr.as_readonly())

    mock_client = mock.Mock()
    mock_client.get_metadata.return_value = meta

    (tmp_path / "cache").mkdir()
    cache = create_cache(tmp_path, client=mock_client)

    for _ in range(10):
        assert cache.get_metadata("/") == meta

    assert mock_client.get_metadata.call_count == 1


def test_concurrent_cache_open_content(tmp_path):
    fs = LocalCacheService()

    (tmp_path / "cache").mkdir()
    (tmp_path / "hello").write_text("world")

    mock_client = mock.Mock()
    mock_client.readfile.side_effect = fs.readfile

    cache = create_cache(tmp_path, client=mock_client)

    for _ in range(10):
        fd = cache.open_contents(str(tmp_path / "hello"), os.O_RDONLY)
        try:
            os.lseek(fd, 0, 0)
            assert os.read(fd, 1024) == b"world"
        finally:
            os.close(fd)

    assert mock_client.readfile.call_count == 1


def test_concurrent_cache_load_save(tmp_path):
    meta = LocalCacheService().get_metadata("/")
    meta = dataclasses.replace(meta, attr=meta.attr.as_readonly())

    mock_client = mock.Mock()
    mock_client.get_metadata.return_value = meta

    (tmp_path / "cache").mkdir()

    cache_a = create_cache(tmp_path, client=mock_client)
    assert cache_a.get_metadata("/") == meta
    cache_a.save()

    cache_b = create_cache(tmp_path, client=mock_client)
    cache_b.load()
    assert cache_b.get_metadata("/") == meta

    assert mock_client.get_metadata.call_count == 1


def test_concurrent_cache_per_machine(tmp_path):
    meta = LocalCacheService().get_metadata("/")
    meta = dataclasses.replace(meta, attr=meta.attr.as_readonly())

    mock_client = mock.Mock()
    mock_client.get_metadata.return_value = meta

    (tmp_path / "cache").mkdir()

    cache_a = create_cache(tmp_path, machine_id="machine_a", client=mock_client)
    assert cache_a.get_metadata("/") == meta
    cache_a.save()

    cache_b = create_cache(tmp_path, machine_id="machine_b", client=mock_client)
    cache_b.load()
    assert cache_b.get_metadata("/") == meta

    assert mock_client.get_metadata.call_count == 2


def test_concurrent_cache_lru_entries(tmp_path):
    (tmp_path / "cache").mkdir()

    cache = create_cache(tmp_path, max_entries=3)

    for x in ["a", "b", "c", "d"]:
        with contextlib.suppress(OSError):
            cache.get_metadata(f"/{x}")

    cache.save()
    cache.load()

    assert cache.count() == 3
    assert cache.size() == 0


def test_concurrent_cache_lru_size(tmp_path):
    (tmp_path / "cache").mkdir()

    cache = create_cache(tmp_path, max_size=3)

    for x in ["a", "b", "c", "d"]:
        (tmp_path / x).write_text(" ")

    for x in ["a", "b", "c", "d"]:
        fd = cache.open_contents(str(tmp_path / x), os.O_RDONLY)
        os.close(fd)

    cache.save()
    cache.load()

    assert cache.count() == 4
    assert cache.size() == 3


def test_concurrent_cache_content_cleanup(tmp_path):
    (tmp_path / "cache").mkdir()
    cache = create_cache(tmp_path, max_size=3)

    for x in ["a", "b", "c", "d"]:
        (tmp_path / x).write_text("123")

    for x in ["a", "b", "c", "d"]:
        fd = cache.open_contents(str(tmp_path / x), os.O_RDONLY)
        os.close(fd)

    cache.save()

    assert len(os.listdir(tmp_path / "cache" / "contents")) == 1

    cache = RemoteCache(
        str(tmp_path / "cache"),
        "machine",
        LocalCacheService(),
        prefetch=False,
        max_entries=1024,
        max_size=1024 * 1024,
        cacheable_paths=["/"],
    )
    cache.save(merge_disk_cache=False)

    assert len(os.listdir(tmp_path / "cache" / "contents")) == 0


def test_concurrent_cache_refresh_metadata(tmp_path):
    (tmp_path / "file").write_text("foo")
    (tmp_path / "cache").mkdir()

    cache = create_cache(tmp_path)

    meta_1 = cache.get_metadata(str(tmp_path / "file"))

    os.truncate(tmp_path / "file", 20)

    meta_2 = cache.get_metadata(str(tmp_path / "file"))

    assert meta_2 == meta_1

    cache.sync()

    meta_3 = cache.get_metadata(str(tmp_path / "file"))

    assert meta_3 != meta_1


def test_concurrent_cache_refresh_contents(tmp_path):
    (tmp_path / "file").write_text("foo")
    (tmp_path / "cache").mkdir()

    cache = create_cache(tmp_path)

    fd = cache.open_contents(str(tmp_path / "file"), os.O_RDONLY)
    try:
        os.lseek(fd, 0, 0)
        assert os.read(fd, 1024) == b"foo"
    finally:
        os.close(fd)

    (tmp_path / "file").write_text("foobar")

    fd = cache.open_contents(str(tmp_path / "file"), os.O_RDONLY)
    try:
        os.lseek(fd, 0, 0)
        assert os.read(fd, 1024) == b"foo"
    finally:
        os.close(fd)

    cache.sync()

    fd = cache.open_contents(str(tmp_path / "file"), os.O_RDONLY)
    try:
        os.lseek(fd, 0, 0)
        assert os.read(fd, 1024) == b"foobar"
    finally:
        os.close(fd)


def test_concurrent_cache_disk_merge(tmp_path):
    (tmp_path / "foo").touch()
    (tmp_path / "bar").touch()

    cache_a = create_cache(tmp_path)
    cache_b = create_cache(tmp_path)

    cache_a.get_metadata(str(tmp_path / "foo"))
    cache_b.get_metadata(str(tmp_path / "bar"))

    cache_a.save()
    cache_b.save()

    cache_c = RemoteCache(
        str(tmp_path / "cache"),
        "machine",
        LocalCacheService(),
        prefetch=False,
        max_entries=1024,
        max_size=1024 * 1024,
        cacheable_paths=["/"],
    )
    cache_c.load()

    assert cache_c.count() == 2


def test_concurrent_cache_prefetch_symlink(tmp_path):
    os.symlink("bar", tmp_path / "foo")

    cache = create_cache(tmp_path, prefetch=True)

    cache.get_metadata(str(tmp_path / "foo"))

    assert cache.count() == 2


def test_concurrent_cache_prefetch_contents_upon_access(tmp_path):
    (tmp_path / "test.py").write_text("abc")

    cache = create_cache(tmp_path, prefetch=True)

    cache.get_metadata(str(tmp_path / "test.py"))

    assert cache.size() == 3


def test_concurrent_cache_mark_fetched_contents(tmp_path):
    (tmp_path / "file").touch()

    # Cache contents of a file
    cache_a = create_cache(tmp_path, prefetch=True)

    fd = cache_a.open_contents(str(tmp_path / "file"), os.O_RDONLY)
    os.close(fd)

    cache_a.save()

    # Reload cache and expect that local is informed about cached contents
    mock_client = mock.Mock()
    mock_client.get_changed_metadata.return_value = {}

    cache_b = create_cache(tmp_path, client=mock_client, prefetch=True)
    cache_b.load()
    cache_b.sync()

    mock_client.mark_previously_fetched_contents.assert_called_with(
        [str(tmp_path / "file")]
    )
