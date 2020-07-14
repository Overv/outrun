import os
from pathlib import Path
import shutil
import stat
from unittest import mock

import pytest

from outrun.filesystem.common import Attributes
from outrun.filesystem.caching.common import Metadata
from outrun.filesystem.caching.prefetching import PrefetchSuggestion
from outrun.filesystem.caching.service import LocalCacheService


@pytest.fixture
def service():
    return LocalCacheService()


def test_get_metadata_error(service, tmp_path):
    meta = service.get_metadata(str(tmp_path / "nonexistent"))

    assert meta.attr is None
    assert meta.link is None
    assert isinstance(meta.error, FileNotFoundError)


def test_get_metadata_dir(service, tmp_path):
    (tmp_path / "dir").mkdir()

    meta = service.get_metadata(str(tmp_path / "dir"))

    assert meta.error is None
    assert meta.link is None
    assert stat.S_ISDIR(meta.attr.st_mode)


def test_get_metadata_file(service, tmp_path):
    (tmp_path / "file").write_text("")

    meta = service.get_metadata(str(tmp_path / "file"))

    assert meta.error is None
    assert meta.link is None
    assert stat.S_ISREG(meta.attr.st_mode)


def test_get_metadata_symlink(service, tmp_path):
    os.symlink(tmp_path / "nonexistent", tmp_path / "link")

    meta = service.get_metadata(str(tmp_path / "link"))

    assert meta.error is None
    assert meta.link == str(tmp_path / "nonexistent")
    assert stat.S_ISLNK(meta.attr.st_mode)


def test_changed_metadata(service, tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()

    meta = {
        str(tmp_path / "a"): service.get_metadata(str(tmp_path / "a")),
        str(tmp_path / "b"): service.get_metadata(str(tmp_path / "b")),
        str(tmp_path / "c"): service.get_metadata(str(tmp_path / "c")),
    }

    # No changes yet
    assert list(service.get_changed_metadata(meta).keys()) == []

    # Make changes to metadata
    os.utime(tmp_path / "a", (0, 0))
    os.makedirs(tmp_path / "c")

    # Expect to receive changes
    changed_meta = service.get_changed_metadata(meta)

    assert list(changed_meta.keys()) == [
        str(tmp_path / "a"),
        str(tmp_path / "c"),
    ]

    assert changed_meta[str(tmp_path / "a")] == service.get_metadata(tmp_path / "a")
    assert changed_meta[str(tmp_path / "c")] == service.get_metadata(tmp_path / "c")


def test_access_time_changes_ignored(service):
    metadata = service.get_metadata("/")
    cached_metadata = {"/": metadata}

    with mock.patch(
        "outrun.filesystem.caching.service.LocalCacheService.get_metadata"
    ) as mock_meta:
        new_attr = Attributes(**metadata.attr.__dict__)
        new_attr.st_atime = 0.0
        new_metadata = Metadata(attr=new_attr)

        mock_meta.return_value = new_metadata

        assert service.get_changed_metadata(cached_metadata) == {}


def test_readfile(service, tmp_path):
    (tmp_path / "file").write_text("abc")
    contents = service.readfile(str(tmp_path / "file"))

    assert contents.data == b"abc"

    (tmp_path / "file").write_text("def")
    new_contents = service.readfile(str(tmp_path / "file"))

    assert new_contents.data == b"def"
    assert contents.checksum != new_contents.checksum


def test_readfile_conditional(service, tmp_path):
    (tmp_path / "file").write_text("abc")

    contents = service.readfile_conditional(str(tmp_path / "file"), "")
    assert contents.data == b"abc"

    new_contents = service.readfile_conditional(
        str(tmp_path / "file"), contents.checksum
    )
    assert new_contents is None

    (tmp_path / "file").write_text("def")
    new_contents = service.readfile_conditional(
        str(tmp_path / "file"), contents.checksum
    )

    assert new_contents.data == b"def"
    assert contents.checksum != new_contents.checksum


def test_machine_id_consistent(service):
    machine_id_1 = service.get_app_specific_machine_id()
    machine_id_2 = service.get_app_specific_machine_id()

    assert machine_id_1 == machine_id_2


def test_original_machine_id_not_being_exposed(service):
    machine_id = service.get_app_specific_machine_id()

    assert machine_id.strip() != Path("/etc/machine-id").read_text().strip()


def test_get_metadata_prefetch_symlink(service, tmp_path):
    os.symlink("foo", tmp_path / "link")
    metadata, prefetches = service.get_metadata_prefetch(str(tmp_path / "link"))

    assert metadata.link is not None

    assert len(prefetches) == 1
    assert prefetches[0].path == str(tmp_path / "foo")
    assert isinstance(prefetches[0].metadata.error, FileNotFoundError)


def test_get_metadata_prefetch_symlink_with_previously_fetched_target(
    service, tmp_path
):
    os.symlink("foo", tmp_path / "link")

    service.get_metadata(str(tmp_path / "foo"))
    _metadata, prefetches = service.get_metadata_prefetch(str(tmp_path / "link"))

    assert len(prefetches) == 0


def test_readfile_prefetch_executable(service):
    sh_path = shutil.which("ssh")
    _metadata, prefetches = service.readfile_prefetch(sh_path)

    assert len(prefetches) > 0
    assert all(".so" in p.path for p in prefetches)


def test_readfile_prefetch_executable_with_previously_fetched_contents():
    sh_path = shutil.which("ssh")

    service = LocalCacheService()
    _metadata, prefetches = service.readfile_prefetch(sh_path)

    assert any(p.contents for p in prefetches)

    service = LocalCacheService()
    service.mark_previously_fetched_contents([p.path for p in prefetches])
    _metadata, prefetches = service.readfile_prefetch(sh_path)

    assert not any(p.contents for p in prefetches)


def test_prefetch_inside_prefetchable_paths(service, tmp_path):
    os.symlink("foo", tmp_path / "link")

    service.set_prefetchable_paths([str(tmp_path)])
    _metadata, prefetches = service.get_metadata_prefetch(str(tmp_path / "link"))

    assert len(prefetches) != 0


def test_prefetch_outside_prefetchable_paths(service, tmp_path):
    os.symlink("foo", tmp_path / "link")

    service.set_prefetchable_paths(["/nonexistent"])
    _metadata, prefetches = service.get_metadata_prefetch(str(tmp_path / "link"))

    assert len(prefetches) == 0


def test_get_metadata_prefetch_failure_handling(service, tmp_path):
    with mock.patch(
        "outrun.filesystem.caching.prefetching.file_access"
    ) as mock_prefetch:
        mock_prefetch.side_effect = Exception()

        service.get_metadata_prefetch(str(tmp_path))


def test_readfile_prefetch_failure_handling(service, tmp_path):
    (tmp_path / "foo").write_text("bar")

    with mock.patch("outrun.filesystem.caching.prefetching.file_read") as mock_prefetch:
        mock_prefetch.side_effect = Exception()

        service.readfile_prefetch(str(tmp_path / "foo"))


def test_prefetching_unreadable_file(service, tmp_path):
    with mock.patch(
        "outrun.filesystem.caching.prefetching.file_access"
    ) as mock_prefetch:
        mock_prefetch.return_value = [
            PrefetchSuggestion(str(tmp_path / "nonexistent"), contents=True)
        ]

        _metadata, prefetches = service.get_metadata_prefetch("/")

        # Assert that the metadata (non-contents) are still successfully prefetched
        assert len(prefetches) == 1
        assert isinstance(prefetches[0].metadata.error, FileNotFoundError)
        assert prefetches[0].path == str(tmp_path / "nonexistent")
