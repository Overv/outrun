import hashlib

from outrun.filesystem.caching.common import FileContents, LockIndex


def test_file_contents_from_data():
    contents = FileContents.from_data(b"abc")

    assert contents.size == 3
    assert contents.checksum == hashlib.sha256(b"abc").hexdigest()


def test_file_contents_data():
    contents = FileContents.from_data(b"abc")

    assert contents.data == b"abc"


def test_lock_index():
    index = LockIndex()

    with index.lock("a"):
        with index.lock("b"):
            with index.lock("c"):
                assert index.lock_count == 3

        assert index.lock_count == 1

    assert index.lock_count == 0
