import os
import stat

from outrun.filesystem.common import Attributes


def test_base_attributes():
    st = os.lstat(__file__)
    attribs = Attributes.from_stat(st)

    assert st.st_atime == attribs.st_atime
    assert st.st_size == attribs.st_size

    assert all(value is not None for value in attribs.__dict__.values())


def test_extra_attributes():
    st = os.lstat(__file__)
    attribs = Attributes.from_stat(st)

    assert st.st_atime_ns == getattr(attribs, "st_atime_ns")


def test_readonly():
    st = os.lstat(__file__)

    attribs = Attributes.from_stat(st)
    readonly_attribs = attribs.as_readonly()

    assert attribs.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH) != 0
    assert readonly_attribs.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH) == 0

