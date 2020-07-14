"""Data structures used by multiple file system components."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
import os
import stat
from typing import Union


@dataclass
class Attributes:
    """Container of file system attributes (basically os.stat_result as a dataclass)."""

    st_mode: int
    st_ino: int
    st_dev: int
    st_nlink: int
    st_uid: int
    st_gid: int
    st_size: int
    st_atime_ns: int
    st_mtime_ns: int
    st_ctime_ns: int

    def __init__(self, **attribs: Union[int, float]) -> None:
        """
        Instantiate with the specified file system attributes.

        You must specify the attributes declared in this class, but you may include any
        number of extra attributes, like st_atime_ns.
        """
        for name, value in attribs.items():
            setattr(self, name, value)

    @staticmethod
    def from_stat(st: os.stat_result) -> Attributes:
        """Instantiate from the attributes contained within an os.stat_result object."""
        st_dict = {k: getattr(st, k) for k in dir(st) if k.startswith("st_")}
        return Attributes(**st_dict)

    def as_readonly(self) -> Attributes:
        """Copy the attributes with write permissions removed from the mode."""
        readonly_mode = self.st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

        return dataclasses.replace(self, st_mode=readonly_mode)
