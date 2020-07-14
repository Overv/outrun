"""
Module with ctypes bindings for the high-level FUSE 3.x API.

These are based on the following libfuse headers:

* fuse/fuse.h
* fuse/fuse_common.h
* fuse/fuse_opt.h

Currently the implementation assumes an x86-64 system.
"""

import ctypes
import ctypes.util
from enum import Enum

#
# Helpers
#


def struct_to_dict(struct: ctypes.Structure) -> dict:
    """Create a dict from a struct's fields and their values."""
    return {name: getattr(struct, name) for name, _ in getattr(struct, "_fields_")}


#
# Types
#

# sys/types.h
off_t = ctypes.c_long
mode_t = ctypes.c_uint
dev_t = ctypes.c_ulong
uid_t = ctypes.c_uint
gid_t = ctypes.c_uint
fsblkcnt_t = ctypes.c_ulong
fsfilcnt_t = ctypes.c_ulong


class fuse_args(ctypes.Structure):
    """fuse/fuse_opt.h: struct fuse_args."""

    _fields_ = [
        ("argc", ctypes.c_int),
        ("argv", ctypes.POINTER(ctypes.POINTER(ctypes.c_char))),
        ("allocated", ctypes.c_int),
    ]


fuse_args_p = ctypes.POINTER(fuse_args)


class fuse_opt(ctypes.Structure):
    """fuse/fuse_opt.h: struct fuse_args."""

    _fields_ = [
        ("templ", ctypes.c_char_p),
        ("offset", ctypes.c_ulong),
        ("value", ctypes.c_int),
    ]


fuse_opt_p = ctypes.POINTER(fuse_opt)


class fuse_conn_info(ctypes.Structure):
    """fuse/fuse_common.h: struct fuse_conn_info."""

    _fields_ = [
        ("proto_major", ctypes.c_uint),
        ("proto_minor", ctypes.c_uint),
        ("max_write", ctypes.c_uint),
        ("max_read", ctypes.c_uint),
        ("max_readahead", ctypes.c_uint),
        ("capable", ctypes.c_uint),
        ("want", ctypes.c_uint),
        ("max_background", ctypes.c_uint),
        ("congestion_threshold", ctypes.c_uint),
        ("time_gran", ctypes.c_uint),
    ] + [("reserved", ctypes.c_uint)] * 22


fuse_conn_info_p = ctypes.POINTER(fuse_conn_info)


class fuse_config(ctypes.Structure):
    """fuse/fuse.h: struct fuse_config."""

    _fields_ = [
        ("set_gid", ctypes.c_int),
        ("gid", ctypes.c_uint),
        ("set_uid", ctypes.c_int),
        ("uid", ctypes.c_uint),
        ("set_mode", ctypes.c_int),
        ("umask", ctypes.c_uint),
        ("entry_timeout", ctypes.c_double),
        ("negative_timeout", ctypes.c_double),
        ("attr_timeout", ctypes.c_double),
        ("intr", ctypes.c_int),
        ("intr_signal", ctypes.c_int),
        ("remember", ctypes.c_int),
        ("hard_remove", ctypes.c_int),
        ("use_ino", ctypes.c_int),
        ("readdir_ino", ctypes.c_int),
        ("direct_io", ctypes.c_int),
        ("kernel_cache", ctypes.c_int),
        ("auto_cache", ctypes.c_int),
        ("ac_attr_timeout_set", ctypes.c_int),
        ("ac_attr_timeout", ctypes.c_double),
        ("nullpath_ok", ctypes.c_int),
        ("show_help", ctypes.c_int),
        ("modules", ctypes.POINTER(ctypes.c_char)),
        ("debug", ctypes.c_int),
    ]


fuse_config_p = ctypes.POINTER(fuse_config)


class fuse_file_info(ctypes.Structure):
    """fuse/fuse_common.h: struct fuse_file_info."""

    _fields_ = [
        ("flags", ctypes.c_int),
        ("writepage", ctypes.c_uint, 1),
        ("direct_io", ctypes.c_uint, 1),
        ("keep_cache", ctypes.c_uint, 1),
        ("flush", ctypes.c_uint, 1),
        ("nonseekable", ctypes.c_uint, 1),
        ("flock_release", ctypes.c_uint, 1),
        ("cache_readdir", ctypes.c_uint, 1),
        ("padding", ctypes.c_uint, 25),
        ("padding2", ctypes.c_uint, 32),
        ("fh", ctypes.c_uint64),
        ("lock_owner", ctypes.c_uint64),
        ("poll_events", ctypes.c_uint32),
    ]


fuse_file_info_p = ctypes.POINTER(fuse_file_info)


class timespec(ctypes.Structure):
    """bits/types/struct_timespec.h: struct timespec."""

    _fields_ = [
        ("tv_sec", ctypes.c_long),
        ("tv_nsec", ctypes.c_long),
    ]


timespec_p = ctypes.POINTER(timespec)


class stat(ctypes.Structure):
    """bits/stat.h: struct stat."""

    _fields_ = [
        ("st_dev", ctypes.c_ulong),
        ("st_ino", ctypes.c_ulong),
        ("st_nlink", ctypes.c_ulong),
        ("st_mode", ctypes.c_uint),
        ("st_uid", ctypes.c_uint),
        ("st_gid", ctypes.c_uint),
        ("__pad0", ctypes.c_int),
        ("st_rdev", ctypes.c_ulong),
        ("st_size", ctypes.c_long),
        ("st_blksize", ctypes.c_long),
        ("st_blocks", ctypes.c_long),
        ("st_atim", timespec),
        ("st_mtim", timespec),
        ("st_ctim", timespec),
    ] + [("__glibc_reserved", ctypes.c_long)] * 3


stat_p = ctypes.POINTER(stat)


class statvfs_t(ctypes.Structure):
    """bits/statvfs.h: struct statvfs."""

    _fields_ = [
        ("f_bsize", ctypes.c_ulong),
        ("f_frsize", ctypes.c_ulong),
        ("f_blocks", fsblkcnt_t),
        ("f_bfree", fsblkcnt_t),
        ("f_bavail", fsblkcnt_t),
        ("f_files", fsfilcnt_t),
        ("f_ffree", fsfilcnt_t),
        ("f_favail", fsfilcnt_t),
        ("f_fsid", ctypes.c_ulong),
        ("f_flag", ctypes.c_ulong),
        ("f_namemax", ctypes.c_ulong),
        ("__f_spare", ctypes.c_int * 6),
    ]


statvfs_t_p = ctypes.POINTER(statvfs_t)


class fuse_readdir_flags(int, Enum):
    """fuse/fuse.h: enum fuse_readdir_flags."""

    FUSE_READDIR_PLUS = 1 << 0


class fuse_fill_dir_flags(int, Enum):
    """fuse/fuse.h: enum fuse_fill_dir_flags."""

    FUSE_FILL_DIR_PLUS = 1 << 1


# fuse/fuse_opt.h
fuse_opt_proc_t = ctypes.CFUNCTYPE(
    ctypes.c_int, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int, fuse_args_p
)

# fuse/fuse.h
fuse_fill_dir_t = ctypes.CFUNCTYPE(
    ctypes.c_int, ctypes.c_void_p, ctypes.c_char_p, stat_p, off_t, ctypes.c_int
)


class fuse_operations(ctypes.Structure):
    """fuse/fuse.h: struct fuse_operations."""

    _fields_ = [
        (
            "getattr",
            ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, stat_p, fuse_file_info_p),
        ),
        (
            "readlink",
            ctypes.CFUNCTYPE(
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.POINTER(ctypes.c_char),
                ctypes.c_size_t,
            ),
        ),
        ("mknod", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, mode_t, dev_t)),
        ("mkdir", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, mode_t)),
        ("unlink", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p)),
        ("rmdir", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p)),
        ("symlink", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p)),
        (
            "rename",
            ctypes.CFUNCTYPE(
                ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint
            ),
        ),
        ("link", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p)),
        (
            "chmod",
            ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, mode_t, fuse_file_info_p),
        ),
        (
            "chown",
            ctypes.CFUNCTYPE(
                ctypes.c_int, ctypes.c_char_p, uid_t, gid_t, fuse_file_info_p,
            ),
        ),
        (
            "truncate",
            ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, off_t, fuse_file_info_p),
        ),
        ("open", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, fuse_file_info_p)),
        (
            "read",
            ctypes.CFUNCTYPE(
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.POINTER(ctypes.c_char),
                ctypes.c_size_t,
                off_t,
                fuse_file_info_p,
            ),
        ),
        (
            "write",
            ctypes.CFUNCTYPE(
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.POINTER(ctypes.c_char),
                ctypes.c_size_t,
                off_t,
                fuse_file_info_p,
            ),
        ),
        ("statfs", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, statvfs_t_p)),
        ("flush", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, fuse_file_info_p)),
        ("release", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, fuse_file_info_p)),
        (
            "fsync",
            ctypes.CFUNCTYPE(
                ctypes.c_int, ctypes.c_char_p, ctypes.c_int, fuse_file_info_p
            ),
        ),
        ("setxattr", ctypes.c_void_p),
        ("getxattr", ctypes.c_void_p),
        ("listxattr", ctypes.c_void_p),
        ("removexattr", ctypes.c_void_p),
        ("opendir", ctypes.c_void_p),
        (
            "readdir",
            ctypes.CFUNCTYPE(
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_void_p,
                fuse_fill_dir_t,
                off_t,
                fuse_file_info_p,
                ctypes.c_int,
            ),
        ),
        ("releasedir", ctypes.c_void_p),
        ("fsyncdir", ctypes.c_void_p),
        ("init", ctypes.CFUNCTYPE(None, fuse_conn_info_p, fuse_config_p)),
        ("destroy", ctypes.CFUNCTYPE(None, ctypes.c_void_p)),
        ("access", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, ctypes.c_int)),
        (
            "create",
            ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_char_p, mode_t, fuse_file_info_p),
        ),
        ("lock", ctypes.c_void_p),
        (
            "utimens",
            ctypes.CFUNCTYPE(
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.POINTER(timespec),
                fuse_file_info_p,
            ),
        ),
        ("bmap", ctypes.c_void_p),
        ("ioctl", ctypes.c_void_p),
        ("poll", ctypes.c_void_p),
        ("write_buf", ctypes.c_void_p),
        ("read_buf", ctypes.c_void_p),
        ("flock", ctypes.c_void_p),
        ("fallocate", ctypes.c_void_p),
        ("copy_file_range", ctypes.c_void_p),
        (
            "lseek",
            ctypes.CFUNCTYPE(
                off_t, ctypes.c_char_p, off_t, ctypes.c_int, fuse_file_info_p
            ),
        ),
    ]


fuse_operations_p = ctypes.POINTER(fuse_operations)

#
# Macros
#


def FUSE_ARGS_INIT(
    argc: ctypes.c_int, argv: ctypes.POINTER(ctypes.POINTER(ctypes.c_char))
) -> fuse_args:
    """fuse/fuse_opt.h: macro FUSE_ARGS_INIT."""
    return fuse_args(argc=argc, argv=argv, allocated=0)


#
# Constants
#

# bits/stat.h
UTIME_NOW = (1 << 30) - 1
UTIME_OMIT = (1 << 30) - 2

# fuse/fuse_common.h
FUSE_CAP_ASYNC_READ = 1 << 0
FUSE_CAP_POSIX_LOCKS = 1 << 1
FUSE_CAP_ATOMIC_O_TRUNC = 1 << 3
FUSE_CAP_EXPORT_SUPPORT = 1 << 4
FUSE_CAP_DONT_MASK = 1 << 6
FUSE_CAP_SPLICE_WRITE = 1 << 7
FUSE_CAP_SPLICE_MOVE = 1 << 8
FUSE_CAP_SPLICE_READ = 1 << 9
FUSE_CAP_FLOCK_LOCKS = 1 << 10
FUSE_CAP_IOCTL_DIR = 1 << 11
FUSE_CAP_AUTO_INVAL_DATA = 1 << 12
FUSE_CAP_READDIRPLUS = 1 << 13
FUSE_CAP_READDIRPLUS_AUTO = 1 << 14
FUSE_CAP_ASYNC_DIO = 1 << 15
FUSE_CAP_WRITEBACK_CACHE = 1 << 16
FUSE_CAP_NO_OPEN_SUPPORT = 1 << 17
FUSE_CAP_PARALLEL_DIROPS = 1 << 18
FUSE_CAP_POSIX_ACL = 1 << 19
FUSE_CAP_HANDLE_KILLPRIV = 1 << 20
FUSE_CAP_NO_OPENDIR_SUPPORT = 1 << 24
FUSE_CAP_EXPLICIT_INVAL_DATA = 1 << 25

#
# Functions
#

fuse3_so = ctypes.util.find_library("fuse3")

if not fuse3_so:
    raise RuntimeError("failed to find fuse3 library")

fuse3 = ctypes.cdll.LoadLibrary(fuse3_so)

# fuse/fuse.h
fuse_main_real = fuse3.fuse_main_real
fuse_main_real.restype = ctypes.c_int
fuse_main_real.argtypes = [
    ctypes.c_int,
    ctypes.POINTER(ctypes.POINTER(ctypes.c_char)),
    fuse_operations_p,
    ctypes.c_size_t,
    ctypes.c_void_p,
]

# fuse/fuse_opt.h
fuse_opt_parse = fuse3.fuse_opt_parse
fuse_opt_parse.restype = ctypes.c_int
fuse_opt_parse.argtypes = [
    fuse_args_p,
    ctypes.c_void_p,
    fuse_opt_p,
    fuse_opt_proc_t,
]

# fuse/fuse_opt.h
fuse_opt_add_arg = fuse3.fuse_opt_add_arg
fuse_opt_add_arg.restype = ctypes.c_int
fuse_opt_add_arg.argtypes = [fuse_args_p, ctypes.c_char_p]
