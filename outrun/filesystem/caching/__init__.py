"""
Modules that extend the mirrored file system with caching and prefetching logic.

The network file system is the primary performance bottleneck of outrun, primarily
because every I/O operation incurs significant latency overhead. Therefore this module
aims to extend the file system with a caching and prefetching layer that significantly
reduces the total number of I/O calls.

It's based on the idea that a lot of the I/O calls are the result of the application and
its dependencies being loaded from directories like /lib and /usr/bin. We can optimize
the access of these directories in two dictinct ways: caching and prefetching.

We consider these directories to be read-only and unchanging for the duration of an
outrun session, which is a relatively safe assumptions because they are generally only
modified because of system updates. Therefore all file system metadata and contents that
are read from these directories are persistently cached on the remote machine. When a
new session starts it will verify the freshness of the cache by doing a metadata check
with the local machine.

While caching makes subsequent runs of a program much faster, it doesn't do much more
the initial startup time, and that is where prefetching comes into play. We assume that
bandwidth is much cheaper than latency. In other words, we'd rather do a single I/O call
that pulls in a bit too much data that we don't use, than make a lot of specific I/O
calls that just pull in the data we need.

For example, when a file is opened for reading from the aforementioned directories, we
assume that all of its contents will eventually be read and transfer its entire
compressed contents over the network immediately. This may waste a bit of bandwidth for
the parts that aren't accessed (e.g. code in a shared library that isn't used), but it
significantly reduces the number of read() calls. A more complex example is the opening
of an ELF binary, where outrun will scan its shared library dependencies using ldd and
will send those along with the binary, all in a single request. This massively reduces
latency when running programs like ffmpeg and blender.

The prefetching is implemented as a push model where the local machine decides upon an
I/O call which additional data to push that it may think the remote machine will ask for
next. This allows for making a lot of prefetching decisions without additional round
trips.

The optimizations are implemented by hosting an additional RPC service alongside the
existing file system RPC service, and extending the FUSE file system to make use of
these caching and prefetching operations.
"""

from .filesystem import RemoteCachedFileSystem
from .service import LocalCacheService

__all__ = [
    "RemoteCachedFileSystem",
    "LocalCacheService",
]
