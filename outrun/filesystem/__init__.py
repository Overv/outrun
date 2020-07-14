"""
Modules that take care of mirroring the local file system on the remote machine.

Outrun comes with its own network file system to expose the local machine's file system
on the remote machine. The local machine runs an RPC service that allows for calling
common I/O functions like open(), stat(), and readdir(). The remote machine mounts a
FUSE file system that mostly simply forwards all of its calls to that RPC service.

So, why not use an existing solution like NFS or SSHFS? While these are very capable
generic network file system solutions, they are unsuitable for use in outrun for a
couple of reasons:

* NFS
    * Designed for local networks and isn't easy to tunnel over SSH.
    * Designed for long term mounts rather than quick set up and tear down of sessions.
* SSHFS
    * While it is designed to easily and quickly set up sessions, it would require the
    remote machine to be able to SSH back to the local machine. This would require
    complex solutions like having outrun host its own SSH server and tunneling that over
    the existing SSH session.

The file system in this module tackles all of these issues by being very easy to tunnel
(just one TCP port), all implemented in user mode (very easy to start and shutdown), and
simple per-session tokens for authentication.

Once a network file system is up and running, the next most important concern is
performance. Outrun is designed to mount a file system over the internet and latency is
significant bottleneck there for all I/O calls. A lot of this latency can be avoided
using clever caching and prefetching and this is another key advantage of having a
custom implementation. Outrun's file system is purpose built for running programs off of
it and this enables it to have a caching layer on top of the base file system that can
make a lot of assumptions about I/O operations. All of this logic is implemented in the
'caching' submodule.
"""

from .filesystem import RemoteFileSystem
from .service import LocalFileSystemService

__all__ = [
    "RemoteFileSystem",
    "LocalFileSystemService",
]
