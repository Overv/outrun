# Outrun

[![](https://img.shields.io/pypi/v/outrun.svg?style=flat-square&label=latest%20stable%20version)](https://pypi.org/project/outrun/)

Outrun lets you execute a local command using the processing power of another Linux machine.

![](https://github.com/Overv/outrun/raw/master/outrun.gif)

* No need to first install the command on the other machine.
* Reference local files and paths like you would normally.
* Works across wildly different Linux distributions, like Ubuntu and Alpine.

Contents

* [Installation](#installation)
* [Usage](#usage)
* [How it works](#how-it-works)
* [Limitations](#limitations)
* [FAQ](#faq)
* [Development](#development)
* [License](#license)

## Installation

Outrun requires Python 3.7 and can simply be installed using [pip](https://pip.pypa.io/en/stable/installing/):

    pip3 install outrun

It must be installed on your own machine and any machines that you'll be using to run commands. On those other machines you must make sure to install it globally, such that it can be started with a command like `ssh user@host outrun`.

In addition to outrun itself, you also have to install the [FUSE 3.x](https://github.com/libfuse/libfuse) library. Most Linux distributions include it in a package that is simply named `fuse3`.

## Usage

### Prerequisites

You must have root access to the other machine, either by being able to directly SSH into it as `root` or by having `sudo` access. This is necessary because outrun uses [chroot](https://en.wikipedia.org/wiki/Chroot).

Since the remote machine will have full access to your local file system as your current user, make sure to only use machines that you trust.

### Example

If you would normally run:

    ffmpeg -i input.mp4 -vcodec libx265 -crf 28 output.mp4

Then you can let outrun execute that same command using the processing power of `user@host` like this:

    outrun user@host ffmpeg -i input.mp4 -vcodec libx265 -crf 28 output.mp4

FFMPEG does not need to be installed on the other machine and `input.mp4`/`output.mp4` will be read from and written to your local hard drive as you would expect.

The first time you run a new command, it may take some time to start because its dependencies must first be copied to the other machine. This may happen even if another machine has already run FFMPEG on it before, because it likely has a slightly different version. The time this takes is highly dependent on your bandwidth and latency.

### Configuration

See the output of `outrun --help` for an overview of available options to customize behaviour. The persistent cache on each remote machine can be configured by creating a `~/.outrun/config` file like this:

```ini
[cache]
path = ~/.outrun/cache
max_entries = 1024
max_size = 21474836480 # bytes -> 20 GB
```

## How it works

### Making a command work on a different machine

Let’s consider the `ffmpeg` command from the example and evaluate what’s necessary for it to run on the other machine as if it were running locally.

First we need some way of running commands and observing their output on another machine in the first place, so it all starts with a normal SSH session. Try running `ssh -tt user@host htop` and you'll see that it's easy to run an interactive remote program with SSH that provides a very similar experience to a locally running program.

Of course, we're not interested in running software that's installed on the *other* machine, but rather the `ffmpeg` program on our *own* machine. A straightforward way to get started would be to [scp](https://linux.die.net/man/1/scp) the `/usr/bin/ffmpeg` executable to the other machine and try to run it there. Unfortunately you'll likely find that the following will happen when you try to execute it:

    $ ./ffmpeg
    ./ffmpeg: not found

That’s because the executable is [dynamically linked](https://en.wikipedia.org/wiki/Dynamic_linker) and it tries to load its library dependencies from the file system, which don’t exist on the other machine. You can use [ldd](https://linux.die.net/man/1/ldd) to see which shared libraries an ELF executable like this depends on:

    $ ldd `which ffmpeg`
        linux-vdso.so.1 (0x00007fffb7796000)
        libavdevice.so.58 => /usr/lib/libavdevice.so.58 (0x00007f2407f2a000)
        libavfilter.so.7 => /usr/lib/libavfilter.so.7 (0x00007f2407be0000)
        libavformat.so.58 => /usr/lib/libavformat.so.58 (0x00007f2407977000)
        libavcodec.so.58 => /usr/lib/libavcodec.so.58 (0x00007f2406434000)
        libpostproc.so.55 => /usr/lib/libpostproc.so.55 (0x00007f2406414000)
        libswresample.so.3 => /usr/lib/libswresample.so.3 (0x00007f24063f4000)
        ...

We could painstakingly copy all of these libraries as well, but that wouldn’t necessarily be the end of it. Software like [Blender](https://www.blender.org/), for example, additionally loads a lot of other dependencies like Python files after it has started running. Even if we were to clutter the remote file system with all of these dependencies, we would still not be able to run the original command since `input.mp4` doesn't exist on the other machine.

To handle the inevitable reality that programs may need access to any file in the local file system, outrun simply mirrors the entire file system of the local machine by mounting it over the network. This is done by mounting a FUSE file system on the remote machine that forwards all of its operations, like listing files in a directory, to an RPC service on the local machine. This RPC service simply exposes functions like `readdir()` and `stat()` to interact with the local file system.

You may wonder why we’re not using an existing network file system solution like [NFS](https://en.wikipedia.org/wiki/Network_File_System) or [SSHFS](https://en.wikipedia.org/wiki/SSHFS). The trouble with these is that it’s difficult to automate quick setup for ad-hoc sessions. NFS needs to be set up with configuration files and SSHFS requires the remote machine to be able to SSH back to the local machine. In contrast, the included RPC file system is a lightweight TCP server with a per-session token that can be securely tunneled over the SSH session that we're already using. Having a custom solution also opens up opportunities for a lot of optimizations as we'll see later on.

Let's say we mount the local machine's file system at `/tmp/local_fs`. We can now easily access FFMPEG at `/tmp/local_fs/usr/bin/ffmpeg` and all of its library dependencies at `/tmp/local_fs/usr/lib`. Unfortunately it doesn't seem like we've made much progress yet:

    $ /tmp/local_fs/usr/bin/ffmpeg
    /tmp/local_fs/usr/bin/ffmpeg: not found

The problem is that we're still looking for its dependencies in `/usr/lib` on the remote machine. We could work around this problem by playing with environment variables like `$LD_LIBRARY_PATH` to make FFMPEG look for libraries in `/tmp/local_fs/usr/lib`, but then we're just back to solving one small problem at a time again. We wouldn't have to worry about this if we could pretend that `/tmp/local_fs` *is* the root file system so that `/usr/lib` automatically redirects to `/tmp/local_fs/usr/lib` instead. We can do exactly that using [chroot](https://en.wikipedia.org/wiki/Chroot).

    $ chroot /tmp/local_fs /bin/bash
    # ffmpeg -i input.mp4 -vcodec libx265 -crf 28 output.mp4
    ffmpeg version n4.2.3 Copyright (c) 2000-2020 the FFmpeg developers
    ...
    input.mp4: No such file or directory

It works! Well, almost. We're still missing some of the context in which the original command was to be executed. To be able to find `input.mp4` and to store `output.mp4` in the right place, we also need to switch to the same original working directory.

    # cd /home/user/videos
    # ffmpeg -i input.mp4 -vcodec libx265 -crf 28 output.mp4
    ...
    # ls
    input.mp4   output.mp4

While FFMPEG already works as expected, we should also bring in the right environment variables. For example, `$HOME` may affect where configuration files are loaded from and `$LANG` may affect the display language of certain programs.

If you would now go back to your own machine and look at the original `/home/user/videos`, you'll see that `output.mp4` is right there as if we didn't just run FFMPEG on a completely different machine!

### Optimization

While this approach works, the performance leaves a lot to be desired. If you're connecting to a server over the internet, even one in a region close to you, your latency to it will likely be at least 20 ms. This is also how long every file system operation will take and that quickly adds up. For example, on my machine FFMPEG has 110 shared libraries that need to be loaded, which means that just looking up their attributes will already take 2.2 seconds! If we had to naively do every single file system operation over the network like this then outrun wouldn't really be feasible. That's why outrun's network file system comes with two types of optimizations: caching and prefetching.

Outrun persistently caches all files that are read from system directories like `/usr/bin` and `/lib`, that are known to contain applications and their dependencies. These directories are assumed to remain unchanged for the duration of an outrun session. Each new session will check if any of the cached files have changed and update them as necessary. This simple strategy is sufficient to make the same program start much faster the second time around, and often also helps with other programs since many dependencies like [glibc](https://www.gnu.org/software/libc/) are shared. An [LRU cache policy](https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)) is used to throw out files that haven’t been used in a while.

For other directories, outrun lets the kernel perform basic optimizations like reading files in large chunks and buffering small writes into one large write, much like NFS.

Where outrun's file system really deviates from generic network file systems is its aggressive approach to prefetching. For example, when an executable like `/usr/bin/ffmpeg` is read, it is very likely that it will be executed and its shared libraries will be loaded next. Therefore, when the `open()` call for `/usr/bin/ffmpeg` comes in, it will not just transfer that executable in its entirety ahead of time, but also transfer all of its 110 shared libraries and associated file system metadata in a single operation. If this assumption is correct, which it generally will be, we've just reduced hundreds of `stat()`/`readlink()`/`open()`/`read()`/`close()` calls to a single RPC call with a one-time 20 ms overhead.

Another example of such a prefetch is the assumption that if a `.py` file is accessed, it is likely being interpreted and Python will soon look for its compiled `.pyc` companion. Therefore that file is immediately sent along with it. If the `__pycache__` directory doesn't exist, then we simply prefetch the `ENOENT` error that would result from trying to access it instead.

Since compiled files tend to compress well, all file contents that are read in their entirety are also transferred with [LZ4](https://github.com/lz4/lz4) compression to save on bandwidth.

#### Documentation

If you would like to read more details about outrun and its design decisions, then have a look at the source code. Each module (file system, RPC, operation orchestration) contains more specific documentation in its docstrings.

## Limitations

There are a couple of things to keep in mind while using outrun:

* File system performance remains a bottleneck, so the most suitable workloads are computationally constrained tasks like ray tracing and video encoding. Using outrun for something like `git status` works, but is not recommended.
* Since the software to be executed is copied from your own machine to the remote machine, it must be binary compatible. It’s not possible to set up a session from an x86 machine to an ARM machine, for example.
* The command will use the network and date/time of the remote machine.
    * If you want to access local endpoints, then they must be explicitly forwarded by using the SSH flags parameter to set up [remote forwarding](https://www.ssh.com/ssh/tunneling/example#remote-forwarding).

## FAQ

* Does outrun support multiple sessions at the same time?
    * Yes, you can run software on many different machines simultaneously. Each remote machine can also support many different machines connecting to it at the same time with wildly different file systems.
* Why was outrun written in Python?
    * The original prototype for outrun was written in C++, but it turned out that Python and its standard library make it much easier to ~~glue~~ orchestrate processes and operating system interactions. With regards to file system performance, it doesn’t make much of a difference since network latency is by far the greatest bottleneck.


## Development

### Static analysis and code style

Outrun was written to heavily rely on [type hints](https://docs.python.org/3/library/typing.html) for documentation purposes and to allow for static analysis using [mypy](http://mypy-lang.org/). In addition to that, [flake8](https://flake8.pycqa.org/en/latest/) is used to enforce code style and [pylint](https://www.pylint.org/) is used to catch additional problems.

    mypy outrun
    flake8
    pylint outrun

### Testing

Outrun comes with a test suite for all individual modules based on [pytest](https://docs.pytest.org/en/latest/).

    pytest --fuse --cov=outrun outrun/tests

Since a lot of the functionality in outrun depends on OS interaction, its test suite also includes full integration tests that simulate usage with a VM connecting to another VM. These are set up using [Vagrant](https://www.vagrantup.com/) and can be run by including the `--vagrant` flag:

    pytest --vagrant --cov=outrun outrun/tests

## License

Outrun is licensed under [version 2.0 of the Apache License](http://www.apache.org/licenses/LICENSE-2.0).
