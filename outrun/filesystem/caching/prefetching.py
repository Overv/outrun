"""
Module that implements prefetching heuristics for the caching file system.

The idea of prefetching is to send metadata and file contents for caching if we suspect
that they'll be accessed in the future based on current file system operations. This is
an extension of the cache's assumption that bandwidth is cheap and latency is expensive.
We try to reduce the number of individual RPC calls as much as possible by predicting
which data will be required next and sending it in bulk.

One of the simplest examples is readlink() on a symlink that will very likely lead to a
stat() for the entry targeted by that symlink. A slightly more complex example is the
reading of an ELF binary that will likely lead to its shared library dependencies being
accessed and read shortly afterwards.

Each prefetch rule is either based on the event of a file being accessed
(e.g. stat/readlink) or a file being read (open). These can be triggered by calling the
file_access and file_read functions, respectively. Each rule returns suggestions of data
to be prefetched where a suggestion is comprised of a path and the decision if only its
metadata should be loaded or if its contents should be included as well.

Rules are currently intentionally not recursive (e.g. a symlink with another symlink as
target doesn't result in a chain of suggestions) to stop the prefetching from
extrapolating too much.
"""

from dataclasses import dataclass
import glob
import os
import re
import subprocess
from typing import List

from outrun.logger import log


@dataclass
class PrefetchSuggestion:
    """A suggestion to prefetch the specified path's metadata and maybe its contents."""

    path: str
    contents: bool


def file_access(path: str) -> List[PrefetchSuggestion]:
    """Prefetch data based on the fact that the specified file is accessed."""
    suggestions = []

    # Prefetch the target when a symlink is accessed.
    suggestions += symlink_target(path)

    # Prefetch __pycache__ when a Python source file is accessed.
    suggestions += python_pycache(path)

    # Prefetch Perl module when its compiled version is accessed.
    suggestions += compiled_perl_module(path)

    return suggestions


def file_read(path: str) -> List[PrefetchSuggestion]:
    """Prefetch data based on the fact that the specified file is read."""
    suggestions = []

    # Prefetch shared libraries when ELF executables are opened.
    suggestions += elf_dependencies(path)

    return suggestions


def symlink_target(path: str) -> List[PrefetchSuggestion]:
    """
    Prefetch the entry that a symlink points to.

    This rule is based on the idea that it is likely for the target of a symlink to be
    looked up after accessing the symlink itself.
    """
    prefetches = []

    if os.path.islink(path):
        link_path = os.path.normpath(os.path.join(path, "..", os.readlink(path)))
        prefetches.append(PrefetchSuggestion(path=link_path, contents=False))

    return prefetches


def python_pycache(path: str) -> List[PrefetchSuggestion]:
    """
    Prefetch the associated __pycache__ file(s) when accessing a .py file.

    This rule is based on the way CPython looks for previously compiled bytecode when
    accessing a Python source file.
    """
    prefetches = []

    if path.endswith(".py") and os.path.isfile(path):
        # Prefetch Python source file itself.
        prefetches.append(PrefetchSuggestion(path=path, contents=True))

        # Prefetch __pycache__ directory itself.
        pycache_path = os.path.normpath(os.path.join(path, "..", "__pycache__"))
        prefetches.append(PrefetchSuggestion(path=pycache_path, contents=False))

        # Look for .pyc files matching the .py's filename and fully prefetch them.
        pyc_pattern = os.path.basename(path).replace(".py", "") + "*"
        cache_files = glob.glob(pycache_path + f"/{pyc_pattern}")

        for full_path in cache_files:
            prefetches.append(PrefetchSuggestion(path=full_path, contents=True))

    return prefetches


def compiled_perl_module(path: str) -> List[PrefetchSuggestion]:
    """
    Prefetch .pm file when its compiled .pmc associate is accessed.

    Note that the .pmc file doesn't necessarily need to exist. This rule is based on
    observing Perl behaviour while running "cowsay".
    """
    prefetches = []

    if path.endswith(".pmc"):
        module_path = path.replace(".pmc", ".pm")
        prefetches.append(PrefetchSuggestion(path=module_path, contents=True))

    return prefetches


def elf_dependencies(path: str) -> List[PrefetchSuggestion]:
    """
    Prefetch shared libraries dependencies of an ELF binary.

    This rule is based on the assumption that if an ELF binary is read then that's
    likely because it's being executed and its dependencies will soon be loaded as well.
    """
    prefetches = []

    if is_elf_binary(path):
        try:
            dependencies = read_elf_dependencies(path)
        except Exception as e:
            log.warning(f"failed to read elf dependencies of {path}: {e}")
            dependencies = []

        # Dependencies may be symlinks, so prefetch those.
        prefetches += [
            PrefetchSuggestion(path=dep, contents=False) for dep in dependencies
        ]

        # Prefetch contents of the final shared libraries.
        prefetches += [
            PrefetchSuggestion(path=os.path.realpath(dep), contents=True)
            for dep in dependencies
        ]

    return prefetches


def is_elf_binary(path: str) -> bool:
    """Check if the specified file is an ELF binary."""
    try:
        output = subprocess.check_output(["file", path]).decode()
        return "ELF" in output
    except Exception as e:
        log.warning(f"failed to check if {path} is elf binary: {e}")
        return False


def read_elf_dependencies(path: str) -> List[str]:
    """
    Retrieve the shared library dependencies of an ELF binary.

    Dependencies with spaces in the name are ignored because they cannot easily be
    extracted from ldd output, especially when you consider that the name itself may
    include sequences like '=>'.
    """
    output = subprocess.check_output(["ldd", path], stderr=subprocess.DEVNULL).decode()

    dependencies = []

    for line in output.splitlines():
        match = re.search(r"^[^ ]+ => ([^ ]+) \([0-9a-fx]+\)$", line.lstrip())

        if match:
            dependencies.append(match.group(1))

    return dependencies
