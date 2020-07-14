"""
Module implementing the command-line interface and invoking the main logic of outrun.

outrun is started on the local machine and launches another copy of itself on the remote
machine through an SSH session. The role of the local instance is to expose the local
file system, context (environment variables and working directory), and command to
execute. The remote instance will use this to set up a chroot environment that resembles
the local environment as closely as possible and then executes the command within it.
"""

import logging
import platform
import signal
import sys
from typing import List, NoReturn, Optional

from semver import VersionInfo

import outrun.constants as constants
from outrun.logger import log
import outrun.operations as operations
from .args import Arguments


def main(arguments: Optional[List[str]] = None) -> NoReturn:
    """
    Run either the local or remote side of outrun with the given arguments.

    Defaults to parsing command-line arguments from sys.argv if none are specified.
    """
    # Parse command-line arguments.
    args = Arguments.parse(arguments)

    # Check if the local and remote instance use compatible protocols.
    if args.protocol.major != VersionInfo.parse(constants.PROTOCOL_VERSION).major:
        log.error(
            f"incompatible protocol ({args.protocol} != {constants.PROTOCOL_VERSION})"
        )
        sys.exit(constants.OUTRUN_ERROR_CODE)

    # Check if the local and remote machines have matching platforms.
    if args.platform != platform.machine():
        log.error(f"incompatible platform ({args.platform} != {platform.machine()}")
        sys.exit(constants.OUTRUN_ERROR_CODE)

    # Configure debug logging.
    if args.debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.ERROR)

    # Run operations of the local or remote side.
    ops: operations.Operations

    if args.remote:
        ops = operations.RemoteOperations(args)
    else:
        ops = operations.LocalOperations(args)

    try:
        exit_code = ops.run()
    except KeyboardInterrupt:
        exit_code = 128 + signal.SIGINT
    except Exception as e:
        log.error(f"failed to run command: {e}")
        exit_code = constants.OUTRUN_ERROR_CODE

    # Exit with either the exit code of the original command, 255 for SSH errors, or
    # OUTRUN_ERROR_CODE for outrun failures.
    sys.exit(exit_code)
