"""Module defining the command-line arguments and providing a parser for them."""

from __future__ import annotations

import argparse
import platform
import shlex
from typing import List, Optional

import semver

from outrun.constants import PROTOCOL_VERSION, VERSION


class Arguments(argparse.Namespace):
    """Parsed command-line arguments."""

    destination: str
    command: str
    args: List[str]

    extra_ssh_args: List[str]

    remote: bool
    unshare: bool

    protocol: semver.VersionInfo

    config: str

    environment_port: Optional[int]
    filesystem_port: Optional[int]
    cache_port: Optional[int]

    debug: bool
    cache: bool
    prefetch: bool
    writeback_cache: bool
    timeout: int
    workers: int

    @classmethod
    def parse(cls, args: Optional[List[str]] = None) -> Arguments:
        """
        Parse command-line arguments from the given list of strings.

        Defaults to sys.argv if none are specified.
        """
        return cls._get_parser().parse_args(args, namespace=cls())

    @classmethod
    def _get_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Delegate execution of a local command to a remote machine.",
            usage="outrun [option...] destination command [arg...]",
        )

        parser.add_argument(
            "--version",
            action="version",
            version=f"%(prog)s {VERSION} (protocol {PROTOCOL_VERSION})",
            help="show the program version and protocol version",
        )

        # Primary arguments
        parser.add_argument("destination", type=str, help="remote host to execute on")
        parser.add_argument("command", type=str, help="command to execute")
        parser.add_argument(
            "args", type=str, nargs=argparse.REMAINDER, help="arguments for command"
        )

        # Flag to pass additional options to SSH
        parser.add_argument(
            "--ssh",
            type=cls._parse_extra_args,
            help="additional arguments to pass to SSH",
            dest="extra_ssh_args",
            default=[],
        )

        # Hidden flag to indicate that this is the outrun process on the remote side
        parser.add_argument("--remote", action="store_true", help=argparse.SUPPRESS)

        # Hidden flag to indicate that process has yet to be unshared
        parser.add_argument("--unshare", action="store_true", help=argparse.SUPPRESS)

        # Hidden flag to indicate expected protocol version
        parser.add_argument(
            "--protocol",
            type=cls._parse_version,
            default=semver.VersionInfo.parse(PROTOCOL_VERSION),
            help=argparse.SUPPRESS,
        )

        # Hidden flag to indicate expected platform
        parser.add_argument(
            "--platform", type=str, default=platform.machine(), help=argparse.SUPPRESS,
        )

        # Path to (optional) config file on remote
        parser.add_argument(
            "--config",
            type=str,
            help="path to config file (default is ~/.outrun/config)",
            default="~/.outrun/config",
        )

        # Communication ports, default to a random choice
        parser.add_argument(
            "--environment-port", type=int, help="port to use for environment service"
        )
        parser.add_argument(
            "--filesystem-port", type=int, help="port to use for file system service"
        )
        parser.add_argument(
            "--cache-port", type=int, help="port to use for cache service"
        )

        # Enable debug output for development
        parser.add_argument(
            "--debug", action="store_true", help="enable debug information"
        )

        # Disable file system caching
        parser.add_argument(
            "--no-cache",
            action="store_false",
            help="disable file system caching",
            dest="cache",
        )

        # Disable file system prefetching
        parser.add_argument(
            "--no-prefetch",
            action="store_false",
            help="disable file system prefetching",
            dest="prefetch",
        )

        # Disable write-back cache on the remote
        parser.add_argument(
            "--sync-writes",
            action="store_false",
            help="disable write-back cache",
            dest="writeback_cache",
        )

        # Configure network timeout
        parser.add_argument(
            "--timeout",
            type=cls._parse_timeout,
            help="timeout for network communications in milliseconds",
            default=5000,
        )

        # Configure number of file system workers
        parser.add_argument(
            "--workers", type=int, help="number of local file system workers", default=4
        )

        return parser

    @staticmethod
    def _parse_extra_args(arg: str) -> List[str]:
        return shlex.split(arg)

    @staticmethod
    def _parse_version(arg: str) -> semver.VersionInfo:
        try:
            return semver.VersionInfo.parse(arg)
        except (ValueError, TypeError):
            raise argparse.ArgumentTypeError("expected semantic version string")

    @staticmethod
    def _parse_timeout(arg: str) -> int:
        try:
            val = int(arg)
            assert val > 0
            return val
        except (ValueError, AssertionError):
            raise argparse.ArgumentTypeError("expected number > 0")
