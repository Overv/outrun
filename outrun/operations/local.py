"""Module that implements the local logic of outrun and the environment RPC service."""

import contextlib
import curses.ascii
import hashlib
import os
import random
import signal
import subprocess
import sys
from typing import Any, Callable, Iterable, List

from outrun.args import Arguments
import outrun.filesystem as filesystem
import outrun.filesystem.caching as caching
from outrun.logger import log
import outrun.rpc as rpc
from .common import Operations
from .environment import LocalEnvironmentService
from .events import Event, EventQueue, UnexpectedEvent


class LocalOperations(Operations):
    """Class that encapsulates all work on the local side."""

    def __init__(self, args: Arguments):
        """Initialize local operations based on command-line arguments."""
        self._args = args

        # Distinct random ports to support concurrent outrun sessions
        ports = random.sample(range(30000, 32000), 3)

        self._environment_port = args.environment_port or ports[0]
        self._filesystem_port = args.filesystem_port or ports[1]
        self._cache_port = args.cache_port or ports[2]

    def _run(self, stack: contextlib.ExitStack) -> int:
        """Run the operations on the local side."""
        events = EventQueue()

        # Set up stdout redirection where RPC token can be read from remote outrun
        out_reader, out_writer = os.pipe()
        token_thread = self._start_thread(self._run_token_skimmer, events, out_reader)
        stack.callback(token_thread.join, timeout=5.0)
        stack.callback(os.close, out_writer)

        # Start SSH session with redirected stdout
        # stderr is suppressed in TTY mode for undesired output like "connection closed"
        ssh_proc = self._start_ssh(out_writer, self._is_tty())
        ssh_thread = self._start_thread(self._watch_ssh, events, ssh_proc)
        stack.callback(ssh_thread.join, timeout=5.0)
        stack.callback(self._ignore_process_error(ssh_proc.terminate))

        # Wait for RPC token to be read
        try:
            token: str = events.expect(Event.TOKEN_READ)
        except UnexpectedEvent as e:
            if e.actual_event == Event.PROGRAM_EXIT:
                raise RuntimeError("remote outrun failed to start")
            else:
                raise e

        # Start services to expose local environment
        self._start_disposable_thread(self._run_environment_service, events, token)
        self._start_disposable_thread(self._run_filesystem_service, events, token)

        if self._args.cache:
            self._start_disposable_thread(self._run_cache_service, events, token)

        # Wait for program on remote to finish executing
        exit_code: int = events.expect(Event.PROGRAM_EXIT)

        return exit_code

    @classmethod
    def _run_token_skimmer(cls, events: EventQueue, stdout_reader: int) -> None:
        """Forward SSH's stdout to the actual stdout while capturing the token."""
        # Forward output until start marker of token
        cls._read_until_symbol(stdout_reader, curses.ascii.SOH, cls._write_stdout)

        # Read token
        buf_bytes: List[bytes] = []
        cls._read_until_symbol(stdout_reader, curses.ascii.STX, buf_bytes.append)

        buf = b"".join(buf_bytes)

        token = buf[:32].decode()
        token_checksum = buf[32:].decode()

        token_expected_checksum = hashlib.sha256(token.encode()).hexdigest()

        if token_checksum != token_expected_checksum:
            events.exception("handshake failed (invalid token checksum)")

            # If the output was not a valid token then it should be forwarded as normal
            cls._write_stdout(buf)
        else:
            events.notify(Event.TOKEN_READ, token)

        # Simply pass through all other output from this point
        while True:
            chunk = os.read(stdout_reader, 1024)

            if len(chunk) > 0:
                cls._write_stdout(chunk)
            else:
                # End of stream
                break

    @staticmethod
    def _write_stdout(data: bytes):
        """Write text to stdout and immediately flush it."""
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()

    @staticmethod
    def _read_until_symbol(
        fd: int, ascii_code: int, callback: Callable[[bytes], Any]
    ) -> None:
        """Read and forward bytes from the file descriptor until specific symbol."""
        symbol = chr(ascii_code).encode()

        while True:
            c = os.read(fd, 1)

            if c == symbol or len(c) == 0:
                break
            else:
                callback(c)

    def _start_ssh(self, stdout_writer: int, suppress_stderr: bool) -> subprocess.Popen:
        """Start the SSH process to run the remote outrun instance."""
        try:
            # Set up command to run to start remote session
            outrun_command = self._compose_remote_outrun_command()
            ssh_command = self._compose_ssh_command(outrun_command)

            def preexec_fn() -> None:
                # Terminate ssh if outrun is terminated
                self._set_death_signal(signal.SIGTERM)

            # Start SSH session that invokes outrun on the remote
            log.debug(f"running {ssh_command}")

            ssh = subprocess.Popen(
                ssh_command,
                # Proxy stdout to token skimmer
                stdout=stdout_writer,
                # Conditionally capture stderr
                stderr=subprocess.PIPE if suppress_stderr else None,
                preexec_fn=preexec_fn,
            )

            return ssh
        except Exception as e:
            raise RuntimeError(f"failed to start ssh: {e}")

    @staticmethod
    def _watch_ssh(events: EventQueue, ssh: subprocess.Popen) -> None:
        """Wait for the SSH process to finish successfully or with an error."""
        try:
            _, stderr = ssh.communicate()

            # SSH exits with the remote command's exit code or 255 in case of failure
            if ssh.returncode == 255:
                if stderr is not None:
                    events.exception(f"ssh failed: {stderr.decode().strip()}")
                else:
                    events.exception("ssh failed")
            else:
                events.notify(Event.PROGRAM_EXIT, ssh.returncode)
        except Exception as e:
            events.exception(f"ssh failed: {e}")

    def _run_environment_service(self, events: EventQueue, token: str) -> None:
        """Serve the environment RPC service."""
        try:
            service = LocalEnvironmentService([self._args.command] + self._args.args)

            server = rpc.Server(service, token)
            server.serve(f"tcp://127.0.0.1:{self._environment_port}")

            # This service should never stop running
            events.exception("environment service unexpectedly stopped")
        except Exception as e:
            events.exception(f"environment service failed: {e}")

    def _run_filesystem_service(self, events: EventQueue, token: str) -> None:
        """Serve the local file system RPC service."""
        try:
            service = filesystem.LocalFileSystemService()

            server = rpc.Server(service, token, self._args.workers)
            server.serve(f"tcp://127.0.0.1:{self._filesystem_port}")

            # This service should never stop running
            events.exception("file system service unexpectedly stopped")
        except Exception as e:
            events.exception(f"file system service failed: {e}")

    def _run_cache_service(self, events: EventQueue, token: str) -> None:
        """Serve the local file system cache RPC service."""
        try:
            service = caching.LocalCacheService()

            server = rpc.Server(service, token, self._args.workers)
            server.serve(f"tcp://127.0.0.1:{self._cache_port}")

            # This service should never stop running
            events.exception("cache service unexpectedly stopped")
        except Exception as e:
            events.exception(f"cache service failed: {e}")

    def _compose_remote_outrun_command(self) -> List[str]:
        """Compose the command for invoking outrun on the remote host."""
        outrun_command = ["outrun"]

        # Arguments
        outrun_command.extend(
            [
                "--remote",
                "--unshare",
                f"--protocol={self._args.protocol}",
                f"--platform={self._args.platform}",
                f"--config={self._args.config}",
                f"--timeout={self._args.timeout}",
                f"--environment-port={self._environment_port}",
                f"--filesystem-port={self._filesystem_port}",
            ]
        )
        if self._args.debug:
            outrun_command.append("--debug")

        if not self._args.cache:
            outrun_command.append("--no-cache")
        else:
            outrun_command.append(f"--cache-port={self._cache_port}")

        if not self._args.prefetch:
            outrun_command.append("--no-prefetch")

        if not self._args.writeback_cache:
            outrun_command.append("--sync-writes")

        # Pass dummy remote and command arguments
        outrun_command.extend([".", "."])

        return outrun_command

    def _compose_ssh_command(self, outrun_command: Iterable[str]) -> List[str]:
        """
        Compose the full command for starting the SSH session on the remote.

        This includes the SSH tunnels for the RPC services, and the command to start the
        remote outrun instance.
        """
        ssh_command = ["ssh"]

        # Disable SSH INFO messages like the TCP tunnel not being able to connect yet
        ssh_command.extend(["-o", "LogLevel=error"])

        # Configure the port forwards for the communication channels
        ssh_command.extend(
            ["-R", f"{self._environment_port}:localhost:{self._environment_port}"]
        )

        ssh_command.extend(
            ["-R", f"{self._filesystem_port}:localhost:{self._filesystem_port}"]
        )

        if self._args.cache:
            ssh_command.extend(
                ["-R", f"{self._cache_port}:localhost:{self._cache_port}"]
            )

        # Enable/disable interactive terminal based on whether outrun itself
        # is interacting with an interactive terminal (rather than being piped
        # for example)
        if self._is_tty():
            ssh_command.append("-tt")
        else:
            ssh_command.append("-T")

        # Append any additional arguments
        ssh_command.extend(self._args.extra_ssh_args)

        # Specify the remote host
        ssh_command.append(self._args.destination)

        # Invoke outrun command on remote
        ssh_command.extend(outrun_command)

        return ssh_command

    @staticmethod
    def _is_tty():
        """Check if outrun is being executed in an interactive terminal."""
        # stderr is not considered because it's not used for primary I/O
        # For example, 2>/dev/null should not affect TTY status
        return sys.stdout.isatty() and sys.stdin.isatty()
