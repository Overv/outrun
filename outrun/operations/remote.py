"""Module that implements the remote logic of outrun."""

import contextlib
import curses.ascii
import hashlib
import os
import secrets
import shlex
import signal
import subprocess
import sys
import tempfile

from outrun.args import Arguments
from outrun.config import Config
import outrun.constants as constants
import outrun.filesystem as filesystem
import outrun.filesystem.caching as caching
import outrun.filesystem.fuse as fuse
from outrun.filesystem.fuse import FUSE, FuseConfig
from outrun.logger import log
import outrun.operations.local as local
import outrun.rpc as rpc
from .common import Operations
from .events import Event, EventQueue, UnexpectedEvent


class RemoteOperations(Operations):
    """Class that encapsulates all work on the remote side."""

    def __init__(self, args: Arguments):
        """Initialize remote operations based on the command-line arguments."""
        self._args = args
        self._config = Config()

    def _run(self, stack: contextlib.ExitStack) -> int:
        """Run the operations on the remote side."""
        self._setup()

        # Generate RPC token and communicate it over stdout
        token = self._token_handshake()

        events = EventQueue()

        # Mount local file system and overlay special file systems
        fs_thread = self._start_thread(self._mount_root_filesystem, events, token)
        stack.callback(fs_thread.join, timeout=5.0)

        fs_path: str = events.expect(Event.ROOT_FILESYSTEM_MOUNT)
        stack.callback(self._unmount_all_filesystems, fs_path)

        self._mount_special_filesystems(fs_path)

        # Start running command
        cmd_proc = self._start_command(token, fs_path)
        cmd_thread = self._start_thread(self._watch_command, events, cmd_proc)
        stack.callback(cmd_thread.join, timeout=5.0)
        stack.callback(self._ignore_process_error(cmd_proc.terminate))

        # Forward SIGINT (Ctrl-C) to command process
        signal.signal(signal.SIGINT, lambda *_: cmd_proc.send_signal(signal.SIGINT))

        # Wait for program to finish or error
        try:
            exit_code: int = events.expect(Event.PROGRAM_EXIT)

            return exit_code
        except UnexpectedEvent as e:
            # If something unexpected happened then the program is likely not able
            # to exit normally
            self._ignore_process_error(cmd_proc.kill)

            if e.actual_event == Event.ROOT_FILESYSTEM_UNMOUNT:
                raise RuntimeError("root file system unexpectedly unmounted")
            else:
                raise e

    def _mount_root_filesystem(self, events: EventQueue, token: str) -> None:
        """Mount the mirrored local file system."""
        try:
            # No timeout is applied since it would interfere with slow I/O operations.
            client = rpc.Client(
                filesystem.LocalFileSystemService,
                f"tcp://localhost:{self._args.filesystem_port}",
                token,
            )

            # Ensure availability of the file system service.
            client.ping(self._args.timeout)

            # Pick a random temporary directory to enable concurrent sessions.
            # This directory will be cleaned up automatically.
            mount_dir = tempfile.TemporaryDirectory(prefix="outrun_fs_")

            def mount_callback() -> None:
                events.notify(Event.ROOT_FILESYSTEM_MOUNT, mount_dir.name)

            fs: fuse.Operations

            if self._args.cache:
                cache = self._init_filesystem_cache(token)
                fs = caching.RemoteCachedFileSystem(client, mount_callback, cache)
            else:
                fs = filesystem.RemoteFileSystem(client, mount_callback)

            # Mount
            config = FuseConfig()
            config.auto_cache = True
            config.use_ino = True
            config.writeback_cache = self._args.writeback_cache

            instance = FUSE(fs, config)
            instance.mount(constants.FILESYSTEM_NAME, mount_dir.name)

            events.notify(Event.ROOT_FILESYSTEM_UNMOUNT)
        except Exception as e:
            events.exception(f"root file system mount failed: {e}")

    def _init_filesystem_cache(self, token: str) -> caching.cache.RemoteCache:
        """Initialize the cache for the caching remote file system."""
        # No timeout is applied since it would interfere with slow I/O operations.
        client = rpc.Client(
            caching.LocalCacheService,
            f"tcp://localhost:{self._args.cache_port}",
            token,
        )

        # Ensure availability of the cache service.
        client.ping(self._args.timeout)

        cache = caching.cache.RemoteCache(
            base_path=self._config.cache.path,
            machine_id=client.get_app_specific_machine_id(),
            client=client,
            prefetch=self._args.prefetch,
            max_entries=self._config.cache.max_entries,
            max_size=self._config.cache.max_size,
        )

        # Load the disk cache.
        try:
            cache.load()
        except FileNotFoundError:
            log.debug("starting with fresh cache")
        except Exception as e:
            log.error(f"failed to load cache: {e}")

        # Synchronize changed files with the local machine.
        cache.sync()

        return cache

    @staticmethod
    def _mount_special_filesystems(root_path: str) -> None:
        """Mount special remote file systems over the mirrored file system."""
        # File systems that should be overlayed from the remote rather than being
        # serviced by the local machine
        special_filesystems = ["dev", "proc", "sys", "run"]

        for name in special_filesystems:
            try:
                # Only overlay file systems that exist on both systems
                os.stat(f"/{name}")
                os.stat(os.path.join(root_path, name))

                mount_path = os.path.join(root_path, name)
                subprocess.check_output(["mount", "--rbind", f"/{name}", mount_path])
                subprocess.check_output(["mount", "--make-rslave", mount_path])
            except FileNotFoundError:
                continue
            except Exception as e:
                raise RuntimeError(f"failed to mount special file system {name}: {e}")

    @staticmethod
    def _unmount_all_filesystems(root_path: str) -> None:
        """
        Find and undo all mounts under the root path tree.

        We can't simply do the reverse of _mount_special_filesystems() because things
        like /sys result in a lot of nested mounts.
        """
        # Find all mounts from the root directory
        with open("/proc/mounts", "rb") as f:
            mount_lines = f.readlines()

        mount_paths = []

        for line in mount_lines:
            path = line.split(b" ")[1]
            if path.startswith(root_path.encode()):
                # Unescape paths with spaces and other strange characters
                mount_paths.append(path.decode("unicode-escape"))

        # Unmount paths from most nested all the way up to the root directory
        for mount_path in reversed(sorted(mount_paths)):
            subprocess.call(["umount", "-f", mount_path], stderr=subprocess.DEVNULL)

    def _start_command(self, token: str, root_path: str) -> subprocess.Popen:
        """Start the command in an environment mirrored from the local machine."""
        try:
            # Gather info about the command and local machine environment
            environment_client = rpc.Client(
                local.LocalEnvironmentService,
                f"tcp://localhost:{self._args.environment_port}",
                token,
                self._args.timeout,
            )
            command = environment_client.get_command()
            working_dir = environment_client.get_working_dir()
            environment = environment_client.get_environment()

            # Escape command into shell execution
            shell_command = " ".join(map(shlex.quote, command))

            def preexec_fn() -> None:
                # Terminate the command if outrun is terminated
                self._set_death_signal(signal.SIGTERM)

                # Chroot into mounted local file system
                os.chroot(root_path)

                # Set working directory
                os.chdir(working_dir)

            # Start the actual command
            proc = subprocess.Popen(
                shell_command,
                env=environment,
                # Make it possible to run shell commands
                shell=True,
                preexec_fn=preexec_fn,
            )

            return proc
        except Exception as e:
            raise RuntimeError(f"failed to start command: {e}")

    @staticmethod
    def _watch_command(events: EventQueue, proc: subprocess.Popen) -> None:
        """Wait for the command process to finish successfully or with an error."""
        try:
            # Wait for process to exit
            proc.communicate()

            if proc.returncode >= 0:
                events.notify(Event.PROGRAM_EXIT, proc.returncode)
            else:
                # Killed by a signal
                # https://www.tldp.org/LDP/abs/html/exitcodes.html
                events.notify(Event.PROGRAM_EXIT, 128 - proc.returncode)
        except Exception as e:
            events.exception(f"command failed: {e}")

    @staticmethod
    def _token_handshake() -> str:
        """Generate and communicate a random authentication token over stdout."""
        token = secrets.token_hex(16)
        token_signature = hashlib.sha256(token.encode()).hexdigest()

        # Output token and its checksum as in-band signal
        sys.stdout.buffer.write(chr(curses.ascii.SOH).encode())
        sys.stdout.buffer.write(f"{token}{token_signature}".encode())
        sys.stdout.buffer.write(chr(curses.ascii.STX).encode())

        sys.stdout.buffer.flush()

        return token

    def _setup(self) -> None:
        """Ensure that the remote server is set up correctly."""
        self._become_root()
        self._unshare_mounts()
        self._enable_fuse()

        # Ensure that outrun files are only accessibly by root
        os.umask(0o077)

        # Load config
        self._config = Config.load(os.path.expanduser(self._args.config))

    @staticmethod
    def _become_root() -> None:
        """Ensure that we're running as root to be able to use chroot."""
        if os.geteuid() != 0:
            try:
                home_env = f"HOME={os.getenv('HOME')}"
                os.execvp("sudo", ["sudo", home_env, sys.executable] + sys.argv)
            except OSError as e:
                raise RuntimeError(f"failed to become root using sudo: {e}")

    def _unshare_mounts(self) -> None:
        """Unshare mount namespace to hide file system mount from other processes."""
        if self._args.unshare:
            try:
                new_args = [arg for arg in sys.argv if arg != "--unshare"]
                os.execvp("unshare", ["unshare", "-m", sys.executable] + new_args)
            except OSError as e:
                raise RuntimeError(f"failed to unshare mount namespace: {e}")

    @staticmethod
    def _enable_fuse() -> None:
        """Ensure that the FUSE kernel module is installed and enabled."""
        try:
            output = subprocess.check_output(["lsmod"], stderr=subprocess.PIPE).decode()
        except (OSError, subprocess.CalledProcessError) as e:
            raise RuntimeError(f"failed to look for FUSE kernel module: {e}")

        if "fuse" not in output:
            try:
                subprocess.check_output(["modprobe", "fuse"], stderr=subprocess.PIPE)
            except (OSError, subprocess.CalledProcessError) as e:
                raise RuntimeError(f"failed to enable FUSE kernel module: {e}")
