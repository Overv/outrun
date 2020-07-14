"""Shared functionality between local and remote operations."""

from abc import ABC
import contextlib
import ctypes
import signal
import threading
from typing import Any, Callable


class Operations(ABC):
    """Base class for local or remote operations logic."""

    def run(self) -> int:
        """Run the operations and clean up properly in case of errors."""
        with contextlib.ExitStack() as stack:
            return self._run(stack)

        # https://github.com/python/mypy/issues/7726
        assert False, "unreachable"

    def _run(self, stack: contextlib.ExitStack) -> int:
        """Run the actual operations."""
        raise NotImplementedError()

    @staticmethod
    def _start_thread(target: Callable[..., None], *args: Any) -> threading.Thread:
        """
        Start a thread with the specified function.

        It is still made a daemon just in case the thread fails to exit properly and
        blocks the shutting down of the program.
        """
        t = threading.Thread(target=target, args=args, daemon=True)
        t.start()
        return t

    @staticmethod
    def _start_disposable_thread(target: Callable[..., None], *args: Any) -> None:
        """Start a disposable thread with the specified function."""
        t = threading.Thread(target=target, args=args, daemon=True)
        t.start()

    # https://stackoverflow.com/a/19448096/238180
    @staticmethod
    def _set_death_signal(sig: signal.Signals) -> int:
        """Set the signal that the current process gets when its parent dies."""
        libc = ctypes.CDLL("libc.so.6")

        # https://github.com/torvalds/linux/blob/master/include/uapi/linux/prctl.h#L9
        PR_SET_PDEATHSIG = 1

        return libc.prctl(PR_SET_PDEATHSIG, sig)

    @staticmethod
    def _ignore_process_error(call: Callable[[], Any]) -> Callable[[], None]:
        """
        Workaround for race condition in Popen.terminate/Popen.kill.

        https://bugs.python.org/issue40550
        """

        def wrapper() -> None:
            with contextlib.suppress(ProcessLookupError):
                call()

        return wrapper
