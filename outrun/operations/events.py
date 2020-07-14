"""
Module with utilities for coordinating a linear sequence of events across threads.

outrun depends on multiple threads to asynchronously work together and it is important
to assure that operations are executed in the right order and without errors to catch
unexpected events and properly shut down. Therefore the main thread sets up a central
event bus that the threads use to signal important events with the main thread asserting
that these happen (are posted to the queue) one after another in the expected order.

For example, consider a producer thread that provides work items to a consumer thread,
where these consumer and producer threads also depend on a bunch of background services
to function. Then we could enforce proper operations like this:

def main():
    q = EventQueue()

    # These threads signal HELPER_STOPPED when exiting.
    start_thread(helper_service_a, q)
    start_thread(helper_service_b, q)

    # These threads signal PRODUCER_FINISHED and CONSUMER_FINISHED, respectively.
    start_thread(producer, q)
    start_thread(consumer, q)

    q.expect(PRODUCER_FINISHED)
    q.expect(CONSUMER_FINISHED)

    stop_helpers()

    q.expect(HELPER_STOPPED)
    q.expect(HELPER_STOPPED)

This code clearly describes that we expect the consumer to only finish once the producer
has finished generating work, and that we don't expect helpers to stop in the meanwhile.
Any deviation from this raises an exception that we can use to abort the program.
"""

from __future__ import annotations

from enum import auto, Enum
import queue
from typing import Any, Tuple, Union


class Event(Enum):
    """Types of events."""

    # Local specific events
    SSH_START = auto()
    TOKEN_READ = auto()

    # Remote specific events
    PROCESS_START = auto()

    ROOT_FILESYSTEM_MOUNT = auto()
    ROOT_FILESYSTEM_UNMOUNT = auto()

    # Shared events
    PROGRAM_EXIT = auto()

    EXCEPTION = auto()


class UnexpectedEvent(Exception):
    """Exception raised when an event occurs that is not being waited upon."""

    def __init__(
        self,
        message: str,
        expected_event: Event,
        actual_event: Event,
        actual_value: Any,
    ) -> None:
        """Instantiate the exception with a description of what happened."""
        super().__init__(message, expected_event, actual_event, actual_value)

        self.message = message

        self.expected_event = expected_event
        self.actual_event = actual_event
        self.actual_value = actual_value


class EventQueue:
    """Thread-safe queue of events that can be notified of and waited upon."""

    def __init__(self) -> None:
        """Instantiate a new EventQueue."""
        self._queue: queue.Queue[Tuple[Event, Any]] = queue.Queue()

    def notify(self, event: Event, value: Any = None) -> None:
        """Post an event and any associated value to the queue."""
        self._queue.put((event, value))

    def exception(self, exception: Union[Exception, str]) -> None:
        """Post an exception event to the queue."""
        if isinstance(exception, Exception):
            self.notify(Event.EXCEPTION, exception)
        else:
            self.notify(Event.EXCEPTION, RuntimeError(exception))

    def expect(self, expected_event: Event) -> Any:
        """Wait for the next event on the queue and check if it matches."""
        event, value = self._queue.get()

        if event == expected_event:
            return value
        elif event == Event.EXCEPTION:
            raise value
        else:
            raise UnexpectedEvent(
                f"expected {expected_event}, but got {event}",
                expected_event,
                event,
                value,
            )
