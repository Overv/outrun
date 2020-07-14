import pytest

from outrun.operations.events import Event, EventQueue, UnexpectedEvent


def test_expect_expected():
    q = EventQueue()
    q.notify(Event.SSH_START)
    q.expect(Event.SSH_START)


def test_expect_unexpected():
    q = EventQueue()
    q.notify(Event.SSH_START, 1234)

    with pytest.raises(UnexpectedEvent) as e:
        q.expect(Event.TOKEN_READ)

    assert e.value.expected_event == Event.TOKEN_READ
    assert e.value.actual_event == Event.SSH_START
    assert e.value.actual_value == 1234


def test_exception_from_string():
    q = EventQueue()
    q.exception("foo")

    with pytest.raises(RuntimeError) as e:
        q.expect(Event.SSH_START)
    assert e.value.args == ("foo",)


def test_builtin_exception():
    q = EventQueue()
    q.exception(OSError(1))

    with pytest.raises(OSError) as e:
        q.expect(Event.SSH_START)
    assert e.value.args == (1,)
