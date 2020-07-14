import pytest

from outrun.args import Arguments


def test_no_args():
    with pytest.raises(SystemExit):
        Arguments.parse()


def test_basic_usage():
    args = Arguments.parse(["hostname", "command", "arg1", "arg2"])

    assert args.destination == "hostname"
    assert args.command == "command"
    assert args.args == ["arg1", "arg2"]

    assert not args.remote


def test_command_without_args():
    args = Arguments.parse(["hostname", "command"])

    assert args.command == "command"
    assert args.args == []


def test_protocol_parsing():
    args = Arguments.parse(["--protocol=1.2.3", "hostname", "command", "arg"])

    assert args.protocol.major == 1
    assert args.protocol.minor == 2
    assert args.protocol.patch == 3

    with pytest.raises(SystemExit):
        Arguments.parse(["--protocol=abc", "hostname", "command", "arg"])


def test_cache():
    args = Arguments.parse(["hostname", "command", "arg"])
    assert args.cache

    args = Arguments.parse(["--no-cache", "hostname", "command", "arg"])
    assert not args.cache


def test_timeout():
    args = Arguments.parse(["--timeout=1234", "hostname", "command", "arg"])
    assert args.timeout == 1234

    with pytest.raises(SystemExit):
        Arguments.parse(["--timeout=-1", "hostname", "command", "arg"])


def test_command_arguments_that_resemble_flags():
    args = Arguments.parse(["hostname", "command", "args", "--debug"])

    assert not args.debug
    assert args.args == ["args", "--debug"]


def test_extra_ssh_args():
    args = Arguments.parse(["--ssh=-4 -E logfile", "hostname", "command"])

    assert args.extra_ssh_args == ["-4", "-E", "logfile"]
