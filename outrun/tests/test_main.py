from unittest import mock
import logging

import semver
import pytest

from outrun.__main__ import main
from outrun.logger import log


def test_no_args():
    with pytest.raises(SystemExit):
        main(["outrun"])


def test_protocol_check(caplog):
    mismatching_major = semver.VersionInfo.parse("0.0.0")

    with pytest.raises(SystemExit):
        main(
            ["outrun", f"--protocol={mismatching_major}", "hostname", "command", "arg",]
        )

    assert "incompatible protocol" in caplog.text


def test_debug_flag_set():
    with mock.patch("outrun.operations.LocalOperations"):
        with pytest.raises(SystemExit):
            main(["outrun", "--debug", "host", "command"])

        assert log.getEffectiveLevel() == logging.DEBUG


def test_debug_flag_not_set():
    with mock.patch("outrun.operations.LocalOperations"):
        with pytest.raises(SystemExit):
            main(["outrun", "host", "command"])

        assert log.getEffectiveLevel() == logging.ERROR


def test_local_operations():
    with mock.patch("outrun.operations.LocalOperations") as mock_operations:
        with pytest.raises(SystemExit):
            main(["outrun", "host", "command"])

        assert mock_operations().run.called


def test_remote_operations():
    with mock.patch("outrun.operations.RemoteOperations") as mock_operations:
        with pytest.raises(SystemExit):
            main(["outrun", "--remote", "host", "command"])

        assert mock_operations().run.called


def test_command_failure(caplog):
    with mock.patch("outrun.operations.LocalOperations") as mock_operations:
        mock_operations().run.side_effect = Exception("foo")

        with pytest.raises(SystemExit):
            main(["outrun", "host", "command"])

    assert "failed to run command: foo" in caplog.text
