import hashlib
from unittest import mock
import secrets
import subprocess

import pytest

from outrun.operations.local import LocalOperations
from outrun.args import Arguments
from outrun.constants import PROTOCOL_VERSION


def mock_ssh(substitute_command: str):
    realPopen = subprocess.Popen

    def wrapper(_command, *args, **kwargs):
        return realPopen(substitute_command, shell=True, *args, **kwargs)

    return wrapper


def test_missing_ssh():
    args = Arguments.parse(["dest", "cmd"])
    ops = LocalOperations(args)

    with mock.patch("subprocess.Popen") as mock_popen:
        mock_popen.side_effect = FileNotFoundError()

        with pytest.raises(RuntimeError) as e:
            ops.run()

    assert "failed to start ssh" in str(e.value)


def test_ssh_error_tty():
    args = Arguments.parse(["dest", "cmd"])
    ops = LocalOperations(args)

    callback = mock_ssh("echo 'could not resolve hostname' >&2; exit 255")

    with mock.patch("outrun.operations.local.LocalOperations._is_tty") as m:
        m.return_value = True

        with mock.patch("subprocess.Popen", side_effect=callback):
            with pytest.raises(RuntimeError) as e:
                ops.run()

    assert "ssh failed" in str(e.value)
    assert "could not resolve hostname" in str(e.value)


def test_ssh_error_no_tty():
    args = Arguments.parse(["dest", "cmd"])
    ops = LocalOperations(args)

    callback = mock_ssh("echo 'could not resolve hostname' >&2; exit 255")

    with mock.patch("subprocess.Popen", side_effect=callback):
        with pytest.raises(RuntimeError) as e:
            ops.run()

    assert "ssh failed" in str(e.value)
    assert "could not resolve hostname" not in str(e.value)


def test_remote_outrun_missing():
    args = Arguments.parse(["dest", "cmd"])
    ops = LocalOperations(args)

    callback = mock_ssh("echo 'outrun not found'; exit 127")

    with mock.patch("subprocess.Popen", side_effect=callback):
        with pytest.raises(RuntimeError) as e:
            ops.run()

    assert "remote outrun failed to start" in str(e.value)


def test_remote_outrun_bad_checksum():
    args = Arguments.parse(["dest", "cmd"])
    ops = LocalOperations(args)

    # Sleep simulates the time taken to connect to the RPC services after the failed
    # handshake. This is to prevent the test from triggering an unexpected control flow.
    callback = mock_ssh("echo '\x01abc\x02'; sleep 0.1s; echo 'success!'")

    with mock.patch("subprocess.Popen", side_effect=callback):
        with pytest.raises(RuntimeError) as e:
            ops.run()

    assert "handshake failed" in str(e.value)


def test_command_success(capsys):
    args = Arguments.parse(["dest", "cmd"])
    ops = LocalOperations(args)

    token = secrets.token_hex(16)
    token_signature = hashlib.sha256(token.encode()).hexdigest()
    handshake = f"\x01{token}{token_signature}\x02"

    # Sleep simulates the time taken to connect to the RPC services after handshake.
    # This is to prevent the test from triggering an unexpected control flow.
    callback = mock_ssh(f"echo 'foobar'; echo '{handshake}'; sleep 0.1s; echo 'ok!'")

    with mock.patch("subprocess.Popen", side_effect=callback):
        exit_code = ops.run()
        assert exit_code == 0

    assert "ok" in capsys.readouterr().out


def test_command_nonzero_exit(capsys):
    args = Arguments.parse(["dest", "cmd"])
    ops = LocalOperations(args)

    token = secrets.token_hex(16)
    token_signature = hashlib.sha256(token.encode()).hexdigest()
    handshake = f"\x01{token}{token_signature}\x02"

    # See test_command_success() for why the sleep is there
    callback = mock_ssh(f"echo '{handshake}'; sleep 0.1s; echo 'failed!'; exit 123")

    with mock.patch("subprocess.Popen", side_effect=callback):
        exit_code = ops.run()
        assert exit_code == 123

    assert "failed" in capsys.readouterr().out


def test_without_flags():
    args = Arguments.parse(["dest", "cmd"])
    ops = LocalOperations(args)

    with mock.patch("subprocess.Popen") as mock_popen:
        mock_popen.side_effect = Exception()

        with pytest.raises(RuntimeError):
            ops.run()

        assert "--debug" not in mock_popen.call_args[0][0]
        assert "--no-cache" not in mock_popen.call_args[0][0]
        assert "--no-prefetch" not in mock_popen.call_args[0][0]

        assert f"--protocol={PROTOCOL_VERSION}" in mock_popen.call_args[0][0]
        assert "--remote" in mock_popen.call_args[0][0]


def test_with_flags():
    args = Arguments.parse(
        [
            "--debug",
            "--no-cache",
            "--no-prefetch",
            "--sync-writes",
            "--timeout=1234",
            "dest",
            "cmd",
        ]
    )
    ops = LocalOperations(args)

    with mock.patch("subprocess.Popen") as mock_popen:
        mock_popen.side_effect = Exception()

        with pytest.raises(RuntimeError):
            ops.run()

        assert "--debug" in mock_popen.call_args[0][0]
        assert "--no-cache" in mock_popen.call_args[0][0]
        assert "--no-prefetch" in mock_popen.call_args[0][0]
        assert "--sync-writes" in mock_popen.call_args[0][0]
        assert "--timeout=1234" in mock_popen.call_args[0][0]


def test_ssh_port_forwarding():
    args = Arguments.parse(
        [
            "--environment-port=1234",
            "--filesystem-port=5678",
            "--cache-port=4321",
            "dest",
            "cmd",
        ]
    )
    ops = LocalOperations(args)

    with mock.patch("subprocess.Popen") as mock_popen:
        mock_popen.side_effect = Exception()

        with pytest.raises(RuntimeError):
            ops.run()

        assert "1234:localhost:1234" in mock_popen.call_args[0][0]
        assert "5678:localhost:5678" in mock_popen.call_args[0][0]
        assert "4321:localhost:4321" in mock_popen.call_args[0][0]
        assert "--environment-port=1234" in mock_popen.call_args[0][0]
        assert "--filesystem-port=5678" in mock_popen.call_args[0][0]
        assert "--cache-port=4321" in mock_popen.call_args[0][0]


def test_service_failure_detection():
    args = Arguments.parse(["--environment-port=-1", "dest", "cmd"])
    ops = LocalOperations(args)

    token = secrets.token_hex(16)
    token_signature = hashlib.sha256(token.encode()).hexdigest()
    handshake = f"\x01{token}{token_signature}\x02"

    # exec is necessary to ensure that sleep receives the termination signal
    callback = mock_ssh(f"echo '{handshake}'; exec sleep 10s")

    with mock.patch("subprocess.Popen", side_effect=callback):
        with mock.patch("outrun.rpc.Server") as mock_rpc:
            mock_rpc().serve.return_value = None

            with pytest.raises(RuntimeError) as e:
                ops.run()

            assert "service unexpectedly stopped" in str(e.value)
