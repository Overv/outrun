from base64 import b64encode
import os
import subprocess

import pytest

from vagrant import Vagrant


@pytest.fixture(scope="module")
def vagrant():
    v = Vagrant(os.path.dirname(__file__))

    # Start remote VM first so local VM can copy its SSH key to it afterwards
    v.up(vm_name="remote")
    v.up(vm_name="local")

    yield v

    v.halt()


@pytest.mark.vagrant
def test_basic_command(vagrant):
    output = vagrant.ssh(vm_name="local", command="outrun remote echo hi").strip()
    assert output == "hi"


@pytest.mark.vagrant
def test_ffmpeg_help(vagrant):
    output = vagrant.ssh(vm_name="local", command="outrun remote ffmpeg -h").strip()
    assert "Hyper fast Audio and Video encoder" in output


@pytest.mark.vagrant
def test_stdin(vagrant):
    output = vagrant.ssh(
        vm_name="local", command="printf foobar | outrun remote base64"
    ).strip()
    assert output == b64encode(b"foobar").strip().decode()


@pytest.mark.vagrant
def test_file_manipulation(vagrant):
    output = vagrant.ssh(
        vm_name="local", command="rm -f foo && outrun remote touch foo && stat foo"
    ).strip()
    assert "No such file or directory" not in output


@pytest.mark.vagrant
def test_lua(vagrant):
    output = vagrant.ssh(
        vm_name="local",
        command="echo 'print(123)' > script.lua && outrun remote lua script.lua",
    ).strip()
    assert "123" in output


@pytest.mark.vagrant
def test_interruption(vagrant):
    with pytest.raises(subprocess.CalledProcessError) as e:
        vagrant.ssh(
            vm_name="local",
            command="timeout --signal=INT --kill-after=10s 5s outrun remote sleep 10s",
        )

    # Timeout exits with code 124 if the signal was used successfully
    # See also 'man timeout'
    assert e.value.args[0] == 124


@pytest.mark.vagrant
def test_ssh_error(vagrant):
    output = vagrant.ssh(
        vm_name="local", command="outrun nonexistent_host echo hi 2>&1 || true"
    )

    assert "Could not resolve hostname" in output
