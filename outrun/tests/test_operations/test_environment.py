import os

import pytest

from outrun.operations.local import LocalEnvironmentService


@pytest.fixture
def service():
    return LocalEnvironmentService(["a", "b", "c"])


def test_get_command(service):
    assert service.get_command() == ["a", "b", "c"]


def test_get_working_dir(service):
    assert service.get_working_dir() == os.getcwd()


def test_get_environment(service):
    assert service.get_environment() == dict(os.environ)
