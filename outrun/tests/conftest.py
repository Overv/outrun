"""Module that adds flags to pytest to enable certain extra tests."""

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--vagrant", action="store_true", default=False, help="Run Vagrant tests"
    )

    parser.addoption(
        "--fuse", action="store_true", default=False, help="Run FUSE tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "vagrant: mark test as requiring Vagrant to run")
    config.addinivalue_line("markers", "fuse: mark test as requiring FUSE to run")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--vagrant"):
        skip_vagrant = pytest.mark.skip(reason="only runs with --vagrant option")

        for item in items:
            if "vagrant" in item.keywords:
                item.add_marker(skip_vagrant)

    if not config.getoption("--fuse"):
        skip_fuse = pytest.mark.skip(reason="only runs with --fuse option")

        for item in items:
            if "fuse" in item.keywords:
                item.add_marker(skip_fuse)
