"""Packaging information for outrun."""

import sys

import setuptools

from outrun.constants import VERSION

if sys.version_info[:3] < (3, 7, 0):
    print("outrun requires Python 3.7 to run.")
    sys.exit(1)

install_requires = [
    "msgpack>=1.0.0",
    "pyzmq>=19.0.0",
    "lz4>=3.0.2",
    "fasteners>=0.15",
    "semver>=2.9.1",
]

extras_require = {
    "dev": [
        "rope>=0.14.0",
        "flake8>=3.7.9",
        "flake8-docstrings>=1.5.0",
        "flake8-import-order>=0.18.1",
        "black>=19.10b0",
        "pylint>=2.4.4",
        "mypy>=0.770",
        "pytest>=5.4.1",
        "pytest-cov>=2.8.1",
        "python-vagrant>=0.5.15",
    ]
}


def _long_description():
    with open("README.md") as f:
        return f.read()


setuptools.setup(
    name="outrun",
    version=VERSION,
    description="Delegate execution of a local command to a remote machine.",
    long_description=_long_description(),
    long_description_content_type="text/markdown",
    url="https://github.com/Overv/outrun",
    download_url="https://github.com/Overv/outrun",
    author="Alexander Overvoorde",
    author_email="overv161@gmail.com",
    license="Apache",
    packages=setuptools.find_packages(),
    entry_points={"console_scripts": ["outrun = outrun.__main__:main"]},
    install_requires=install_requires,
    extras_require=extras_require,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Topic :: System :: Clustering",
    ],
    python_requires=">=3.7",
)
