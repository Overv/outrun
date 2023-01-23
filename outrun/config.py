"""Module for configuration variables with defaults that are overridable by a file."""

from __future__ import annotations

from configparser import ConfigParser, SectionProxy
from dataclasses import dataclass, field
import os

from outrun.logger import log


@dataclass
class CacheConfig:
    """Configuration variables related to file system caching."""

    path: str = os.path.expanduser("~/.outrun/cache")

    max_entries: int = 1024
    max_size: int = 20 * 1024 * 1024 * 1024  # 20 GB

    @staticmethod
    def load(section: SectionProxy) -> CacheConfig:
        """Load overridden variables from a section within a config file."""
        config = CacheConfig()

        config.path = os.path.expanduser(section.get("path", fallback=config.path))

        config.max_entries = section.getint("max_entries", fallback=config.max_entries)
        config.max_size = section.getint("max_size", fallback=config.max_size)

        return config


@dataclass
class Config:
    """Configuration variables."""

    cache: CacheConfig = field(default_factory=CacheConfig)

    @staticmethod
    def load(filename: str) -> Config:
        """Load overridden configuration variables from a config file."""
        parser = ConfigParser()

        config = Config()

        try:
            with open(filename, "r") as f:
                parser.read_string(f.read(), filename)

            if "cache" in parser:
                config.cache = CacheConfig.load(parser["cache"])
        except FileNotFoundError:
            log.info(f"no config file at {filename}")
        except Exception as e:
            # An unreadable config file is not considered a fatal error since we can
            # fall back to defaults.
            log.error(f"failed to read config file {filename}: {e}")
        else:
            log.info(f"loaded config: {config}")

        return config
