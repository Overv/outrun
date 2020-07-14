import os.path

from configparser import ConfigParser

from outrun.config import Config, CacheConfig


def test_cache_config_defaults():
    parser = ConfigParser()
    parser.read_string("[cache]")

    cfg = CacheConfig.load(parser["cache"])

    assert cfg.path is not None
    assert cfg.max_entries is not None
    assert cfg.max_size is not None


def test_cache_config_load():
    parser = ConfigParser()
    parser.read_string(
        """
        [cache]
        path = ~/test
        max_entries = 123
        max_size = 456
        """
    )

    cfg = CacheConfig.load(parser["cache"])

    assert cfg.path == os.path.expanduser("~/test")
    assert cfg.max_entries == 123
    assert cfg.max_size == 456


def test_config_defaults(tmpdir):
    cfg = Config.load(str(tmpdir / "nonexistent"))

    assert cfg.cache is not None


def test_config_load(tmp_path):
    (tmp_path / "config").write_text(
        """
        [cache]
        path = ~/test
        max_entries = 123
        max_size = 456
        """
    )

    cfg = Config.load(str(tmp_path / "config"))

    assert cfg.cache.path == os.path.expanduser("~/test")
    assert cfg.cache.max_entries == 123
    assert cfg.cache.max_size == 456


def test_config_load_failure_nonfatal(tmp_path):
    (tmp_path / "config").write_text("blabla")

    cfg = Config.load(str(tmp_path / "config"))

    assert cfg.cache is not None
