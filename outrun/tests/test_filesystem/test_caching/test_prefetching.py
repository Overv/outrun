import os
import shutil
from unittest import mock

from outrun.filesystem.caching.prefetching import PrefetchSuggestion
import outrun.filesystem.caching.prefetching as prefetching


def test_symlink_target_on_symlink(tmp_path):
    os.symlink(tmp_path / "bar", tmp_path / "foo")

    suggestions = prefetching.symlink_target(str(tmp_path / "foo"))
    assert suggestions == [PrefetchSuggestion(str(tmp_path / "bar"), contents=False)]


def test_symlink_target_on_non_symlink(tmp_path):
    suggestions = prefetching.symlink_target(str(tmp_path))
    assert len(suggestions) == 0


def test_python_pycache(tmp_path):
    (tmp_path / "sample.py").touch()
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "sample.cpython-38.pyc").touch()

    suggestions = prefetching.python_pycache(str(tmp_path / "sample.py"))

    assert (
        PrefetchSuggestion(path=str(tmp_path / "sample.py"), contents=True)
        in suggestions
    )

    assert (
        PrefetchSuggestion(path=str(tmp_path / "__pycache__"), contents=False)
        in suggestions
    )

    assert (
        PrefetchSuggestion(
            path=str(tmp_path / "__pycache__" / "sample.cpython-38.pyc"), contents=True
        )
        in suggestions
    )


def test_python_pycache_non_python_file(tmp_path):
    suggestions = prefetching.python_pycache(str(tmp_path / "nonexistent"))
    assert len(suggestions) == 0


def test_compiled_perl_module(tmp_path):
    suggestions = prefetching.compiled_perl_module(str(tmp_path / "sample.pmc"))
    assert PrefetchSuggestion(str(tmp_path / "sample.pm"), contents=True) in suggestions


def test_compiled_perl_module_non_perl_file(tmp_path):
    suggestions = prefetching.compiled_perl_module(str(tmp_path / "nonexistent"))
    assert len(suggestions) == 0


def test_elf_dependencies():
    sh_path = shutil.which("ssh")

    suggestions = prefetching.elf_dependencies(sh_path)

    assert len(suggestions) > 0
    assert all(".so" in s.path for s in suggestions)


def test_elf_dependencies_non_executable(tmp_path):
    (tmp_path / "non_elf_executable").touch()
    (tmp_path / "non_elf_executable").chmod(0o777)

    suggestions = prefetching.elf_dependencies(str(tmp_path / "non_elf_executable"))
    assert len(suggestions) == 0


def test_elf_dependencies_symlinks(tmp_path):
    os.symlink("bar.so", tmp_path / "foo.so")

    with mock.patch(
        "outrun.filesystem.caching.prefetching.is_elf_binary"
    ) as mock_is_elf_binary:
        mock_is_elf_binary.return_value = True

        with mock.patch(
            "outrun.filesystem.caching.prefetching.read_elf_dependencies"
        ) as mock_read_elf_dependencies:
            mock_read_elf_dependencies.return_value = [str(tmp_path / "foo.so")]

            suggestions = prefetching.elf_dependencies("dummy")

            assert (
                PrefetchSuggestion(str(tmp_path / "foo.so"), contents=False)
                in suggestions
            )
            assert (
                PrefetchSuggestion(str(tmp_path / "bar.so"), contents=True)
                in suggestions
            )


def test_elf_dependencies_with_weird_characters():
    with mock.patch("subprocess.check_output") as mock_check_output:
        mock_check_output.return_value = b"""
            linux-vdso.so.1 (0x123abc)
            foo bar.so => /usr/lib/foo bar.so (0x123abc)
            foo=>bar.so => /usr/lib/foo=>bar.so (0x123abc)
        """

        dependencies = prefetching.read_elf_dependencies("dummy")

        assert dependencies == ["/usr/lib/foo=>bar.so"]
