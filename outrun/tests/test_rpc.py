from dataclasses import dataclass
import multiprocessing
from io import StringIO
from unittest import mock
from typing import List

from pytest_cov.embed import cleanup_on_sigterm
import pytest

from outrun.rpc import Client, Encoding, InvalidTokenError, Server


def start_server_process(server: Server) -> multiprocessing.Process:
    def run_server():
        cleanup_on_sigterm()
        server.serve("tcp://127.0.0.1:8000")

    proc = multiprocessing.Process(target=run_server)
    proc.start()

    return proc


def test_call():
    class Service:
        @staticmethod
        def add(a, b):
            return a + b

    server = Server(Service())
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)
        assert client.add(3, 5) == 8
    finally:
        server_process.terminate()
        server_process.join()


def test_nonexistent_call():
    class Service:
        pass

    server = Server(Service())
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)

        with pytest.raises(AttributeError):
            client.foo()
    finally:
        server_process.terminate()
        server_process.join()


def test_successful_ping():
    class Service:
        pass

    server = Server(Service())
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)
        client.ping()
    finally:
        server_process.terminate()
        server_process.join()


def test_failing_ping_with_custom_timeout():
    class Service:
        pass

    client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=-1)

    with pytest.raises(IOError):
        client.ping(timeout_ms=1)


def test_timeout():
    class Service:
        pass

    client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1)

    with pytest.raises(IOError):
        client.foo()


def test_socket_per_thread():
    class Service:
        pass

    server = Server(Service())
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)

        with mock.patch("threading.current_thread") as m:
            m.return_value = 1
            client.ping()
            m.return_value = 2
            client.ping()

        assert client.socket_count == 2
    finally:
        server_process.terminate()
        server_process.join()


def test_tuple_serialization():
    class Service:
        @staticmethod
        def get_tuple():
            return (1, 2, 3)

    server = Server(Service())
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)

        # tuples are serialized as lists
        assert client.get_tuple() == [1, 2, 3]
    finally:
        server_process.terminate()
        server_process.join()


def test_dataclasses():
    @dataclass
    class Point:
        x: int
        y: int

    @dataclass
    class Line:
        p1: Point
        p2: Point

    class Service:
        @staticmethod
        def make_line(p1: Point, p2: Point) -> Line:
            return Line(p1, p2)

    server = Server(Service())
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)

        p1 = Point(1, 2)
        p2 = Point(3, 4)

        assert client.make_line(p1, p2) == Line(p1, p2)
    finally:
        server_process.terminate()
        server_process.join()


def test_dataclass_in_container_type():
    @dataclass
    class Point:
        x: int
        y: int

    class Service:
        @staticmethod
        def make_point_list(x: int, y: int) -> List[Point]:
            return [Point(x, y)]

    server = Server(Service())
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)

        assert client.make_point_list(1, 2) == [Point(1, 2)]
    finally:
        server_process.terminate()
        server_process.join()


def test_builtin_exceptions():
    class Service:
        @staticmethod
        def os_failure():
            raise OSError("foo")

        @staticmethod
        def value_failure():
            raise ValueError("bar")

    server = Server(Service())
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)

        with pytest.raises(OSError) as e:
            client.os_failure()
        assert e.value.args == ("foo",)

        with pytest.raises(ValueError) as e:
            client.value_failure()
        assert e.value.args == ("bar",)
    finally:
        server_process.terminate()
        server_process.join()


def test_custom_exception():
    class CustomException(Exception):
        pass

    class Service:
        @staticmethod
        def custom_failure():
            raise CustomException("a", "b", "c")

    server = Server(Service())
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)

        with pytest.raises(Exception) as e:
            client.custom_failure()
        assert e.value.args == ("a", "b", "c")
    finally:
        server_process.terminate()
        server_process.join()


def test_missing_token():
    class Service:
        pass

    server = Server(Service(), token="1234")
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", timeout_ms=1000)
        with pytest.raises(InvalidTokenError):
            client.ping()
    finally:
        server_process.terminate()
        server_process.join()


def test_invalid_token():
    class Service:
        pass

    server = Server(Service(), token="1234")
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", token="5678", timeout_ms=1000)
        with pytest.raises(InvalidTokenError):
            client.ping()
    finally:
        server_process.terminate()
        server_process.join()


def test_valid_token():
    class Service:
        pass

    server = Server(Service(), token="1234")
    server_process = start_server_process(server)

    try:
        client = Client(Service, "tcp://127.0.0.1:8000", token="1234", timeout_ms=1000)
        client.ping()
    finally:
        server_process.terminate()
        server_process.join()


def test_json_encoding_dataclasses():
    @dataclass
    class Point:
        x: int
        y: int

    @dataclass
    class Line:
        p1: Point
        p2: Point

    encoding = Encoding(Line)

    obj_in = ["abc", True, Line(Point(1, 2), Point(3, 4)), Point(5, 6)]

    io = StringIO()
    encoding.dump_json(obj_in, io)

    io.seek(0)
    obj_out = encoding.load_json(io)

    assert obj_in == obj_out


def test_json_encoding_exceptions():
    encoding = Encoding()

    exceptions_in = [OSError("a", "b"), TypeError("c"), NotImplementedError()]

    io = StringIO()
    encoding.dump_json(exceptions_in, io)

    io.seek(0)
    exceptions_out = encoding.load_json(io)

    with pytest.raises(OSError) as e:
        raise exceptions_out[0]
    assert e.value.args == ("a", "b")

    with pytest.raises(TypeError) as e:
        raise exceptions_out[1]
    assert e.value.args == ("c",)

    with pytest.raises(NotImplementedError) as e:
        raise exceptions_out[2]
    assert e.value.args == ()


def test_unserializable_object():
    encoding = Encoding()

    with pytest.raises(ValueError):
        encoding.serialize_obj(set())


def test_deserialize_unknown_dataclass():
    @dataclass
    class Point:
        x: int
        y: int

    encoding = Encoding(Point)
    serialized = encoding.serialize_obj(Point(1, 2))

    with pytest.raises(TypeError):
        encoding = Encoding()
        encoding.deserialize_obj(serialized)
