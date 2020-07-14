"""
Flexible RPC client and server for Python classes based on ZeroMQ and MessagePack.

outrun primarily relies on RPC to expose the local file system by simply executing calls
like open(), readdir(), truncate(), symlink() over the network. With that it has a
couple of requirements that were not met by popular existing libraries.

* Ease of exposing a large number of different functions with minimal boilerplate
    * gRPC is not suitable due to its reliance on .proto files and code generation.
    * outrun only needs to communicate with itself, so its benefits are not worth it.
* Low overhead per call
    * Latency is key with file system operations.
    * xmlrpc suffers from HTTP overhead.
* Multithreading support
    * zerorpc (https://github.com/0rpc/zerorpc-python) does not function well with
    multithreading due to its reliance on gevent.

For that reason outrun ships with a custom RPC implementation that is largely inspired
by zerorpc, but with the following differences:

* Native support for multithreading
    * On the server side with multiple workers
    * On the client side with a socket per thread
* Automatic serialization and deserialization of dataclasses based on type annotations
* Transport and faithful recreation of builtin exceptions
    * As opposed to wrapping all exceptions into a generic RPC exception type
    * This makes it easy to forward things like FileNotFoundError.
* Support for shared secret authentication
    * There is no need for encryption because the SSH tunnels take care of this.

MessagePack supports fast and compact serialization, and allows for custom types without
the need for writing a custom spec and generating code (like gRPC). ZeroMQ is perfect
for protocol handling thanks to its builtin DEALER/ROUTER and REQUEST/REPLY patterns.
"""

from abc import ABC
import builtins
from dataclasses import is_dataclass
from enum import auto, Enum
import json
import logging
import threading
import time
import typing
from typing import Any, Callable, Dict, IO, List, NoReturn, Optional, Tuple

import msgpack
import zmq

from outrun.logger import log, summarize


class Encoding:
    """
    Serialization and deserialization of objects using JSON or MessagePack.

    MessagePack serialization can be used for efficient network transfer and JSON
    serialization for simple disk storage.
    """

    def __init__(self, *dataclasses: type):
        """Initialize a (de)serializer with support for the given dataclass types."""
        self._dataclasses: Dict[str, type] = {}

        for dataclass in dataclasses:
            self.register_dataclasses(dataclass)

    def register_dataclasses(self, seed_type: type) -> None:
        """
        Register all dataclass types used within the specified type.

        This includes the class itself, its class members, nested dataclasses, and
        container types like List and Optional.
        """
        for dataclass in self._discover_dataclasses(seed_type):
            self._dataclasses[dataclass.__qualname__] = dataclass

    def pack(self, obj: Any) -> bytes:
        """Serialize an object using MessagePack."""
        return msgpack.packb(obj, default=self.serialize_obj)

    def unpack(self, data: bytes) -> Any:
        """Deserialize an object using MessagePack."""
        return msgpack.unpackb(data, object_hook=self.deserialize_obj)

    def dump_json(self, obj: Any, fp: IO[str]) -> None:
        """Serialize an object to JSON."""
        json.dump(obj, fp, default=self.serialize_obj)

    def load_json(self, fp: IO[str]) -> Any:
        """Deserialize an object from JSON."""
        return json.load(fp, object_hook=self.deserialize_obj)

    def serialize_obj(self, obj: Any) -> Any:
        """Turn a dataclass or object into a serialization friendly representation."""
        if isinstance(obj, BaseException):
            return self._serialize_exception(obj)
        elif obj.__class__.__qualname__ in self._dataclasses:
            return self._serialize_dataclass(obj)
        else:
            raise ValueError(f"unserializable object {obj}")

    def deserialize_obj(self, obj: Any) -> Any:
        """Reconstruct a dataclass or exception from a serialized representation."""
        if isinstance(obj, dict) and "__exception__" in obj:
            return self._deserialize_exception(obj)
        elif isinstance(obj, dict) and "__data__" in obj:
            return self._deserialize_dataclass(obj)
        else:
            return obj

    #
    # Exception serialization
    #

    @staticmethod
    def _serialize_exception(exc: BaseException) -> Dict:
        """Turn an exception into a serialization friendly dict."""
        return {"__exception__": {"name": exc.__class__.__qualname__, "args": exc.args}}

    @staticmethod
    def _deserialize_exception(obj: Dict) -> BaseException:
        """
        Reconstruct an exception from its serialized representation.

        If it was a builtin exception (like IOError) then it is reconstructed
        faithfully, otherwise as a generic Exception with the original arguments.
        """
        name = obj["__exception__"]["name"]
        args = obj["__exception__"]["args"]

        builtin_exc = getattr(builtins, name, None.__class__)

        if isinstance(builtin_exc, type) and issubclass(builtin_exc, BaseException):
            return builtin_exc(*args)
        else:
            return Exception(*args)

    #
    # Data class serialization
    #

    @classmethod
    def _serialize_dataclass(cls, obj: Any) -> Dict:
        """Turn a dataclass into a serialization friendly dict."""
        return {"__data__": {"type": obj.__class__.__qualname__, "data": obj.__dict__}}

    def _deserialize_dataclass(self, obj: Dict) -> Any:
        """
        Reconstruct a dataclass from its serialized representation.

        Only previously registered dataclass types can be deserialized.
        """
        type_name = obj["__data__"]["type"]
        type_data = obj["__data__"]["data"]

        if type_name in self._dataclasses:
            try:
                return self._dataclasses[type_name](**type_data)
            except Exception as e:
                raise TypeError(f"failed to deserialize {type_name}: {e}")
        else:
            raise TypeError(f"unknown dataclass '{type_name}'")

    @staticmethod
    def _discover_dataclasses(*seed_types: type) -> List[type]:
        """
        Find all dataclass types used with the specified type.

        This includes the class itself, its class members, nested dataclasses, and
        container types like List and Optional.
        """
        candidates = set(seed_types)
        explored = set()
        dataclasses = set()

        while len(candidates) > 0:
            candidate = candidates.pop()

            if candidate not in explored:
                explored.add(candidate)
            else:
                continue

            if is_dataclass(candidate):
                dataclasses.add(candidate)

                # Discover member types of dataclass
                for subtype in typing.get_type_hints(candidate).values():
                    candidates.add(subtype)
            elif hasattr(candidate, "__origin__"):
                # Discover types nested in constructs like Union[T] and List[T]
                # (No typing.get_args/get_origin for Python 3.7 compatibility)
                for subtype in getattr(candidate, "__args__"):
                    candidates.add(subtype)

        return list(dataclasses)


class ReturnType(Enum):
    """Type of result for an RPC call."""

    NORMAL = auto()
    EXCEPTION = auto()
    TOKEN_ERROR = auto()


class InvalidTokenError(RuntimeError):
    """Exception raised when an RPC call is made with a wrong authentication token."""


class Base(ABC):
    """Shared logic between RPC client and server implementation."""

    def __init__(self, service_type: type):
        """Initialize RPC (de)serialization to support the specified service class."""
        function_types = self._discover_function_types(service_type)
        self._encoding = Encoding(*function_types)

    @staticmethod
    def _discover_function_types(service_type: type) -> List[type]:
        """Discover all types used as parameters or return values in the RPC service."""
        exposed_functions = [
            getattr(service_type, name)
            for name in dir(service_type)
            if callable(getattr(service_type, name))
        ]

        function_types: List[type] = []

        for func in exposed_functions:
            function_types += typing.get_type_hints(func).values()

        return function_types


class Server(Base):
    """
    RPC server to expose a service defined through members of a class instance.

    Example:
    ```
    class Foo:
        def bar(a, b):
            return a + b

    server = rpc.Server(Foo())
    server.serve("tcp://0.0.0.0:1234")
    ```
    """

    def __init__(
        self, service: Any, token: Optional[str] = None, worker_count: int = 1
    ):
        """
        Instantiate an RPC server for the given service class instance.

        The server will expose all methods in the class to clients. If a token is
        specified then clients will need to be initialized with that same token to be
        allowed to make calls. Incoming calls will be distributed across the specified
        number of worker threads.
        """
        super().__init__(service.__class__)

        self.context = zmq.Context()

        self.service = service
        self.token = token
        self.worker_count = worker_count

    def serve(self, endpoint: str) -> NoReturn:
        """
        Start listening and handling calls for clients on the specified endpoint.

        The endpoint should have the format of endpoint in zmq_bind
        (http://api.zeromq.org/2-1:zmq-bind), for example "tcp://0.0.0.0:1234".
        """
        socket = self.context.socket(zmq.ROUTER)
        socket.bind(endpoint)

        workers_socket = self.context.socket(zmq.DEALER)
        workers_socket.bind(f"inproc://{id(self)}")

        for _ in range(self.worker_count):
            t = threading.Thread(target=self._run_worker, daemon=True)
            t.start()

        zmq.proxy(socket, workers_socket)

        assert False, "unreachable"

    def _run_worker(self) -> NoReturn:
        """Request/response loop to handle calls for a single worker thread."""
        socket = self.context.socket(zmq.REP)
        socket.connect(f"inproc://{id(self)}")

        while True:
            # Wait for a call to come in
            token, function, *args = self._encoding.unpack(socket.recv())

            if token != self.token:
                # Authentication token mismatch between client/server
                socket.send(self._encoding.pack((ReturnType.TOKEN_ERROR.value, None)))
            else:
                # Invoke the method and return the response (value/raised exception)
                try:
                    if function is None:
                        ret = None
                    else:
                        ret = getattr(self.service, function)(*args)

                    socket.send(self._encoding.pack((ReturnType.NORMAL.value, ret)))
                except Exception as e:
                    socket.send(self._encoding.pack((ReturnType.EXCEPTION.value, e)))


class Client(Base):
    """
    RPC client to invoke methods on a service instance exposed by an RPC server.

    A single client can be used by multiple threads and will internally create multiple
    socket connections as needed.

    Example:
    ```
    foo = rpc.Client(Foo, "tcp://localhost:1234")
    c = foo.bar(1, 2)
    ```
    """

    def __init__(
        self,
        service_type: type,
        endpoint: str,
        token: Optional[str] = None,
        timeout_ms: int = -1,
    ) -> None:
        """
        Instantiate an RPC client for the service type at the given endpoint.

        The endpoint should follow the format of endpoint in zmq_connect
        (http://api.zeromq.org/3-2:zmq-connect), for example "tcp://localhost:1234".
        """
        super().__init__(service_type)

        self.endpoint = endpoint
        self.token = token
        self.timeout_ms = timeout_ms

        self.context = zmq.Context()

        self._socket_pool: Dict[threading.Thread, zmq.Socket] = {}
        self._socket_pool_lock = threading.Lock()

    def _socket(self, timeout_ms: Optional[int] = None) -> zmq.Socket:
        """
        Return a socket to be used for the current thread.

        Each thread needs its own socket because REQUEST-REPLY need to happen in
        lockstep per socket. The (initial) timeout is set to the constructor specified
        timeout, but can be overridden.
        """
        if timeout_ms is None:
            timeout_ms = self.timeout_ms

        t = threading.current_thread()

        with self._socket_pool_lock:
            if t not in self._socket_pool:
                sock = self.context.socket(zmq.REQ)

                sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
                sock.setsockopt(zmq.SNDTIMEO, timeout_ms)

                sock.connect(self.endpoint)

                self._socket_pool[t] = sock

            return self._socket_pool[t]

    def ping(self, timeout_ms: Optional[int] = None) -> None:
        """
        Check if the service is available.

        The check will use the timeout from the constructor by default, but this timeout
        can be overridden using the parameter.
        """
        sock = self._socket(timeout_ms)

        # Temporarily override timeout
        if timeout_ms is not None:
            sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
            sock.setsockopt(zmq.SNDTIMEO, timeout_ms)

        try:
            self.__getattr__(None)()
        finally:
            # Restore to the constructor timeout
            if timeout_ms is not None:
                sock.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
                sock.setsockopt(zmq.SNDTIMEO, self.timeout_ms)

    def __del__(self) -> None:
        """Close the client sockets and their ZeroMQ context."""
        with self._socket_pool_lock:
            for sock in self._socket_pool.values():
                sock.close(linger=0)

            self.context.destroy()

    @property
    def socket_count(self) -> int:
        """Return the number of sockets for this client."""
        with self._socket_pool_lock:
            return len(self._socket_pool)

    @staticmethod
    def _summarize_args(args: tuple) -> Tuple[str, ...]:
        """Summarize a tuple of function arguments."""
        return tuple([summarize(arg) for arg in args])

    def __getattr__(self, name: Optional[str]) -> Callable[..., Any]:
        """Retrieve a wrapper to call the specified remote function."""

        def fn(*args: Any) -> Any:
            """
            Call wrapped remote function with the given arguments.

            Serializes the arguments, makes the call and deserializes the resulting
            return value or raises the resulting exception.

            ZeroMQ connections are stateless so the token is sent again with every call.
            """
            sock = self._socket()

            t_call = time.time()

            # Serialize arguments and invoke remote function
            call = self._encoding.pack((self.token, name, *args))
            sock.send(call)

            # Wait for answer (return value, exception, token error, or RPC error)
            try:
                typ, *ret = self._encoding.unpack(sock.recv())
            except zmq.ZMQError:
                raise IOError("rpc call timed out")

            t_return = time.time()

            # Explicit check before logging because _summarize_args is relatively slow
            if log.isEnabledFor(logging.DEBUG):
                t_millis = round((t_return - t_call) * 1000)
                log.debug(f"rpc::{name}{self._summarize_args(args)} - {t_millis} ms")

            if typ == ReturnType.NORMAL.value:
                if len(ret) == 1:
                    return ret[0]
                else:
                    return ret
            elif typ == ReturnType.EXCEPTION.value:
                raise ret[0]
            elif typ == ReturnType.TOKEN_ERROR.value:
                raise InvalidTokenError("token mismatch between client and server")
            else:
                raise ValueError(f"unexpected return type {typ}")

        return fn
