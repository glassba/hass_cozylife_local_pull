"""Regression tests for Transmission Control Protocol send failures."""

from __future__ import annotations

import json
import queue
import threading
import unittest
from unittest.mock import Mock, patch

from custom_components.hass_cozylife_local_pull.tcp_client import (
    CMD_QUERY,
    DeviceCommandRejectedError,
    tcp_client,
)

TEST_TIMEOUT = 5
_REAL_THREAD = threading.Thread

VALID_PID_LIST = [
    {
        "c": "01",
        "m": [
            {
                "pid": "product-1234",
                "i": "mdi:lightbulb",
                "n": "Test Device",
                "dpid": [1, 4],
            }
        ],
    }
]


class BrokenSocket:
    """Fail every attempted socket send."""

    def __init__(self) -> None:
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        pass

    def sendall(self, payload: bytes) -> None:
        """Simulate a disconnected device socket."""
        raise OSError("device offline")

    def close(self) -> None:
        """Record invalidation of the failed connection."""
        self.closed = True


class RecordingSocket:
    """Record sent data and return a matching protocol response."""

    def __init__(
        self,
        *,
        cmd: object = 3,
        res: object = 0,
        include_cmd: bool = True,
        include_data: bool = True,
        include_res: bool = True,
        response_data: dict | None = None,
        response_message: object | None = None,
    ) -> None:
        self.payload: bytes | None = None
        self.cmd = cmd
        self.res = res
        self.include_cmd = include_cmd
        self.include_data = include_data
        self.include_res = include_res
        self.response_data = response_data
        self.response_message = response_message
        self.recv_calls = 0
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        pass

    def sendall(self, payload: bytes) -> None:
        """Record a successful send."""
        self.payload = payload

    def recv(self, bufsize: int) -> bytes:
        """Return a complete acknowledgement for the recorded request."""
        self.recv_calls += 1
        request = json.loads(self.payload)
        if self.response_message is None:
            message = {"attr": [1]}
            if self.include_data:
                message["data"] = (
                    request["msg"]["data"]
                    if self.response_data is None
                    else self.response_data
                )
        else:
            message = self.response_message
        response = {
            "pv": 0,
            "sn": request["sn"],
            "msg": message,
        }
        if self.include_cmd:
            response["cmd"] = self.cmd
        if self.include_res:
            response["res"] = self.res
        return bytes(
            json.dumps(
                response,
                separators=(",", ":"),
            )
            + "\r\n",
            encoding="utf8",
        )

    def close(self) -> None:
        """Record connection invalidation."""
        self.closed = True


class BlockingSendSocket:
    """Hold one admitted send until the lifecycle test releases it."""

    def __init__(self) -> None:
        self.send_started = threading.Event()
        self.allow_send = threading.Event()
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        pass

    def sendall(self, payload: bytes) -> None:
        """Block inside the operating-system send boundary."""
        self.send_started.set()
        if not self.allow_send.wait(TEST_TIMEOUT * 2):
            raise OSError("Timed out waiting to release the test send")
        raise OSError("Injected send release")

    def shutdown(self, how: int) -> None:
        """Release the blocked send during client cleanup."""
        self.allow_send.set()

    def close(self) -> None:
        """Record connection invalidation and release the send."""
        self.closed = True
        self.allow_send.set()


class PartialWriteSocket:
    """Expose whether the client relies on a potentially partial send."""

    def __init__(self) -> None:
        self.payload: bytes | None = None
        self.send_calls = 0
        self.sendall_calls = 0
        self.recv_calls = 0

    def settimeout(self, timeout: float) -> None:
        pass

    def send(self, payload: bytes) -> int:
        """Simulate a successful one-byte partial write."""
        self.send_calls += 1
        self.payload = payload[:1]
        return 1

    def sendall(self, payload: bytes) -> None:
        """Record a complete socket write."""
        self.sendall_calls += 1
        self.payload = payload

    def recv(self, bufsize: int) -> bytes:
        """Acknowledge the complete frame sent through sendall."""
        self.recv_calls += 1
        request = json.loads(self.payload)
        return bytes(
            json.dumps(
                {
                    "cmd": 3,
                    "pv": 0,
                    "sn": request["sn"],
                    "msg": request["msg"],
                    "res": 0,
                },
                separators=(",", ":"),
            )
            + "\r\n",
            encoding="utf8",
        )


class DormantThread:
    """Record thread creation without running the reconnect worker."""

    def __init__(self, target) -> None:
        self.target = target
        self.daemon = False
        self._alive = False

    def start(self) -> None:
        self._alive = True

    def is_alive(self) -> bool:
        return self._alive


class LingeringThread(DormantThread):
    """Finish the target while retaining the operating-system alive boundary."""

    def __init__(self, target) -> None:
        super().__init__(target)
        self.target_finished = threading.Event()
        self.allow_target = threading.Event()
        self.real_thread = None

    def start(self) -> None:
        self._alive = True

        def run_target() -> None:
            try:
                self.allow_target.wait(TEST_TIMEOUT)
                self.target()
            finally:
                self.target_finished.set()

        self.real_thread = _REAL_THREAD(target=run_target, daemon=True)
        self.real_thread.start()

    def join_real_thread(self, timeout: float) -> None:
        """Wait for the operating-system thread owned by this test double."""
        if self.real_thread is not None:
            self.real_thread.join(timeout)


class ImmediateThread:
    """Run a reconnect worker synchronously for deterministic testing."""

    def __init__(self, target) -> None:
        self.target = target
        self.daemon = False
        self._alive = False

    def start(self) -> None:
        self._alive = True
        try:
            self.target()
        finally:
            self._alive = False

    def is_alive(self) -> bool:
        return self._alive


class CandidateSocket:
    """Provide one configured device-information response."""

    def __init__(self, response: bytes) -> None:
        self._response = response
        self.closed = False
        self.closed_event = threading.Event()

    def settimeout(self, timeout: float) -> None:
        pass

    def connect(self, address: tuple[str, int]) -> None:
        pass

    def sendall(self, payload: bytes) -> None:
        pass

    def recv(self, bufsize: int) -> bytes:
        response, self._response = self._response, b""
        return response

    def close(self) -> None:
        self.closed = True
        self.closed_event.set()


class BlockingReconnectSocket:
    """Block one reconnect phase until client shutdown closes the socket."""

    def __init__(self, block_on: str) -> None:
        self.block_on = block_on
        self.connect_started = threading.Event()
        self.receive_started = threading.Event()
        self.released = threading.Event()
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        pass

    def connect(self, address: tuple[str, int]) -> None:
        if self.block_on != "connect":
            return
        self.connect_started.set()
        if not self.released.wait(TEST_TIMEOUT * 2):
            raise OSError("Timed out waiting to release blocked connect")
        raise OSError("Socket closed during connect")

    def sendall(self, payload: bytes) -> None:
        pass

    def recv(self, bufsize: int) -> bytes:
        if self.block_on != "receive":
            raise AssertionError("Unexpected receive during connect test")
        self.receive_started.set()
        if not self.released.wait(TEST_TIMEOUT * 2):
            raise OSError("Timed out waiting to release blocked receive")
        raise OSError("Socket closed during receive")

    def shutdown(self, how: int) -> None:
        self.released.set()

    def close(self) -> None:
        self.closed = True
        self.released.set()


class CoordinatedRLock:
    """Let two callers finish their first critical section together."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._barrier = threading.Barrier(2)
        self._local = threading.local()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._lock.release()
        exit_count = getattr(self._local, "exit_count", 0) + 1
        self._local.exit_count = exit_count
        if exit_count == 1:
            self._barrier.wait(TEST_TIMEOUT)


class TrackingRLock:
    """Expose the current reentrant lock depth for failure-path assertions."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.depth = 0

    def __enter__(self):
        self._lock.acquire()
        self.depth += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.depth -= 1
        self._lock.release()


class ObservedRLock:
    """Signal when another thread waits for the owned reentrant lock."""

    def __init__(self, contender_waiting: threading.Event) -> None:
        self._lock = threading.RLock()
        self._contender_waiting = contender_waiting

    def __enter__(self):
        if not self._lock.acquire(blocking=False):
            self._contender_waiting.set()
            self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._lock.release()


class PausingConnectPhaseLock:
    """Pause the first owner immediately after releasing admission."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._paused = False
        self.phase_released = threading.Event()
        self.allow_owner = threading.Event()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        with self._state_lock:
            should_pause = not self._paused
            self._paused = True
        self._lock.release()
        if should_pause:
            self.phase_released.set()
            if not self.allow_owner.wait(TEST_TIMEOUT):
                raise TimeoutError(
                    "Timed out waiting to release connection phase owner"
                )


class PausingStopEvent:
    """Pause after one stop-state read to expose a publication race."""

    def __init__(self, pause_on_call: int) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._calls = 0
        self._pause_on_call = pause_on_call
        self.read_paused = threading.Event()
        self.allow_read_return = threading.Event()

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        with self._lock:
            self._calls += 1
            call = self._calls
        result = self._event.is_set()
        if call == self._pause_on_call:
            self.read_paused.set()
            if not self.allow_read_return.wait(TEST_TIMEOUT):
                raise TimeoutError("Timed out waiting to release stop-state read")
        return result

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


def _make_client(connection) -> tcp_client:
    """Create a transport client without starting a network connection."""
    with patch.object(tcp_client, "_reconnect"):
        client = tcp_client("192.0.2.1")
    client._connect = connection
    return client


def _valid_device_information_response(
    sequence_number: str,
    *,
    cmd: object = 0,
    res: object = 0,
    did: object = "device-1234",
    pid: object = "product-1234",
    include_did: bool = True,
    include_pid: bool = True,
) -> bytes:
    """Encode the minimum valid device-information response."""
    message = {}
    if include_did:
        message["did"] = did
    if include_pid:
        message["pid"] = pid
    return bytes(
        json.dumps(
            {
                "cmd": cmd,
                "pv": 0,
                "sn": sequence_number,
                "msg": message,
                "res": res,
            },
            separators=(",", ":"),
        )
        + "\r\n",
        encoding="utf8",
    )


class TransmissionControlProtocolFailureTest(unittest.TestCase):
    """Verify network send failures follow the client failure contract."""

    def test_query_returns_empty_state_when_send_fails(self) -> None:
        """Query send failures return an empty state and start reconnection."""
        for connection in (None, BrokenSocket()):
            with self.subTest(connection=connection):
                client = _make_client(connection)

                with patch.object(client, "_reconnect") as reconnect:
                    try:
                        state = client.query()
                    except (AttributeError, OSError) as err:
                        self.fail(f"Query send failure escaped the client: {err}")

                self.assertEqual(state, {})
                self.assertIsNone(client._connect)
                if connection is not None:
                    self.assertTrue(connection.closed)
                reconnect.assert_called_once_with()

    def test_query_returns_state_after_successful_response(self) -> None:
        """A valid query response publishes its device state."""
        connection = RecordingSocket(
            cmd=CMD_QUERY,
            response_data={"1": 1},
        )
        client = _make_client(connection)

        with patch.object(client, "_reconnect") as reconnect:
            state = client.query()

        self.assertEqual(state, {"1": 1})
        self.assertIs(client._connect, connection)
        self.assertFalse(connection.closed)
        reconnect.assert_not_called()

    def test_query_rejection_returns_empty_without_reconnecting(self) -> None:
        """A valid query rejection keeps the protocol connection reusable."""
        connection = RecordingSocket(
            cmd=CMD_QUERY,
            res=1,
            include_data=False,
        )
        client = _make_client(connection)

        with patch.object(client, "_reconnect") as reconnect:
            state = client.query()

        self.assertEqual(state, {})
        self.assertIs(client._connect, connection)
        self.assertFalse(connection.closed)
        reconnect.assert_not_called()

    def test_query_invalid_response_reconnects(self) -> None:
        """A malformed matching query response invalidates the connection."""
        state = {"1": 1}
        connections = (
            RecordingSocket(cmd=3, response_data=state),
            RecordingSocket(cmd=False, response_data=state),
            RecordingSocket(cmd=2.0, response_data=state),
            RecordingSocket(
                cmd=CMD_QUERY,
                include_cmd=False,
                response_data=state,
            ),
            RecordingSocket(cmd=CMD_QUERY, res=False, response_data=state),
            RecordingSocket(cmd=CMD_QUERY, res=0.0, response_data=state),
            RecordingSocket(
                cmd=CMD_QUERY,
                include_res=False,
                response_data=state,
            ),
            RecordingSocket(cmd=CMD_QUERY, response_message=[]),
            RecordingSocket(cmd=CMD_QUERY, include_data=False),
            RecordingSocket(
                cmd=CMD_QUERY,
                response_message={"attr": [1], "data": []},
            ),
        )

        for connection in connections:
            with self.subTest(connection=connection):
                client = _make_client(connection)

                with patch.object(client, "_reconnect") as reconnect:
                    result = client.query()

                self.assertEqual(result, {})
                self.assertTrue(connection.closed)
                self.assertIsNone(client._connect)
                reconnect.assert_called_once_with()

    def test_control_returns_false_when_send_fails(self) -> None:
        """Control send failures return False and start reconnection."""
        for connection in (None, BrokenSocket()):
            with self.subTest(connection=connection):
                client = _make_client(connection)

                with patch.object(client, "_reconnect") as reconnect:
                    try:
                        result = client.control({"1": 255})
                    except (AttributeError, OSError) as err:
                        self.fail(f"Control send failure escaped the client: {err}")

                self.assertFalse(result)
                self.assertIsNone(client._connect)
                if connection is not None:
                    self.assertTrue(connection.closed)
                reconnect.assert_called_once_with()

    def test_query_rejects_network_io_after_stop_request(self) -> None:
        """A stopped client returns no state without using its socket."""
        connection = RecordingSocket(response_data={})
        client = _make_client(connection)
        client.request_stop()

        with patch.object(client, "_reconnect") as reconnect:
            state = client.query()

        self.assertEqual(state, {})
        self.assertIsNone(connection.payload)
        self.assertEqual(connection.recv_calls, 0)
        self.assertFalse(connection.closed)
        reconnect.assert_not_called()

    def test_control_rejects_network_io_after_stop_request(self) -> None:
        """A stopped client rejects control without using its socket."""
        connection = RecordingSocket()
        client = _make_client(connection)
        client.request_stop()

        with patch.object(client, "_reconnect") as reconnect:
            result = client.control({"1": 255})

        self.assertFalse(result)
        self.assertIsNone(connection.payload)
        self.assertEqual(connection.recv_calls, 0)
        self.assertFalse(connection.closed)
        reconnect.assert_not_called()

    def test_query_does_not_send_after_concurrent_stop_returns(self) -> None:
        """A stop completed after admission starts still prevents query I/O."""
        connection = RecordingSocket(response_data={})
        client = _make_client(connection)
        stop_event = PausingStopEvent(1)
        client._stop_event = stop_event
        stop_waiting = threading.Event()
        client._io_lock = ObservedRLock(stop_waiting)
        results = []
        errors: queue.Queue[BaseException] = queue.Queue()
        stop_errors: queue.Queue[BaseException] = queue.Queue()
        stop_finished = threading.Event()

        def query() -> None:
            try:
                results.append(client.query())
            except BaseException as err:
                errors.put(err)

        def request_stop() -> None:
            try:
                client.request_stop()
            except BaseException as err:
                stop_errors.put(err)
            finally:
                stop_finished.set()

        worker = threading.Thread(target=query, daemon=True)
        stop_thread = threading.Thread(target=request_stop, daemon=True)
        worker.start()
        try:
            self.assertTrue(stop_event.read_paused.wait(TEST_TIMEOUT))
            stop_thread.start()
            self.assertTrue(stop_waiting.wait(TEST_TIMEOUT))
            self.assertFalse(stop_finished.is_set())
            stop_event.allow_read_return.set()
            worker.join(TEST_TIMEOUT)
            stop_thread.join(TEST_TIMEOUT)
        finally:
            stop_event.allow_read_return.set()
            worker.join(TEST_TIMEOUT)
            if stop_thread.ident is not None:
                stop_thread.join(TEST_TIMEOUT)

        self.assertFalse(worker.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertTrue(errors.empty())
        self.assertTrue(stop_errors.empty())
        self.assertTrue(stop_finished.is_set())
        self.assertEqual(results, [{}])
        self.assertIsNone(connection.payload)
        self.assertEqual(connection.recv_calls, 0)

    def test_control_does_not_send_after_concurrent_stop_returns(self) -> None:
        """A stop completed after admission starts still prevents control I/O."""
        connection = RecordingSocket()
        client = _make_client(connection)
        stop_event = PausingStopEvent(1)
        client._stop_event = stop_event
        stop_waiting = threading.Event()
        client._io_lock = ObservedRLock(stop_waiting)
        results = []
        errors: queue.Queue[BaseException] = queue.Queue()
        stop_errors: queue.Queue[BaseException] = queue.Queue()
        stop_finished = threading.Event()

        def control() -> None:
            try:
                results.append(client.control({"1": 255}))
            except BaseException as err:
                errors.put(err)

        def request_stop() -> None:
            try:
                client.request_stop()
            except BaseException as err:
                stop_errors.put(err)
            finally:
                stop_finished.set()

        worker = threading.Thread(target=control, daemon=True)
        stop_thread = threading.Thread(target=request_stop, daemon=True)
        worker.start()
        try:
            self.assertTrue(stop_event.read_paused.wait(TEST_TIMEOUT))
            stop_thread.start()
            self.assertTrue(stop_waiting.wait(TEST_TIMEOUT))
            self.assertFalse(stop_finished.is_set())
            stop_event.allow_read_return.set()
            worker.join(TEST_TIMEOUT)
            stop_thread.join(TEST_TIMEOUT)
        finally:
            stop_event.allow_read_return.set()
            worker.join(TEST_TIMEOUT)
            if stop_thread.ident is not None:
                stop_thread.join(TEST_TIMEOUT)

        self.assertFalse(worker.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertTrue(errors.empty())
        self.assertTrue(stop_errors.empty())
        self.assertTrue(stop_finished.is_set())
        self.assertEqual(results, [False])
        self.assertIsNone(connection.payload)
        self.assertEqual(connection.recv_calls, 0)

    def test_control_failure_cleanup_keeps_the_transaction_lock(self) -> None:
        """A replacement connection cannot appear before cleanup finishes."""
        client = _make_client(BrokenSocket())
        transaction_lock = TrackingRLock()
        client._io_lock = transaction_lock
        reconnect_lock_depths: list[int] = []

        with patch.object(
            client,
            "_reconnect",
            side_effect=lambda: reconnect_lock_depths.append(
                transaction_lock.depth
            ),
        ):
            result = client.control({"1": 255})

        self.assertFalse(result)
        self.assertEqual(reconnect_lock_depths, [1])

    def test_control_returns_true_after_successful_acknowledgement(self) -> None:
        """Control succeeds only after consuming the device acknowledgement."""
        connection = RecordingSocket()
        client = _make_client(connection)

        self.assertTrue(client.control({"1": 255}))
        self.assertIsNotNone(connection.payload)
        self.assertEqual(connection.recv_calls, 1)

    def test_control_rejection_raises_without_reconnecting(self) -> None:
        """A valid protocol rejection keeps the healthy connection reusable."""
        connection = RecordingSocket(res=1, include_data=False)
        client = _make_client(connection)

        with patch.object(client, "_reconnect") as reconnect:
            with self.assertRaises(DeviceCommandRejectedError):
                client.control({"1": 255})

        self.assertIs(client._connect, connection)
        self.assertFalse(connection.closed)
        self.assertEqual(connection.recv_calls, 1)
        reconnect.assert_not_called()

    def test_control_invalid_acknowledgement_reconnects(self) -> None:
        """A malformed matching response invalidates the protocol connection."""
        for connection in (
            RecordingSocket(cmd=2),
            RecordingSocket(cmd=3.0),
            RecordingSocket(include_data=False),
            RecordingSocket(include_res=False),
            RecordingSocket(response_data={"1": 0}),
            RecordingSocket(response_data={}),
            RecordingSocket(response_data={"1": 255, "2": 0}),
        ):
            with self.subTest(connection=connection):
                client = _make_client(connection)

                with patch.object(client, "_reconnect") as reconnect:
                    result = client.control({"1": 255})

                self.assertFalse(result)
                self.assertTrue(connection.closed)
                self.assertIsNone(client._connect)
                reconnect.assert_called_once_with()

    def test_control_sends_the_complete_frame(self) -> None:
        """Control uses sendall so a partial send cannot truncate a frame."""
        connection = PartialWriteSocket()
        client = _make_client(connection)

        self.assertTrue(client.control({"1": 255}))
        self.assertEqual(connection.send_calls, 0)
        self.assertEqual(connection.sendall_calls, 1)
        self.assertEqual(connection.recv_calls, 1)
        self.assertIsNotNone(connection.payload)
        self.assertTrue(connection.payload.endswith(b"\r\n"))

    def test_client_buffers_are_instance_local_and_close_clears_them(self) -> None:
        """Connection lifecycle does not share or retain receive fragments."""
        with patch.object(tcp_client, "_reconnect"):
            first = tcp_client("192.0.2.1")
            second = tcp_client("192.0.2.2")

        first._receive_buffer = b"partial"
        self.assertEqual(second._receive_buffer, b"")

        first._close_connection()
        self.assertEqual(first._receive_buffer, b"")

    def test_close_is_idempotent_and_releases_the_connection(self) -> None:
        """Stopping a client interrupts recovery and closes its active socket."""
        connection = RecordingSocket()
        client = _make_client(connection)
        close = getattr(client, "close", None)

        self.assertTrue(callable(close))
        close()
        close()

        self.assertTrue(client._stop_event.is_set())
        self.assertTrue(connection.closed)
        self.assertIsNone(client._connect)

    def test_close_after_handshake_prevents_ready_publication(self) -> None:
        """Closing after a handshake cannot publish stale readiness."""
        client = _make_client(None)
        callback_clients = []
        client.add_ready_callback(callback_clients.append)
        ready_socket = CandidateSocket(
            _valid_device_information_response("1000")
        )
        publish_started = threading.Event()
        allow_publish = threading.Event()
        close_finished = threading.Event()
        close_errors: queue.Queue[BaseException] = queue.Queue()
        publish_ready = client._publish_ready
        worker = None
        closer = None

        def publish_after_close() -> None:
            publish_started.set()
            allow_publish.wait(TEST_TIMEOUT)
            publish_ready()

        def close_client() -> None:
            try:
                client.close()
            except BaseException as err:
                close_errors.put(err)
            finally:
                close_finished.set()

        with patch.object(
            client, "_publish_ready", side_effect=publish_after_close
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            return_value=ready_socket,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            try:
                client._reconnect()
                worker = client._reconnect_thread
                self.assertTrue(publish_started.wait(TEST_TIMEOUT))
                closer = threading.Thread(target=close_client, daemon=True)
                closer.start()
                self.assertTrue(ready_socket.closed_event.wait(TEST_TIMEOUT))
                allow_publish.set()
                self.assertTrue(close_finished.wait(TEST_TIMEOUT))
                closer.join(TEST_TIMEOUT)
                worker.join(TEST_TIMEOUT)
            finally:
                allow_publish.set()
                client.close()
                if closer is not None:
                    closer.join(TEST_TIMEOUT)
                if worker is not None:
                    worker.join(TEST_TIMEOUT)

        self.assertFalse(closer.is_alive())
        self.assertFalse(worker.is_alive())
        self.assertTrue(close_errors.empty())
        self.assertTrue(ready_socket.closed)
        self.assertIsNone(client._connect)
        self.assertFalse(client._ready)
        self.assertEqual(callback_clients, [])

    def test_close_interrupts_real_reconnect_worker_wait(self) -> None:
        """Closing wakes a real worker waiting for its next reconnect attempt."""
        client = _make_client(None)
        connection = BrokenSocket()
        stop_event = threading.Event()
        wait_started = threading.Event()
        wait_timeouts = []
        wait_for_stop = stop_event.wait

        def observed_wait(timeout: float | None = None) -> bool:
            wait_timeouts.append(timeout)
            wait_started.set()
            return wait_for_stop(timeout)

        stop_event.wait = observed_wait
        client._stop_event = stop_event
        worker = None

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            return_value=connection,
        ):
            try:
                client._reconnect()
                worker = client._reconnect_thread
                self.assertTrue(wait_started.wait(TEST_TIMEOUT))

                client.close()
                worker.join(TEST_TIMEOUT)
            finally:
                client.close()
                if worker is not None:
                    worker.join(TEST_TIMEOUT)

        self.assertEqual(wait_timeouts, [60])
        self.assertFalse(worker.is_alive())
        self.assertTrue(connection.closed)
        self.assertIsNone(client._connect)

    def test_close_interrupts_each_in_progress_reconnect_phase(self) -> None:
        """Closing releases connect and handshake I/O before joining the worker."""
        for block_on in ("connect", "receive"):
            with self.subTest(block_on=block_on):
                client = _make_client(None)
                connection = BlockingReconnectSocket(block_on)
                close_finished = threading.Event()
                close_errors: queue.Queue[BaseException] = queue.Queue()
                worker = None
                closer = None

                def close_client() -> None:
                    try:
                        client.close()
                    except BaseException as err:
                        close_errors.put(err)
                    finally:
                        close_finished.set()

                with patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
                    return_value=connection,
                ):
                    try:
                        client._reconnect()
                        worker = client._reconnect_thread
                        started = (
                            connection.connect_started
                            if block_on == "connect"
                            else connection.receive_started
                        )
                        self.assertTrue(started.wait(TEST_TIMEOUT))
                        closer = threading.Thread(
                            target=close_client, daemon=True
                        )
                        closer.start()
                        self.assertTrue(close_finished.wait(TEST_TIMEOUT))
                        self.assertTrue(connection.closed)
                        worker.join(TEST_TIMEOUT)
                        self.assertFalse(worker.is_alive())
                        self.assertTrue(close_errors.empty())
                        self.assertIsNone(client._connect)
                        self.assertIsNone(
                            getattr(client, "_connecting_socket", None)
                        )
                    finally:
                        connection.close()
                        client.close()
                        if closer is not None:
                            closer.join(TEST_TIMEOUT)
                        if worker is not None:
                            worker.join(TEST_TIMEOUT)

    def test_reconnect_thread_is_published_after_start(self) -> None:
        """Shutdown cannot observe a reconnect thread before it is started."""
        client = _make_client(None)
        published_during_start = []
        workers = []

        class InspectingThread(DormantThread):
            def start(self) -> None:
                published_during_start.append(
                    client._reconnect_thread is self
                )
                super().start()

        def create_thread(*, target) -> InspectingThread:
            worker = InspectingThread(target)
            workers.append(worker)
            return worker

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
            side_effect=create_thread,
        ):
            client._reconnect()

        self.assertEqual(published_during_start, [False])
        self.assertIs(client._reconnect_thread, workers[0])

    def test_close_synchronizes_connecting_socket_publication(self) -> None:
        """Shutdown cannot miss a socket published after its stop check."""
        client = _make_client(None)
        connection = BlockingReconnectSocket("connect")
        stop_event = PausingStopEvent(pause_on_call=3)
        close_reached_boundary = threading.Event()
        close_finished = threading.Event()
        close_errors: queue.Queue[BaseException] = queue.Queue()
        client._stop_event = stop_event
        client._lifecycle_lock = ObservedRLock(close_reached_boundary)
        interrupt_socket = client._interrupt_socket

        def observe_interrupt(connection_to_close) -> None:
            if connection_to_close is None:
                close_reached_boundary.set()
            interrupt_socket(connection_to_close)

        def close_client() -> None:
            try:
                client.close()
            except BaseException as err:
                close_errors.put(err)
            finally:
                close_finished.set()

        client._interrupt_socket = observe_interrupt
        closer = threading.Thread(target=close_client, daemon=True)
        closer_started = False
        worker = None
        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            return_value=connection,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.RESPONSE_TIMEOUT_SECONDS",
            0.05,
        ):
            try:
                client._reconnect()
                worker = client._reconnect_thread
                self.assertTrue(stop_event.read_paused.wait(TEST_TIMEOUT))
                closer.start()
                closer_started = True
                self.assertTrue(
                    close_reached_boundary.wait(TEST_TIMEOUT)
                )
                stop_event.allow_read_return.set()
                self.assertTrue(
                    connection.connect_started.wait(TEST_TIMEOUT)
                )
                self.assertTrue(close_finished.wait(TEST_TIMEOUT))
                closer.join(TEST_TIMEOUT)
                self.assertTrue(close_errors.empty())
                self.assertFalse(closer.is_alive())
                self.assertTrue(connection.closed)
                self.assertFalse(worker.is_alive())
            finally:
                stop_event.allow_read_return.set()
                connection.close()
                client.close()
                if closer_started:
                    closer.join(TEST_TIMEOUT)
                if worker is not None:
                    worker.join(TEST_TIMEOUT)

    def test_device_info_requires_product_metadata(self) -> None:
        """An empty metadata result cannot make an unmapped device ready."""
        client = _make_client(
            CandidateSocket(_valid_device_information_response("1000"))
        )

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=[],
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            self.assertFalse(client._device_info())

    def test_failed_device_info_preserves_previous_complete_identity(self) -> None:
        """A failed reconnect cannot mix new identifiers with old metadata."""
        client = _make_client(
            CandidateSocket(_valid_device_information_response("1000"))
        )

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            side_effect=["1000", "1001"],
        ):
            self.assertTrue(client._device_info())
            previous_identity = (
                client._device_id,
                client._pid,
                client._device_type_code,
                client._icon,
                client._device_model_name,
                client._dpid,
            )
            client._connect = CandidateSocket(
                _valid_device_information_response(
                    "1001",
                    did="replacement-device",
                    pid="unknown-product",
                )
            )

            self.assertFalse(client._device_info())

        self.assertEqual(
            (
                client._device_id,
                client._pid,
                client._device_type_code,
                client._icon,
                client._device_model_name,
                client._dpid,
            ),
            previous_identity,
        )

    def test_device_info_rejects_non_integer_status_fields(self) -> None:
        """Boolean and floating status fields cannot complete a handshake."""
        cases = (
            {"cmd": False, "res": 0},
            {"cmd": 0.0, "res": 0},
            {"cmd": 0, "res": False},
            {"cmd": 0, "res": 0.0},
        )

        for case in cases:
            with self.subTest(case=case):
                client = _make_client(
                    CandidateSocket(
                        _valid_device_information_response("1000", **case)
                    )
                )

                with patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
                    return_value=VALID_PID_LIST,
                ), patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
                    return_value="1000",
                ):
                    self.assertFalse(client._device_info())

    def test_device_info_requires_non_empty_string_identifiers(self) -> None:
        """Device identifiers are validated before product metadata is loaded."""
        cases = (
            {"include_did": False},
            {"include_pid": False},
            {"did": ""},
            {"pid": ""},
            {"did": []},
            {"pid": {}},
        )

        for case in cases:
            with self.subTest(case=case):
                client = _make_client(
                    CandidateSocket(
                        _valid_device_information_response("1000", **case)
                    )
                )

                with patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
                    return_value=VALID_PID_LIST,
                ) as get_metadata, patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
                    return_value="1000",
                ):
                    self.assertFalse(client._device_info())

                get_metadata.assert_not_called()

    def test_device_info_uses_configured_metadata_language(self) -> None:
        """Reconnect metadata requests preserve the integration language."""
        client = _make_client(
            CandidateSocket(_valid_device_information_response("1000"))
        )
        client._lang = "zh"

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ) as get_metadata, patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            self.assertTrue(client._device_info())

        get_metadata.assert_called_once_with("zh")

    def test_device_info_rejects_protocol_errors_and_unmapped_products(self) -> None:
        """Readiness requires a successful info response and product mapping."""
        cases = (
            {"cmd": 2, "res": 0, "pid": "product-1234"},
            {"cmd": 0, "res": 1, "pid": "product-1234"},
            {"cmd": 0, "res": 0, "pid": "unknown-product"},
        )

        for case in cases:
            with self.subTest(case=case):
                response = bytes(
                    json.dumps(
                        {
                            "cmd": case["cmd"],
                            "pv": 0,
                            "sn": "1000",
                            "msg": {
                                "did": "device-1234",
                                "pid": case["pid"],
                            },
                            "res": case["res"],
                        },
                        separators=(",", ":"),
                    )
                    + "\r\n",
                    encoding="utf8",
                )
                client = _make_client(CandidateSocket(response))

                with patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
                    return_value=VALID_PID_LIST,
                ), patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
                    return_value="1000",
                ):
                    self.assertFalse(client._device_info())

    def test_reconnect_reuses_the_active_worker(self) -> None:
        """Concurrent reconnect requests do not start duplicate workers."""
        created_threads: list[DormantThread] = []

        def create_thread(*, target) -> DormantThread:
            thread = DormantThread(target)
            created_threads.append(thread)
            return thread

        client = _make_client(None)
        client._io_lock = CoordinatedRLock()
        real_thread = threading.Thread
        caller_errors: queue.Queue[BaseException] = queue.Queue()

        def reconnect() -> None:
            try:
                client._reconnect()
            except BaseException as err:
                caller_errors.put(err)

        callers = [real_thread(target=reconnect, daemon=True) for _ in range(2)]

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
            side_effect=create_thread,
        ):
            for caller in callers:
                caller.start()
            for caller in callers:
                caller.join(TEST_TIMEOUT)

        self.assertTrue(all(not caller.is_alive() for caller in callers))
        self.assertTrue(caller_errors.empty())
        self.assertEqual(len(created_threads), 1)

    def test_reconnect_retries_after_invalid_device_information(self) -> None:
        """An invalid handshake closes its socket and keeps recovery active."""
        invalid_socket = CandidateSocket(b"{}\r\n")
        valid_socket = CandidateSocket(_valid_device_information_response("1001"))
        client = _make_client(None)
        stop_event = Mock()
        stop_event.is_set.return_value = False
        stop_event.wait.side_effect = [False, True]
        client._stop_event = stop_event

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
            side_effect=lambda *, target: ImmediateThread(target),
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            side_effect=[invalid_socket, valid_socket],
        ) as socket_factory, patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.time.sleep",
            return_value=None,
        ) as sleep, patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            client._reconnect()

        self.assertEqual(socket_factory.call_count, 2)
        self.assertTrue(invalid_socket.closed)
        self.assertIs(client._connect, valid_socket)
        stop_event.wait.assert_called_once_with(60)
        sleep.assert_not_called()

    def test_ready_callback_runs_once_after_handshake_outside_lock(self) -> None:
        """A successful handshake publishes readiness once without holding I/O."""
        with patch.object(tcp_client, "_reconnect"):
            client = tcp_client("192.0.2.1")
        transaction_lock = TrackingRLock()
        client._io_lock = transaction_lock
        callback_clients: list[tcp_client] = []
        callback_lock_depths: list[int] = []
        register_callback = getattr(client, "add_ready_callback", None)
        self.assertTrue(callable(register_callback))
        register_callback(
            lambda ready_client: (
                callback_clients.append(ready_client),
                callback_lock_depths.append(transaction_lock.depth),
            )
        )
        workers: list[DormantThread] = []

        def create_thread(*, target) -> DormantThread:
            worker = DormantThread(target)
            workers.append(worker)
            return worker

        sockets = [
            CandidateSocket(_valid_device_information_response("1000")),
            CandidateSocket(_valid_device_information_response("1001")),
        ]
        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
            side_effect=create_thread,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            side_effect=sockets,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            client._reconnect()
            workers.pop(0).target()
            client._reconnect()
            workers.pop(0).target()

        self.assertEqual(callback_clients, [client])
        self.assertEqual(callback_lock_depths, [0])

    def test_ready_callback_registered_after_handshake_runs_immediately(self) -> None:
        """Platform registration cannot miss an already completed handshake."""
        with patch.object(tcp_client, "_reconnect"):
            client = tcp_client("192.0.2.1")
        workers: list[DormantThread] = []

        def create_thread(*, target) -> DormantThread:
            worker = DormantThread(target)
            workers.append(worker)
            return worker

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
            side_effect=create_thread,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            return_value=CandidateSocket(
                _valid_device_information_response("1000")
            ),
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            client._reconnect()
            workers[0].target()

        callback_clients: list[tcp_client] = []
        register_callback = getattr(client, "add_ready_callback", None)
        self.assertTrue(callable(register_callback))
        register_callback(callback_clients.append)

        self.assertEqual(callback_clients, [client])

    def test_failing_ready_callback_does_not_block_other_callbacks(self) -> None:
        """A platform callback failure cannot invalidate a ready connection."""
        with patch.object(tcp_client, "_reconnect"):
            client = tcp_client("192.0.2.1")
        callback_clients: list[tcp_client] = []
        register_callback = getattr(client, "add_ready_callback", None)
        self.assertTrue(callable(register_callback))

        def fail_callback(ready_client: tcp_client) -> None:
            raise RuntimeError("Injected callback failure")

        register_callback(fail_callback)
        register_callback(callback_clients.append)
        workers: list[DormantThread] = []

        def create_thread(*, target) -> DormantThread:
            worker = DormantThread(target)
            workers.append(worker)
            return worker

        ready_socket = CandidateSocket(_valid_device_information_response("1000"))
        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
            side_effect=create_thread,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            return_value=ready_socket,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ), self.assertLogs(
            "custom_components.hass_cozylife_local_pull.tcp_client", level="ERROR"
        ):
            client._reconnect()
            workers[0].target()

        self.assertEqual(callback_clients, [client])
        self.assertIs(client._connect, ready_socket)
        self.assertFalse(ready_socket.closed)

    def test_ready_callback_close_skips_remaining_callbacks(self) -> None:
        """Closing from one ready callback stops subsequent publication."""
        with patch.object(tcp_client, "_reconnect"):
            client = tcp_client("192.0.2.1")
        callback_clients: list[tcp_client] = []
        client.add_ready_callback(lambda ready_client: ready_client.close())
        client.add_ready_callback(callback_clients.append)

        client._publish_ready()

        self.assertEqual(callback_clients, [])

    def test_ready_callback_registered_after_close_is_discarded(self) -> None:
        """A closed client does not retain or invoke new ready callbacks."""
        with patch.object(tcp_client, "_reconnect"):
            client = tcp_client("192.0.2.1")
        callback_clients: list[tcp_client] = []

        client.close()
        client.add_ready_callback(callback_clients.append)

        self.assertEqual(callback_clients, [])
        self.assertEqual(client._ready_callbacks, [])

    def test_request_stop_rejects_ready_callback_before_cleanup(self) -> None:
        """A stop request closes callback registration before full cleanup."""
        with patch.object(tcp_client, "_reconnect"):
            client = tcp_client("192.0.2.1")
        callback_clients: list[tcp_client] = []

        client.request_stop()
        client.add_ready_callback(callback_clients.append)

        self.assertTrue(client._stop_event.is_set())
        self.assertEqual(callback_clients, [])
        self.assertEqual(client._ready_callbacks, [])

    def test_signal_stop_does_not_wait_for_admitted_ready_callback(self) -> None:
        """Stop publication returns while an admitted callback is running."""
        with patch.object(tcp_client, "_reconnect"):
            client = tcp_client("192.0.2.1")
        callback_started = threading.Event()
        allow_callback = threading.Event()

        def blocking_callback(_client: tcp_client) -> None:
            callback_started.set()
            allow_callback.wait(TEST_TIMEOUT)

        client.add_ready_callback(blocking_callback)
        callback_thread = threading.Thread(
            target=client._publish_ready, daemon=True
        )
        callback_thread.start()
        try:
            self.assertTrue(callback_started.wait(TEST_TIMEOUT))
            signal_stop = getattr(client, "signal_stop", None)
            self.assertTrue(callable(signal_stop))
            signal_stop()
            self.assertTrue(client._stop_event.is_set())
            self.assertTrue(callback_thread.is_alive())
        finally:
            allow_callback.set()
            callback_thread.join(TEST_TIMEOUT)

        self.assertFalse(callback_thread.is_alive())

    def test_signal_stop_does_not_wait_for_admitted_send(self) -> None:
        """Stop publication returns while an admitted socket send is blocked."""
        connection = BlockingSendSocket()
        client = _make_client(connection)
        signal_finished = threading.Event()
        signal_errors: queue.Queue[BaseException] = queue.Queue()

        def signal_stop() -> None:
            try:
                client.signal_stop()
            except BaseException as err:
                signal_errors.put(err)
            finally:
                signal_finished.set()

        query_thread = threading.Thread(target=client.query, daemon=True)
        signal_thread = threading.Thread(target=signal_stop, daemon=True)
        try:
            query_thread.start()
            self.assertTrue(connection.send_started.wait(TEST_TIMEOUT))
            signal_thread.start()
            self.assertTrue(signal_finished.wait(TEST_TIMEOUT))
            self.assertTrue(client._stop_event.is_set())
            self.assertTrue(query_thread.is_alive())
        finally:
            connection.allow_send.set()
            client.close()
            if query_thread.ident is not None:
                query_thread.join(TEST_TIMEOUT)
            if signal_thread.ident is not None:
                signal_thread.join(TEST_TIMEOUT)

        self.assertTrue(signal_errors.empty())
        self.assertFalse(query_thread.is_alive())
        self.assertFalse(signal_thread.is_alive())

    def test_request_stop_waits_for_admitted_send(self) -> None:
        """A full stop waits for a send admitted before stop publication."""
        connection = BlockingSendSocket()
        client = _make_client(connection)
        request_waiting = threading.Event()
        request_finished = threading.Event()
        request_errors: queue.Queue[BaseException] = queue.Queue()
        client._lifecycle_lock = ObservedRLock(request_waiting)

        def request_stop() -> None:
            try:
                client.request_stop()
            except BaseException as err:
                request_errors.put(err)
            finally:
                request_finished.set()

        query_thread = threading.Thread(target=client.query, daemon=True)
        request_thread = threading.Thread(target=request_stop, daemon=True)
        try:
            query_thread.start()
            self.assertTrue(connection.send_started.wait(TEST_TIMEOUT))
            request_thread.start()
            self.assertTrue(request_waiting.wait(TEST_TIMEOUT))
            self.assertFalse(request_finished.is_set())
        finally:
            connection.allow_send.set()
            client.close()
            if query_thread.ident is not None:
                query_thread.join(TEST_TIMEOUT)
            if request_thread.ident is not None:
                request_thread.join(TEST_TIMEOUT)

        self.assertTrue(request_errors.empty())
        self.assertTrue(request_finished.is_set())
        self.assertFalse(query_thread.is_alive())
        self.assertFalse(request_thread.is_alive())

    def test_request_stop_waits_for_admitted_connect(self) -> None:
        """A full stop waits for an admitted socket connection to finish."""
        connection = BlockingReconnectSocket("connect")
        client = _make_client(None)
        self.addCleanup(client.close)
        self.addCleanup(connection.released.set)
        request_waiting = threading.Event()
        request_finished = threading.Event()
        request_errors: queue.Queue[BaseException] = queue.Queue()
        client._connect_phase_lock = ObservedRLock(request_waiting)

        def request_stop() -> None:
            try:
                client.request_stop()
            except BaseException as err:
                request_errors.put(err)
            finally:
                request_finished.set()

        request_thread = threading.Thread(target=request_stop, daemon=True)
        worker = None
        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            return_value=connection,
        ):
            client._reconnect()
            worker = client._reconnect_thread
            self.assertIsNotNone(worker)
            self.assertTrue(connection.connect_started.wait(TEST_TIMEOUT))
            request_thread.start()
            try:
                self.assertTrue(request_waiting.wait(TEST_TIMEOUT))
                self.assertFalse(request_finished.is_set())
            finally:
                connection.released.set()
                worker.join(TEST_TIMEOUT)
                request_thread.join(TEST_TIMEOUT)

        self.assertTrue(request_errors.empty())
        self.assertTrue(request_finished.is_set())
        self.assertFalse(worker.is_alive())
        self.assertFalse(request_thread.is_alive())

    def test_request_stop_waits_for_connect_to_io_handoff(self) -> None:
        """A full stop drains the admitted connection handoff."""
        connection = CandidateSocket(b"")
        client = _make_client(None)
        phase_lock = PausingConnectPhaseLock()
        client._connect_phase_lock = phase_lock
        io_phase_finished = threading.Event()
        stop_finished = threading.Event()
        errors: queue.Queue[BaseException] = queue.Queue()

        def finish_io_phase() -> bool:
            io_phase_finished.set()
            return True

        def stop_client() -> None:
            try:
                client.request_stop()
            except BaseException as err:
                errors.put(err)
            finally:
                stop_finished.set()

        stop_thread = threading.Thread(target=stop_client, daemon=True)
        worker = None
        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            return_value=connection,
        ), patch.object(client, "_device_info", side_effect=finish_io_phase):
            try:
                client._reconnect()
                worker = client._reconnect_thread
                self.assertIsNotNone(worker)
                self.assertTrue(phase_lock.phase_released.wait(TEST_TIMEOUT))
                stop_thread.start()
                self.assertTrue(stop_finished.wait(TEST_TIMEOUT))
                self.assertTrue(io_phase_finished.is_set())
            finally:
                phase_lock.allow_owner.set()
                client.close()
                if worker is not None:
                    worker.join(TEST_TIMEOUT)
                if stop_thread.ident is not None:
                    stop_thread.join(TEST_TIMEOUT)

        self.assertTrue(errors.empty())
        self.assertFalse(worker.is_alive())
        self.assertFalse(stop_thread.is_alive())

    def test_request_stop_waits_for_admitted_handshake(self) -> None:
        """A full stop waits for an admitted device handshake to finish."""
        client = _make_client(None)
        connection = CandidateSocket(
            _valid_device_information_response("1000")
        )
        handshake_started = threading.Event()
        allow_handshake = threading.Event()
        stop_waiting = threading.Event()
        stop_finished = threading.Event()
        errors: queue.Queue[BaseException] = queue.Queue()
        order: list[str] = []
        client._connect_phase_lock = ObservedRLock(stop_waiting)
        device_info = client._device_info

        def pause_handshake() -> bool:
            handshake_started.set()
            if not allow_handshake.wait(TEST_TIMEOUT):
                raise TimeoutError("Timed out waiting to release device handshake")
            order.append("handshake")
            return device_info()

        def stop_client() -> None:
            try:
                client.request_stop()
                order.append("stop_returned")
            except BaseException as err:
                errors.put(err)
            finally:
                stop_finished.set()

        stop_thread = threading.Thread(target=stop_client, daemon=True)
        worker = None
        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            return_value=connection,
        ), patch.object(
            client, "_device_info", side_effect=pause_handshake
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            try:
                client._reconnect()
                worker = client._reconnect_thread
                self.assertIsNotNone(worker)
                self.assertTrue(handshake_started.wait(TEST_TIMEOUT))
                stop_thread.start()
                self.assertTrue(stop_waiting.wait(TEST_TIMEOUT))
                self.assertFalse(stop_finished.is_set())
            finally:
                allow_handshake.set()
                client.close()
                if worker is not None:
                    worker.join(TEST_TIMEOUT)
                if stop_thread.ident is not None:
                    stop_thread.join(TEST_TIMEOUT)

        self.assertTrue(errors.empty())
        self.assertTrue(stop_finished.is_set())
        self.assertFalse(worker.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertEqual(order, ["handshake", "stop_returned"])

    def test_request_stop_waits_for_admitted_receive(self) -> None:
        """A full stop waits for an admitted socket receive to finish."""
        connection = BlockingReconnectSocket("receive")
        client = _make_client(connection)
        self.addCleanup(client.close)
        self.addCleanup(connection.released.set)
        request_waiting = threading.Event()
        request_finished = threading.Event()
        request_errors: queue.Queue[BaseException] = queue.Queue()
        client._io_lock = ObservedRLock(request_waiting)

        def request_stop() -> None:
            try:
                client.request_stop()
            except BaseException as err:
                request_errors.put(err)
            finally:
                request_finished.set()

        query_thread = threading.Thread(target=client.query, daemon=True)
        request_thread = threading.Thread(target=request_stop, daemon=True)
        query_thread.start()
        self.assertTrue(connection.receive_started.wait(TEST_TIMEOUT))
        request_thread.start()
        try:
            self.assertTrue(request_waiting.wait(TEST_TIMEOUT))
            self.assertFalse(request_finished.is_set())
        finally:
            connection.released.set()
            query_thread.join(TEST_TIMEOUT)
            request_thread.join(TEST_TIMEOUT)

        self.assertTrue(request_errors.empty())
        self.assertTrue(request_finished.is_set())
        self.assertFalse(query_thread.is_alive())
        self.assertFalse(request_thread.is_alive())

    def test_request_stop_waits_for_admitted_ready_callback(self) -> None:
        """Stop returns only after an admitted readiness callback completes."""
        with patch.object(tcp_client, "_reconnect"):
            client = tcp_client("192.0.2.1")
        stop_event = PausingStopEvent(1)
        stop_waiting = threading.Event()
        stop_finished = threading.Event()
        errors: queue.Queue[BaseException] = queue.Queue()
        order = []
        client._stop_event = stop_event
        client._ready_callback_lock = ObservedRLock(stop_waiting)

        def run_callback() -> None:
            try:
                client._run_ready_callback(
                    lambda _client: order.append("callback")
                )
            except BaseException as err:
                errors.put(err)

        def stop_client() -> None:
            try:
                client.request_stop()
                order.append("stop_returned")
            except BaseException as err:
                errors.put(err)
            finally:
                stop_finished.set()

        callback_thread = threading.Thread(target=run_callback, daemon=True)
        stop_thread = threading.Thread(target=stop_client, daemon=True)
        callback_thread.start()
        try:
            self.assertTrue(stop_event.read_paused.wait(TEST_TIMEOUT))
            stop_thread.start()
            self.assertTrue(stop_waiting.wait(TEST_TIMEOUT))
        finally:
            stop_event.allow_read_return.set()
            callback_thread.join(TEST_TIMEOUT)
            if stop_thread.ident is not None:
                stop_thread.join(TEST_TIMEOUT)

        self.assertTrue(errors.empty())
        self.assertFalse(callback_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertTrue(stop_finished.is_set())
        self.assertEqual(order, ["callback", "stop_returned"])

    def test_completed_worker_allows_immediate_reconnect(self) -> None:
        """A completed target does not hide behind a still-alive thread object."""
        client = _make_client(None)
        valid_socket = CandidateSocket(_valid_device_information_response("1000"))
        created_threads: list[DormantThread] = []

        def create_thread(*, target) -> DormantThread:
            thread_type = LingeringThread if not created_threads else DormantThread
            thread = thread_type(target)
            created_threads.append(thread)
            return thread

        try:
            with patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
                side_effect=create_thread,
            ), patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
                return_value=valid_socket,
            ), patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
                return_value=VALID_PID_LIST,
            ), patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
                return_value="1000",
            ):
                client._reconnect()
                first_worker = created_threads[0]
                first_worker.allow_target.set()
                self.assertTrue(
                    first_worker.target_finished.wait(TEST_TIMEOUT)
                )
                client._reconnect()
        finally:
            if created_threads:
                first_worker = created_threads[0]
                first_worker.allow_target.set()
                first_worker.join_real_thread(TEST_TIMEOUT)

        self.assertEqual(len(created_threads), 2)

    def test_ready_callback_query_failure_starts_replacement_worker(self) -> None:
        """A failed first entity query hands recovery to a new worker."""
        client = _make_client(None)
        first_socket = CandidateSocket(
            _valid_device_information_response("1000")
        )
        replacement_socket = CandidateSocket(
            _valid_device_information_response("1002")
        )
        created_threads: list[threading.Thread] = []
        replacement_started = threading.Event()
        query_results = []

        def create_thread(*, target) -> threading.Thread:
            is_replacement = bool(created_threads)

            def run_target() -> None:
                if is_replacement:
                    replacement_started.set()
                target()

            worker = _REAL_THREAD(target=run_target)
            created_threads.append(worker)
            return worker

        client.add_ready_callback(
            lambda ready_client: query_results.append(ready_client.query())
        )
        try:
            with patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
                side_effect=create_thread,
            ), patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
                side_effect=[first_socket, replacement_socket],
            ) as socket_factory, patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
                return_value=VALID_PID_LIST,
            ), patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
                side_effect=["1000", "1001", "1002"],
            ):
                client._reconnect()
                self.assertTrue(replacement_started.wait(TEST_TIMEOUT))
                for worker in created_threads:
                    worker.join(TEST_TIMEOUT)

                self.assertEqual(query_results, [{}])
                self.assertEqual(socket_factory.call_count, 2)
                self.assertTrue(first_socket.closed)
                self.assertIs(client._connect, replacement_socket)
                self.assertFalse(replacement_socket.closed)
                self.assertTrue(
                    all(not worker.is_alive() for worker in created_threads)
                )
        finally:
            client.close()
            for worker in created_threads:
                worker.join(TEST_TIMEOUT)

    def test_external_query_failure_during_ready_callback_starts_replacement(
        self,
    ) -> None:
        """A concurrent failed query can replace a worker publishing readiness."""
        client = _make_client(None)
        first_socket = CandidateSocket(
            _valid_device_information_response("1000")
        )
        replacement_socket = CandidateSocket(
            _valid_device_information_response("1002")
        )
        created_threads: list[threading.Thread] = []
        ready_callback_started = threading.Event()
        allow_ready_callback = threading.Event()
        replacement_started = threading.Event()

        def block_ready_callback(_client: tcp_client) -> None:
            ready_callback_started.set()
            allow_ready_callback.wait()

        def create_thread(*, target) -> threading.Thread:
            is_replacement = bool(created_threads)

            def run_target() -> None:
                if is_replacement:
                    replacement_started.set()
                target()

            worker = _REAL_THREAD(target=run_target)
            created_threads.append(worker)
            return worker

        client.add_ready_callback(block_ready_callback)
        try:
            with patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
                side_effect=create_thread,
            ), patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
                side_effect=[first_socket, replacement_socket],
            ) as socket_factory, patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
                return_value=VALID_PID_LIST,
            ), patch(
                "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
                side_effect=["1000", "1001", "1002"],
            ):
                client._reconnect()
                self.assertTrue(ready_callback_started.wait(TEST_TIMEOUT))

                self.assertEqual(client.query(), {})
                self.assertTrue(replacement_started.wait(TEST_TIMEOUT))
                allow_ready_callback.set()
                for worker in created_threads:
                    worker.join(TEST_TIMEOUT)

                self.assertEqual(socket_factory.call_count, 2)
                self.assertTrue(first_socket.closed)
                self.assertIs(client._connect, replacement_socket)
                self.assertFalse(replacement_socket.closed)
                self.assertTrue(
                    all(not worker.is_alive() for worker in created_threads)
                )
        finally:
            allow_ready_callback.set()
            client.close()
            for worker in created_threads:
                worker.join(TEST_TIMEOUT)

    def test_sequence_state_is_instance_local_and_returned(self) -> None:
        """Packaging and sending return each client's exact sequence value."""
        with patch.object(tcp_client, "_reconnect"), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            first = tcp_client("192.0.2.1")
            second = tcp_client("192.0.2.2")
            first._connect = RecordingSocket()
            second._connect = RecordingSocket()

            first_frame, first_sequence = first._get_package(CMD_QUERY, {})
            first_sent_sequence = first._only_send(CMD_QUERY, {})
            second_frame, second_sequence = second._get_package(CMD_QUERY, {})

        self.assertEqual(json.loads(first_frame)["sn"], first_sequence)
        self.assertEqual(first_sequence, "1000")
        self.assertEqual(first_sent_sequence, "1001")
        self.assertEqual(json.loads(second_frame)["sn"], second_sequence)
        self.assertEqual(second_sequence, "1000")
