"""Regression tests for Transmission Control Protocol send failures."""

from __future__ import annotations

import json
import queue
import threading
import unittest
from unittest.mock import patch

from custom_components.hass_cozylife_local_pull.tcp_client import CMD_QUERY, tcp_client

TEST_TIMEOUT = 5


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
    """Record successfully sent socket data."""

    def __init__(self) -> None:
        self.payload: bytes | None = None

    def sendall(self, payload: bytes) -> None:
        """Record a successful send."""
        self.payload = payload


class PartialWriteSocket:
    """Expose whether the client relies on a potentially partial send."""

    def __init__(self) -> None:
        self.payload: bytes | None = None
        self.send_calls = 0
        self.sendall_calls = 0

    def send(self, payload: bytes) -> int:
        """Simulate a successful one-byte partial write."""
        self.send_calls += 1
        self.payload = payload[:1]
        return 1

    def sendall(self, payload: bytes) -> None:
        """Record a complete socket write."""
        self.sendall_calls += 1
        self.payload = payload


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

    def start(self) -> None:
        self._alive = True
        self.target()


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


def _make_client(connection) -> tcp_client:
    """Create a transport client without starting a network connection."""
    client = object.__new__(tcp_client)
    client._ip = "192.0.2.1"
    client._port = 5555
    client._connect = connection
    client._receive_buffer = b""
    client._io_lock = threading.RLock()
    client._reconnect_thread = None
    client._last_sequence_number = None
    client._device_id = str
    client._pid = str
    client._device_type_code = str
    client._icon = str
    client._device_model_name = str
    client._dpid = []
    return client


def _valid_device_information_response(sequence_number: str) -> bytes:
    """Encode the minimum valid device-information response."""
    return bytes(
        json.dumps(
            {
                "sn": sequence_number,
                "msg": {"did": "device-1234", "pid": "product-1234"},
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

    def test_control_returns_true_after_successful_send(self) -> None:
        """A successful control send preserves the True result contract."""
        connection = RecordingSocket()
        client = _make_client(connection)

        self.assertTrue(client.control({"1": 255}))
        self.assertIsNotNone(connection.payload)

    def test_control_sends_the_complete_frame(self) -> None:
        """Control uses sendall so a partial send cannot truncate a frame."""
        connection = PartialWriteSocket()
        client = _make_client(connection)

        self.assertTrue(client.control({"1": 255}))
        self.assertEqual(connection.send_calls, 0)
        self.assertEqual(connection.sendall_calls, 1)
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

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
            side_effect=lambda *, target: ImmediateThread(target),
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            side_effect=[invalid_socket, valid_socket],
        ) as socket_factory, patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.time.sleep",
            side_effect=[
                None,
                AssertionError("Unexpected additional reconnect retry"),
            ],
        ) as sleep, patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=[],
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            client._reconnect()

        self.assertEqual(socket_factory.call_count, 2)
        self.assertTrue(invalid_socket.closed)
        self.assertIs(client._connect, valid_socket)
        sleep.assert_called_once_with(60)

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

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.threading.Thread",
            side_effect=create_thread,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.socket.socket",
            return_value=valid_socket,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=[],
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            client._reconnect()
            client._reconnect()

        self.assertEqual(len(created_threads), 2)

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
