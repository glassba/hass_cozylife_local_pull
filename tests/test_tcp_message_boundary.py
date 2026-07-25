"""Regression tests for Transmission Control Protocol message framing.

Background:
    Transmission Control Protocol (TCP) is a byte stream, not a message
    stream. A single ``recv`` may return a partial message (split across
    segments) or multiple messages merged together. The protocol used by
    this integration frames messages with ``\\r\\n`` (see
    ``tcp_client._get_package``), so receive paths must extract complete
    JavaScript Object Notation messages before parsing them.

These tests require fragments to be reassembled and merged frames to be
consumed one at a time.
"""

from __future__ import annotations

import json
import queue
import threading
import unittest
from unittest.mock import patch

from custom_components.hass_cozylife_local_pull.tcp_client import tcp_client

TEST_TIMEOUT = 5


VALID_INFO_RESPONSE = {
    "cmd": 0,
    "pv": 0,
    "sn": "1636463553873",
    "msg": {
        "did": "629168597cb94c4c1d8f",
        "dtp": "02",
        "pid": "e2s64v",
        "mac": "7cb94c4c1d8f",
        "ip": "192.168.123.57",
        "rssi": -33,
        "sv": "1.0.0",
        "hv": "0.0.1",
    },
    "res": 0,
}

VALID_PID_LIST = [
    {
        "c": "01",
        "m": [
            {
                "pid": VALID_INFO_RESPONSE["msg"]["pid"],
                "i": "mdi:lightbulb",
                "n": "Test Device",
                "dpid": [1, 4],
            }
        ],
    }
]


def _frame(obj: dict) -> bytes:
    """Encode a single protocol message exactly as _get_package does."""
    return bytes(json.dumps(obj, separators=(",", ":")) + "\r\n", encoding="utf8")


class SplitRecvSocket:
    """Socket that returns the response in two fragments on consecutive recv calls."""

    def __init__(self, first: bytes, second: bytes) -> None:
        self._fragments = [first, second]
        self.sent: list[bytes] = []
        self.timeouts: list[float] = []
        self.send_timeout: float | None = None

    def settimeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def send(self, payload: bytes) -> int:
        self.sent.append(payload)
        return len(payload)

    def sendall(self, payload: bytes) -> None:
        self.send_timeout = self.timeouts[-1] if self.timeouts else None
        self.sent.append(payload)

    def recv(self, bufsize: int) -> bytes:
        if not self._fragments:
            return b""
        return self._fragments.pop(0)


class MergedRecvSocket:
    """Socket that returns two protocol messages inside a single recv call."""

    def __init__(self, merged: bytes) -> None:
        self._merged = merged
        self._served = False
        self.sent: list[bytes] = []

    def settimeout(self, timeout: float) -> None:
        pass

    def send(self, payload: bytes) -> int:
        self.sent.append(payload)
        return len(payload)

    def sendall(self, payload: bytes) -> None:
        self.sent.append(payload)

    def recv(self, bufsize: int) -> bytes:
        if self._served:
            return b""
        self._served = True
        return self._merged


class FiniteUnmatchedSocket:
    """Return unmatched frames until the test detects its response deadline."""

    def __init__(self, frame: bytes, frame_count: int) -> None:
        self._frame = frame
        self._frame_count = frame_count
        self.recv_calls = 0
        self.timeouts: list[float] = []
        self.send_timeout: float | None = None
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def sendall(self, payload: bytes) -> None:
        self.send_timeout = self.timeouts[-1]

    def recv(self, bufsize: int) -> bytes:
        self.recv_calls += 1
        if self.recv_calls <= self._frame_count:
            return self._frame
        return b""

    def close(self) -> None:
        self.closed = True


class OversizedUndelimitedSocket:
    """Stream an oversized frame while respecting the requested receive size."""

    def __init__(self, byte_count: int) -> None:
        self.remaining = byte_count
        self.recv_calls = 0
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        pass

    def sendall(self, payload: bytes) -> None:
        pass

    def recv(self, bufsize: int) -> bytes:
        self.recv_calls += 1
        if self.remaining == 0:
            return b""
        size = min(bufsize, self.remaining)
        self.remaining -= size
        return b"x" * size

    def close(self) -> None:
        self.closed = True


class CoordinatedQuerySocket:
    """Hold the first response while observing whether a second send overlaps."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: dict[int, tuple[int, str]] = {}
        self.first_receive_started = threading.Event()
        self.release_first_receive = threading.Event()
        self.second_send_started = threading.Event()

    def settimeout(self, timeout: float) -> None:
        pass

    def _record_send(self, payload: bytes) -> int:
        request = json.loads(payload)
        thread_id = threading.get_ident()
        with self._lock:
            order = len(self._requests)
            self._requests[thread_id] = (order, request["sn"])
            if order == 1:
                self.second_send_started.set()
        return len(payload)

    def send(self, payload: bytes) -> int:
        return self._record_send(payload)

    def sendall(self, payload: bytes) -> None:
        self._record_send(payload)

    def recv(self, bufsize: int) -> bytes:
        thread_id = threading.get_ident()
        with self._lock:
            order, sequence_number = self._requests[thread_id]

        if order == 0:
            self.first_receive_started.set()
            if not self.release_first_receive.wait(TEST_TIMEOUT):
                raise TimeoutError("First query was not released")

        return _frame(
            {
                "cmd": 2,
                "pv": 0,
                "sn": sequence_number,
                "msg": {"attr": [1], "data": {"1": order}},
                "res": 0,
            }
        )


class ObservableRLock:
    """Signal when another thread attempts to enter an owned lock."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._state_lock = threading.Lock()
        self._owner: int | None = None
        self._depth = 0
        self.contender_waiting = threading.Event()

    def __enter__(self):
        thread_id = threading.get_ident()
        with self._state_lock:
            if self._owner is not None and self._owner != thread_id:
                self.contender_waiting.set()

        self._lock.acquire()
        with self._state_lock:
            self._owner = thread_id
            self._depth += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        with self._state_lock:
            self._depth -= 1
            if self._depth == 0:
                self._owner = None
        self._lock.release()


def _make_client(connection) -> tcp_client:
    """Create a transport client with production synchronization state."""
    with patch.object(tcp_client, "_reconnect"):
        client = tcp_client("192.0.2.1")
    client._connect = connection
    return client


class TransmissionControlProtocolMessageBoundaryTest(unittest.TestCase):
    """Verify split and merged receive calls preserve protocol frames."""

    def test_device_info_reassembles_split_frame(self) -> None:
        """A device information response may span multiple receive calls."""
        full = _frame(VALID_INFO_RESPONSE)
        midpoint = len(full) // 2
        sock = SplitRecvSocket(full[:midpoint], full[midpoint:])

        client = _make_client(sock)
        client._device_id = str  # mirror the class-attribute default
        client._pid = str
        client._device_type_code = str
        client._icon = str
        client._device_model_name = str
        client._dpid = []

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=VALID_INFO_RESPONSE["sn"],
        ):
            client._device_info()

        self.assertEqual(client._device_id, VALID_INFO_RESPONSE["msg"]["did"])

    def test_device_info_shares_one_send_receive_deadline(self) -> None:
        """Handshake send and fragmented receives consume one timeout budget."""
        full = _frame(VALID_INFO_RESPONSE)
        midpoint = len(full) // 2
        sock = SplitRecvSocket(full[:midpoint], full[midpoint:])
        client = _make_client(sock)
        client._device_id = str
        client._pid = str
        client._device_type_code = str
        client._icon = str
        client._device_model_name = str
        client._dpid = []

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=VALID_INFO_RESPONSE["sn"],
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.time.monotonic",
            side_effect=[0, 0.5, 2.5, 2.9],
        ):
            handshake_succeeded = client._device_info()

        self.assertTrue(handshake_succeeded)
        self.assertEqual(sock.send_timeout, 2.5)
        self.assertEqual(sock.timeouts[:2], [2.5, 0.5])
        self.assertAlmostEqual(sock.timeouts[-2], 0.1)
        self.assertEqual(sock.timeouts[-1], 3)

    def test_device_info_preserves_extra_merged_frame(self) -> None:
        """A merged receive parses one frame and buffers the next frame."""
        frame = _frame(VALID_INFO_RESPONSE)
        merged = frame + frame
        sock = MergedRecvSocket(merged)

        client = _make_client(sock)
        client._device_id = str
        client._pid = str
        client._device_type_code = str
        client._icon = str
        client._device_model_name = str
        client._dpid = []

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=VALID_INFO_RESPONSE["sn"],
        ):
            client._device_info()

        self.assertEqual(client._device_id, VALID_INFO_RESPONSE["msg"]["did"])
        self.assertEqual(client._receive_buffer, frame)

    def test_device_info_skips_an_unmatched_frame(self) -> None:
        """An unsolicited update cannot replace the handshake response."""
        unsolicited_response = {
            "cmd": 10,
            "pv": 0,
            "sn": "unmatched",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        sock = MergedRecvSocket(
            _frame(unsolicited_response) + _frame(VALID_INFO_RESPONSE)
        )
        client = _make_client(sock)
        client._device_id = str
        client._pid = str
        client._device_type_code = str
        client._icon = str
        client._device_model_name = str
        client._dpid = []

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
            return_value=VALID_PID_LIST,
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=VALID_INFO_RESPONSE["sn"],
        ):
            handshake_succeeded = client._device_info()

        self.assertTrue(handshake_succeeded)
        self.assertEqual(client._device_id, VALID_INFO_RESPONSE["msg"]["did"])

    def test_oversized_undelimited_frame_is_rejected_without_an_extra_receive(
        self,
    ) -> None:
        """An oversized frame is rejected before more socket data is requested."""
        maximum_frame_size = 64 * 1024
        sock = OversizedUndelimitedSocket(maximum_frame_size + 1)
        client = _make_client(sock)

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ), patch.object(client, "_reconnect") as reconnect:
            result = client.query()

        self.assertEqual(result, {})
        self.assertEqual(sock.recv_calls, 65)
        self.assertEqual(sock.remaining, 0)
        self.assertTrue(sock.closed)
        self.assertIsNone(client._connect)
        reconnect.assert_called_once_with()

    def test_queries_consume_merged_frames_without_reconnecting(self) -> None:
        """Consecutive queries consume buffered frames without reconnecting."""
        first_response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1001",
            "msg": {"attr": [1, 2, 3], "data": {"1": 0}},
            "res": 0,
        }
        second_response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1002",
            "msg": {"attr": [1, 2, 3], "data": {"1": 255}},
            "res": 0,
        }
        stale_response = {**first_response, "sn": "1001-extra"}
        merged = (
            _frame(stale_response)
            + _frame(first_response)
            + _frame(second_response)
        )
        sock = MergedRecvSocket(merged)
        client = _make_client(sock)

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            side_effect=["1001", "1002"],
        ), patch.object(client, "_reconnect") as reconnect:
            first_result = client.query()
            second_result = client.query()

        self.assertEqual(first_result, {"1": 0})
        self.assertEqual(second_result, {"1": 255})
        reconnect.assert_not_called()

    def test_control_and_query_use_unique_sequence_numbers(self) -> None:
        """A query ignores a same-millisecond control response."""
        control_response = {
            "cmd": 3,
            "pv": 0,
            "sn": "1000",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        query_response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1001",
            "msg": {"attr": [1], "data": {"1": 0}},
            "res": 0,
        }
        sock = MergedRecvSocket(_frame(control_response) + _frame(query_response))
        client = _make_client(sock)

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ), patch.object(client, "_reconnect") as reconnect:
            control_result = client.control({"1": 255})
            query_result = client.query()

        sent_sequence_numbers = [json.loads(frame)["sn"] for frame in sock.sent]
        self.assertTrue(control_result)
        self.assertEqual(sent_sequence_numbers, ["1000", "1001"])
        self.assertEqual(query_result, {"1": 0})
        reconnect.assert_not_called()

    def test_query_skips_all_queued_control_responses(self) -> None:
        """A burst of control acknowledgements cannot hide the query response."""
        control_responses = [
            _frame(
                {
                    "cmd": 3,
                    "pv": 0,
                    "sn": str(1000 + index),
                    "msg": {"attr": [1], "data": {"1": 255}},
                    "res": 0,
                }
            )
            for index in range(10)
        ]
        query_response = _frame(
            {
                "cmd": 2,
                "pv": 0,
                "sn": "1010",
                "msg": {"attr": [1], "data": {"1": 0}},
                "res": 0,
            }
        )
        sock = MergedRecvSocket(b"".join(control_responses) + query_response)
        client = _make_client(sock)

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ), patch.object(client, "_reconnect") as reconnect:
            for _ in range(10):
                self.assertTrue(client.control({"1": 255}))
            query_result = client.query()

        self.assertEqual(query_result, {"1": 0})
        reconnect.assert_not_called()

    def test_query_stops_at_total_response_deadline(self) -> None:
        """Continuous unmatched frames cannot hold the transaction lock forever."""
        unmatched_response = _frame(
            {
                "cmd": 10,
                "pv": 0,
                "sn": "999",
                "msg": {"attr": [1], "data": {"1": 255}},
                "res": 0,
            }
        )
        sock = FiniteUnmatchedSocket(unmatched_response, frame_count=4)
        client = _make_client(sock)

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ), patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.time.monotonic",
            side_effect=[0, 0, 1, 1, 2.9, 2.9, 3],
        ), patch.object(client, "_reconnect") as reconnect:
            query_result = client.query()

        self.assertEqual(query_result, {})
        self.assertEqual(sock.recv_calls, 2)
        self.assertEqual(sock.send_timeout, 3)
        self.assertEqual(sock.timeouts[1], 2)
        self.assertAlmostEqual(sock.timeouts[-1], 0.1)
        self.assertTrue(sock.closed)
        self.assertIsNone(client._connect)
        reconnect.assert_called_once_with()

    def test_queries_serialize_complete_request_response_transactions(self) -> None:
        """Concurrent queries do not share sequence numbers or receive data."""
        sock = CoordinatedQuerySocket()
        client = _make_client(sock)
        transaction_lock = ObservableRLock()
        client._io_lock = transaction_lock
        results: dict[str, dict] = {}
        query_errors: queue.Queue[BaseException] = queue.Queue()

        def query(name: str) -> None:
            try:
                results[name] = client.query()
            except BaseException as err:
                query_errors.put(err)

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            side_effect=["1001", "1002"],
        ), patch.object(client, "_reconnect") as reconnect:
            first = threading.Thread(target=query, args=("first",), daemon=True)
            second = threading.Thread(target=query, args=("second",), daemon=True)
            first.start()
            self.assertTrue(sock.first_receive_started.wait(TEST_TIMEOUT))
            second.start()

            try:
                self.assertTrue(
                    transaction_lock.contender_waiting.wait(TEST_TIMEOUT)
                )
                self.assertFalse(sock.second_send_started.is_set())
            finally:
                sock.release_first_receive.set()

            first.join(TEST_TIMEOUT)
            second.join(TEST_TIMEOUT)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertTrue(query_errors.empty())
        self.assertEqual(results, {"first": {"1": 0}, "second": {"1": 1}})
        reconnect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
