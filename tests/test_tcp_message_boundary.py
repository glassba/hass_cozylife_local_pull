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
import socket
import threading
import unittest
from unittest.mock import patch

from custom_components.hass_cozylife_local_pull.tcp_client import (
    DeviceCommandRejectedError,
    tcp_client,
)

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


class FirstReceiveObservedSocket:
    """Wrap a real socket and expose when its first receive has completed."""

    def __init__(self, connection: socket.socket) -> None:
        self._connection = connection
        self.first_receive_completed = threading.Event()

    def recv(self, bufsize: int) -> bytes:
        """Delegate receive and publish that the listener consumed a fragment."""
        data = self._connection.recv(bufsize)
        self.first_receive_completed.set()
        return data

    def __getattr__(self, name):
        """Delegate the remaining socket interface to the real connection."""
        return getattr(self._connection, name)


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

    def test_device_info_matches_numeric_response_sequence(self) -> None:
        """A numeric response timestamp matches the string sent on the wire."""
        response = {
            **VALID_INFO_RESPONSE,
            "sn": int(VALID_INFO_RESPONSE["sn"]),
        }
        client = _make_client(MergedRecvSocket(_frame(response)))
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

    def test_device_info_processes_interleaved_state_commands(self) -> None:
        """Handshake input records cmd 2, cmd 3, and cmd 10 state frames."""
        for command in (2, 3, 10):
            with self.subTest(command=command):
                state_response = {
                    "cmd": command,
                    "pv": 0,
                    "sn": "1700000000000",
                    "msg": {"attr": [1], "data": {"1": 255}},
                }
                if command in (2, 3):
                    state_response["res"] = 0
                sock = MergedRecvSocket(
                    _frame(state_response) + _frame(VALID_INFO_RESPONSE)
                )
                client = _make_client(sock)
                client._device_id = str
                client._pid = str
                client._device_type_code = str
                client._icon = str
                client._device_model_name = str
                client._dpid = []
                received_updates: list[tuple[dict, int]] = []
                client.add_state_callback(
                    lambda state, sequence_number: received_updates.append(
                        (state, sequence_number)
                    )
                )

                with patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.get_pid_list",
                    return_value=VALID_PID_LIST,
                ), patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
                    return_value=VALID_INFO_RESPONSE["sn"],
                ):
                    handshake_succeeded = client._device_info()

                self.assertTrue(handshake_succeeded)
                self.assertEqual(
                    client._device_id,
                    VALID_INFO_RESPONSE["msg"]["did"],
                )
                self.assertEqual(
                    received_updates,
                    [({"1": 255}, 1700000000000)],
                )

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

    def test_query_dispatches_state_data_with_sequence_number(self) -> None:
        """A valid query response publishes its data and device timestamp."""
        response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        client = _make_client(MergedRecvSocket(_frame(response)))
        received_updates: list[tuple[dict, int]] = []
        client.add_state_callback(
            lambda state, sequence_number: received_updates.append(
                (state, sequence_number)
            )
        )

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=response["sn"],
        ):
            query_result = client.query()

        self.assertEqual(query_result, {"1": 255})
        self.assertEqual(
            received_updates,
            [({"1": 255}, 1700000000000)],
        )

    def test_query_matches_numeric_response_sequence(self) -> None:
        """A numeric query response timestamp completes the string request."""
        response = {
            "cmd": 2,
            "pv": 0,
            "sn": 1700000000000,
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        client = _make_client(MergedRecvSocket(_frame(response)))

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1700000000000",
        ), patch.object(client, "_reconnect") as reconnect:
            query_result = client.query()

        self.assertEqual(query_result, {"1": 255})
        reconnect.assert_not_called()

    def test_newer_report_discards_older_query_response(self) -> None:
        """A queued query snapshot cannot replace a newer device report."""
        report = {
            "cmd": 10,
            "pv": 0,
            "sn": "1700000000001",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [1], "data": {"1": 0}},
            "res": 0,
        }
        client = _make_client(
            MergedRecvSocket(_frame(report) + _frame(response))
        )
        received_updates: list[tuple[dict, int]] = []
        client.add_state_callback(
            lambda state, sequence_number: received_updates.append(
                (state, sequence_number)
            )
        )

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=response["sn"],
        ):
            query_result = client.query()

        self.assertEqual(query_result, {})
        self.assertEqual(
            received_updates,
            [({"1": 255}, 1700000000001)],
        )
        self.assertEqual(client.last_state_sequence_number, 1700000000001)

    def test_control_dispatches_acknowledged_state(self) -> None:
        """A valid control acknowledgement publishes authoritative state."""
        response = {
            "cmd": 3,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        client = _make_client(MergedRecvSocket(_frame(response)))
        received_updates: list[tuple[dict, int]] = []
        client.add_state_callback(
            lambda state, sequence_number: received_updates.append(
                (state, sequence_number)
            )
        )

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=response["sn"],
        ):
            control_result = client.control({"1": 255})

        self.assertTrue(control_result)
        self.assertEqual(
            received_updates,
            [({"1": 255}, 1700000000000)],
        )

    def test_control_matches_numeric_response_sequence(self) -> None:
        """A numeric control response timestamp completes the string request."""
        response = {
            "cmd": 3,
            "pv": 0,
            "sn": 1700000000000,
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        client = _make_client(MergedRecvSocket(_frame(response)))

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1700000000000",
        ), patch.object(client, "_reconnect") as reconnect:
            control_result = client.control({"1": 255})

        self.assertTrue(control_result)
        reconnect.assert_not_called()

    def test_query_processes_interleaved_control_state(self) -> None:
        """A control reply updates state without completing an active query."""
        control_response = {
            "cmd": 3,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [13], "data": {"13": 5}},
            "res": 0,
        }
        query_response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [1], "data": {"1": 0}},
            "res": 0,
        }
        client = _make_client(
            MergedRecvSocket(
                _frame(control_response) + _frame(query_response)
            )
        )
        received_updates: list[tuple[dict, int]] = []
        client.add_state_callback(
            lambda state, sequence_number: received_updates.append(
                (state, sequence_number)
            )
        )

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=query_response["sn"],
        ), patch.object(client, "_reconnect") as reconnect:
            query_result = client.query()

        self.assertEqual(query_result, {"1": 0})
        self.assertEqual(
            received_updates,
            [
                ({"13": 5, "1": 0}, 1700000000000),
            ],
        )
        reconnect.assert_not_called()

    def test_control_processes_interleaved_query_state(self) -> None:
        """A query reply updates state without completing an active control."""
        query_response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [13], "data": {"13": 5}},
            "res": 0,
        }
        control_response = {
            "cmd": 3,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        client = _make_client(
            MergedRecvSocket(
                _frame(query_response) + _frame(control_response)
            )
        )
        received_updates: list[tuple[dict, int]] = []
        client.add_state_callback(
            lambda state, sequence_number: received_updates.append(
                (state, sequence_number)
            )
        )

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=control_response["sn"],
        ), patch.object(client, "_reconnect") as reconnect:
            control_result = client.control({"1": 255})

        self.assertTrue(control_result)
        self.assertEqual(
            received_updates,
            [
                ({"13": 5, "1": 255}, 1700000000000),
            ],
        )
        reconnect.assert_not_called()

    def test_equal_sequence_number_merges_state_responses(self) -> None:
        """Multiple state frames from one timestamp are merged before dispatch."""
        report = {
            "cmd": 10,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [4], "data": {"4": 200}},
        }
        response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        client = _make_client(
            MergedRecvSocket(_frame(report) + _frame(response))
        )
        received_updates: list[tuple[dict, int]] = []
        client.add_state_callback(
            lambda state, sequence_number: received_updates.append(
                (state, sequence_number)
            )
        )

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value=response["sn"],
        ):
            query_result = client.query()

        self.assertEqual(query_result, {"1": 255})
        self.assertEqual(
            received_updates,
            [
                ({"4": 200, "1": 255}, 1700000000000),
            ],
        )

    def test_state_commands_share_one_sequence_order(self) -> None:
        """All state commands use one global sequence number order."""
        client = _make_client(None)
        received_updates: list[tuple[dict, int]] = []
        client.add_state_callback(
            lambda state, sequence_number: received_updates.append(
                (state, sequence_number)
            )
        )
        older_response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [1], "data": {"1": 0}},
            "res": 0,
        }
        newer_response = {
            "cmd": 3,
            "pv": 0,
            "sn": "1700000000001",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        equal_response = {
            "cmd": 10,
            "pv": 0,
            "sn": "1700000000001",
            "msg": {"attr": [13], "data": {"13": 20}},
        }

        self.assertTrue(client._queue_state_response(older_response))
        self.assertTrue(client._queue_state_response(newer_response))
        self.assertTrue(client._queue_state_response(equal_response))
        client._drain_state_reports()

        self.assertEqual(
            received_updates,
            [
                ({"1": 255, "13": 20}, 1700000000001),
            ],
        )

    def test_unknown_command_is_not_queued_as_state(self) -> None:
        """Only cmd 2, cmd 3, and cmd 10 can publish device state."""
        client = _make_client(None)
        response = {
            "cmd": 0,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"data": {"1": 255}},
            "res": 0,
        }

        self.assertFalse(client._queue_state_response(response))
        self.assertIsNone(client.last_state_sequence_number)

    def test_rejected_reply_logs_and_discards_state_payload(self) -> None:
        """Nonzero cmd 2 and cmd 3 results never publish their payload."""
        for command in (2, 3):
            with self.subTest(command=command):
                client = _make_client(None)
                response = {
                    "cmd": command,
                    "pv": 0,
                    "sn": "1700000000000",
                    "msg": {"data": {"1": 255}},
                    "res": 1,
                }

                with self.assertLogs(
                    "custom_components.hass_cozylife_local_pull.tcp_client",
                    level="INFO",
                ) as logs:
                    queued = client._queue_state_response(response)

                self.assertFalse(queued)
                self.assertIsNone(client.last_state_sequence_number)
                self.assertTrue(
                    any(
                        f"cmd={command}" in message
                        and "sn=1700000000000" in message
                        and "res=1" in message
                        for message in logs.output
                    )
                )

    def test_invalid_report_sequence_number_is_ignored(self) -> None:
        """Malformed report timestamps cannot affect accepted state order."""
        response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        for invalid_sequence_number in ("invalid", True, -1, None, 1.5):
            with self.subTest(sequence_number=invalid_sequence_number):
                invalid_report = {
                    "cmd": 10,
                    "pv": 0,
                    "sn": invalid_sequence_number,
                    "msg": {"attr": [1], "data": {"1": 0}},
                    "res": 0,
                }
                client = _make_client(
                    MergedRecvSocket(
                        _frame(invalid_report) + _frame(response)
                    )
                )
                received_updates: list[tuple[dict, int]] = []
                client.add_state_callback(
                    lambda state, sequence_number: received_updates.append(
                        (state, sequence_number)
                    )
                )

                with patch(
                    "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
                    return_value=response["sn"],
                ):
                    query_result = client.query()

                self.assertEqual(query_result, {"1": 255})
                self.assertEqual(
                    received_updates,
                    [({"1": 255}, 1700000000000)],
                )

    def test_newer_query_response_discards_older_active_report(self) -> None:
        """A newer query response discards an older queued active report."""
        report = {
            "cmd": 10,
            "pv": 0,
            "sn": "999",
            "msg": {
                "attr": [1, 4, 5, 6, 13],
                "data": {"1": 255, "4": 400, "5": 120, "6": 500, "13": 30},
            },
            "res": 0,
        }
        response = {
            "cmd": 2,
            "pv": 0,
            "sn": "1000",
            "msg": {"attr": [1], "data": {"1": 255}},
            "res": 0,
        }
        client = _make_client(
            MergedRecvSocket(_frame(report) + _frame(response))
        )
        received_reports: list[dict] = []
        client.add_state_callback(
            lambda state, _sequence_number: received_reports.append(state)
        )

        with patch(
            "custom_components.hass_cozylife_local_pull.tcp_client.get_sn",
            return_value="1000",
        ):
            query_result = client.query()

        self.assertEqual(query_result, {"1": 255})
        self.assertEqual(
            received_reports,
            [response["msg"]["data"]],
        )

    def test_state_listener_hands_socket_to_query_without_losing_report(
        self,
    ) -> None:
        """A live listener yields to a query and drops its older report."""
        client_socket, device_socket = socket.socketpair()
        client = _make_client(client_socket)
        report_received = threading.Event()
        received_reports: list[dict] = []
        query_results: queue.Queue[dict] = queue.Queue()
        query_errors: queue.Queue[BaseException] = queue.Queue()
        query_thread = None

        def receive_report(state: dict, _sequence_number: int) -> None:
            received_reports.append(state)
            report_received.set()

        def query() -> None:
            try:
                query_results.put(client.query([13]))
            except BaseException as err:
                query_errors.put(err)

        try:
            client.add_state_callback(receive_report)
            client._start_state_listener(client_socket)
            self.assertIsNotNone(client._state_listener_thread)
            self.assertTrue(client._state_listener_thread.is_alive())

            query_thread = threading.Thread(target=query, daemon=True)
            query_thread.start()
            device_socket.settimeout(TEST_TIMEOUT)
            request_data = b""
            while b"\r\n" not in request_data:
                chunk = device_socket.recv(4096)
                if not chunk:
                    self.fail("Query socket closed before sending a request")
                request_data += chunk
            request = json.loads(request_data.split(b"\r\n", 1)[0])

            report = {
                "cmd": 10,
                "pv": 0,
                "sn": str(int(request["sn"]) - 1),
                "msg": {"attr": [13], "data": {"13": 20}},
                "res": 0,
            }
            response = {
                "cmd": 2,
                "pv": 0,
                "sn": request["sn"],
                "msg": {"attr": [13], "data": {"13": 19}},
                "res": 0,
            }
            device_socket.sendall(_frame(report) + _frame(response))

            query_thread.join(TEST_TIMEOUT)
            self.assertFalse(query_thread.is_alive())
            self.assertTrue(query_errors.empty())
            self.assertEqual(query_results.get_nowait(), {"13": 19})
            self.assertTrue(report_received.wait(TEST_TIMEOUT))
            self.assertEqual(received_reports, [{"13": 19}])
        finally:
            client.close()
            device_socket.close()
            if query_thread is not None:
                query_thread.join(TEST_TIMEOUT)

    def test_state_listener_preserves_report_before_control_rejection(
        self,
    ) -> None:
        """A valid report is dispatched even when the following command is rejected."""
        client_socket, device_socket = socket.socketpair()
        client = _make_client(client_socket)
        report_received = threading.Event()
        received_reports: list[dict] = []
        control_errors: queue.Queue[BaseException] = queue.Queue()
        control_thread = None

        def receive_report(state: dict, _sequence_number: int) -> None:
            received_reports.append(state)
            report_received.set()

        def control() -> None:
            try:
                client.control({"13": 30})
            except BaseException as err:
                control_errors.put(err)

        try:
            client.add_state_callback(receive_report)
            client._start_state_listener(client_socket)
            control_thread = threading.Thread(target=control, daemon=True)
            control_thread.start()
            device_socket.settimeout(TEST_TIMEOUT)
            request_data = b""
            while b"\r\n" not in request_data:
                chunk = device_socket.recv(4096)
                if not chunk:
                    self.fail("Control socket closed before sending a request")
                request_data += chunk
            request = json.loads(request_data.split(b"\r\n", 1)[0])
            report = {
                "cmd": 10,
                "pv": 0,
                "sn": str(int(request["sn"]) - 1),
                "msg": {"attr": [13], "data": {"13": 5}},
                "res": 0,
            }
            rejection = {
                "cmd": 3,
                "pv": 0,
                "sn": request["sn"],
                "res": 1,
            }
            device_socket.sendall(_frame(report) + _frame(rejection))

            control_thread.join(TEST_TIMEOUT)
            self.assertFalse(control_thread.is_alive())
            error = control_errors.get_nowait()
            self.assertIsInstance(error, DeviceCommandRejectedError)
            self.assertTrue(report_received.wait(TEST_TIMEOUT))
            self.assertEqual(received_reports, [{"13": 5}])
        finally:
            client.close()
            device_socket.close()
            if control_thread is not None:
                control_thread.join(TEST_TIMEOUT)

    def test_partial_idle_report_releases_socket_for_query(self) -> None:
        """An incomplete older report releases the socket and is discarded."""
        raw_client_socket, device_socket = socket.socketpair()
        client_socket = FirstReceiveObservedSocket(raw_client_socket)
        client = _make_client(client_socket)
        report_received = threading.Event()
        received_reports: list[dict] = []
        query_results: queue.Queue[dict] = queue.Queue()
        query_errors: queue.Queue[BaseException] = queue.Queue()
        query_thread = None
        report = {
            "cmd": 10,
            "pv": 0,
            "sn": "999",
            "msg": {"attr": [13], "data": {"13": 20}},
            "res": 0,
        }
        report_frame = _frame(report)
        split_at = len(report_frame) // 2

        def receive_report(state: dict, _sequence_number: int) -> None:
            received_reports.append(state)
            report_received.set()

        def query() -> None:
            try:
                query_results.put(client.query([13]))
            except BaseException as err:
                query_errors.put(err)

        def receive_request() -> dict:
            request_data = b""
            while b"\r\n" not in request_data:
                chunk = device_socket.recv(4096)
                if not chunk:
                    self.fail("Query socket closed before sending a request")
                request_data += chunk
            return json.loads(request_data.split(b"\r\n", 1)[0])

        try:
            client.add_state_callback(receive_report)
            client._start_state_listener(client_socket)
            device_socket.sendall(report_frame[:split_at])
            self.assertTrue(
                client_socket.first_receive_completed.wait(TEST_TIMEOUT)
            )

            query_thread = threading.Thread(target=query, daemon=True)
            query_thread.start()
            device_socket.settimeout(TEST_TIMEOUT)
            request_sent_before_remainder = True
            try:
                request = receive_request()
            except TimeoutError:
                request_sent_before_remainder = False
                device_socket.sendall(report_frame[split_at:])
                device_socket.settimeout(TEST_TIMEOUT)
                request = receive_request()

            response = {
                "cmd": 2,
                "pv": 0,
                "sn": request["sn"],
                "msg": {"attr": [13], "data": {"13": 19}},
                "res": 0,
            }
            if request_sent_before_remainder:
                device_socket.sendall(
                    report_frame[split_at:] + _frame(response)
                )
            else:
                device_socket.sendall(_frame(response))

            query_thread.join(TEST_TIMEOUT)
            self.assertFalse(query_thread.is_alive())
            self.assertTrue(query_errors.empty())
            self.assertEqual(query_results.get_nowait(), {"13": 19})
            self.assertTrue(report_received.wait(TEST_TIMEOUT))
            self.assertEqual(received_reports, [{"13": 19}])
            self.assertTrue(request_sent_before_remainder)
        finally:
            client.close()
            device_socket.close()
            if query_thread is not None:
                query_thread.join(TEST_TIMEOUT)

    def test_idle_connection_dispatches_every_state_command(self) -> None:
        """Idle cmd 2, cmd 3, and cmd 10 frames all publish state."""
        client_socket, device_socket = socket.socketpair()
        client = _make_client(client_socket)
        report_received = threading.Event()
        received_reports: list[dict] = []

        def receive_report(state: dict, _sequence_number: int) -> None:
            received_reports.append(state)
            if state.get("sentinel") == 1:
                report_received.set()

        try:
            client.add_state_callback(receive_report)
            client._start_state_listener(client_socket)
            query_response = {
                "cmd": 2,
                "pv": 0,
                "sn": "1700000000000",
                "msg": {"attr": [1], "data": {"1": 0}},
                "res": 0,
            }
            control_response = {
                "cmd": 3,
                "pv": 0,
                "sn": "1700000000000",
                "msg": {"attr": [13], "data": {"13": 20}},
                "res": 0,
            }
            report = {
                "cmd": 10,
                "pv": 0,
                "sn": "1700000000000",
                "msg": {"attr": [99], "data": {"sentinel": 1}},
            }
            device_socket.sendall(
                _frame(query_response)
                + _frame(control_response)
                + _frame(report)
            )

            self.assertTrue(report_received.wait(TEST_TIMEOUT))
            self.assertEqual(
                received_reports,
                [{"1": 0, "13": 20, "sentinel": 1}],
            )
        finally:
            client.close()
            device_socket.close()

    def test_idle_connection_dispatches_complete_report_before_eof(self) -> None:
        """A complete state frame is published before EOF starts recovery."""
        client_socket, device_socket = socket.socketpair()
        client = _make_client(client_socket)
        received_reports: list[dict] = []
        reconnect_started = threading.Event()
        report = {
            "cmd": 10,
            "pv": 0,
            "sn": "1700000000000",
            "msg": {"attr": [13], "data": {"13": 20}},
        }

        try:
            client.add_state_callback(
                lambda state, _sequence_number: received_reports.append(state)
            )
            device_socket.sendall(_frame(report))
            device_socket.shutdown(socket.SHUT_WR)
            with patch.object(
                client, "_reconnect", side_effect=reconnect_started.set
            ):
                client._start_state_listener(client_socket)
                self.assertTrue(reconnect_started.wait(TEST_TIMEOUT))

            self.assertEqual(received_reports, [{"13": 20}])
        finally:
            client.close()
            device_socket.close()

    def test_unsubscribe_waits_for_in_flight_state_callback(self) -> None:
        """Unsubscribe returns only after an admitted callback has finished."""
        client = _make_client(None)
        callback_started = threading.Event()
        allow_callback = threading.Event()
        callback_timed_out = threading.Event()
        remove_started = threading.Event()
        remove_finished = threading.Event()
        callback_calls = []
        callback_lock = ObservableRLock()
        client._state_callback_lock = callback_lock

        def receive_report(state: dict, _sequence_number: int) -> None:
            callback_calls.append(state)
            callback_started.set()
            if not allow_callback.wait(TEST_TIMEOUT):
                callback_timed_out.set()
                raise TimeoutError("State callback was not released")

        remove_callback = client.add_state_callback(receive_report)

        def remove_subscription() -> None:
            remove_started.set()
            remove_callback()
            remove_finished.set()

        dispatch_thread = threading.Thread(
            target=client._dispatch_state_reports,
            args=([({"1": 255}, 1700000000000)],),
            daemon=True,
        )
        remove_thread = threading.Thread(
            target=remove_subscription,
            daemon=True,
        )
        dispatch_thread.start()
        try:
            self.assertTrue(callback_started.wait(TEST_TIMEOUT))
            remove_thread.start()
            self.assertTrue(remove_started.wait(TEST_TIMEOUT))
            self.assertTrue(
                callback_lock.contender_waiting.wait(TEST_TIMEOUT)
            )
            self.assertFalse(remove_finished.is_set())
        finally:
            allow_callback.set()
            dispatch_thread.join(TEST_TIMEOUT)
            if remove_thread.ident is not None:
                remove_thread.join(TEST_TIMEOUT)

        client._dispatch_state_reports([({"1": 0}, 1700000000001)])
        self.assertFalse(dispatch_thread.is_alive())
        self.assertFalse(remove_thread.is_alive())
        self.assertTrue(remove_finished.is_set())
        self.assertFalse(callback_timed_out.is_set())
        self.assertEqual(callback_calls, [{"1": 255}])

    def test_state_report_queue_preserves_order_across_drainers(self) -> None:
        """Concurrent drainers publish reports in socket receive order."""
        client = _make_client(None)
        self.assertTrue(hasattr(client, "_queue_state_report"))
        self.assertTrue(hasattr(client, "_drain_state_reports"))
        first_callback_started = threading.Event()
        allow_first_callback = threading.Event()
        second_report_queued = threading.Event()
        second_callback_started = threading.Event()
        received_reports = []
        dispatch_lock = ObservableRLock()
        client._state_report_dispatch_lock = dispatch_lock

        def receive_report(state: dict, _sequence_number: int) -> None:
            received_reports.append(state)
            if state == {"1": 1}:
                first_callback_started.set()
                if not allow_first_callback.wait(TEST_TIMEOUT):
                    raise TimeoutError("First report callback was not released")
            elif state == {"1": 2}:
                second_callback_started.set()

        client.add_state_callback(receive_report)
        client._queue_state_report({"1": 1}, 1700000000000)
        first_drainer = threading.Thread(
            target=client._drain_state_reports,
            daemon=True,
        )

        def queue_and_drain_second_report() -> None:
            client._queue_state_report({"1": 2}, 1700000000001)
            second_report_queued.set()
            client._drain_state_reports()

        second_drainer = threading.Thread(
            target=queue_and_drain_second_report,
            daemon=True,
        )
        first_drainer.start()
        try:
            self.assertTrue(first_callback_started.wait(TEST_TIMEOUT))
            second_drainer.start()
            self.assertTrue(second_report_queued.wait(TEST_TIMEOUT))
            self.assertTrue(
                dispatch_lock.contender_waiting.wait(TEST_TIMEOUT)
            )
            self.assertFalse(second_callback_started.is_set())
        finally:
            allow_first_callback.set()
            first_drainer.join(TEST_TIMEOUT)
            if second_drainer.ident is not None:
                second_drainer.join(TEST_TIMEOUT)

        self.assertFalse(first_drainer.is_alive())
        self.assertFalse(second_drainer.is_alive())
        self.assertEqual(received_reports, [{"1": 1}, {"1": 2}])

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
