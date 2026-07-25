"""Regression tests for isolated User Datagram Protocol discovery timeouts."""

from __future__ import annotations

import socket
import json
import unittest
from unittest.mock import patch

from custom_components.hass_cozylife_local_pull.udp_discover import (
    DISCOVERY_COLLECTION_TIMEOUT_SECONDS,
    get_ip,
)


_DEFAULT_DISCOVERY_MESSAGE = object()


def _discovery_response(
    *,
    cmd: object = 0,
    pv: object = 0,
    sn: object = "1000",
    res: object = 0,
    did: object = "device-1234",
    pid: object = "product-1234",
    include_pv: bool = True,
    include_sn: bool = True,
    include_did: bool = True,
    include_pid: bool = True,
    message: object = _DEFAULT_DISCOVERY_MESSAGE,
    include_message: bool = True,
) -> bytes:
    """Encode a complete CozyLife discovery response."""
    if message is _DEFAULT_DISCOVERY_MESSAGE:
        message = {}
        if include_did:
            message["did"] = did
        if include_pid:
            message["pid"] = pid
    response = {
        "cmd": cmd,
        "res": res,
    }
    if include_pv:
        response["pv"] = pv
    if include_sn:
        response["sn"] = sn
    if include_message:
        response["msg"] = message
    return bytes(
        json.dumps(
            response,
            separators=(",", ":"),
        ),
        encoding="utf8",
    )


class FakeDatagramSocket:
    """Record socket configuration while returning one discovered device."""

    def __init__(
        self,
        discover_device: bool = True,
        responses: list[tuple[bytes, tuple[str, int]]] | None = None,
        send_error: OSError | None = None,
        receive_error_at: int | None = None,
        configuration_error_at: str | None = None,
        close_error: OSError | None = None,
    ) -> None:
        self.timeout: float | None = None
        self.timeouts: list[float] = []
        self.receive_count = 0
        self.responses = (
            responses
            if responses is not None
            else [(_discovery_response(), ("192.0.2.10", 6095))]
            if discover_device
            else []
        )
        self.send_error = send_error
        self.receive_error_at = receive_error_at
        self.configuration_error_at = configuration_error_at
        self.close_error = close_error
        self.closed = False

    def setsockopt(self, *args: object) -> None:
        """Accept socket options used by discovery."""
        if self.configuration_error_at == "setsockopt":
            raise OSError("Injected setsockopt failure")

    def settimeout(self, value: float) -> None:
        """Record the timeout applied directly to this socket."""
        if self.configuration_error_at == "settimeout":
            raise OSError("Injected settimeout failure")
        self.timeout = value
        self.timeouts.append(value)

    def sendto(self, *args: object) -> None:
        """Accept discovery broadcasts or raise an injected failure."""
        if self.send_error is not None:
            raise self.send_error

    def recvfrom(self, *args: object) -> tuple[bytes, tuple[str, int]]:
        """Return one response, then end collection with a timeout."""
        self.receive_count += 1
        if self.receive_count == self.receive_error_at:
            raise OSError("Injected receive failure")
        if self.responses:
            response = self.responses[0]
            if len(args) < 2 or args[1] != socket.MSG_PEEK:
                self.responses.pop(0)
            return response
        raise TimeoutError

    def close(self) -> None:
        """Record release of the discovery socket."""
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class UserDatagramProtocolDiscoveryTimeoutTest(unittest.TestCase):
    """Verify discovery timeout configuration remains socket-local."""

    def setUp(self) -> None:
        """Use one deterministic sequence number for request correlation."""
        sequence_patcher = patch(
            "custom_components.hass_cozylife_local_pull.udp_discover.get_sn",
            return_value="1000",
        )
        sequence_patcher.start()
        self.addCleanup(sequence_patcher.stop)

    def test_get_ip_limits_timeout_to_discovery_socket(self) -> None:
        """Discovery does not alter the process-wide default socket timeout."""
        previous_default = socket.getdefaulttimeout()
        self.addCleanup(socket.setdefaulttimeout, previous_default)
        socket.setdefaulttimeout(None)
        fake_socket = FakeDatagramSocket()

        with (
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                return_value=fake_socket,
            ),
            patch("custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"),
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.socket.setdefaulttimeout"
            ) as set_default_timeout,
        ):
            discovered_ips = get_ip()

        set_default_timeout.assert_not_called()
        self.assertEqual(discovered_ips, ["192.0.2.10"])
        self.assertIsNotNone(fake_socket.timeout)
        self.assertGreater(fake_socket.timeout, 0)
        self.assertLessEqual(
            fake_socket.timeout,
            DISCOVERY_COLLECTION_TIMEOUT_SECONDS,
        )
        self.assertTrue(fake_socket.closed)
        self.assertIsNone(socket.getdefaulttimeout())

    def test_collection_waits_for_slower_device_within_deadline(self) -> None:
        """Discovery retains a slower response within the collection budget."""
        fake_socket = FakeDatagramSocket()
        first_response = (
            _discovery_response(did="device-1"),
            ("192.0.2.10", 6095),
        )
        second_response = (
            _discovery_response(did="device-2"),
            ("192.0.2.11", 6095),
        )
        receive_count = 0

        def receive(*args: object) -> tuple[bytes, tuple[str, int]]:
            nonlocal receive_count
            receive_count += 1
            if len(args) >= 2 and args[1] == socket.MSG_PEEK:
                return first_response
            if receive_count == 2:
                return first_response
            if receive_count == 3 and fake_socket.timeout >= 0.2:
                return second_response
            raise socket.timeout

        with (
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                return_value=fake_socket,
            ),
            patch("custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"),
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.time.monotonic",
                return_value=0.0,
            ),
            patch.object(fake_socket, "recvfrom", side_effect=receive),
        ):
            discovered_ips = get_ip()

        self.assertEqual(discovered_ips, ["192.0.2.10", "192.0.2.11"])
        self.assertTrue(fake_socket.closed)

    def test_get_ip_closes_socket_when_no_device_responds(self) -> None:
        """Discovery releases its socket before the no-device return."""
        fake_socket = FakeDatagramSocket(discover_device=False)

        with (
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                return_value=fake_socket,
            ),
            patch("custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"),
        ):
            discovered_ips = get_ip()

        self.assertEqual(discovered_ips, [])
        self.assertTrue(fake_socket.closed)

    def test_get_ip_closes_socket_when_operation_fails(self) -> None:
        """Discovery failures return no addresses after socket cleanup."""
        cases = (
            FakeDatagramSocket(configuration_error_at="setsockopt"),
            FakeDatagramSocket(configuration_error_at="settimeout"),
            FakeDatagramSocket(send_error=OSError("Injected send failure")),
            FakeDatagramSocket(receive_error_at=1),
            FakeDatagramSocket(receive_error_at=2),
        )

        for fake_socket in cases:
            with self.subTest(fake_socket=fake_socket):
                with (
                    patch(
                        "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                        return_value=fake_socket,
                    ),
                    patch(
                        "custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"
                    ),
                ):
                    self.assertEqual(get_ip(), [])

                self.assertTrue(fake_socket.closed)

    def test_collection_failure_preserves_discovered_addresses(self) -> None:
        """A later socket failure keeps addresses already validated."""
        fake_socket = FakeDatagramSocket(
            responses=[
                (_discovery_response(), ("192.0.2.10", 6095)),
            ],
            receive_error_at=3,
        )

        with (
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                return_value=fake_socket,
            ),
            patch("custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"),
            self.assertLogs(
                "custom_components.hass_cozylife_local_pull.udp_discover",
                level="WARNING",
            ),
        ):
            discovered_ips = get_ip()

        self.assertEqual(discovered_ips, ["192.0.2.10"])
        self.assertTrue(fake_socket.closed)

    def test_get_ip_ignores_socket_close_failure(self) -> None:
        """Socket cleanup failure does not discard discovered addresses."""
        fake_socket = FakeDatagramSocket(
            close_error=OSError("Injected close failure")
        )

        with (
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                return_value=fake_socket,
            ),
            patch("custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"),
            self.assertLogs(
                "custom_components.hass_cozylife_local_pull.udp_discover",
                level="WARNING",
            ),
        ):
            discovered_ips = get_ip()

        self.assertEqual(discovered_ips, ["192.0.2.10"])
        self.assertTrue(fake_socket.closed)

    def test_get_ip_ignores_invalid_protocol_responses(self) -> None:
        """Only structurally valid successful device responses are accepted."""
        fake_socket = FakeDatagramSocket(
            responses=[
                (b"not-json", ("192.0.2.10", 6095)),
                (b"[]", ("192.0.2.9", 6095)),
                (_discovery_response(cmd=2), ("192.0.2.11", 6095)),
                (_discovery_response(res=1), ("192.0.2.12", 6095)),
                (
                    _discovery_response(include_did=False, include_pid=False),
                    ("192.0.2.13", 6095),
                ),
                (_discovery_response(), ("192.0.2.14", 6095)),
                (_discovery_response(), ("192.0.2.14", 6095)),
            ]
        )

        with (
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                return_value=fake_socket,
            ),
            patch("custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"),
        ):
            discovered_ips = get_ip()

        self.assertEqual(discovered_ips, ["192.0.2.14"])
        self.assertTrue(fake_socket.closed)

    def test_get_ip_rejects_malformed_protocol_field_types(self) -> None:
        """Discovery validates protocol fields and non-empty identifiers."""
        cases = (
            ("boolean command", _discovery_response(cmd=False)),
            ("floating command", _discovery_response(cmd=0.0)),
            ("boolean result", _discovery_response(res=False)),
            ("floating result", _discovery_response(res=0.0)),
            ("boolean protocol version", _discovery_response(pv=False)),
            ("floating protocol version", _discovery_response(pv=0.0)),
            ("unsupported protocol version", _discovery_response(pv=1)),
            ("missing protocol version", _discovery_response(include_pv=False)),
            ("non-string sequence number", _discovery_response(sn=1000)),
            ("mismatched sequence number", _discovery_response(sn="1001")),
            ("missing sequence number", _discovery_response(include_sn=False)),
            ("missing message", _discovery_response(include_message=False)),
            ("null message", _discovery_response(message=None)),
            ("list message", _discovery_response(message=[])),
            ("scalar message", _discovery_response(message="invalid")),
            ("missing device identifier", _discovery_response(include_did=False)),
            ("missing product identifier", _discovery_response(include_pid=False)),
            ("empty device identifier", _discovery_response(did="")),
            ("empty product identifier", _discovery_response(pid="")),
            (
                "non-string device identifier",
                _discovery_response(did=["device-1234"]),
            ),
            (
                "non-string product identifier",
                _discovery_response(pid={"value": "product-1234"}),
            ),
        )

        for name, malformed_response in cases:
            with self.subTest(name=name):
                fake_socket = FakeDatagramSocket(
                    responses=[
                        (malformed_response, ("192.0.2.13", 6095)),
                        (_discovery_response(), ("192.0.2.14", 6095)),
                    ]
                )

                with (
                    patch(
                        "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                        return_value=fake_socket,
                    ),
                    patch(
                        "custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"
                    ),
                ):
                    discovered_ips = get_ip()

                self.assertEqual(discovered_ips, ["192.0.2.14"])
                self.assertTrue(fake_socket.closed)

    def test_invalid_packets_do_not_consume_the_device_limit(self) -> None:
        """A valid response remains discoverable after rejected datagrams."""
        invalid_responses = [
            (b"not-json", ("192.0.2.13", 6095))
            for _ in range(255)
        ]
        fake_socket = FakeDatagramSocket(
            responses=[
                *invalid_responses,
                (_discovery_response(), ("192.0.2.14", 6095)),
            ]
        )

        with (
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                return_value=fake_socket,
            ),
            patch("custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"),
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.time.monotonic",
                return_value=0.0,
            ),
        ):
            discovered_ips = get_ip()

        self.assertEqual(discovered_ips, ["192.0.2.14"])
        self.assertTrue(fake_socket.closed)

    def test_invalid_packet_stream_stops_at_collection_deadline(self) -> None:
        """Continuous rejected traffic cannot keep discovery running."""
        fake_socket = FakeDatagramSocket(
            responses=[
                (b"not-json", ("192.0.2.13", 6095))
                for _ in range(300)
            ]
        )

        with (
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.socket.socket",
                return_value=fake_socket,
            ),
            patch("custom_components.hass_cozylife_local_pull.udp_discover.time.sleep"),
            patch(
                "custom_components.hass_cozylife_local_pull.udp_discover.time.monotonic",
                side_effect=(0.0, 0.2, 0.6, 1.0),
            ),
        ):
            discovered_ips = get_ip()

        self.assertEqual(discovered_ips, [])
        self.assertEqual(fake_socket.receive_count, 3)
        self.assertEqual(fake_socket.timeouts[0], 0.1)
        self.assertAlmostEqual(fake_socket.timeouts[1], 0.8)
        self.assertAlmostEqual(fake_socket.timeouts[2], 0.4)
        self.assertTrue(fake_socket.closed)
