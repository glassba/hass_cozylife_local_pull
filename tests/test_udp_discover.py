"""Regression tests for isolated User Datagram Protocol discovery timeouts."""

from __future__ import annotations

import socket
import unittest
from unittest.mock import patch

from custom_components.hass_cozylife_local_pull.udp_discover import get_ip


class FakeDatagramSocket:
    """Record socket configuration while returning one discovered device."""

    def __init__(
        self,
        discover_device: bool = True,
        send_error: OSError | None = None,
        receive_error_at: int | None = None,
        configuration_error_at: str | None = None,
    ) -> None:
        self.timeout: float | None = None
        self.receive_count = 0
        self.discover_device = discover_device
        self.send_error = send_error
        self.receive_error_at = receive_error_at
        self.configuration_error_at = configuration_error_at
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

    def sendto(self, *args: object) -> None:
        """Accept discovery broadcasts or raise an injected failure."""
        if self.send_error is not None:
            raise self.send_error

    def recvfrom(self, *args: object) -> tuple[bytes, tuple[str, int]]:
        """Return one response, then end collection with a timeout."""
        self.receive_count += 1
        if self.receive_count == self.receive_error_at:
            raise OSError("Injected receive failure")
        if self.discover_device and self.receive_count <= 2:
            return b"response", ("192.0.2.10", 6095)
        raise TimeoutError

    def close(self) -> None:
        """Record release of the discovery socket."""
        self.closed = True


class UserDatagramProtocolDiscoveryTimeoutTest(unittest.TestCase):
    """Verify discovery timeout configuration remains socket-local."""

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
        self.assertEqual(fake_socket.timeout, 0.1)
        self.assertTrue(fake_socket.closed)
        self.assertIsNone(socket.getdefaulttimeout())

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
        """Unexpected discovery failures propagate after socket cleanup."""
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
                    with self.assertRaises(OSError):
                        get_ip()

                self.assertTrue(fake_socket.closed)
