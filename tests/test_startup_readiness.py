"""Verify discovery registers ready devices before creating entities."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import patch

import custom_components.hass_cozylife_local_pull as integration
from custom_components.hass_cozylife_local_pull import light, motor, number, switch
from custom_components.hass_cozylife_local_pull.const import (
    DOMAIN,
    LIGHT_TYPE_CODE,
    MOTOR_TYPE_CODE,
    SUPPORT_DEVICE_CATEGORY,
    SWITCH_TYPE_CODE,
)
from custom_components.hass_cozylife_local_pull.device import CozyLifeDevice


class DelayedTransport:
    """Expose the complete transport contract without network input or output."""

    device_model_name = "Test Device"
    dpid = [1, 2, 4, 6, 13]
    last_state_sequence_number = 1700000000000

    def __init__(
        self,
        ip: str = "192.0.2.10",
        lang: str = "en",
        device_type_code: str | None = None,
        device_id: str | None = None,
    ) -> None:
        self.ip = ip
        self.lang = lang
        self.device_id = device_id or f"device-{ip}"
        self.device_type_code = device_type_code
        self.ready_callbacks = []
        self.state_callbacks = []
        self.registration_count = 0
        self.query_count = 0
        self.closed = False
        self.stop_signaled = False

    def add_ready_callback(self, callback) -> None:
        """Run immediately when ready or retain the one-shot callback."""
        if self.stop_signaled:
            return
        self.registration_count += 1
        if self.device_type_code is None:
            self.ready_callbacks.append(callback)
        else:
            callback(self)

    def become_ready(self, device_type_code: str) -> None:
        """Complete the delayed handshake and notify subscribers once."""
        if self.stop_signaled:
            return
        self.device_type_code = device_type_code
        callbacks, self.ready_callbacks = self.ready_callbacks, []
        for callback in callbacks:
            callback(self)

    def query(self, attributes: list[int] | None = None) -> dict[str, int]:
        """Return all requested properties with an inactive value."""
        self.query_count += 1
        dpids = attributes or self.dpid
        return {str(dp_id): 0 for dp_id in dpids}

    def control(self, payload: dict[str, int]) -> bool:
        """Accept entity control requests."""
        return True

    def add_state_callback(self, callback):
        """Register a state subscriber and return its removal callback."""
        self.state_callbacks.append(callback)

        def remove_callback() -> None:
            self.state_callbacks.remove(callback)

        return remove_callback

    def report(self, state: dict[str, int], sequence_number: int) -> None:
        """Deliver one state report to current transport subscribers."""
        for callback in tuple(self.state_callbacks):
            callback(state, sequence_number)

    def signal_stop(self) -> None:
        """Reject new readiness callbacks."""
        self.stop_signaled = True

    def close(self) -> None:
        """Record final transport cleanup."""
        self.closed = True


def make_runtime() -> dict:
    """Build the production runtime shape used by discovery and platforms."""
    return {
        "ip": [],
        "known_ips": set(),
        "tcp_client": [],
        "devices": {},
        "device_callbacks": [],
        "lock": threading.RLock(),
        "discovery_lock": threading.RLock(),
        "stopped": False,
        "cancel_discovery": None,
        "remove_stop_listener": None,
    }


class DeviceRegistrationTest(unittest.TestCase):
    """Verify transports become devices only after a valid handshake."""

    def test_client_waits_for_readiness_before_device_registration(self) -> None:
        """An address alone cannot create a device or entity."""
        runtime = make_runtime()
        client = DelayedTransport()
        notified_devices = []
        integration.register_device_callback(runtime, notified_devices.append)

        with patch.object(integration, "tcp_client", return_value=client):
            integration._add_new_clients(runtime, [client.ip], "zh")

        self.assertEqual(runtime["devices"], {})
        self.assertEqual(notified_devices, [])
        client.become_ready(LIGHT_TYPE_CODE)

        self.assertEqual(list(runtime["devices"]), [client.device_id])
        self.assertEqual(notified_devices, [runtime["devices"][client.device_id]])
        self.assertIsInstance(notified_devices[0], CozyLifeDevice)

    def test_one_batch_deduplicates_discovered_and_configured_address(self) -> None:
        """The same address from both sources creates one transport and device."""
        runtime = make_runtime()
        created_clients = []

        def create_client(ip: str, lang: str) -> DelayedTransport:
            client = DelayedTransport(ip, lang, SWITCH_TYPE_CODE)
            created_clients.append(client)
            return client

        with patch.object(integration, "get_ip", return_value=["192.0.2.10"]), patch.object(
            integration, "tcp_client", side_effect=create_client
        ):
            integration._discover_new_clients(
                runtime, "zh", ("192.0.2.10", "192.0.2.10")
            )

        self.assertEqual(len(created_clients), 1)
        self.assertEqual(runtime["ip"], ["192.0.2.10"])
        self.assertEqual(len(runtime["devices"]), 1)

    def test_client_construction_failure_keeps_address_retryable(self) -> None:
        """A failed configured address can succeed during later discovery."""
        runtime = make_runtime()
        attempts = 0

        def create_client(ip: str, lang: str) -> DelayedTransport:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError("Injected construction failure")
            return DelayedTransport(ip, lang, SWITCH_TYPE_CODE)

        with self.assertLogs(integration.__name__, level="ERROR"), patch.object(
            integration, "tcp_client", side_effect=create_client
        ):
            integration._add_new_clients(runtime, ["192.0.2.10"], "en")
            integration._add_new_clients(runtime, ["192.0.2.10"], "en")

        self.assertEqual(attempts, 2)
        self.assertEqual(runtime["known_ips"], {"192.0.2.10"})
        self.assertEqual(len(runtime["devices"]), 1)

    def test_device_callbacks_replay_existing_and_receive_future_devices(self) -> None:
        """A platform cannot miss devices registered before or after setup."""
        runtime = make_runtime()
        first_client = DelayedTransport(
            device_type_code=LIGHT_TYPE_CODE,
            device_id="device-first",
        )
        integration._register_ready_device(runtime, first_client)
        notified_devices = []

        integration.register_device_callback(runtime, notified_devices.append)
        second_client = DelayedTransport(
            device_type_code=SWITCH_TYPE_CODE,
            device_id="device-second",
        )
        integration._register_ready_device(runtime, second_client)

        self.assertEqual(
            [device.device_id for device in notified_devices],
            ["device-first", "device-second"],
        )

    def test_new_transport_replaces_duplicate_device_identifier(self) -> None:
        """A new address takes over an existing physical device."""
        runtime = make_runtime()
        notified_devices = []
        integration.register_device_callback(runtime, notified_devices.append)
        first_client = DelayedTransport(
            device_type_code=SWITCH_TYPE_CODE,
            device_id="same-device",
        )
        second_client = DelayedTransport(
            ip="192.0.2.20",
            device_type_code=SWITCH_TYPE_CODE,
            device_id="same-device",
        )
        received_reports = []

        with patch.object(
            integration,
            "tcp_client",
            side_effect=[first_client, second_client],
        ):
            integration._add_new_clients(runtime, [first_client.ip], "en")
            device = runtime["devices"]["same-device"]
            remove_callback = device.add_state_callback(
                lambda state, sequence_number: received_reports.append(
                    (state, sequence_number)
                )
            )
            integration._add_new_clients(runtime, [second_client.ip], "en")

        first_client.report({"1": 0}, 1700000000001)
        second_client.report({"1": 255}, 1700000000002)
        device.query([1])

        self.assertEqual(len(runtime["devices"]), 1)
        self.assertIs(runtime["devices"]["same-device"], device)
        self.assertEqual(notified_devices, [device])
        self.assertEqual(runtime["tcp_client"], [second_client])
        self.assertEqual(runtime["ip"], [second_client.ip])
        self.assertEqual(
            runtime["known_ips"],
            {first_client.ip, second_client.ip},
        )
        self.assertTrue(first_client.closed)
        self.assertFalse(second_client.closed)
        self.assertEqual(first_client.query_count, 0)
        self.assertEqual(second_client.query_count, 1)
        self.assertEqual(
            received_reports,
            [({"1": 255}, 1700000000002)],
        )

        remove_callback()
        second_client.report({"1": 0}, 1700000000003)
        self.assertEqual(
            received_reports,
            [({"1": 255}, 1700000000002)],
        )

    def test_callback_failure_does_not_hide_device_from_other_platforms(self) -> None:
        """One platform callback failure cannot block remaining platforms."""
        runtime = make_runtime()
        notified_devices = []

        def fail_callback(device: CozyLifeDevice) -> None:
            raise RuntimeError("Injected callback failure")

        integration.register_device_callback(runtime, fail_callback)
        integration.register_device_callback(runtime, notified_devices.append)
        client = DelayedTransport(device_type_code=SWITCH_TYPE_CODE)

        with self.assertLogs(integration.__name__, level="ERROR"):
            integration._register_ready_device(runtime, client)

        self.assertEqual(notified_devices, [runtime["devices"][client.device_id]])

    def test_stopped_runtime_rejects_callbacks_and_new_addresses(self) -> None:
        """Shutdown prevents retaining callbacks or constructing transports."""
        runtime = make_runtime()
        integration._close_clients(runtime)
        callback = unittest.mock.Mock()

        integration.register_device_callback(runtime, callback)
        with patch.object(integration, "tcp_client") as create_client:
            integration._add_new_clients(runtime, ["192.0.2.10"], "en")

        callback.assert_not_called()
        create_client.assert_not_called()
        self.assertEqual(runtime["device_callbacks"], [])

    def test_close_failure_does_not_block_remaining_clients(self) -> None:
        """One transport cleanup error cannot leave another transport open."""
        runtime = make_runtime()

        class FailingCloseTransport(DelayedTransport):
            def close(self) -> None:
                raise RuntimeError("Injected close failure")

        first = FailingCloseTransport()
        second = DelayedTransport(ip="192.0.2.20")
        runtime["tcp_client"] = [first, second]

        with self.assertLogs(integration.__name__, level="ERROR"):
            integration._close_clients(runtime)

        self.assertTrue(first.stop_signaled)
        self.assertTrue(second.stop_signaled)
        self.assertTrue(second.closed)

    def test_motor_category_remains_supported(self) -> None:
        """The internal device layer preserves motor platform support."""
        self.assertIn(MOTOR_TYPE_CODE, SUPPORT_DEVICE_CATEGORY)


class DevicePlatformRegistrationTest(unittest.IsolatedAsyncioTestCase):
    """Verify platforms map registered device capabilities to entities."""

    async def add_for_platform(
        self,
        platform,
        device_type_code: str,
        dpids: list[int],
        *,
        register_after_setup: bool = False,
    ) -> list:
        """Set up one platform and return entities added on the event loop."""
        runtime = make_runtime()
        client = DelayedTransport(device_type_code=device_type_code)
        client.dpid = dpids
        device = CozyLifeDevice(client)
        if not register_after_setup:
            runtime["devices"][device.device_id] = device
        entry = SimpleNamespace(runtime_data=runtime)
        added_entities = []
        hass = SimpleNamespace(loop=asyncio.get_running_loop())

        await platform.async_setup_entry(hass, entry, added_entities.extend)
        if register_after_setup:
            integration._register_ready_device(runtime, client)
        await asyncio.sleep(0)
        return added_entities

    async def test_platforms_map_all_supported_device_variants(self) -> None:
        """Every supported type creates its specialized platform entity."""
        cases = (
            (light, LIGHT_TYPE_CODE, [1, 4], light.CozyLifeLight),
            (switch, SWITCH_TYPE_CODE, [1, 2], switch.CozyLifeSwitch),
            (switch, MOTOR_TYPE_CODE, [1, 6], motor.CozyLifeMotorSwitch),
            (number, LIGHT_TYPE_CODE, [1, 13], number.CozyLifeCountdown),
            (number, SWITCH_TYPE_CODE, [1, 2], number.CozyLifeCountdown),
            (number, MOTOR_TYPE_CODE, [1, 6], motor.CozyLifeMotorCountdown),
        )

        for platform, device_type_code, dpids, entity_type in cases:
            with self.subTest(platform=platform.__name__, type=device_type_code):
                entities = await self.add_for_platform(
                    platform, device_type_code, dpids
                )
                self.assertEqual(len(entities), 1)
                self.assertIsInstance(entities[0], entity_type)
                self.assertEqual(entities[0].device_info["identifiers"], {
                    (DOMAIN, "device-192.0.2.10")
                })

    async def test_loaded_platform_receives_device_registered_later(self) -> None:
        """A configured address that comes online later still creates an entity."""
        entities = await self.add_for_platform(
            light,
            LIGHT_TYPE_CODE,
            [1, 4],
            register_after_setup=True,
        )

        self.assertEqual(len(entities), 1)
        self.assertIsInstance(entities[0], light.CozyLifeLight)

    async def test_network_thread_registers_entity_on_home_assistant_loop(
        self,
    ) -> None:
        """A network-thread handshake queues entity creation on the event loop."""
        runtime = make_runtime()
        client = DelayedTransport()
        with patch.object(integration, "tcp_client", return_value=client):
            integration._add_new_clients(runtime, [client.ip], "en")

        entities = []
        callback_thread_ids = []
        event_loop_thread_id = threading.get_ident()
        entry = SimpleNamespace(runtime_data=runtime)
        hass = SimpleNamespace(loop=asyncio.get_running_loop())

        def add_entities(new_entities) -> None:
            callback_thread_ids.append(threading.get_ident())
            entities.extend(new_entities)

        def publish_readiness() -> int:
            client.become_ready(LIGHT_TYPE_CODE)
            return threading.get_ident()

        await light.async_setup_entry(hass, entry, add_entities)
        worker_thread_id = await asyncio.to_thread(publish_readiness)
        await asyncio.sleep(0)

        self.assertNotEqual(worker_thread_id, event_loop_thread_id)
        self.assertEqual(callback_thread_ids, [event_loop_thread_id])
        self.assertEqual(len(entities), 1)
        self.assertIsInstance(entities[0], light.CozyLifeLight)

    async def test_platforms_ignore_mismatched_types_and_capabilities(self) -> None:
        """Unsupported type and countdown combinations create no entities."""
        cases = (
            (light, SWITCH_TYPE_CODE, [1, 2]),
            (switch, LIGHT_TYPE_CODE, [1, 4]),
            (number, LIGHT_TYPE_CODE, [1, 2]),
            (number, SWITCH_TYPE_CODE, [1, 13]),
            (number, MOTOR_TYPE_CODE, [1, 2]),
            (number, "99", [1, 2, 6, 13]),
        )

        for platform, device_type_code, dpids in cases:
            with self.subTest(platform=platform.__name__, type=device_type_code):
                self.assertEqual(
                    await self.add_for_platform(platform, device_type_code, dpids),
                    [],
                )

    async def test_countdown_platform_uses_expected_data_point(self) -> None:
        """Each device category binds countdown to its protocol data point."""
        cases = (
            (LIGHT_TYPE_CODE, [1, 13], "13"),
            (SWITCH_TYPE_CODE, [1, 2], "2"),
            (MOTOR_TYPE_CODE, [1, 6], "6"),
        )

        for device_type_code, dpids, expected_dp_id in cases:
            with self.subTest(type=device_type_code):
                entities = await self.add_for_platform(
                    number, device_type_code, dpids
                )
                self.assertEqual(entities[0]._dp_id, expected_dp_id)
