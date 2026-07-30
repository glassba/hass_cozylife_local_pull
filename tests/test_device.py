"""Verify the runtime device boundary shared by CozyLife entities."""

from __future__ import annotations

import asyncio
from importlib.util import find_spec
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import custom_components.hass_cozylife_local_pull as integration
from custom_components.hass_cozylife_local_pull import light as light_platform
from custom_components.hass_cozylife_local_pull import number as number_platform
from custom_components.hass_cozylife_local_pull import switch as switch_platform
from custom_components.hass_cozylife_local_pull.const import DOMAIN
from custom_components.hass_cozylife_local_pull.device import CozyLifeDevice
from custom_components.hass_cozylife_local_pull.light import CozyLifeLight
from custom_components.hass_cozylife_local_pull.motor import (
    CozyLifeMotorCountdown,
    CozyLifeMotorSwitch,
)
from custom_components.hass_cozylife_local_pull.number import CozyLifeCountdown
from custom_components.hass_cozylife_local_pull.switch import CozyLifeSwitch


DEVICE_MODULE = "custom_components.hass_cozylife_local_pull.device"


class RecordingClient:
    """Provide the complete transport surface used by the device object."""

    device_id = "device-1234"
    device_model_name = "Test Socket"
    device_type_code = "00"
    dpid = [1, 2]
    last_state_sequence_number = 1700000000000

    def __init__(self) -> None:
        self.query_calls: list[list[int] | None] = []
        self.control_calls: list[dict[str, int]] = []
        self.ready_callbacks = []
        self.state_callbacks = []
        self.closed = False
        self.stop_signaled = False

    def add_ready_callback(self, callback) -> None:
        """Retain a transport callback until the handshake is published."""
        self.ready_callbacks.append(callback)

    def become_ready(self) -> None:
        """Publish the completed handshake to transport subscribers."""
        for callback in tuple(self.ready_callbacks):
            callback(self)

    def query(self, attr: list[int] | None = None) -> dict[str, int]:
        """Record a query and return one device state."""
        self.query_calls.append(attr)
        return {"1": 255}

    def control(self, payload: dict[str, int]) -> bool:
        """Record a control request."""
        self.control_calls.append(payload)
        return True

    def add_state_callback(self, callback):
        """Register a state callback and return its removal callback."""
        self.state_callbacks.append(callback)

        def remove_callback() -> None:
            self.state_callbacks.remove(callback)

        return remove_callback

    def signal_stop(self) -> None:
        """Record the non-blocking shutdown signal."""
        self.stop_signaled = True

    def close(self) -> None:
        """Record transport cleanup."""
        self.closed = True


class CozyLifeDeviceContractTest(unittest.TestCase):
    """Define the public runtime device contract before implementation."""

    def test_device_class_exists(self) -> None:
        """The integration exposes one device object above the transport."""
        module_spec = find_spec(DEVICE_MODULE)
        self.assertIsNotNone(module_spec)
        if module_spec is None:
            return

        module = __import__(DEVICE_MODULE, fromlist=["CozyLifeDevice"])
        self.assertTrue(hasattr(module, "CozyLifeDevice"))

    def test_device_exposes_identity_and_transport_operations(self) -> None:
        """Entities use the device without reaching into its client."""
        client = RecordingClient()
        device = CozyLifeDevice(client)

        def callback(_state, _sequence_number) -> None:
            pass

        self.assertEqual(device.device_id, "device-1234")
        self.assertEqual(device.device_model_name, "Test Socket")
        self.assertEqual(device.device_type_code, "00")
        self.assertEqual(device.dpid, [1, 2])
        self.assertEqual(device.last_state_sequence_number, 1700000000000)
        self.assertEqual(device.query([2]), {"1": 255})
        self.assertTrue(device.control({"1": 0}))
        remove_callback = device.add_state_callback(callback)

        self.assertEqual(client.query_calls, [[2]])
        self.assertEqual(client.control_calls, [{"1": 0}])
        self.assertEqual(client.state_callbacks, [callback])
        remove_callback()
        self.assertEqual(client.state_callbacks, [])

    def test_replacing_client_moves_operations_and_state_subscription(self) -> None:
        """Existing entities follow a device when its transport changes."""
        first_client = RecordingClient()
        second_client = RecordingClient()
        device = CozyLifeDevice(first_client)
        received_reports = []

        def callback(state, sequence_number) -> None:
            received_reports.append((state, sequence_number))

        remove_callback = device.add_state_callback(callback)

        replaced_client = device.replace_client(second_client)
        query_result = device.query([2])
        control_result = device.control({"1": 0})
        for state_callback in tuple(second_client.state_callbacks):
            state_callback({"1": 255}, 1700000000001)

        self.assertIs(replaced_client, first_client)
        self.assertEqual(first_client.state_callbacks, [])
        self.assertEqual(first_client.query_calls, [])
        self.assertEqual(first_client.control_calls, [])
        self.assertEqual(second_client.query_calls, [[2]])
        self.assertEqual(second_client.control_calls, [{"1": 0}])
        self.assertEqual(query_result, {"1": 255})
        self.assertTrue(control_result)
        self.assertEqual(
            received_reports,
            [({"1": 255}, 1700000000001)],
        )

        remove_callback()
        self.assertEqual(second_client.state_callbacks, [])
        self.assertIsNone(device.replace_client(second_client))

    def test_device_info_groups_entities_by_physical_device(self) -> None:
        """The device publishes stable metadata for Home Assistant grouping."""
        device = CozyLifeDevice(RecordingClient())

        self.assertEqual(
            device.device_info,
            {
                "identifiers": {(DOMAIN, "device-1234")},
                "manufacturer": "CozyLife",
                "model": "Test Socket",
                "name": "Test Socket 1234",
            },
        )

    def test_ready_client_registers_device_before_notifying_subscribers(self) -> None:
        """One ready transport becomes one registered runtime device."""
        register_device_callback = getattr(
            integration, "register_device_callback", None
        )
        self.assertTrue(callable(register_device_callback))
        if not callable(register_device_callback):
            return

        runtime = {
            "ip": [],
            "known_ips": set(),
            "tcp_client": [],
            "devices": {},
            "device_callbacks": [],
            "lock": threading.RLock(),
            "discovery_lock": threading.RLock(),
            "stopped": False,
        }
        client = RecordingClient()
        notified_devices = []
        register_device_callback(runtime, notified_devices.append)

        with patch.object(integration, "tcp_client", return_value=client):
            integration._add_new_clients(runtime, ["192.0.2.10"], "en")

        self.assertEqual(runtime["devices"], {})
        self.assertEqual(notified_devices, [])

        client.become_ready()
        client.become_ready()

        self.assertEqual(list(runtime["devices"]), ["device-1234"])
        self.assertEqual(notified_devices, [runtime["devices"]["device-1234"]])
        self.assertIsInstance(notified_devices[0], CozyLifeDevice)
        self.assertFalse(client.closed)
        self.assertEqual(runtime["tcp_client"], [client])

    def test_all_entity_variants_belong_to_the_runtime_device(self) -> None:
        """Every entity variant exposes the same Home Assistant device info."""
        device = CozyLifeDevice(RecordingClient())
        entities = (
            CozyLifeLight(device),
            CozyLifeSwitch(device),
            CozyLifeCountdown(device, "2", "Countdown 1"),
            CozyLifeMotorSwitch(device),
            CozyLifeMotorCountdown(device),
        )
        expected_identities = (
            ("device-1234", "Test Socket 1234"),
            ("device-1234", "Test Socket 1234"),
            ("device-1234_countdown_1", "Test Socket 1234 Countdown 1"),
            ("device-1234", "Test Socket 1234"),
            ("device-1234_countdown", "Test Socket 1234 Countdown"),
        )

        for entity, (unique_id, name) in zip(
            entities, expected_identities, strict=True
        ):
            with self.subTest(entity=type(entity).__name__):
                self.assertEqual(entity.device_info, device.device_info)
                self.assertEqual(entity.unique_id, unique_id)
                self.assertEqual(entity.name, name)


class DevicePlatformSetupTest(unittest.IsolatedAsyncioTestCase):
    """Verify platforms consume registered devices rather than transports."""

    async def test_light_platform_adds_registered_device_without_querying(self) -> None:
        """Config entry setup adds a device-backed light without blocking I/O."""
        setup_entry = getattr(light_platform, "async_setup_entry", None)
        self.assertTrue(callable(setup_entry))
        if not callable(setup_entry):
            return

        client = RecordingClient()
        client.device_type_code = "01"
        client.dpid = [1, 4]
        device = CozyLifeDevice(client)
        runtime = {
            "devices": {device.device_id: device},
            "device_callbacks": [],
            "lock": threading.RLock(),
            "stopped": False,
        }
        entry = SimpleNamespace(runtime_data=runtime)
        added_entities = []
        hass = SimpleNamespace(loop=asyncio.get_running_loop())

        await setup_entry(hass, entry, added_entities.extend)
        await asyncio.sleep(0)

        self.assertEqual(len(added_entities), 1)
        self.assertIsInstance(added_entities[0], CozyLifeLight)
        self.assertEqual(added_entities[0].device_info, device.device_info)
        self.assertEqual(client.query_calls, [])

    async def test_switch_platform_adds_registered_device_without_querying(self) -> None:
        """Config entry setup adds a device-backed switch without blocking I/O."""
        setup_entry = getattr(switch_platform, "async_setup_entry", None)
        self.assertTrue(callable(setup_entry))
        if not callable(setup_entry):
            return

        client = RecordingClient()
        device = CozyLifeDevice(client)
        runtime = {
            "devices": {device.device_id: device},
            "device_callbacks": [],
            "lock": threading.RLock(),
            "stopped": False,
        }
        entry = SimpleNamespace(runtime_data=runtime)
        added_entities = []
        hass = SimpleNamespace(loop=asyncio.get_running_loop())

        await setup_entry(hass, entry, added_entities.extend)
        await asyncio.sleep(0)

        self.assertEqual(len(added_entities), 1)
        self.assertIsInstance(added_entities[0], CozyLifeSwitch)
        self.assertEqual(added_entities[0].device_info, device.device_info)
        self.assertEqual(client.query_calls, [])

    async def test_number_platform_adds_registered_countdown(self) -> None:
        """Config entry setup adds countdown entities from device capabilities."""
        setup_entry = getattr(number_platform, "async_setup_entry", None)
        self.assertTrue(callable(setup_entry))
        if not callable(setup_entry):
            return

        client = RecordingClient()
        device = CozyLifeDevice(client)
        runtime = {
            "devices": {device.device_id: device},
            "device_callbacks": [],
            "lock": threading.RLock(),
            "stopped": False,
        }
        entry = SimpleNamespace(runtime_data=runtime)
        added_entities = []
        hass = SimpleNamespace(loop=asyncio.get_running_loop())

        await setup_entry(hass, entry, added_entities.extend)
        await asyncio.sleep(0)

        self.assertEqual(len(added_entities), 1)
        self.assertIsInstance(added_entities[0], CozyLifeCountdown)
        self.assertEqual(added_entities[0]._dp_id, "2")
        self.assertEqual(added_entities[0].device_info, device.device_info)
        self.assertEqual(client.query_calls, [])
