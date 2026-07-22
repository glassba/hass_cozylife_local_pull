"""Regression tests for adding devices after their startup handshake completes."""

from __future__ import annotations

import importlib
import inspect
import unittest
from unittest.mock import Mock, patch

from custom_components.hass_cozylife_local_pull import light, switch
from custom_components.hass_cozylife_local_pull.const import (
    DOMAIN,
    LIGHT_TYPE_CODE,
    SWITCH_TYPE_CODE,
)


class DelayedDeviceClient:
    """Expose the production readiness callback contract without network I/O."""

    device_id = "device-1234"
    device_model_name = "Test Device"
    dpid = [1, 4]

    def __init__(self, device_type_code: str | type = str) -> None:
        self.device_type_code = device_type_code
        self._ready_callbacks = []
        self.registration_count = 0

    def add_ready_callback(self, callback) -> None:
        """Run immediately when ready or retain the one-shot callback."""
        self.registration_count += 1
        if self.device_type_code is not str:
            callback(self)
            return
        self._ready_callbacks.append(callback)

    def become_ready(self, device_type_code: str) -> None:
        """Complete the delayed handshake and publish readiness."""
        self.device_type_code = device_type_code
        callbacks, self._ready_callbacks = self._ready_callbacks, []
        for callback in callbacks:
            callback(self)

    def query(self) -> dict[str, int]:
        """Return a complete state for either supported entity type."""
        return {"1": 0, "4": 0}

    def control(self, payload: dict[str, int]) -> bool:
        """Accept entity commands used during setup tests."""
        return True


class RecordingLoop:
    """Execute thread-safe scheduling calls while recording their payloads."""

    def __init__(self) -> None:
        self.scheduled = []

    def call_soon_threadsafe(self, callback, *args) -> None:
        """Record and execute a scheduled callback."""
        self.scheduled.append(args)
        callback(*args)


class RecordingHomeAssistant:
    """Provide the Home Assistant attributes used by synchronous setup."""

    def __init__(self, clients=None) -> None:
        self.data = {DOMAIN: {"tcp_client": clients or []}}
        self.loop = RecordingLoop()
        self.created_tasks = []

    def async_create_task(self, task) -> None:
        """Record scheduled platform loads without starting an event loop."""
        self.created_tasks.append(task)


class StartupReadinessTest(unittest.TestCase):
    """Verify startup never depends on a fixed connection delay."""

    def test_setup_loads_platforms_without_fixed_sleep(self) -> None:
        """Integration setup schedules platforms immediately after clients."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )

        hass = RecordingHomeAssistant()
        platform_loads = []

        async def platform_load(platform: str) -> str:
            """Represent one Home Assistant asynchronous platform load."""
            return platform

        def create_platform_load(hass, platform, *args):
            load = platform_load(platform)
            platform_loads.append(load)
            self.addCleanup(load.close)
            return load

        load_platform = Mock(side_effect=create_platform_load)
        with patch.object(integration, "get_ip", return_value=["192.0.2.1"]), patch.object(
            integration, "get_pid_list", return_value=[]
        ), patch.object(
            integration, "tcp_client", return_value=DelayedDeviceClient()
        ), patch.object(
            integration, "async_load_platform", load_platform
        ), patch.object(
            integration, "time", create=True
        ) as time_module:
            setup_succeeded = integration.setup(hass, {DOMAIN: {}})

        self.assertTrue(setup_succeeded)
        time_module.sleep.assert_not_called()
        self.assertEqual(
            [call.args[1] for call in load_platform.call_args_list],
            ["light", "switch"],
        )
        self.assertTrue(all(inspect.isawaitable(load) for load in platform_loads))
        self.assertEqual(
            [args[0] for args in hass.loop.scheduled], platform_loads
        )
        self.assertEqual(hass.created_tasks, platform_loads)

    def test_platforms_add_clients_that_become_ready_later(self) -> None:
        """A delayed handshake adds the entity without reloading its platform."""
        cases = (
            (light, LIGHT_TYPE_CODE, light.CozyLifeLight),
            (switch, SWITCH_TYPE_CODE, switch.CozyLifeSwitch),
        )

        for platform, device_type_code, entity_type in cases:
            with self.subTest(platform=platform.__name__):
                client = DelayedDeviceClient()
                hass = RecordingHomeAssistant([client])
                added_entities = []

                platform.setup_platform(
                    hass, {}, added_entities.extend, discovery_info={}
                )
                self.assertEqual(added_entities, [])

                client.become_ready(device_type_code)

                self.assertEqual(len(added_entities), 1)
                self.assertIsInstance(added_entities[0], entity_type)

    def test_platforms_register_callbacks_for_already_ready_clients(self) -> None:
        """A handshake completed before platform setup cannot be missed."""
        cases = (
            (light, LIGHT_TYPE_CODE, light.CozyLifeLight),
            (switch, SWITCH_TYPE_CODE, switch.CozyLifeSwitch),
        )

        for platform, device_type_code, entity_type in cases:
            with self.subTest(platform=platform.__name__):
                client = DelayedDeviceClient(device_type_code)
                hass = RecordingHomeAssistant([client])
                added_entities = []

                platform.setup_platform(
                    hass, {}, added_entities.extend, discovery_info={}
                )

                self.assertEqual(client.registration_count, 1)
                self.assertEqual(len(added_entities), 1)
                self.assertIsInstance(added_entities[0], entity_type)

    def test_platforms_ignore_ready_clients_for_another_device_type(self) -> None:
        """A readiness callback preserves each platform's type boundary."""
        cases = (
            (light, SWITCH_TYPE_CODE),
            (switch, LIGHT_TYPE_CODE),
        )

        for platform, device_type_code in cases:
            with self.subTest(platform=platform.__name__):
                client = DelayedDeviceClient(device_type_code)
                hass = RecordingHomeAssistant([client])
                added_entities = []

                platform.setup_platform(
                    hass, {}, added_entities.extend, discovery_info={}
                )

                self.assertEqual(client.registration_count, 1)
                self.assertEqual(added_entities, [])
