"""Regression tests for the CozyLife light platform compatibility layer."""

from __future__ import annotations

import asyncio
import importlib
import threading
import unittest
from unittest.mock import patch

from homeassistant.exceptions import HomeAssistantError


class FakeTcpClient:
    """Provide deterministic device capabilities and state to a light entity."""

    device_id = "device-1234"
    device_model_name = "Test Light"

    def __init__(
        self,
        dpid: list[int],
        state: dict[str, int],
        control_result: bool = True,
    ) -> None:
        self.dpid = dpid
        self.state = state
        self.control_result = control_result
        self.query_count = 0
        self.last_payload: dict[str, int] | None = None
        self.state_callbacks = []
        self.last_state_sequence_number = 1699999999999

    def query(self) -> dict[str, int]:
        """Return the configured device state."""
        self.query_count += 1
        state = self.state.copy()
        if state and self.state_callbacks:
            self.report(state)
        return state

    def control(self, payload: dict[str, int]) -> bool:
        """Record the latest control payload."""
        self.last_payload = payload
        if self.control_result:
            self.report(payload)
        return self.control_result

    def add_state_callback(self, callback):
        """Subscribe one entity to active device reports."""
        self.state_callbacks.append(callback)

        def remove_callback() -> None:
            self.state_callbacks.remove(callback)

        return remove_callback

    def report(
        self,
        state: dict[str, int],
        sequence_number: int | None = None,
    ) -> None:
        """Deliver one active report to all subscribed entities."""
        if sequence_number is None:
            sequence_number = self.last_state_sequence_number + 1
        if sequence_number < self.last_state_sequence_number:
            return
        self.last_state_sequence_number = sequence_number
        for callback in tuple(self.state_callbacks):
            callback(state, sequence_number)


class TimestampedLightClient(FakeTcpClient):
    """Publish timestamped control replies to a light entity."""

    def __init__(
        self,
        dpid: list[int],
        state: dict[str, int],
        control_reply: dict[str, int],
    ) -> None:
        super().__init__(dpid, state)
        self.control_reply = control_reply

    def control(self, payload: dict[str, int]) -> bool:
        """Publish the device reply while retaining the requested payload."""
        self.last_payload = payload
        self.report(self.control_reply, 1700000000000)
        return True


class ImmediateLoop:
    """Run thread-safe callbacks immediately in entity unit tests."""

    def call_soon_threadsafe(self, callback, *args) -> None:
        """Execute one event-loop callback without another thread."""
        callback(*args)


class QueuedLoop:
    """Queue thread-safe callbacks until the test drains them."""

    def __init__(self) -> None:
        self.callbacks = []

    def call_soon_threadsafe(self, callback, *args) -> None:
        """Record one callback without running it on the caller thread."""
        self.callbacks.append((callback, args))

    def drain(self) -> None:
        """Run every queued callback in scheduling order."""
        callbacks, self.callbacks = self.callbacks, []
        for callback, args in callbacks:
            callback(*args)


class FakeHomeAssistant:
    """Provide the event-loop boundary used by light entities."""

    def __init__(self, loop=None) -> None:
        self.loop = loop or ImmediateLoop()

    async def async_add_executor_job(self, target, *args):
        """Run one blocking device operation in a worker thread."""
        result = await asyncio.to_thread(target, *args)
        await asyncio.sleep(0)
        return result


class LightImportTest(unittest.TestCase):
    """Verify the light platform uses the current Home Assistant API."""

    def test_light_module_imports_with_current_home_assistant(self) -> None:
        """The light platform imports with the installed Home Assistant API."""
        try:
            importlib.import_module("custom_components.hass_cozylife_local_pull.light")
        except ImportError as err:
            self.fail(f"The light platform did not import: {err}")


class LightColorModeTest(unittest.IsolatedAsyncioTestCase):
    """Verify each light reports a valid, isolated color mode set."""

    def test_color_modes_follow_device_capabilities(self) -> None:
        """Color modes are valid and do not leak between light instances."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        cases = (
            ([1], {"1": 255}, {light.ColorMode.ONOFF}, light.ColorMode.ONOFF),
            (
                [1, 4],
                {"1": 255, "4": 400},
                {light.ColorMode.BRIGHTNESS},
                light.ColorMode.BRIGHTNESS,
            ),
            (
                [1, 3, 4],
                {"1": 255, "3": 300, "4": 400},
                {light.ColorMode.COLOR_TEMP},
                light.ColorMode.COLOR_TEMP,
            ),
            (
                [1, 4, 5, 6],
                {"1": 255, "4": 400, "5": 120, "6": 500},
                {light.ColorMode.HS},
                light.ColorMode.HS,
            ),
            (
                [1, 4, 5],
                {"1": 255, "4": 400, "5": 120},
                {light.ColorMode.BRIGHTNESS},
                light.ColorMode.BRIGHTNESS,
            ),
            (
                [1, 4, 6],
                {"1": 255, "4": 400, "6": 500},
                {light.ColorMode.BRIGHTNESS},
                light.ColorMode.BRIGHTNESS,
            ),
            (
                [1, 3, 4, 5, 6],
                {"1": 255, "3": 300, "4": 400, "5": 120, "6": 500},
                {light.ColorMode.COLOR_TEMP, light.ColorMode.HS},
                light.ColorMode.HS,
            ),
        )
        entities = []

        for dpid, state, supported_modes, current_mode in cases:
            with self.subTest(dpid=dpid):
                entity = light.CozyLifeLight(FakeTcpClient(dpid, state))
                entities.append((entity, supported_modes, current_mode))
                self.assertEqual(entity.supported_color_modes, supported_modes)
                self.assertEqual(entity.color_mode, current_mode)
                entity.state_attributes

        for entity, supported_modes, current_mode in entities:
            self.assertEqual(entity.supported_color_modes, supported_modes)
            self.assertEqual(entity.color_mode, current_mode)
        self.assertEqual(
            len({id(entity.supported_color_modes) for entity, _, _ in entities}),
            len(entities),
        )

    async def test_color_commands_keep_hs_priority_for_device_reply(self) -> None:
        """A dual-mode light derives its mode from the reported device fields."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 2, 3, 4, 5, 6],
            {"1": 255, "3": 300, "4": 400, "5": 120, "6": 500},
        )
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state"):
            await entity.async_turn_on(
                **{light.ATTR_COLOR_TEMP_KELVIN: 2857}
            )
            self.assertEqual(entity.color_mode, light.ColorMode.HS)

            await entity.async_turn_on(**{light.ATTR_HS_COLOR: (240, 75)})
        self.assertEqual(entity.color_mode, light.ColorMode.HS)
        self.assertEqual(
            client.last_payload, {"1": 255, "2": 0, "5": 240, "6": 750}
        )
        self.assertEqual(entity.hs_color, (240, 75))

    async def test_failed_color_command_preserves_previous_cache(self) -> None:
        """A failed transport command does not alter optimistic light state."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4, 5, 6],
            {"1": 255, "3": 300, "4": 400, "5": 120, "6": 500},
        )
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()
        with patch.object(entity, "async_write_ha_state"):
            await entity.async_turn_on(
                **{light.ATTR_COLOR_TEMP_KELVIN: 4000}
            )
        previous_state = (
            entity.brightness,
            entity.hs_color,
            entity.color_temp_kelvin,
            entity.color_mode,
        )
        client.control_result = False

        with patch.object(entity, "async_write_ha_state"):
            with self.assertRaises(HomeAssistantError):
                await entity.async_turn_on(
                    **{
                        light.ATTR_BRIGHTNESS: 128,
                        light.ATTR_HS_COLOR: (240, 75),
                    }
                )

        self.assertEqual(
            (
                entity.brightness,
                entity.hs_color,
                entity.color_temp_kelvin,
                entity.color_mode,
            ),
            previous_state,
        )

        with patch.object(entity, "async_write_ha_state"):
            await entity.async_update()
        self.assertTrue(entity.available)
        self.assertEqual(entity.hs_color, (120, 50))
        self.assertEqual(entity.color_temp_kelvin, 2857)
        self.assertEqual(entity.color_mode, light.ColorMode.HS)

    def test_partial_hs_state_does_not_raise(self) -> None:
        """A partial hue state leaves hue and saturation unknown."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )

        try:
            entity = light.CozyLifeLight(
                FakeTcpClient([1, 5, 6], {"1": 255, "5": 120})
            )
        except KeyError as err:
            self.fail(f"Partial hue state raised an exception: {err}")

        self.assertIsNone(entity.hs_color)


class LightActiveReportTest(unittest.IsolatedAsyncioTestCase):
    """Verify active device reports immediately refresh the light entity."""

    async def test_report_updates_switch_brightness_and_color_once(self) -> None:
        """One full report updates every mapped light field with one write."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 2, 3, 4, 5, 6],
            {
                "1": 255,
                "2": 0,
                "3": 300,
                "4": 400,
                "5": 65535,
                "6": 65535,
            },
        )
        entity = light.CozyLifeLight(client)
        event_loop_thread = threading.get_ident()
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()
        write_threads = []

        with patch.object(
            entity,
            "async_write_ha_state",
            side_effect=lambda: write_threads.append(threading.get_ident()),
        ) as write_state:
            await asyncio.to_thread(
                client.report,
                {
                    "1": 0,
                    "2": 0,
                    "3": 65535,
                    "4": 200,
                    "5": 240,
                    "6": 750,
                }
            )
            await asyncio.sleep(0)

        self.assertFalse(entity.is_on)
        self.assertEqual(entity.brightness, 50)
        self.assertEqual(entity.hs_color, (240, 75))
        self.assertIsNone(entity.color_temp_kelvin)
        self.assertEqual(entity.color_mode, light.ColorMode.HS)
        self.assertTrue(entity.available)
        self.assertEqual(write_threads, [event_loop_thread])
        write_state.assert_called_once_with()

    async def test_subscription_query_recovers_state_missed_before_add(
        self,
    ) -> None:
        """A query after subscribing restores state reported during entity setup."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1, 4], {"1": 0, "4": 0})
        entity = light.CozyLifeLight(client)
        client.state = {"1": 255, "4": 400}
        client.report(client.state, 1700000000000)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        with patch.object(entity, "async_write_ha_state") as write_state:
            await entity.async_added_to_hass()

        self.assertEqual(client.query_count, 2)
        self.assertTrue(entity.is_on)
        self.assertEqual(entity.brightness, 100)
        write_state.assert_not_called()

    async def test_combined_white_and_color_report_uses_hs_mode(self) -> None:
        """A combined white and color frame chooses HS without losing temperature."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4, 5, 6],
            {"1": 255, "3": 300, "4": 400, "5": 65535, "6": 65535},
        )
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant()
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state"):
            client.report({"3": 300, "5": 240, "6": 750})

        self.assertEqual(entity.color_mode, light.ColorMode.HS)
        self.assertEqual(entity.hs_color, (240, 75))
        self.assertEqual(entity.color_temp_kelvin, 2857)

    async def test_equal_sequence_reports_merge_light_color(self) -> None:
        """Same-millisecond hue and saturation reports form one color state."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4, 5, 6],
            {"1": 255, "3": 300, "4": 400},
        )
        loop = QueuedLoop()
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(loop)
        await entity.async_added_to_hass()
        with patch.object(entity, "async_write_ha_state"):
            loop.drain()

        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"5": 240}, 1700000000000)
            client.report({"6": 750}, 1700000000000)
            loop.drain()

        self.assertEqual(entity.hs_color, (240, 75))
        self.assertEqual(entity.color_temp_kelvin, 2857)
        self.assertEqual(entity.color_mode, light.ColorMode.HS)
        self.assertEqual(write_state.call_count, 2)

    async def test_equal_sequence_temperature_report_preserves_hs(self) -> None:
        """Same-millisecond temperature and HS data remain available together."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4, 5, 6],
            {"1": 255, "3": 65535, "4": 400, "5": 240, "6": 750},
        )
        loop = QueuedLoop()
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(loop)
        await entity.async_added_to_hass()
        with patch.object(entity, "async_write_ha_state"):
            loop.drain()

        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"3": 300}, 1700000000000)
            loop.drain()

        self.assertEqual(entity.hs_color, (240, 75))
        self.assertEqual(entity.color_temp_kelvin, 2857)
        self.assertEqual(entity.color_mode, light.ColorMode.HS)
        write_state.assert_called_once_with()

    async def test_removal_unsubscribes_from_active_reports(self) -> None:
        """A removed light is no longer changed by transport callbacks."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1], {"1": 255})
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant()
        await entity.async_added_to_hass()
        await entity.async_will_remove_from_hass()

        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"1": 0})

        self.assertTrue(entity.is_on)
        write_state.assert_not_called()

    async def test_report_switches_from_hs_to_color_temperature(self) -> None:
        """Valid DPID 3 with invalid DPID 5 and 6 selects color temperature."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4, 5, 6],
            {"1": 255, "3": 65535, "4": 400, "5": 240, "6": 750},
        )
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant()
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"3": 300, "5": 65535, "6": 65535})

        self.assertIsNone(entity.hs_color)
        self.assertEqual(entity.color_temp_kelvin, 2857)
        self.assertEqual(entity.color_mode, light.ColorMode.COLOR_TEMP)
        write_state.assert_called_once_with()

    async def test_partial_temperature_report_preserves_cached_hs(self) -> None:
        """A DPID 3 increment leaves unreported hue and saturation unchanged."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4, 5, 6],
            {"1": 255, "3": 65535, "4": 400, "5": 240, "6": 750},
        )
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant()
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"3": 300})

        self.assertEqual(entity.hs_color, (240, 75))
        self.assertIn("5", entity._state)
        self.assertIn("6", entity._state)
        self.assertEqual(entity.color_temp_kelvin, 2857)
        self.assertEqual(entity.color_mode, light.ColorMode.HS)
        write_state.assert_called_once_with()

    async def test_split_hs_reports_preserve_cached_temperature(self) -> None:
        """Incremental HS values leave unreported temperature unchanged."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4, 5, 6],
            {"1": 255, "3": 300, "4": 400, "5": 65535, "6": 65535},
        )
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant()
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"5": 240})
            self.assertEqual(entity.color_mode, light.ColorMode.COLOR_TEMP)
            self.assertIsNone(entity.hs_color)
            self.assertEqual(entity.color_temp_kelvin, 2857)

            client.report({"6": 750})

        self.assertEqual(entity.color_mode, light.ColorMode.HS)
        self.assertEqual(entity.hs_color, (240, 75))
        self.assertEqual(entity.color_temp_kelvin, 2857)
        self.assertIn("3", entity._state)
        self.assertEqual(write_state.call_count, 2)

    async def test_partial_report_without_switch_stays_unavailable(self) -> None:
        """Color-only data cannot establish a reliable light power state."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1, 4], {})
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant()
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"4": 200})

        self.assertFalse(entity.available)
        self.assertEqual(entity.brightness, 50)
        write_state.assert_called_once_with()

    async def test_partial_report_does_not_reuse_stale_switch_state(self) -> None:
        """A failed poll invalidates cached power before a color-only report."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1, 4], {"1": 255, "4": 400})
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()
        client.state = {}
        await entity.async_update()
        self.assertFalse(entity.available)

        with patch.object(entity, "async_write_ha_state"):
            client.report({"4": 200})
            await asyncio.sleep(0)

        self.assertFalse(entity.available)


class LightPollingConcurrencyTest(unittest.IsolatedAsyncioTestCase):
    """Verify polling and active reports use their intended threads."""

    async def test_control_applies_authoritative_light_reply(self) -> None:
        """Light fields come from the control reply, not the request payload."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = TimestampedLightClient(
            [1, 4],
            {"1": 0, "4": 400},
            {"1": 0, "4": 200},
        )
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state") as write_state:
            await entity.async_turn_on(**{light.ATTR_BRIGHTNESS: 128})
            await asyncio.sleep(0)

        self.assertFalse(entity.is_on)
        self.assertEqual(entity.brightness, 50)
        write_state.assert_called_once_with()

    async def test_async_control_applies_state_on_event_loop(self) -> None:
        """Only the blocking control call runs outside the event loop."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1, 4], {"1": 0, "4": 100})
        entity = light.CozyLifeLight(client)
        event_loop_thread = threading.get_ident()
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()
        control_threads = []
        apply_threads = []
        control = client.control
        apply_incremental_state = entity._apply_incremental_state

        def record_control(payload):
            control_threads.append(threading.get_ident())
            return control(payload)

        def record_state_application(state):
            apply_threads.append(threading.get_ident())
            return apply_incremental_state(state)

        with patch.object(
            client, "control", side_effect=record_control
        ), patch.object(
            entity,
            "_apply_incremental_state",
            side_effect=record_state_application,
        ), patch.object(entity, "async_write_ha_state"):
            await entity.async_turn_on(**{light.ATTR_BRIGHTNESS: 128})

        self.assertEqual(len(control_threads), 1)
        self.assertNotIn(event_loop_thread, control_threads)
        self.assertEqual(apply_threads, [event_loop_thread])

    async def test_queued_older_light_state_is_discarded(self) -> None:
        """A pending light event cannot overwrite a newer device timestamp."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1], {"1": 0})
        loop = QueuedLoop()
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(loop)
        await entity.async_added_to_hass()

        client.report({"1": 255}, 1700000000000)
        client.last_state_sequence_number = 1700000000001
        with patch.object(entity, "async_write_ha_state") as write_state:
            loop.drain()

        self.assertFalse(entity.is_on)
        write_state.assert_not_called()

    async def test_query_response_recovers_unavailable_light(self) -> None:
        """A timestamped query response restores an unavailable light."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1, 4], {})
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()
        client.state = {"1": 255, "4": 400}
        with patch.object(entity, "async_write_ha_state"):
            await entity.async_update()

        self.assertTrue(entity.available)
        self.assertTrue(entity.is_on)
        self.assertEqual(entity.brightness, 100)

    async def test_partial_query_preserves_unreported_light_properties(
        self,
    ) -> None:
        """State events merge fields that share the device timeline."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        cases = (
            {"1": 255, "3": 300, "4": 400, "5": 65535, "6": 65535},
            {"1": 255, "3": 65535, "4": 400, "5": 240, "6": 750},
        )

        for initial_state in cases:
            with self.subTest(initial_state=initial_state):
                client = FakeTcpClient(
                    [1, 3, 4, 5, 6], initial_state
                )
                entity = light.CozyLifeLight(client)
                entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
                await entity.async_added_to_hass()
                try:
                    client.state = {"1": 255}
                    await entity.async_update()

                    expected_state = initial_state.copy()
                    self.assertEqual(entity._state, expected_state)
                    self.assertEqual(entity.brightness, 100)
                finally:
                    await entity.async_will_remove_from_hass()


class LightWorkModeTest(unittest.IsolatedAsyncioTestCase):
    """Verify work mode changes only accompany supported static controls."""

    async def test_plain_turn_on_preserves_work_mode(self) -> None:
        """A plain turn-on command does not force the light into static mode."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1, 2, 4], {"1": 0, "4": 0})
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        await entity.async_turn_on()

        self.assertEqual(client.last_payload, {"1": 255})

    async def test_brightness_control_sets_supported_work_mode(self) -> None:
        """A brightness command selects static mode when DPID 2 is supported."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1, 2, 4], {"1": 0, "4": 0})
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        await entity.async_turn_on(**{light.ATTR_BRIGHTNESS: 128})

        self.assertEqual(
            client.last_payload, {"1": 255, "2": 0, "4": 512}
        )

    async def test_static_control_omits_unsupported_work_mode(self) -> None:
        """A light without DPID 2 receives only its supported static values."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient([1, 4], {"1": 0, "4": 0})
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        await entity.async_turn_on(**{light.ATTR_BRIGHTNESS: 128})

        self.assertEqual(client.last_payload, {"1": 255, "4": 512})


class LightColorTemperatureTest(unittest.IsolatedAsyncioTestCase):
    """Verify color temperatures cross the device boundary in correct units."""

    def test_device_temperature_is_reported_in_kelvin(self) -> None:
        """A device temperature value is exposed to Home Assistant in Kelvin."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        entity = light.CozyLifeLight(
            FakeTcpClient([1, 3, 4], {"1": 255, "3": 300, "4": 400})
        )

        self.assertEqual(entity.color_temp_kelvin, 2857)

    async def test_kelvin_control_is_converted_to_device_value(self) -> None:
        """A Kelvin service value is converted to the device temperature value."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 2, 3, 4], {"1": 255, "3": 300, "4": 400}
        )
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state"):
            await entity.async_turn_on(
                **{light.ATTR_COLOR_TEMP_KELVIN: 4000}
            )

        self.assertEqual(client.last_payload, {"1": 255, "2": 0, "3": 500})
        self.assertEqual(entity.color_temp_kelvin, 4000)

    async def test_kelvin_control_rounds_device_value_to_integer(self) -> None:
        """A non-even Kelvin conversion sends an integer device value."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 2, 3, 4], {"1": 255, "3": 300, "4": 400}
        )
        entity = light.CozyLifeLight(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        await entity.async_turn_on(**{light.ATTR_COLOR_TEMP_KELVIN: 3000})

        self.assertEqual(client.last_payload, {"1": 255, "2": 0, "3": 334})
        self.assertIsInstance(client.last_payload["3"], int)

    def test_invalid_device_temperature_is_reported_as_unknown(self) -> None:
        """A non-positive mired value does not break entity state refresh."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )

        try:
            entity = light.CozyLifeLight(
                FakeTcpClient([1, 3, 4], {"1": 0, "3": 1000, "4": 0})
            )
        except ZeroDivisionError as err:
            self.fail(f"Invalid device temperature caused division by zero: {err}")

        self.assertIsNone(entity.color_temp_kelvin)
        self.assertIsNone(entity.color_mode)
