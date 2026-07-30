"""Regression tests for entity availability after device query failures."""

from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import patch

from custom_components.hass_cozylife_local_pull.const import DOMAIN
from custom_components.hass_cozylife_local_pull.light import CozyLifeLight
from custom_components.hass_cozylife_local_pull.motor import (
    CozyLifeMotorSwitch,
)
from custom_components.hass_cozylife_local_pull.switch import CozyLifeSwitch
from custom_components.hass_cozylife_local_pull.tcp_client import (
    DeviceCommandRejectedError,
)
from homeassistant.exceptions import HomeAssistantError


class SequencedDeviceClient:
    """Return configured device states in query order."""

    device_id = "device-1234"
    device_model_name = "Test Device"
    device_info = {
        "identifiers": {(DOMAIN, device_id)},
        "manufacturer": "CozyLife",
        "model": device_model_name,
        "name": "Test Device 1234",
    }
    dpid = [1, 4]

    def __init__(
        self, states: list[dict[str, int]], control_result: bool = True
    ) -> None:
        self._states = states.copy()
        self._control_result = control_result
        self.query_count = 0
        self.last_payload: dict[str, int] | None = None
        self.state_callbacks = []
        self.last_state_sequence_number = 1699999999999

    def query(self) -> dict[str, int]:
        """Return the next configured query response."""
        self.query_count += 1
        state = self._states.pop(0) if len(self._states) > 1 else self._states[0]
        if state and self.state_callbacks:
            self.report(state)
        return state

    def control(self, payload: dict[str, int]) -> bool:
        """Return the configured control result."""
        self.last_payload = payload.copy()
        if self._control_result:
            self.report(payload)
        return self._control_result

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


class TimestampedReplyClient(SequencedDeviceClient):
    """Publish timestamped device replies for asynchronous entity tests."""

    def __init__(
        self,
        states: list[dict[str, int]],
        control_reply: dict[str, int],
    ) -> None:
        super().__init__(states)
        self.control_reply = control_reply

    def publish(self, state: dict[str, int], sequence_number: int) -> None:
        """Deliver one validated device state to subscribed entities."""
        self.report(state, sequence_number)

    def control(self, payload: dict[str, int]) -> bool:
        """Publish the device acknowledgement instead of the request payload."""
        self.publish(self.control_reply, 1700000000000)
        return True


class NewerReportDuringQueryClient(SequencedDeviceClient):
    """Publish a newer report before returning an older query snapshot."""

    def __init__(
        self,
        older_state: dict[str, int],
        newer_state: dict[str, int],
    ) -> None:
        super().__init__([older_state])
        self._older_state = older_state
        self._newer_state = newer_state

    def query(self) -> dict[str, int]:
        """Return the older snapshot after both timestamped replies are queued."""
        self.query_count += 1
        self.report(self._older_state, 1700000000000)
        self.report(self._newer_state, 1700000000001)
        return self._older_state


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
    """Provide the event-loop boundary used by switch entities."""

    def __init__(self, loop=None) -> None:
        self.loop = loop or ImmediateLoop()

    async def async_add_executor_job(self, target, *args):
        """Run one blocking device operation in a worker thread."""
        result = await asyncio.to_thread(target, *args)
        await asyncio.sleep(0)
        return result


class EntityAvailabilityTest(unittest.IsolatedAsyncioTestCase):
    """Verify empty query responses mark entities unavailable without errors."""

    def test_light_recovers_after_empty_query_response(self) -> None:
        """A light preserves its last state across availability changes."""
        valid_state = {"1": 255, "3": 300, "4": 400, "5": 120, "6": 500}
        client = SequencedDeviceClient([{}, valid_state, {}])

        try:
            entity = CozyLifeLight(client)
        except KeyError as err:
            self.fail(f"Empty light state raised an exception: {err}")
        entity.update()
        self.assertFalse(entity.available)

        self.assertTrue(entity.should_poll, "Light must enable polling")
        self.assertTrue(hasattr(entity, "update"), "Light must support polling")
        entity.update()
        self.assertTrue(entity.is_on)
        self.assertTrue(entity.available)
        cached_state = (
            entity.brightness,
            entity.hs_color,
            entity.color_temp_kelvin,
        )

        entity.update()
        self.assertTrue(entity.is_on)
        self.assertFalse(entity.available)
        self.assertEqual(
            (entity.brightness, entity.hs_color, entity.color_temp_kelvin),
            cached_state,
        )

    def test_switch_recovers_after_empty_query_response(self) -> None:
        """A switch preserves its last state across availability changes."""
        client = SequencedDeviceClient([{}, {"1": 255}, {}])

        try:
            entity = CozyLifeSwitch(client)
        except KeyError as err:
            self.fail(f"Empty switch state raised an exception: {err}")
        entity.update()
        self.assertFalse(entity.available)

        self.assertTrue(entity.should_poll, "Switch must enable polling")
        self.assertTrue(hasattr(entity, "update"), "Switch must support polling")
        entity.update()
        self.assertTrue(entity.is_on)
        self.assertTrue(entity.available)

        entity.update()
        self.assertTrue(entity.is_on)
        self.assertFalse(entity.available)

    def test_light_state_properties_do_not_query_device(self) -> None:
        """Reading cached light state does not perform network input or output."""
        state = {"1": 255, "4": 400, "5": 120, "6": 500}
        client = SequencedDeviceClient([state.copy()] * 4)
        client.dpid = [1, 4, 5, 6]
        entity = CozyLifeLight(client)
        entity.update()

        self.assertTrue(entity.is_on)
        self.assertEqual(entity.brightness, 100)
        self.assertEqual(entity.hs_color, (120, 50))
        self.assertEqual(client.query_count, 1)

    def test_switch_preserves_off_state_after_empty_query(self) -> None:
        """An empty response does not change the last valid switch state."""
        client = SequencedDeviceClient([{"1": 0}, {}])
        entity = CozyLifeSwitch(client)

        entity.update()
        entity.update()
        self.assertFalse(entity.is_on)
        self.assertFalse(entity.available)

    async def test_switch_variants_use_their_protocol_on_values(self) -> None:
        """Ordinary switches and motors send their own DPID 1 on value."""
        cases = (
            (CozyLifeSwitch, 255),
            (CozyLifeMotorSwitch, 1),
        )

        for entity_type, expected_on_value in cases:
            with self.subTest(entity_type=entity_type):
                client = SequencedDeviceClient([{"1": 0}])
                entity = entity_type(client)
                entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

                await entity.async_turn_on()
                self.assertEqual(
                    client.last_payload, {"1": expected_on_value}
                )

                await entity.async_turn_off()
                self.assertEqual(client.last_payload, {"1": 0})

    async def test_motor_switch_maps_nonzero_reports_to_started(self) -> None:
        """DPID 1 values 1 through 3 all represent a non-stopped motor."""
        client = SequencedDeviceClient([{"1": 0}])
        entity = CozyLifeMotorSwitch(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state") as write_state:
            for value in (1, 2, 3):
                client.report({"1": value})
                await asyncio.sleep(0)
                self.assertTrue(entity.is_on)

            client.report({"1": 0})
            await asyncio.sleep(0)

        self.assertFalse(entity.is_on)
        self.assertEqual(write_state.call_count, 4)

    async def test_control_rejection_preserves_entity_availability(self) -> None:
        """A device rejection reports failure without marking it offline."""
        cases = (
            (CozyLifeLight, {"1": 0, "4": 0}, "async_turn_on"),
            (CozyLifeLight, {"1": 255, "4": 400}, "async_turn_off"),
            (CozyLifeSwitch, {"1": 0}, "async_turn_on"),
            (CozyLifeSwitch, {"1": 255}, "async_turn_off"),
        )

        for entity_type, state, command_name in cases:
            with self.subTest(entity_type=entity_type, command=command_name):
                client = SequencedDeviceClient([state.copy()])
                entity = entity_type(client)
                entity.update()
                entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
                previous_is_on = entity.is_on
                error = None

                with patch.object(
                    client,
                    "control",
                    side_effect=DeviceCommandRejectedError(
                        "Device rejected command with result 1"
                    ),
                ), patch.object(entity, "async_write_ha_state") as write_state:
                    try:
                        await getattr(entity, command_name)()
                    except Exception as err:
                        error = err

                self.assertIsInstance(error, HomeAssistantError)
                self.assertIn("rejected", str(error).lower())
                self.assertTrue(entity.available)
                self.assertEqual(entity.is_on, previous_is_on)
                write_state.assert_not_called()

    async def test_switch_control_uses_device_reply_for_user_interface(
        self,
    ) -> None:
        """A control request cannot optimistically replace the device reply."""
        client = TimestampedReplyClient([{"1": 0}], {"1": 0})
        entity = CozyLifeSwitch(client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()

        with patch.object(
            entity, "async_write_ha_state"
        ) as write_state:
            await entity.async_turn_on()
            await asyncio.sleep(0)

        self.assertFalse(entity.is_on)
        self.assertTrue(entity.available)
        write_state.assert_called_once_with()

    async def test_queued_older_switch_state_is_not_applied(self) -> None:
        """A pending event is discarded after a newer timestamp is recorded."""
        client = TimestampedReplyClient([{"1": 0}], {"1": 0})
        loop = QueuedLoop()
        entity = CozyLifeSwitch(client)
        entity.hass = FakeHomeAssistant(loop)
        await entity.async_added_to_hass()
        with patch.object(entity, "async_write_ha_state"):
            loop.drain()

        client.publish({"1": 255}, 1700000000000)
        client.last_state_sequence_number = 1700000000001
        with patch.object(entity, "async_write_ha_state") as write_state:
            loop.drain()

        self.assertFalse(entity.is_on)
        write_state.assert_not_called()

    async def test_newer_report_wins_over_completed_query(self) -> None:
        """An older query snapshot cannot replace a newer device report."""
        cases = (
            (
                CozyLifeSwitch,
                {"1": 0},
                {"1": 255},
                None,
            ),
            (
                CozyLifeLight,
                {"1": 0, "4": 0},
                {"1": 255, "4": 400},
                100,
            ),
        )

        for entity_type, older_state, newer_state, expected_brightness in cases:
            with self.subTest(entity_type=entity_type):
                client = NewerReportDuringQueryClient(
                    older_state, newer_state
                )
                entity = entity_type(client)
                entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

                try:
                    await entity.async_added_to_hass()

                    self.assertEqual(
                        client.last_state_sequence_number,
                        1700000000001,
                    )
                    self.assertTrue(entity.is_on)
                    self.assertTrue(entity.available)
                    if expected_brightness is not None:
                        self.assertEqual(entity.brightness, expected_brightness)
                finally:
                    await entity.async_will_remove_from_hass()

    async def test_unexpected_control_exception_allows_later_reports(
        self,
    ) -> None:
        """A runtime exception cannot leave entity state updates blocked."""
        cases = (
            (CozyLifeLight, {"1": 0, "4": 0}),
            (CozyLifeSwitch, {"1": 0}),
        )

        for entity_type, state in cases:
            with self.subTest(entity_type=entity_type):
                client = SequencedDeviceClient([state])
                entity = entity_type(client)
                entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
                await entity.async_added_to_hass()
                try:
                    with patch.object(
                        client,
                        "control",
                        side_effect=RuntimeError("Control executor failed"),
                    ), patch.object(
                        entity, "async_write_ha_state"
                    ) as write_state:
                        with self.assertRaisesRegex(
                            RuntimeError, "Control executor failed"
                        ):
                            await entity.async_turn_on()
                        await asyncio.to_thread(client.report, {"1": 255})
                        await asyncio.sleep(0)

                    self.assertTrue(entity.is_on)
                    self.assertTrue(entity.available)
                    write_state.assert_called_once_with()
                finally:
                    await entity.async_will_remove_from_hass()

    async def test_control_failure_marks_entities_unavailable(self) -> None:
        """Failed light and switch commands report a Home Assistant error."""
        cases = (
            (CozyLifeLight, {"1": 0, "4": 0}, "async_turn_on"),
            (CozyLifeLight, {"1": 255, "4": 400}, "async_turn_off"),
            (CozyLifeSwitch, {"1": 0}, "async_turn_on"),
            (CozyLifeSwitch, {"1": 255}, "async_turn_off"),
        )

        for entity_type, state, command_name in cases:
            with self.subTest(entity_type=entity_type, command=command_name):
                client = SequencedDeviceClient(
                    [state.copy(), state.copy()], control_result=False
                )
                entity = entity_type(client)
                entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
                previous_is_on = entity.is_on

                with patch.object(
                    entity, "async_write_ha_state"
                ) as write_state:
                    with self.assertRaises(HomeAssistantError):
                        await getattr(entity, command_name)()

                self.assertFalse(entity.available)
                self.assertEqual(entity.is_on, previous_is_on)
                write_state.assert_called_once_with()

                entity.update()
                self.assertTrue(entity.available)
                self.assertEqual(entity.is_on, state["1"] != 0)

    async def test_successful_control_recovers_entity_availability(self) -> None:
        """A successful retry makes a light or switch immediately available."""
        cases = (
            (CozyLifeLight, {"1": 0, "4": 0}, "async_turn_on"),
            (CozyLifeLight, {"1": 255, "4": 400}, "async_turn_off"),
            (CozyLifeSwitch, {"1": 0}, "async_turn_on"),
            (CozyLifeSwitch, {"1": 255}, "async_turn_off"),
        )

        for entity_type, state, command_name in cases:
            with self.subTest(entity_type=entity_type, command=command_name):
                client = SequencedDeviceClient(
                    [state.copy()], control_result=False
                )
                entity = entity_type(client)
                entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
                await entity.async_added_to_hass()

                with patch.object(entity, "async_write_ha_state"):
                    with self.assertRaises(HomeAssistantError):
                        await getattr(entity, command_name)()
                self.assertFalse(entity.available)

                client._control_result = True
                await getattr(entity, command_name)()

                self.assertTrue(entity.available)
                self.assertEqual(client.query_count, 1)


class SwitchActiveReportTest(unittest.IsolatedAsyncioTestCase):
    """Verify active device reports immediately refresh switch entities."""

    async def test_report_updates_switch_once_and_removal_unsubscribes(
        self,
    ) -> None:
        """DPID 1 updates the UI until Home Assistant removes the entity."""
        client = SequencedDeviceClient([{"1": 255}])
        entity = CozyLifeSwitch(client)
        event_loop_thread = threading.get_ident()
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()
        write_threads = []

        with patch.object(
            entity,
            "async_write_ha_state",
            side_effect=lambda: write_threads.append(threading.get_ident()),
        ) as write_state:
            await asyncio.to_thread(client.report, {"1": 0})
            await asyncio.sleep(0)

        self.assertFalse(entity.is_on)
        self.assertTrue(entity.available)
        self.assertEqual(write_threads, [event_loop_thread])
        write_state.assert_called_once_with()

        await entity.async_will_remove_from_hass()
        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"1": 255})

        self.assertFalse(entity.is_on)
        write_state.assert_not_called()

    async def test_report_recovers_switch_after_initial_empty_query(self) -> None:
        """The first DPID 1 report can initialize an unavailable switch."""
        client = SequencedDeviceClient([{}])
        entity = CozyLifeSwitch(client)
        entity.hass = FakeHomeAssistant()
        await entity.async_added_to_hass()

        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"1": 255})

        self.assertTrue(entity.available)
        self.assertTrue(entity.is_on)
        write_state.assert_called_once_with()

    async def test_subscription_query_recovers_switch_state_missed_before_add(
        self,
    ) -> None:
        """A query after subscribing restores a switch setup-time report."""
        client = SequencedDeviceClient([{"1": 255}])
        entity = CozyLifeSwitch(client)
        client.report({"1": 255}, 1700000000000)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        with patch.object(entity, "async_write_ha_state") as write_state:
            await entity.async_added_to_hass()

        self.assertEqual(client.query_count, 1)
        self.assertTrue(entity.is_on)
        self.assertTrue(entity.available)
        write_state.assert_not_called()
