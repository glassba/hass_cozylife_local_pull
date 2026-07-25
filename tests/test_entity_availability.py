"""Regression tests for entity availability after device query failures."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from custom_components.hass_cozylife_local_pull.light import CozyLifeLight
from custom_components.hass_cozylife_local_pull.switch import CozyLifeSwitch
from custom_components.hass_cozylife_local_pull.tcp_client import (
    DeviceCommandRejectedError,
)
from homeassistant.exceptions import HomeAssistantError


class SequencedDeviceClient:
    """Return configured device states in query order."""

    device_id = "device-1234"
    device_model_name = "Test Device"
    dpid = [1, 4]

    def __init__(
        self, states: list[dict[str, int]], control_result: bool = True
    ) -> None:
        self._states = states.copy()
        self._control_result = control_result
        self.query_count = 0

    def query(self) -> dict[str, int]:
        """Return the next configured query response."""
        self.query_count += 1
        return self._states.pop(0)

    def control(self, payload: dict[str, int]) -> bool:
        """Return the configured control result."""
        return self._control_result


class EntityAvailabilityTest(unittest.TestCase):
    """Verify empty query responses mark entities unavailable without errors."""

    def test_light_recovers_after_empty_query_response(self) -> None:
        """A light preserves its last state across availability changes."""
        valid_state = {"1": 255, "3": 300, "4": 400, "5": 120, "6": 500}
        client = SequencedDeviceClient([{}, valid_state, {}])

        try:
            entity = CozyLifeLight(client)
        except KeyError as err:
            self.fail(f"Empty light state raised an exception: {err}")
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
        entity = CozyLifeLight(client)

        self.assertTrue(entity.is_on)
        self.assertEqual(entity.brightness, 100)
        self.assertEqual(entity.hs_color, (120, 50))
        self.assertEqual(client.query_count, 1)

    def test_switch_preserves_off_state_after_empty_query(self) -> None:
        """An empty response does not change the last valid switch state."""
        client = SequencedDeviceClient([{"1": 0}, {}])
        entity = CozyLifeSwitch(client)

        entity.update()
        self.assertFalse(entity.is_on)
        self.assertFalse(entity.available)

    def test_control_rejection_preserves_entity_availability(self) -> None:
        """A device rejection reports failure without marking it offline."""
        cases = (
            (CozyLifeLight, {"1": 0, "4": 0}, "turn_on"),
            (CozyLifeLight, {"1": 255, "4": 400}, "turn_off"),
            (CozyLifeSwitch, {"1": 0}, "turn_on"),
            (CozyLifeSwitch, {"1": 255}, "turn_off"),
        )

        for entity_type, state, command_name in cases:
            with self.subTest(entity_type=entity_type, command=command_name):
                client = SequencedDeviceClient([state.copy()])
                entity = entity_type(client)
                previous_is_on = entity.is_on
                error = None

                with patch.object(
                    client,
                    "control",
                    side_effect=DeviceCommandRejectedError(
                        "Device rejected command with result 1"
                    ),
                ), patch.object(
                    entity, "schedule_update_ha_state"
                ) as schedule:
                    try:
                        getattr(entity, command_name)()
                    except Exception as err:
                        error = err

                self.assertIsInstance(error, HomeAssistantError)
                self.assertIn("rejected", str(error).lower())
                self.assertTrue(entity.available)
                self.assertEqual(entity.is_on, previous_is_on)
                schedule.assert_not_called()

    def test_control_failure_marks_entities_unavailable(self) -> None:
        """Failed light and switch commands report a Home Assistant error."""
        cases = (
            (CozyLifeLight, {"1": 0, "4": 0}, "turn_on"),
            (CozyLifeLight, {"1": 255, "4": 400}, "turn_off"),
            (CozyLifeSwitch, {"1": 0}, "turn_on"),
            (CozyLifeSwitch, {"1": 255}, "turn_off"),
        )

        for entity_type, state, command_name in cases:
            with self.subTest(entity_type=entity_type, command=command_name):
                client = SequencedDeviceClient(
                    [state.copy(), state.copy()], control_result=False
                )
                entity = entity_type(client)
                previous_is_on = entity.is_on

                with patch.object(entity, "schedule_update_ha_state") as schedule:
                    with self.assertRaises(HomeAssistantError):
                        getattr(entity, command_name)()

                self.assertFalse(entity.available)
                self.assertEqual(entity.is_on, previous_is_on)
                schedule.assert_called_once_with()

                entity.update()
                self.assertTrue(entity.available)
                self.assertEqual(entity.is_on, state["1"] != 0)

    def test_successful_control_recovers_entity_availability(self) -> None:
        """A successful retry makes a light or switch immediately available."""
        cases = (
            (CozyLifeLight, {"1": 0, "4": 0}, "turn_on"),
            (CozyLifeLight, {"1": 255, "4": 400}, "turn_off"),
            (CozyLifeSwitch, {"1": 0}, "turn_on"),
            (CozyLifeSwitch, {"1": 255}, "turn_off"),
        )

        for entity_type, state, command_name in cases:
            with self.subTest(entity_type=entity_type, command=command_name):
                client = SequencedDeviceClient(
                    [state.copy()], control_result=False
                )
                entity = entity_type(client)

                with patch.object(entity, "schedule_update_ha_state"):
                    with self.assertRaises(HomeAssistantError):
                        getattr(entity, command_name)()
                self.assertFalse(entity.available)

                client._control_result = True
                getattr(entity, command_name)()

                self.assertTrue(entity.available)
                self.assertEqual(client.query_count, 1)
