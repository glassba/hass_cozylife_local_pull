"""Regression tests for the CozyLife light platform compatibility layer."""

from __future__ import annotations

import importlib
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
        self.last_payload: dict[str, int] | None = None

    def query(self) -> dict[str, int]:
        """Return the configured device state."""
        return self.state.copy()

    def control(self, payload: dict[str, int]) -> bool:
        """Record the latest control payload."""
        self.last_payload = payload
        return self.control_result


class LightImportTest(unittest.TestCase):
    """Verify the light platform uses the current Home Assistant API."""

    def test_light_module_imports_with_current_home_assistant(self) -> None:
        """The light platform imports with the installed Home Assistant API."""
        try:
            importlib.import_module("custom_components.hass_cozylife_local_pull.light")
        except ImportError as err:
            self.fail(f"The light platform did not import: {err}")


class LightColorModeTest(unittest.TestCase):
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

    def test_color_commands_update_current_mode(self) -> None:
        """A dual-mode light reports the mode selected by each color command."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4, 5, 6],
            {"1": 255, "3": 300, "4": 400, "5": 120, "6": 500},
        )
        entity = light.CozyLifeLight(client)

        entity.turn_on(**{light.ATTR_COLOR_TEMP_KELVIN: 2857})
        self.assertEqual(entity.color_mode, light.ColorMode.COLOR_TEMP)

        entity.turn_on(**{light.ATTR_HS_COLOR: (240, 75)})
        self.assertEqual(entity.color_mode, light.ColorMode.HS)
        self.assertEqual(
            client.last_payload, {"1": 255, "2": 0, "5": 240, "6": 750}
        )
        self.assertEqual(entity.hs_color, (240, 75))

    def test_failed_color_command_preserves_previous_cache(self) -> None:
        """A rejected color command does not alter optimistic light state."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4, 5, 6],
            {"1": 255, "3": 300, "4": 400, "5": 120, "6": 500},
        )
        entity = light.CozyLifeLight(client)
        entity.turn_on(**{light.ATTR_COLOR_TEMP_KELVIN: 4000})
        previous_state = (
            entity.brightness,
            entity.hs_color,
            entity.color_temp_kelvin,
            entity.color_mode,
        )
        client.control_result = False

        with patch.object(entity, "schedule_update_ha_state"):
            with self.assertRaises(HomeAssistantError):
                entity.turn_on(
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

        entity.update()
        self.assertTrue(entity.available)
        self.assertEqual(entity.hs_color, (120, 50))
        self.assertEqual(entity.color_temp_kelvin, 2857)
        self.assertEqual(entity.color_mode, light.ColorMode.COLOR_TEMP)

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


class LightColorTemperatureTest(unittest.TestCase):
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

    def test_kelvin_control_is_converted_to_device_value(self) -> None:
        """A Kelvin service value is converted to the device temperature value."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4], {"1": 255, "3": 300, "4": 400}
        )
        entity = light.CozyLifeLight(client)

        entity.turn_on(**{light.ATTR_COLOR_TEMP_KELVIN: 4000})

        self.assertEqual(client.last_payload, {"1": 255, "2": 0, "3": 500})
        self.assertEqual(entity.color_temp_kelvin, 4000)

    def test_kelvin_control_rounds_device_value_to_integer(self) -> None:
        """A non-even Kelvin conversion sends an integer device value."""
        light = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.light"
        )
        client = FakeTcpClient(
            [1, 3, 4], {"1": 255, "3": 300, "4": 400}
        )
        entity = light.CozyLifeLight(client)

        entity.turn_on(**{light.ATTR_COLOR_TEMP_KELVIN: 3000})

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
