"""Platform for sensor integration."""
from __future__ import annotations

from collections.abc import Callable
from homeassistant.components.sensor import SensorEntity
from homeassistant.components.switch import SwitchEntity
# from homeassistant.components.light import *
from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ATTR_COLOR_TEMP_KELVIN,
    ATTR_EFFECT,
    ATTR_FLASH,
    ATTR_HS_COLOR,
    ATTR_RGB_COLOR,
    ATTR_TRANSITION,
    FLASH_LONG,
    FLASH_SHORT,
    ColorMode,
    LightEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.util import color as color_util
from typing import Any, Final, Literal, TypedDict, final
from .const import (
    SWITCH_TYPE_CODE,
    LIGHT_TYPE_CODE,
    LIGHT_DPID,
    SWITCH,
    WORK_MODE,
    TEMP,
    BRIGHT,
    HUE,
    SAT,
)
from .device import CozyLifeDevice
from .tcp_client import DeviceCommandRejectedError
from . import register_device_callback
import logging

_LOGGER = logging.getLogger(__name__)
_LOGGER.info(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Add lights for existing and subsequently registered devices."""
    def add_device(device: CozyLifeDevice) -> None:
        if LIGHT_TYPE_CODE == device.device_type_code:
            entity = CozyLifeLight(device)
            hass.loop.call_soon_threadsafe(async_add_entities, [entity])

    register_device_callback(entry.runtime_data, add_device)


class CozyLifeLight(LightEntity):
    # _attr_brightness: int | None = None
    # _attr_color_mode: str | None = None
    # _attr_color_temp_kelvin: int | None = None
    # _attr_hs_color = None
    _device: CozyLifeDevice
    
    _attr_supported_color_modes: set[ColorMode]
    _attr_color_mode: ColorMode | None
    
    # _unique_id = str
    # _attr_is_on = True
    # _name = str
    # _attr_brightness = int
    # _attr_color_temp_kelvin = int
    # _attr_hs_color = (float, float)
    
    def __init__(self, device: CozyLifeDevice) -> None:
        """Initialize color capabilities independently for this light."""
        _LOGGER.info('__init__')
        self._device = device
        self._attr_device_info = device.device_info
        self._state: dict[str, int] = {}
        self._attr_available = False
        self._state_updates_active = False
        self._initial_query_complete = False
        self._remove_state_callback: Callable[[], None] | None = None
        self._unique_id = device.device_id
        self._name = device.device_model_name + ' ' + device.device_id[-4:]
        self._attr_supported_color_modes = set()
        
        _LOGGER.info(f'before:{self._unique_id}._attr_color_mode={self._attr_color_mode}._attr_supported_color_modes='
                     f'{self._attr_supported_color_modes}.dpid={device.dpid}')
        # h s
        if 3 in device.dpid:
            self._attr_color_mode = ColorMode.COLOR_TEMP
            self._attr_supported_color_modes.add(ColorMode.COLOR_TEMP)
        
        if 5 in device.dpid and 6 in device.dpid:
            self._attr_color_mode = ColorMode.HS
            self._attr_supported_color_modes.add(ColorMode.HS)

        if not self._attr_supported_color_modes:
            self._attr_color_mode = (
                ColorMode.BRIGHTNESS if 4 in device.dpid else ColorMode.ONOFF
            )
            self._attr_supported_color_modes.add(self._attr_color_mode)
        
        _LOGGER.info(f'after:{self._unique_id}._attr_color_mode={self._attr_color_mode}._attr_supported_color_modes='
                     f'{self._attr_supported_color_modes}.dpid={device.dpid}')
        

    def _refresh_state(self) -> None:
        """Refresh state, marking the light unavailable without switch data."""
        state = self._device.query()
        _LOGGER.info(f'_state={state}')
        self._apply_polled_state(state)

    def _apply_polled_state(self, state: dict[str, int]) -> None:
        """Apply one complete query result to the light entity."""
        if SWITCH not in state:
            self._state.pop(SWITCH, None)
            self._attr_available = False
            return

        self._state = state
        self._attr_available = True
        self._apply_device_state()

    async def async_update(self) -> None:
        """Query in a worker and apply the result on the event loop."""
        sequence_number_before_query = (
            self._device.last_state_sequence_number
        )
        state = await self.hass.async_add_executor_job(
            self._device.query
        )
        if not self._state_updates_active:
            return
        if (
            SWITCH not in state
            and self._device.last_state_sequence_number
            == sequence_number_before_query
        ):
            self._state.pop(SWITCH, None)
            self._attr_available = False

    def _merge_device_state(self, state: dict[str, int]) -> None:
        """Merge changed DPID values while preserving unreported properties."""
        self._state.update(state)

    def _apply_incremental_state(self, state: dict[str, int]) -> None:
        """Record one newer state source and update mapped entity fields."""
        self._merge_device_state(state)
        self._apply_device_state()

    def _apply_device_state(self) -> None:
        """Map the cached device properties to Home Assistant light fields."""
        if SWITCH in self._state:
            self._attr_is_on = 0 < self._state[SWITCH]

        self._attr_brightness = None
        if BRIGHT in self._state:
            self._attr_brightness = int(self._state[BRIGHT] / 4)

        has_hs_color = (
            ColorMode.HS in self._attr_supported_color_modes
            and HUE in self._state
            and SAT in self._state
            and self._state[HUE] != 65535
            and self._state[SAT] != 65535
        )
        self._attr_hs_color = None
        if has_hs_color:
            self._attr_hs_color = (
                int(self._state[HUE]),
                int(self._state[SAT] / 10),
            )

        has_color_temperature = (
            ColorMode.COLOR_TEMP in self._attr_supported_color_modes
            and TEMP in self._state
            and self._state[TEMP] != 65535
        )
        self._attr_color_temp_kelvin = None
        if has_color_temperature:
            color_temp_mired = 500 - int(self._state[TEMP] / 2)
            if color_temp_mired > 0:
                self._attr_color_temp_kelvin = (
                    color_util.color_temperature_mired_to_kelvin(color_temp_mired)
                )

        color_modes = self._attr_supported_color_modes & {
            ColorMode.COLOR_TEMP,
            ColorMode.HS,
        }
        # Home Assistant requires an on light to report one declared color mode.
        if has_hs_color:
            self._attr_color_mode = ColorMode.HS
        elif self._attr_color_temp_kelvin is not None:
            self._attr_color_mode = ColorMode.COLOR_TEMP
        elif self._attr_is_on and self._attr_color_mode is None and color_modes:
            self._attr_color_mode = (
                ColorMode.HS
                if ColorMode.HS in color_modes
                else ColorMode.COLOR_TEMP
            )
        elif not self._attr_is_on and color_modes:
            self._attr_color_mode = None

    def _handle_state_report(
        self, state: dict, sequence_number: int
    ) -> None:
        """Move device state messages from the network thread to the event loop."""
        relevant_state = {
            key: value for key, value in state.items() if key in LIGHT_DPID
        }
        if relevant_state and self.hass is not None:
            self.hass.loop.call_soon_threadsafe(
                self._apply_state_report,
                relevant_state,
                sequence_number,
            )

    def _apply_state_report(
        self,
        state: dict[str, int],
        sequence_number: int,
    ) -> None:
        """Merge one state message and publish all light fields together."""
        if not self._state_updates_active:
            return
        latest_sequence_number = self._device.last_state_sequence_number
        if (
            latest_sequence_number is not None
            and sequence_number < latest_sequence_number
        ):
            return
        self._apply_incremental_state(state)
        self._attr_available = SWITCH in self._state
        if self._initial_query_complete:
            self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        """Subscribe after Home Assistant can safely receive state writes."""
        await super().async_added_to_hass()
        self._state_updates_active = True
        self._remove_state_callback = self._device.add_state_callback(
            self._handle_state_report
        )
        await self.async_update()
        if self._state_updates_active:
            self._initial_query_complete = True

    async def async_will_remove_from_hass(self) -> None:
        """Stop transport callbacks before Home Assistant removes the entity."""
        self._state_updates_active = False
        if self._remove_state_callback is not None:
            self._remove_state_callback()
            self._remove_state_callback = None
        await super().async_will_remove_from_hass()

    def update(self) -> None:
        """Poll the device so an unavailable light can recover."""
        self._refresh_state()
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def available(self) -> bool:
        """Return if the device is available."""
        return self._attr_available
    
    @property
    def is_on(self) -> bool:
        """Return True if entity is on."""
        return self._attr_is_on
    
    @property
    def color_temp_kelvin(self) -> int | None:
        """Return the color temperature in Kelvin."""
        return self._attr_color_temp_kelvin
    
    @property
    def unique_id(self) -> str | None:
        """Return a unique ID."""
        return self._unique_id

    async def _async_control(self, payload: dict[str, int]) -> bool:
        """Run device input/output in a worker and own state on the event loop."""
        try:
            control_succeeded = await self.hass.async_add_executor_job(
                self._device.control,
                payload,
            )
        except DeviceCommandRejectedError as err:
            raise HomeAssistantError(
                "CozyLife device rejected command"
            ) from err

        if not control_succeeded:
            self._attr_available = False
            self.async_write_ha_state()
            raise HomeAssistantError(
                "Unable to send command to CozyLife device"
            )

        return True

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn on the light without changing state from a worker thread."""
        brightness = kwargs.get(ATTR_BRIGHTNESS)
        # The existing device mapping uses 153..500 mired.
        color_temp_kelvin = kwargs.get(ATTR_COLOR_TEMP_KELVIN)
        # tuple
        hs_color = kwargs.get(ATTR_HS_COLOR)
        rgb = kwargs.get(ATTR_RGB_COLOR)
        flash = kwargs.get(ATTR_FLASH)
        effect = kwargs.get(ATTR_EFFECT)
        _LOGGER.info(f'turn_on.kwargs={kwargs}')
        
        payload = {'1': 255}
        static_control_requested = any(
            value is not None
            for value in (brightness, color_temp_kelvin, hs_color)
        )
        # Preserve the current effect on plain turn-on and unsupported models.
        if 2 in self._device.dpid and static_control_requested:
            payload['2'] = 0
        if brightness is not None:
            payload['4'] = brightness * 4
        
        if hs_color is not None:
            payload['5'] = int(hs_color[0])
            payload['6'] = int(hs_color[1] * 10)
        
        if color_temp_kelvin is not None:
            color_temp_mired = color_util.color_temperature_kelvin_to_mired(
                color_temp_kelvin
            )
            payload['3'] = 1000 - color_temp_mired * 2
        
        await self._async_control(payload)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off the light without changing state from a worker thread."""
        _LOGGER.info(f'turn_off.kwargs={kwargs}')
        await self._async_control({SWITCH: 0})
    
    @property
    def hs_color(self) -> tuple[float, float] | None:
        """Return the hue and saturation color value [float, float]."""
        _LOGGER.info('hs_color')
        return self._attr_hs_color
    
    @property
    def brightness(self) -> int | None:
        """Return the brightness of this light between 0..255."""
        _LOGGER.info('brightness')
        return self._attr_brightness
    
    @property
    def color_mode(self) -> ColorMode | None:
        """Return the color mode of the light."""
        _LOGGER.info('color_mode')
        return self._attr_color_mode
    
    # def set_brightness(self, b):
    #     _LOGGER.info('set_brightness')
    #
    #     self._attr_brightness = b
    #
    # def set_hs(self, hs_color, duration) -> None:
    #     """Set bulb's color."""
    #     _LOGGER.info('set_hs')
    #     self._attr_hs_color = (hs_color[0], hs_color[1])
