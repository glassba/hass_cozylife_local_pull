"""Platform for sensor integration."""
from __future__ import annotations

from collections.abc import Callable
from homeassistant.components.sensor import SensorEntity
from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from typing import Any, Final, Literal, TypedDict, final
from .const import (
    MOTOR_TYPE_CODE,
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
import logging
from . import register_device_callback

_LOGGER = logging.getLogger(__name__)
_LOGGER.info('switch')


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Add switches for existing and subsequently registered devices."""
    from .motor import CozyLifeMotorSwitch

    def add_device(device: CozyLifeDevice) -> None:
        entity = None
        if SWITCH_TYPE_CODE == device.device_type_code:
            entity = CozyLifeSwitch(device)
        elif (
            MOTOR_TYPE_CODE == device.device_type_code
            and int(SWITCH) in device.dpid
        ):
            entity = CozyLifeMotorSwitch(device)
        if entity is not None:
            hass.loop.call_soon_threadsafe(async_add_entities, [entity])

    register_device_callback(entry.runtime_data, add_device)


class CozyLifeSwitch(SwitchEntity):
    _device: CozyLifeDevice
    _attr_is_on = True
    _turn_on_value = 255
    
    def __init__(self, device: CozyLifeDevice) -> None:
        """Initialize the sensor."""
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

    def _refresh_state(self) -> None:
        """Refresh state, marking the switch unavailable without switch data."""
        self._apply_polled_state(self._device.query())

    def _apply_polled_state(self, state: dict[str, int]) -> None:
        """Apply one complete query result to the switch entity."""
        if SWITCH not in state:
            self._attr_available = False
            return

        self._state = state
        self._attr_available = True
        self._attr_is_on = 0 != self._state[SWITCH]

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
            self._attr_available = False

    def _handle_state_report(
        self, state: dict, sequence_number: int
    ) -> None:
        """Move DPID 1 state from the network thread to the event loop."""
        if SWITCH in state and self.hass is not None:
            self.hass.loop.call_soon_threadsafe(
                self._apply_state_report,
                state[SWITCH],
                sequence_number,
            )

    def _apply_state_report(
        self,
        value: int,
        sequence_number: int,
    ) -> None:
        """Publish one switch state message to Home Assistant."""
        if not self._state_updates_active:
            return
        latest_sequence_number = self._device.last_state_sequence_number
        if (
            latest_sequence_number is not None
            and sequence_number < latest_sequence_number
        ):
            return
        self._state[SWITCH] = value
        self._attr_is_on = value != 0
        self._attr_available = True
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
        """Poll the device so an unavailable switch can recover."""
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
    def unique_id(self) -> str | None:
        """Return a unique ID."""
        return self._unique_id

    async def _async_control(self, value: int) -> bool:
        """Run device input/output in a worker and own state on the event loop."""
        try:
            control_succeeded = await self.hass.async_add_executor_job(
                self._device.control,
                {SWITCH: value},
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
        """Turn on the switch without changing state from a worker thread."""
        _LOGGER.info(f'turn_on:{kwargs}')
        await self._async_control(self._turn_on_value)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off the switch without changing state from a worker thread."""
        _LOGGER.info('turn_off')
        await self._async_control(0)
