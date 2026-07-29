"""Platform for sensor integration."""
from __future__ import annotations

from collections.abc import Callable
from homeassistant.components.sensor import SensorEntity
from homeassistant.components.switch import SwitchEntity
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from typing import Any, Final, Literal, TypedDict, final
from .const import (
    DOMAIN,
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
from .tcp_client import DeviceCommandRejectedError
import logging
from . import register_client_callback

_LOGGER = logging.getLogger(__name__)
_LOGGER.info('switch')


def setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None
) -> None:
    """Set up the sensor platform."""
    # We only want this platform to be set up via discovery.
    # logging.info('setup_platform', hass, config, add_entities, discovery_info)
    _LOGGER.info('setup_platform')
    _LOGGER.info(f'ip={hass.data[DOMAIN]}')
    
    if discovery_info is None:
        return


    def add_ready_switch(item) -> None:
        """Add a switch when its device information becomes available."""
        if SWITCH_TYPE_CODE == item.device_type_code:
            add_entities([CozyLifeSwitch(item)])

    def register_client(item) -> None:
        """Attach the switch readiness callback to one network client."""
        item.add_ready_callback(add_ready_switch)

    register_client_callback(hass, register_client)


class CozyLifeSwitch(SwitchEntity):
    _tcp_client = None
    _attr_is_on = True
    
    def __init__(self, tcp_client) -> None:
        """Initialize the sensor."""
        _LOGGER.info('__init__')
        self._tcp_client = tcp_client
        self._state: dict[str, int] = {}
        self._state_updates_active = False
        self._initial_query_complete = False
        self._remove_state_callback: Callable[[], None] | None = None
        self._unique_id = tcp_client.device_id
        self._name = tcp_client.device_model_name + ' ' + tcp_client.device_id[-4:]
        self._refresh_state()
    
    def _refresh_state(self) -> None:
        """Refresh state, marking the switch unavailable without switch data."""
        self._apply_polled_state(self._tcp_client.query())

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
            self._tcp_client.last_state_sequence_number
        )
        state = await self.hass.async_add_executor_job(
            self._tcp_client.query
        )
        if not self._state_updates_active:
            return
        if (
            SWITCH not in state
            and self._tcp_client.last_state_sequence_number
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
        latest_sequence_number = self._tcp_client.last_state_sequence_number
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
        self._remove_state_callback = self._tcp_client.add_state_callback(
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
                self._tcp_client.control,
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
        await self._async_control(255)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off the switch without changing state from a worker thread."""
        _LOGGER.info('turn_off')
        await self._async_control(0)
