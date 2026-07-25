"""Platform for sensor integration."""
from __future__ import annotations

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
        self._unique_id = tcp_client.device_id
        self._name = tcp_client.device_model_name + ' ' + tcp_client.device_id[-4:]
        self._refresh_state()
    
    def _refresh_state(self) -> None:
        """Refresh state, marking the switch unavailable without switch data."""
        self._state = self._tcp_client.query()
        if '1' not in self._state:
            self._attr_available = False
            return

        self._attr_available = True
        self._attr_is_on = 0 != self._state['1']

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
    
    def turn_on(self, **kwargs: Any) -> None:
        """Turn the entity on."""
        _LOGGER.info(f'turn_on:{kwargs}')
        try:
            control_succeeded = self._tcp_client.control({'1': 255})
        except DeviceCommandRejectedError as err:
            raise HomeAssistantError(
                "CozyLife device rejected command"
            ) from err

        if not control_succeeded:
            self._attr_available = False
            self.schedule_update_ha_state()
            raise HomeAssistantError("Unable to send command to CozyLife device")
        self._attr_available = True
        self._attr_is_on = True
        return None
        raise NotImplementedError()
    
    def turn_off(self, **kwargs: Any) -> None:
        """Turn the entity off."""
        _LOGGER.info('turn_off')
        try:
            control_succeeded = self._tcp_client.control({'1': 0})
        except DeviceCommandRejectedError as err:
            raise HomeAssistantError(
                "CozyLife device rejected command"
            ) from err

        if not control_succeeded:
            self._attr_available = False
            self.schedule_update_ha_state()
            raise HomeAssistantError("Unable to send command to CozyLife device")
        self._attr_available = True
        self._attr_is_on = False
        return None
        
        raise NotImplementedError()
