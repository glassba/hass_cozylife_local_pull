"""Platform for sensor integration."""
from __future__ import annotations

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
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.util import color as color_util
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
from .tcp_client import DeviceCommandRejectedError, tcp_client
from . import register_client_callback
import logging
from homeassistant.components import zeroconf

_LOGGER = logging.getLogger(__name__)
_LOGGER.info(__name__)

def setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None
) -> None:
    """Set up the sensor platform."""
    # We only want this platform to be set up via discovery.
    _LOGGER.info(
        f'setup_platform.hass={hass},config={config},add_entities={add_entities},discovery_info={discovery_info}')
    # zc = await zeroconf.async_get_instance(hass)
    # _LOGGER.info(f'zc={zc}')
    _LOGGER.info(f'hass.data={hass.data[DOMAIN]}')
    _LOGGER.info(f'discovery_info={discovery_info}')

    if discovery_info is None:
        return
    
    def add_ready_light(item: tcp_client) -> None:
        """Add a light when its device information becomes available."""
        if LIGHT_TYPE_CODE == item.device_type_code:
            add_entities([CozyLifeLight(item)])

    def register_client(item: tcp_client) -> None:
        """Attach the light readiness callback to one network client."""
        item.add_ready_callback(add_ready_light)

    register_client_callback(hass, register_client)


class CozyLifeLight(LightEntity):
    # _attr_brightness: int | None = None
    # _attr_color_mode: str | None = None
    # _attr_color_temp_kelvin: int | None = None
    # _attr_hs_color = None
    _tcp_client = None
    
    _attr_supported_color_modes: set[ColorMode]
    _attr_color_mode: ColorMode
    
    # _unique_id = str
    # _attr_is_on = True
    # _name = str
    # _attr_brightness = int
    # _attr_color_temp_kelvin = int
    # _attr_hs_color = (float, float)
    
    def __init__(self, tcp_client: tcp_client) -> None:
        """Initialize color capabilities independently for this light."""
        _LOGGER.info('__init__')
        self._tcp_client = tcp_client
        self._unique_id = tcp_client.device_id
        self._name = tcp_client.device_model_name + ' ' + tcp_client.device_id[-4:]
        self._attr_supported_color_modes = set()
        
        _LOGGER.info(f'before:{self._unique_id}._attr_color_mode={self._attr_color_mode}._attr_supported_color_modes='
                     f'{self._attr_supported_color_modes}.dpid={tcp_client.dpid}')
        # h s
        if 3 in tcp_client.dpid:
            self._attr_color_mode = ColorMode.COLOR_TEMP
            self._attr_supported_color_modes.add(ColorMode.COLOR_TEMP)
        
        if 5 in tcp_client.dpid and 6 in tcp_client.dpid:
            self._attr_color_mode = ColorMode.HS
            self._attr_supported_color_modes.add(ColorMode.HS)

        if not self._attr_supported_color_modes:
            self._attr_color_mode = (
                ColorMode.BRIGHTNESS if 4 in tcp_client.dpid else ColorMode.ONOFF
            )
            self._attr_supported_color_modes.add(self._attr_color_mode)
        
        _LOGGER.info(f'after:{self._unique_id}._attr_color_mode={self._attr_color_mode}._attr_supported_color_modes='
                     f'{self._attr_supported_color_modes}.dpid={tcp_client.dpid}')
        
        self._refresh_state()
    
    def _refresh_state(self) -> None:
        """Refresh state, marking the light unavailable without switch data."""
        self._state = self._tcp_client.query()
        _LOGGER.info(f'_state={self._state}')
        if '1' not in self._state:
            self._attr_available = False
            return

        self._attr_available = True
        self._attr_is_on = 0 < self._state['1']
        
        if '4' in self._state:
            self._attr_brightness = int(self._state['4'] / 4)
        
        if '5' in self._state and '6' in self._state:
            self._attr_hs_color = (int(self._state['5']), int(self._state['6'] / 10))
        
        if '3' in self._state:
            color_temp_mired = 500 - int(self._state['3'] / 2)
            self._attr_color_temp_kelvin = None
            if color_temp_mired > 0:
                self._attr_color_temp_kelvin = (
                    color_util.color_temperature_mired_to_kelvin(color_temp_mired)
                )

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

    def turn_on(self, **kwargs: Any) -> None:
        """Turn the entity on."""
        brightness = kwargs.get(ATTR_BRIGHTNESS)
        # The existing device mapping uses 153..500 mired.
        color_temp_kelvin = kwargs.get(ATTR_COLOR_TEMP_KELVIN)
        # tuple
        hs_color = kwargs.get(ATTR_HS_COLOR)
        rgb = kwargs.get(ATTR_RGB_COLOR)
        flash = kwargs.get(ATTR_FLASH)
        effect = kwargs.get(ATTR_EFFECT)
        _LOGGER.info(f'turn_on.kwargs={kwargs}')
        
        payload = {'1': 255, '2': 0}
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
        
        try:
            control_succeeded = self._tcp_client.control(payload)
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
        if brightness is not None:
            self._attr_brightness = brightness
        if hs_color is not None:
            self._attr_hs_color = hs_color
            self._attr_color_mode = ColorMode.HS
        if color_temp_kelvin is not None:
            self._attr_color_temp_kelvin = color_temp_kelvin
            self._attr_color_mode = ColorMode.COLOR_TEMP
        return None
        raise NotImplementedError()
    
    def turn_off(self, **kwargs: Any) -> None:
        """Turn the entity off."""
        _LOGGER.info(f'turn_off.kwargs={kwargs}')
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
