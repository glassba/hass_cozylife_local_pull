"""Expose device-backed numeric controls for CozyLife lights."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
import math
from time import monotonic

from homeassistant.components.number import (
    NumberDeviceClass,
    NumberEntity,
    NumberMode,
)
from homeassistant.const import UnitOfTime
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

from . import register_client_callback
from .const import LIGHT_COUNTDOWN, LIGHT_TYPE_CODE
from .tcp_client import DeviceCommandRejectedError, tcp_client


COUNTDOWN_TICK_INTERVAL = timedelta(seconds=1)
COUNTDOWN_CALIBRATION_INTERVAL = timedelta(seconds=10)


def setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Add countdown controls after each capable light finishes its handshake."""
    if discovery_info is None:
        return

    def add_ready_countdown(client: tcp_client) -> None:
        """Create a countdown only when the model advertises DPID 13."""
        if (
            client.device_type_code == LIGHT_TYPE_CODE
            and int(LIGHT_COUNTDOWN) in client.dpid
        ):
            add_entities([CozyLifeLightCountdown(client)])

    def register_client(client: tcp_client) -> None:
        """Attach the countdown capability check to one network client."""
        client.add_ready_callback(add_ready_countdown)

    register_client_callback(hass, register_client)


class CozyLifeLightCountdown(NumberEntity):
    """Represent a light's device-local countdown in seconds."""

    _attr_device_class = NumberDeviceClass.DURATION
    _attr_mode = NumberMode.BOX
    _attr_native_max_value = 86400
    _attr_native_min_value = 0
    _attr_native_step = 1
    _attr_native_unit_of_measurement = UnitOfTime.SECONDS
    # Device state messages must refresh Home Assistant's last_updated value.
    _attr_force_update = True
    _attr_should_poll = False

    def __init__(self, client) -> None:
        """Initialize the entity from the device's reported countdown."""
        self._tcp_client = client
        self._countdown_deadline: float | None = None
        self._state_updates_active = False
        self._initial_query_complete = False
        self._cancel_countdown_tick: Callable[[], None] | None = None
        self._cancel_countdown_calibration: Callable[[], None] | None = None
        self._remove_state_callback: Callable[[], None] | None = None
        self._attr_name = (
            f"{client.device_model_name} {client.device_id[-4:]} Countdown"
        )
        self._attr_unique_id = f"{client.device_id}_countdown"
        self._attr_available = False
        self._attr_native_value = 0

    def _query_countdown_state(self) -> dict:
        """Query DPID 13 without changing Home Assistant entity state."""
        return self._tcp_client.query([int(LIGHT_COUNTDOWN)])

    def _apply_countdown_query(self, state: dict) -> None:
        """Apply one device query result on the owning execution context."""
        if LIGHT_COUNTDOWN not in state:
            self._attr_available = False
            self._countdown_deadline = None
            return

        countdown_seconds = self._parse_countdown_seconds(
            state[LIGHT_COUNTDOWN]
        )
        if countdown_seconds is None:
            self._attr_available = False
            self._countdown_deadline = None
            self._stop_countdown_updates()
            return

        self._set_countdown_state(countdown_seconds)
        self._attr_available = True

    def _apply_current_countdown_query_failure(
        self,
        state: dict,
        sequence_number_before_query: int | None,
    ) -> bool:
        """Apply an empty query only when no newer state arrived meanwhile."""
        if (
            LIGHT_COUNTDOWN in state
            or self._tcp_client.last_state_sequence_number
            != sequence_number_before_query
        ):
            return False
        self._apply_countdown_query(state)
        self._sync_countdown_updates()
        return True

    def _parse_countdown_seconds(self, value: object) -> int | None:
        """Validate one device countdown value against the entity contract."""
        if isinstance(value, bool):
            return None
        try:
            numeric_value = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(numeric_value) or not numeric_value.is_integer():
            return None
        countdown_seconds = int(numeric_value)
        if not (
            self.native_min_value
            <= countdown_seconds
            <= self.native_max_value
        ):
            return None
        return countdown_seconds

    def _set_countdown_state(self, countdown_seconds: int) -> None:
        """Cache device seconds and derive a monotonic local deadline."""
        self._attr_native_value = countdown_seconds
        self._countdown_deadline = (
            monotonic() + countdown_seconds if countdown_seconds > 0 else None
        )

    def _sync_countdown_updates(self) -> None:
        """Match local tick and device calibration tasks to entity state."""
        if not self._state_updates_active:
            self._stop_countdown_updates()
            return

        should_tick = self.available and self._countdown_deadline is not None
        if should_tick and self._cancel_countdown_tick is None:
            self._cancel_countdown_tick = async_track_time_interval(
                self.hass,
                self._handle_countdown_tick,
                COUNTDOWN_TICK_INTERVAL,
            )
        elif not should_tick:
            self._stop_countdown_tick()

        # Keep probing at zero so idle devices can publish connection failures.
        if self._cancel_countdown_calibration is None:
            self._cancel_countdown_calibration = async_track_time_interval(
                self.hass,
                self._async_calibrate_countdown,
                COUNTDOWN_CALIBRATION_INTERVAL,
            )

    def _stop_countdown_tick(self) -> None:
        """Cancel the local one-second countdown task, if active."""
        if self._cancel_countdown_tick is not None:
            self._cancel_countdown_tick()
            self._cancel_countdown_tick = None

    def _stop_countdown_calibration(self) -> None:
        """Cancel the ten-second device calibration task, if active."""
        if self._cancel_countdown_calibration is not None:
            self._cancel_countdown_calibration()
            self._cancel_countdown_calibration = None

    def _stop_countdown_updates(self) -> None:
        """Cancel local tick and device calibration tasks, if active."""
        self._stop_countdown_tick()
        self._stop_countdown_calibration()

    def _handle_state_report(
        self, state: dict, sequence_number: int
    ) -> None:
        """Move DPID 13 state from the network thread to the event loop."""
        if LIGHT_COUNTDOWN in state and self.hass is not None:
            self.hass.loop.call_soon_threadsafe(
                self._apply_countdown_report,
                state[LIGHT_COUNTDOWN],
                sequence_number,
            )

    def _apply_countdown_report(
        self,
        value: int,
        sequence_number: int,
    ) -> None:
        """Apply one non-stale countdown state message."""
        if not self._state_updates_active:
            return
        latest_sequence_number = self._tcp_client.last_state_sequence_number
        if (
            latest_sequence_number is not None
            and sequence_number < latest_sequence_number
        ):
            return
        countdown_seconds = self._parse_countdown_seconds(value)
        if countdown_seconds is None:
            self._attr_available = False
            self._countdown_deadline = None
            self._stop_countdown_updates()
            if self._initial_query_complete:
                self.async_write_ha_state()
            return
        self._set_countdown_state(countdown_seconds)
        self._attr_available = True
        self._sync_countdown_updates()
        if self._initial_query_complete:
            self.async_write_ha_state()

    async def _async_calibrate_countdown(self, _now: datetime) -> None:
        """Query DPID 13 off the event loop and publish calibrated state."""
        if not self._state_updates_active:
            return
        sequence_number_before_query = (
            self._tcp_client.last_state_sequence_number
        )
        state = await self.hass.async_add_executor_job(
            self._query_countdown_state
        )
        if not self._state_updates_active:
            return
        if self._apply_current_countdown_query_failure(
            state, sequence_number_before_query
        ):
            self.async_write_ha_state()

    @callback
    def _handle_countdown_tick(self, _now: datetime) -> None:
        """Publish locally calculated seconds without querying the device."""
        if not self._state_updates_active:
            self._stop_countdown_updates()
            return
        if not self.available or self._countdown_deadline is None:
            self._stop_countdown_tick()
            self._sync_countdown_updates()
            return

        remaining = max(0, math.ceil(self._countdown_deadline - monotonic()))
        if remaining != self._attr_native_value:
            self._attr_native_value = remaining
            self.async_write_ha_state()

        if remaining == 0:
            self._countdown_deadline = None
            self._sync_countdown_updates()

    async def async_added_to_hass(self) -> None:
        """Start local updates after Home Assistant owns the entity."""
        await super().async_added_to_hass()
        self._state_updates_active = True
        self._remove_state_callback = self._tcp_client.add_state_callback(
            self._handle_state_report
        )
        sequence_number_before_query = (
            self._tcp_client.last_state_sequence_number
        )
        state = await self.hass.async_add_executor_job(
            self._query_countdown_state
        )
        if self._state_updates_active:
            self._apply_current_countdown_query_failure(
                state, sequence_number_before_query
            )
            self._initial_query_complete = True

    async def async_will_remove_from_hass(self) -> None:
        """Stop local updates before Home Assistant removes the entity."""
        self._state_updates_active = False
        if self._remove_state_callback is not None:
            self._remove_state_callback()
            self._remove_state_callback = None
        self._stop_countdown_updates()
        await super().async_will_remove_from_hass()

    async def async_update(self) -> None:
        """Refresh DPID 13 without applying worker-thread state changes."""
        if not self._state_updates_active:
            return
        sequence_number_before_query = (
            self._tcp_client.last_state_sequence_number
        )
        state = await self.hass.async_add_executor_job(
            self._query_countdown_state
        )
        if not self._state_updates_active:
            return
        self._apply_current_countdown_query_failure(
            state, sequence_number_before_query
        )

    async def async_set_native_value(self, value: float) -> None:
        """Set or cancel the device-local countdown on the event loop."""
        countdown_seconds = self._parse_countdown_seconds(value)
        if countdown_seconds is None:
            raise HomeAssistantError(
                "Countdown must be a whole number from 0 to 86400 seconds"
            )

        try:
            control_succeeded = await self.hass.async_add_executor_job(
                self._tcp_client.control,
                {LIGHT_COUNTDOWN: countdown_seconds},
            )
        except DeviceCommandRejectedError as err:
            raise HomeAssistantError(
                "CozyLife device rejected countdown command"
            ) from err

        if not control_succeeded:
            self._attr_available = False
            self._countdown_deadline = None
            self._sync_countdown_updates()
            self.async_write_ha_state()
            raise HomeAssistantError(
                "Unable to send countdown command to CozyLife device"
            )
