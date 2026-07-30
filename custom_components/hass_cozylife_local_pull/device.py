"""Represent one ready CozyLife device above its network transport."""

from __future__ import annotations

from collections.abc import Callable

from homeassistant.helpers.device_registry import DeviceInfo

from .const import DOMAIN
from .tcp_client import tcp_client


class _StateSubscription:
    """Track one entity callback across transport replacement."""

    def __init__(
        self,
        callback: Callable[[dict, int], None],
        remove_callback: Callable[[], None],
    ) -> None:
        self.callback = callback
        self.remove_callback = remove_callback


class CozyLifeDevice:
    """Represent one physical CozyLife device after its handshake completes."""

    def __init__(self, client: tcp_client) -> None:
        """Capture stable identity after the transport handshake has completed."""
        self._client = client
        self._state_subscriptions: list[_StateSubscription] = []
        self._device_info = DeviceInfo(
            identifiers={(DOMAIN, client.device_id)},
            manufacturer="CozyLife",
            model=client.device_model_name,
            name=f"{client.device_model_name} {client.device_id[-4:]}",
        )

    @property
    def device_id(self) -> str:
        """Return the physical device identifier."""
        return self._client.device_id

    @property
    def device_model_name(self) -> str:
        """Return the localized product name."""
        return self._client.device_model_name

    @property
    def device_type_code(self) -> str:
        """Return the device category reported by product metadata."""
        return self._client.device_type_code

    @property
    def dpid(self) -> list[int]:
        """Return the supported device data point identifiers."""
        return self._client.dpid

    @property
    def last_state_sequence_number(self) -> int | None:
        """Return the latest accepted device state timestamp."""
        return self._client.last_state_sequence_number

    @property
    def device_info(self) -> DeviceInfo:
        """Return metadata used to group this device's entities."""
        return self._device_info

    def replace_client(self, client: tcp_client) -> tcp_client | None:
        """Switch transport while preserving existing entity subscriptions."""
        previous_client = self._client
        if client is previous_client:
            return None

        for subscription in self._state_subscriptions:
            subscription.remove_callback()
        self._client = client
        for subscription in self._state_subscriptions:
            subscription.remove_callback = client.add_state_callback(
                subscription.callback
            )
        return previous_client

    def query(self, attributes: list[int] | None = None) -> dict:
        """Query device properties through the transport."""
        return self._client.query(attributes)

    def control(self, payload: dict) -> bool:
        """Send a device control request through the transport."""
        return self._client.control(payload)

    def add_state_callback(
        self, callback: Callable[[dict, int], None]
    ) -> Callable[[], None]:
        """Subscribe to validated device state messages."""
        subscription = _StateSubscription(
            callback,
            self._client.add_state_callback(callback),
        )
        self._state_subscriptions.append(subscription)

        def remove_callback() -> None:
            if subscription not in self._state_subscriptions:
                return
            subscription.remove_callback()
            self._state_subscriptions.remove(subscription)

        return remove_callback
