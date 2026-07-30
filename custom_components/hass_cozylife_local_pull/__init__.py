"""Set up CozyLife discovery, clients, and Home Assistant platforms."""
from __future__ import annotations

from datetime import timedelta
import logging
import threading
from typing import Callable

from homeassistant.config_entries import ConfigEntry, SOURCE_IMPORT
from homeassistant.const import EVENT_HOMEASSISTANT_STOP, Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.typing import ConfigType

from .const import (
    DOMAIN,
    LANG
)
from .device import CozyLifeDevice
from .udp_discover import get_ip
from .tcp_client import tcp_client


DISCOVERY_INTERVAL = timedelta(seconds=60)
PLATFORMS = (Platform.LIGHT, Platform.SWITCH, Platform.NUMBER)
_LOGGER = logging.getLogger(__name__)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Import current YAML settings before Home Assistant loads config entries."""
    domain_config = config.get(DOMAIN)
    if domain_config is None:
        return True

    entry_data = {
        'lang': domain_config.get('lang') or LANG,
        'ip': list(domain_config.get('ip') or []),
    }
    entries = hass.config_entries.async_entries(DOMAIN)
    if entries:
        hass.config_entries.async_update_entry(entries[0], data=entry_data)
        return True

    hass.async_create_task(
        hass.config_entries.flow.async_init(
            DOMAIN,
            context={'source': SOURCE_IMPORT},
            data=entry_data,
        ),
        eager_start=True,
    )
    return True


def register_device_callback(
    state: dict, callback: Callable[[CozyLifeDevice], None]
) -> None:
    """Register a platform for existing and subsequently ready devices."""
    with state['lock']:
        if state['stopped']:
            return
        state['device_callbacks'].append(callback)
        devices = list(state['devices'].values())

    for device in devices:
        with state['lock']:
            if state['stopped']:
                return
        try:
            callback(device)
        except Exception:
            _LOGGER.exception('Device callback failed for %s', device.device_id)


def _register_ready_device(state: dict, client: tcp_client) -> None:
    """Publish one transport as a device after its handshake completes."""
    with state['lock']:
        if state['stopped']:
            return
        existing_device = state['devices'].get(client.device_id)
        if existing_device is not None:
            callbacks = []
        else:
            device = CozyLifeDevice(client)
            state['devices'][device.device_id] = device
            callbacks = list(state['device_callbacks'])

    if existing_device is not None:
        replaced_client = existing_device.replace_client(client)
        if replaced_client is None:
            return
        try:
            replaced_client.close()
        except Exception:
            _LOGGER.exception(
                'Failed to close replaced client for %s', client.device_id
            )
            return

        with state['lock']:
            if replaced_client in state['tcp_client']:
                client_index = state['tcp_client'].index(replaced_client)
                state['tcp_client'].pop(client_index)
                state['ip'].pop(client_index)
        return

    for callback in callbacks:
        with state['lock']:
            if state['stopped']:
                return
        try:
            callback(device)
        except Exception:
            _LOGGER.exception('Device callback failed for %s', device.device_id)


def _add_new_clients(state: dict, ips: list[str], lang: str) -> None:
    """Create one client per new address and notify loaded platforms."""
    for ip in dict.fromkeys(ips):
        with state['lock']:
            if state['stopped']:
                return
            if ip in state['known_ips']:
                continue
            state['known_ips'].add(ip)

        try:
            client = tcp_client(ip, lang=lang)
        except Exception:
            with state['lock']:
                state['known_ips'].discard(ip)
            _LOGGER.exception('Failed to create client for %s', ip)
            continue

        with state['lock']:
            stopped = state['stopped']
            if not stopped:
                state['ip'].append(ip)
                state['tcp_client'].append(client)

        if stopped:
            try:
                client.close()
            except Exception:
                _LOGGER.exception('Failed to close late client for %s', ip)
            continue

        client.add_ready_callback(
            lambda ready_client: _register_ready_device(state, ready_client)
        )


def _discover_new_clients(
    state: dict, lang: str, configured_ips: tuple[str, ...]
) -> None:
    """Discover devices while participating in the shutdown barrier."""
    with state['discovery_lock']:
        with state['lock']:
            if state['stopped']:
                return
        _add_new_clients(state, [*get_ip(), *configured_ips], lang)


def _schedule_periodic_discovery(
    hass: HomeAssistant,
    state: dict,
    lang: str,
    configured_ips: tuple[str, ...],
) -> Callable[[], None]:
    """Schedule blocking discovery in Home Assistant's executor."""
    async def async_discover(_now) -> None:
        await hass.async_add_executor_job(
            _discover_new_clients, state, lang, configured_ips
        )

    return async_track_time_interval(
        hass,
        async_discover,
        DISCOVERY_INTERVAL,
        cancel_on_shutdown=True,
    )


def _signal_clients(state: dict) -> list[tcp_client]:
    """Stop admission before any blocking client cleanup begins."""
    with state['lock']:
        state['stopped'] = True
        clients = list(state['tcp_client'])

    for client in clients:
        try:
            client.signal_stop()
        except Exception:
            _LOGGER.exception('Failed to signal client stop for %s', client)
    return clients


def _close_clients(state: dict) -> None:
    """Stop all network clients owned by this integration."""
    clients = _signal_clients(state)

    # An admitted discovery closes any client constructed across the stop
    # boundary before releasing this barrier.
    with state['discovery_lock']:
        pass

    for client in clients:
        try:
            client.close()
        except Exception:
            _LOGGER.exception('Failed to close client %s', client)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry
) -> bool:
    """Own discovery, devices, and entity platforms for one imported entry."""
    configured_ips = tuple(entry.data.get('ip') or [])
    lang = entry.data.get('lang') or LANG
    state = {
        'ip': [],
        'known_ips': set(),
        'tcp_client': [],
        'devices': {},
        'device_callbacks': [],
        'lock': threading.RLock(),
        'discovery_lock': threading.RLock(),
        'stopped': False,
        'cancel_discovery': None,
        'remove_stop_listener': None,
    }
    entry.runtime_data = state

    async def handle_stop(_event) -> None:
        """Reject new work immediately, then close clients outside the loop."""
        _signal_clients(state)
        await hass.async_add_executor_job(_close_clients, state)

    try:
        state['remove_stop_listener'] = hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP,
            handle_stop,
        )
        await hass.async_add_executor_job(
            _discover_new_clients, state, lang, configured_ips
        )
        with state['lock']:
            if state['stopped']:
                return True

        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        with state['lock']:
            if state['stopped'] or hass.is_stopping:
                return True
            state['cancel_discovery'] = _schedule_periodic_discovery(
                hass, state, lang, configured_ips
            )
        return True
    except Exception:
        if remove_stop_listener := state['remove_stop_listener']:
            remove_stop_listener()
            state['remove_stop_listener'] = None
        _signal_clients(state)
        await hass.async_add_executor_job(_close_clients, state)
        raise


async def async_unload_entry(
    hass: HomeAssistant, entry: ConfigEntry
) -> bool:
    """Unload platforms before releasing discovery and network resources."""
    if not await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        return False

    state = entry.runtime_data
    if cancel_discovery := state['cancel_discovery']:
        cancel_discovery()
        state['cancel_discovery'] = None
    if remove_stop_listener := state['remove_stop_listener']:
        remove_stop_listener()
        state['remove_stop_listener'] = None
    await hass.async_add_executor_job(_close_clients, state)
    return True
