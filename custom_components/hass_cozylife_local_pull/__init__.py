"""Set up CozyLife discovery, clients, and Home Assistant platforms."""
from __future__ import annotations

from datetime import timedelta
import logging
import threading
from typing import Callable

from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant
from homeassistant.helpers.discovery import async_load_platform
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.typing import ConfigType

from .const import (
    DOMAIN,
    LANG
)
from .udp_discover import get_ip
from .tcp_client import tcp_client


DISCOVERY_INTERVAL = timedelta(seconds=60)
_LOGGER = logging.getLogger(__name__)


def register_client_callback(
    hass: HomeAssistant, callback: Callable[[tcp_client], None]
) -> None:
    """Register a platform for existing and subsequently discovered clients."""
    state = hass.data[DOMAIN]
    with state['lock']:
        if state['stopped']:
            return
        state['client_callbacks'].append(callback)
        clients = list(state['tcp_client'])

    for client in clients:
        with state['lock']:
            if state['stopped']:
                return
        try:
            callback(client)
        except Exception:
            _LOGGER.exception('Client callback failed for %s', client)


def _add_new_clients(hass: HomeAssistant, ips: list[str], lang: str) -> None:
    """Create one client per new address and notify loaded platforms."""
    state = hass.data[DOMAIN]
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
                callbacks = list(state['client_callbacks'])

        if stopped:
            try:
                client.close()
            except Exception:
                _LOGGER.exception('Failed to close late client for %s', ip)
            continue

        for callback in callbacks:
            with state['lock']:
                if state['stopped']:
                    break
            try:
                callback(client)
            except Exception:
                _LOGGER.exception('Client callback failed for %s', client)


def _discover_new_clients(
    hass: HomeAssistant, lang: str, configured_ips: tuple[str, ...]
) -> None:
    """Discover devices while participating in the shutdown barrier."""
    state = hass.data[DOMAIN]
    with state['discovery_lock']:
        with state['lock']:
            if state['stopped']:
                return
        _add_new_clients(hass, [*get_ip(), *configured_ips], lang)


def _schedule_periodic_discovery(
    hass: HomeAssistant, lang: str, configured_ips: tuple[str, ...]
) -> None:
    """Schedule blocking discovery in Home Assistant's executor."""
    async def async_discover(_now) -> None:
        await hass.async_add_executor_job(
            _discover_new_clients, hass, lang, configured_ips
        )

    async_track_time_interval(
        hass,
        async_discover,
        DISCOVERY_INTERVAL,
        cancel_on_shutdown=True,
    )


def _close_clients(hass: HomeAssistant) -> None:
    """Stop all network clients owned by this integration."""
    state = hass.data.get(DOMAIN)
    if state is None:
        return

    with state['lock']:
        state['stopped'] = True
        clients = list(state['tcp_client'])

    # Signal every client before close waits for admitted readiness callbacks.
    for client in clients:
        try:
            client.signal_stop()
        except Exception:
            _LOGGER.exception('Failed to signal client stop for %s', client)

    # An admitted discovery closes any client constructed across the stop
    # boundary before releasing this barrier.
    with state['discovery_lock']:
        pass

    for client in clients:
        try:
            client.close()
        except Exception:
            _LOGGER.exception('Failed to close client %s', client)


def setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Initialize clients and keep discovering devices after startup."""
    domain_config = config[DOMAIN]
    configured_ips = tuple(domain_config.get('ip') or [])
    lang = domain_config.get('lang') or LANG
    hass.data[DOMAIN] = {
        'ip': [],
        'known_ips': set(),
        'tcp_client': [],
        'client_callbacks': [],
        'lock': threading.RLock(),
        'discovery_lock': threading.RLock(),
        'stopped': False,
    }
    hass.bus.listen_once(
        EVENT_HOMEASSISTANT_STOP, lambda _event: _close_clients(hass)
    )
    _discover_new_clients(hass, lang, configured_ips)
    state = hass.data[DOMAIN]
    with state['lock']:
        if state['stopped']:
            return True

    def finish_setup() -> None:
        """Create event-loop resources only while startup remains active."""
        with state['lock']:
            if state['stopped'] or hass.is_stopping:
                return
            hass.async_create_task(
                async_load_platform(hass, 'light', DOMAIN, {}, config)
            )
            hass.async_create_task(
                async_load_platform(hass, 'switch', DOMAIN, {}, config)
            )
            hass.async_create_task(
                async_load_platform(hass, 'number', DOMAIN, {}, config)
            )
            _schedule_periodic_discovery(hass, lang, configured_ips)

    hass.loop.call_soon_threadsafe(finish_setup)
    return True
