"""Verify Config Entry setup and teardown for the CozyLife integration."""

from __future__ import annotations

import asyncio
from inspect import isawaitable
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import patch

from homeassistant.config_entries import SOURCE_IMPORT
from homeassistant.const import EVENT_HOMEASSISTANT_STOP, Platform

import custom_components.hass_cozylife_local_pull as integration
from custom_components.hass_cozylife_local_pull.const import DOMAIN


TEST_TIMEOUT_SECONDS = 2


class RecordingFlowManager:
    """Record config flow initialization without Home Assistant storage."""

    def __init__(self) -> None:
        self.calls = []

    async def async_init(self, domain, *, context, data):
        """Record one import request."""
        self.calls.append((domain, context, data))
        return {"type": "create_entry"}


class RecordingConfigEntries:
    """Provide the config entry operations used by YAML setup."""

    def __init__(self, entries=None) -> None:
        self._entries = list(entries or [])
        self.flow = RecordingFlowManager()
        self.updated = []

    def async_entries(self, domain):
        """Return current entries for the integration domain."""
        return self._entries

    def async_update_entry(self, entry, *, data):
        """Record updated YAML-backed entry data."""
        self.updated.append((entry, data))


class RecordingHomeAssistant:
    """Run tasks scheduled by integration-level YAML setup."""

    def __init__(self, entries=None) -> None:
        self.config_entries = RecordingConfigEntries(entries)
        self.tasks = []

    def async_create_task(self, coroutine, *, eager_start=False):
        """Start and retain one Home Assistant task."""
        task = asyncio.create_task(coroutine)
        self.tasks.append((task, eager_start))
        return task


class ReadyTransport:
    """Complete a device handshake as soon as the integration subscribes."""

    device_model_name = "Test Device"
    device_type_code = "00"
    dpid = [1, 2]
    last_state_sequence_number = 1700000000000

    def __init__(self, ip: str, lang: str = "en") -> None:
        self.ip = ip
        self.lang = lang
        self.device_id = f"device-{ip}"
        self.closed = False
        self.stop_signaled = False

    def add_ready_callback(self, callback) -> None:
        """Publish the already completed handshake."""
        callback(self)

    def add_state_callback(self, callback):
        """Return a no-op state subscription removal callback."""
        return lambda: None

    def query(self, attributes=None):
        """Return a complete switch state."""
        return {"1": 0, "2": 0}

    def control(self, payload):
        """Accept a device control request."""
        return True

    def signal_stop(self) -> None:
        """Record the non-blocking stop signal."""
        self.stop_signaled = True

    def close(self) -> None:
        """Record final transport cleanup."""
        self.closed = True


class BlockingCloseTransport(ReadyTransport):
    """Hold client cleanup until the lifecycle test releases it."""

    def __init__(self, ip: str, lang: str = "en") -> None:
        super().__init__(ip, lang)
        self.close_started = threading.Event()
        self.allow_close = threading.Event()

    def close(self) -> None:
        """Block at the transport cleanup boundary."""
        self.close_started.set()
        if not self.allow_close.wait(TEST_TIMEOUT_SECONDS):
            raise AssertionError("Timed out waiting to release client cleanup")
        super().close()


class RecordingBus:
    """Retain the Home Assistant stop listener and its removal callback."""

    def __init__(self) -> None:
        self.listener = None
        self.removed = False

    def async_listen_once(self, event_type, listener):
        """Register one event listener."""
        self.listener = (event_type, listener)

        def remove_listener() -> None:
            self.removed = True

        return remove_listener


class RuntimeConfigEntries:
    """Record platform forwarding and unloading for one config entry."""

    def __init__(self) -> None:
        self.forwarded = []
        self.unloaded = []
        self.forward_error: Exception | None = None

    async def async_forward_entry_setups(self, entry, platforms) -> None:
        """Record forwarded entity platforms."""
        self.forwarded.append((entry, tuple(platforms)))
        if self.forward_error is not None:
            raise self.forward_error

    async def async_unload_platforms(self, entry, platforms) -> bool:
        """Record and approve entity platform unloading."""
        self.unloaded.append((entry, tuple(platforms)))
        return True


class RuntimeHomeAssistant:
    """Provide the event loop and executor used by config entry lifecycle."""

    def __init__(self) -> None:
        self.bus = RecordingBus()
        self.config_entries = RuntimeConfigEntries()
        self.loop = asyncio.get_running_loop()
        self.is_stopping = False
        self.executor_jobs = []
        self.tasks = []

    def async_add_executor_job(self, target, *args):
        """Return the scheduled Future exposed by Home Assistant."""
        future = self.loop.run_in_executor(None, target, *args)
        self.executor_jobs.append(future)
        return future

    def async_create_task(self, coroutine, *, eager_start=False):
        """Start and retain one integration lifecycle task."""
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task


class YamlConfigEntryImportTest(unittest.IsolatedAsyncioTestCase):
    """Verify YAML remains the source for one imported config entry."""

    async def test_first_yaml_setup_starts_import_flow(self) -> None:
        """A first startup imports normalized YAML data into Home Assistant."""
        async_setup = getattr(integration, "async_setup", None)
        self.assertTrue(callable(async_setup))
        if not callable(async_setup):
            return
        hass = RecordingHomeAssistant()

        result = await async_setup(
            hass,
            {DOMAIN: {"lang": "zh", "ip": ["192.0.2.10"]}},
        )
        await asyncio.gather(*(task for task, _ in hass.tasks))

        self.assertTrue(result)
        self.assertEqual(
            hass.config_entries.flow.calls,
            [
                (
                    DOMAIN,
                    {"source": SOURCE_IMPORT},
                    {"lang": "zh", "ip": ["192.0.2.10"]},
                )
            ],
        )
        self.assertEqual([eager for _, eager in hass.tasks], [True])

    async def test_existing_entry_receives_latest_yaml_data(self) -> None:
        """A later startup updates rather than duplicates the imported entry."""
        async_setup = getattr(integration, "async_setup", None)
        self.assertTrue(callable(async_setup))
        if not callable(async_setup):
            return
        entry = SimpleNamespace()
        hass = RecordingHomeAssistant([entry])

        result = await async_setup(hass, {DOMAIN: {}})

        self.assertTrue(result)
        self.assertEqual(
            hass.config_entries.updated,
            [(entry, {"lang": "en", "ip": []})],
        )
        self.assertEqual(hass.tasks, [])

    async def test_setup_without_yaml_does_not_start_import(self) -> None:
        """A stored config entry can load without configuration YAML present."""
        async_setup = getattr(integration, "async_setup", None)
        self.assertTrue(callable(async_setup))
        if not callable(async_setup):
            return
        hass = RecordingHomeAssistant()

        result = await async_setup(hass, {})

        self.assertTrue(result)
        self.assertEqual(hass.config_entries.flow.calls, [])
        self.assertEqual(hass.tasks, [])


class ConfigEntryLifecycleTest(unittest.IsolatedAsyncioTestCase):
    """Verify one config entry owns discovery, devices, and platforms."""

    async def asyncSetUp(self) -> None:
        """Create a fresh runtime and cleanup observations for each test."""
        self.hass = RuntimeHomeAssistant()
        self.entry = SimpleNamespace(
            data={"lang": "zh", "ip": ["192.0.2.20"]},
            runtime_data=None,
        )
        self.clients = []
        self.client_type = ReadyTransport
        self.discovery_cancelled = False
        self.discovery_action = None
        self.discovery_interval = None
        self.discovery_options = None

    def create_client(self, ip: str, lang: str = "en") -> ReadyTransport:
        """Create and retain one complete transport double."""
        client = self.client_type(ip, lang)
        self.clients.append(client)
        return client

    def track_interval(self, hass, action, interval, **kwargs):
        """Capture periodic discovery and return its cancellation callback."""
        self.discovery_action = action
        self.discovery_interval = interval
        self.discovery_options = kwargs

        def cancel_interval() -> None:
            self.discovery_cancelled = True

        return cancel_interval

    async def setup_entry(self) -> bool:
        """Run production setup with network boundaries replaced."""
        async_setup_entry = getattr(integration, "async_setup_entry", None)
        self.assertTrue(callable(async_setup_entry))
        if not callable(async_setup_entry):
            return False

        with patch.object(
            integration, "get_ip", return_value=["192.0.2.10"]
        ), patch.object(
            integration, "tcp_client", side_effect=self.create_client
        ), patch.object(
            integration,
            "async_track_time_interval",
            side_effect=self.track_interval,
        ):
            return await async_setup_entry(self.hass, self.entry)

    async def test_setup_discovers_sources_registers_devices_and_loads_platforms(
        self,
    ) -> None:
        """Both address sources pass through one device registration pipeline."""
        result = await self.setup_entry()

        self.assertTrue(result)
        self.assertEqual(
            [(client.ip, client.lang) for client in self.clients],
            [("192.0.2.10", "zh"), ("192.0.2.20", "zh")],
        )
        self.assertEqual(
            set(self.entry.runtime_data["devices"]),
            {"device-192.0.2.10", "device-192.0.2.20"},
        )
        self.assertEqual(
            self.hass.config_entries.forwarded,
            [
                (
                    self.entry,
                    (Platform.LIGHT, Platform.SWITCH, Platform.NUMBER),
                )
            ],
        )

    async def test_periodic_discovery_adds_each_new_device_once(self) -> None:
        """Scheduled discovery uses the executor and deduplicates later rounds."""
        self.assertTrue(await self.setup_entry())
        action = self.discovery_action
        self.assertIsNotNone(action)
        self.assertEqual(self.discovery_interval, integration.DISCOVERY_INTERVAL)
        self.assertEqual(
            self.discovery_options,
            {"cancel_on_shutdown": True},
        )
        runtime = self.entry.runtime_data

        with patch.object(
            integration,
            "get_ip",
            side_effect=[
                ["192.0.2.10", "192.0.2.30"],
                ["192.0.2.10", "192.0.2.30"],
            ],
        ), patch.object(
            integration, "tcp_client", side_effect=self.create_client
        ), patch.object(
            self.hass,
            "async_add_executor_job",
            wraps=self.hass.async_add_executor_job,
        ) as add_executor_job:
            await action(None)
            await action(None)

        self.assertEqual(add_executor_job.call_count, 2)
        for job_call in add_executor_job.call_args_list:
            self.assertIs(job_call.args[0], integration._discover_new_clients)
            self.assertIs(job_call.args[1], runtime)
            self.assertEqual(job_call.args[2:], ("zh", ("192.0.2.20",)))
        self.assertEqual(
            [(client.ip, client.lang) for client in self.clients],
            [
                ("192.0.2.10", "zh"),
                ("192.0.2.20", "zh"),
                ("192.0.2.30", "zh"),
            ],
        )
        self.assertEqual(
            set(runtime["devices"]),
            {
                "device-192.0.2.10",
                "device-192.0.2.20",
                "device-192.0.2.30",
            },
        )
        self.assertEqual(
            runtime["known_ips"],
            {"192.0.2.10", "192.0.2.20", "192.0.2.30"},
        )

    async def test_unload_stops_discovery_and_closes_clients(self) -> None:
        """Config entry unload releases every runtime resource it owns."""
        self.assertTrue(await self.setup_entry())
        async_unload_entry = getattr(integration, "async_unload_entry", None)
        self.assertTrue(callable(async_unload_entry))
        if not callable(async_unload_entry):
            return

        result = await async_unload_entry(self.hass, self.entry)

        self.assertTrue(result)
        self.assertTrue(self.discovery_cancelled)
        self.assertTrue(self.hass.bus.removed)
        self.assertTrue(all(client.stop_signaled for client in self.clients))
        self.assertTrue(all(client.closed for client in self.clients))
        self.assertEqual(
            self.hass.config_entries.unloaded,
            [
                (
                    self.entry,
                    (Platform.LIGHT, Platform.SWITCH, Platform.NUMBER),
                )
            ],
        )

    async def test_platform_setup_failure_releases_runtime_resources(
        self,
    ) -> None:
        """A failed platform setup cannot leave clients or listeners active."""
        self.hass.config_entries.forward_error = RuntimeError(
            "Injected platform setup failure"
        )

        with self.assertRaisesRegex(
            RuntimeError, "Injected platform setup failure"
        ):
            await self.setup_entry()

        self.assertTrue(self.hass.bus.removed)
        self.assertIsNone(self.entry.runtime_data["remove_stop_listener"])
        self.assertTrue(self.entry.runtime_data["stopped"])
        self.assertTrue(all(client.stop_signaled for client in self.clients))
        self.assertTrue(all(client.closed for client in self.clients))

    async def test_home_assistant_stop_waits_for_client_cleanup(self) -> None:
        """The stop listener does not finish before its clients are closed."""
        self.client_type = BlockingCloseTransport
        self.assertTrue(await self.setup_entry())
        event_type, stop_listener = self.hass.bus.listener
        self.assertEqual(event_type, EVENT_HOMEASSISTANT_STOP)

        stop_result = stop_listener(None)
        self.assertTrue(isawaitable(stop_result))
        stop_task = asyncio.ensure_future(stop_result)
        blocking_clients = [
            client
            for client in self.clients
            if isinstance(client, BlockingCloseTransport)
        ]

        try:
            close_started = await asyncio.to_thread(
                blocking_clients[0].close_started.wait,
                TEST_TIMEOUT_SECONDS,
            )
            listener_waiting = not stop_task.done()
            closed_before_release = any(
                client.closed for client in blocking_clients
            )
        finally:
            for client in blocking_clients:
                client.allow_close.set()
            await asyncio.wait_for(stop_task, TEST_TIMEOUT_SECONDS)

        self.assertTrue(close_started)
        self.assertTrue(listener_waiting)
        self.assertFalse(closed_before_release)
        self.assertTrue(all(client.stop_signaled for client in self.clients))
        self.assertTrue(all(client.closed for client in self.clients))
