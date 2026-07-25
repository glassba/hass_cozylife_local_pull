"""Regression tests for adding devices after their startup handshake completes."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import importlib
import inspect
import queue
import threading
import unittest
from unittest.mock import Mock, patch

from custom_components.hass_cozylife_local_pull import light, switch
from custom_components.hass_cozylife_local_pull.const import (
    DOMAIN,
    LIGHT_TYPE_CODE,
    SWITCH_TYPE_CODE,
)


class DelayedDeviceClient:
    """Expose the production readiness callback contract without network I/O."""

    device_id = "device-1234"
    device_model_name = "Test Device"
    dpid = [1, 4]

    def __init__(self, device_type_code: str | type = str) -> None:
        self.device_type_code = device_type_code
        self._ready_callbacks = []
        self._ready_callback_lock = threading.RLock()
        self.registration_count = 0
        self.closed = False
        self._stopped = False
        self.stop_signaled = threading.Event()

    def add_ready_callback(self, callback) -> None:
        """Run immediately when ready or retain the one-shot callback."""
        with self._ready_callback_lock:
            if self._stopped:
                return
            self.registration_count += 1
            if self.device_type_code is not str:
                callback(self)
                return
            self._ready_callbacks.append(callback)

    def become_ready(self, device_type_code: str) -> None:
        """Complete the delayed handshake and publish readiness."""
        with self._ready_callback_lock:
            if self._stopped:
                return
            self.device_type_code = device_type_code
            callbacks, self._ready_callbacks = self._ready_callbacks, []
            for callback in callbacks:
                callback(self)

    def query(self) -> dict[str, int]:
        """Return a complete state for either supported entity type."""
        return {"1": 0, "4": 0}

    def control(self, payload: dict[str, int]) -> bool:
        """Accept entity commands used during setup tests."""
        return True

    def close(self) -> None:
        """Record lifecycle cleanup."""
        self.request_stop()
        self.closed = True

    def signal_stop(self) -> None:
        """Reject new work without waiting for readiness callbacks."""
        self._stopped = True
        self.stop_signaled.set()

    def request_stop(self) -> None:
        """Reject new readiness callbacks before blocking cleanup begins."""
        self.signal_stop()
        with self._ready_callback_lock:
            pass


class ObservedRLock:
    """Signal when another thread waits for the owned reentrant lock."""

    def __init__(self, contender_waiting: threading.Event) -> None:
        self._lock = threading.RLock()
        self._contender_waiting = contender_waiting

    def __enter__(self):
        if not self._lock.acquire(blocking=False):
            self._contender_waiting.set()
            self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._lock.release()


class BlockingCloseDeviceClient(DelayedDeviceClient):
    """Expose the window before blocking client cleanup marks itself stopped."""

    def __init__(
        self,
        close_started: threading.Event,
        allow_close: threading.Event,
        timeout: float,
    ) -> None:
        super().__init__(LIGHT_TYPE_CODE)
        self._close_started = close_started
        self._allow_close = allow_close
        self._timeout = timeout
        self.release_timed_out = threading.Event()

    def close(self) -> None:
        """Wait at the cleanup boundary before completing lifecycle shutdown."""
        self._close_started.set()
        if not self._allow_close.wait(self._timeout):
            self.release_timed_out.set()
            raise TimeoutError("Timed out waiting to release client close")
        super().close()


class RecordingLoop:
    """Execute thread-safe scheduling calls while recording their payloads."""

    def __init__(self) -> None:
        self.scheduled = []

    def call_soon_threadsafe(self, callback, *args) -> None:
        """Record and execute a scheduled callback."""
        self.scheduled.append(args)
        callback(*args)


class DeferredRecordingLoop:
    """Retain thread-safe callbacks until the test advances the event loop."""

    def __init__(self) -> None:
        self.scheduled = []

    def call_soon_threadsafe(self, callback, *args) -> None:
        """Queue a callback without executing it."""
        self.scheduled.append((callback, args))

    def run_scheduled(self) -> None:
        """Execute the callbacks that were queued before this boundary."""
        scheduled, self.scheduled = self.scheduled, []
        for callback, args in scheduled:
            callback(*args)


class RecordingBus:
    """Record one-shot lifecycle listeners registered by the integration."""

    def __init__(self) -> None:
        self.listeners = []

    def listen_once(self, event_type, listener):
        """Retain a listener so tests can publish the stop event."""
        self.listeners.append((event_type, listener))
        return lambda: None


class RecordingHomeAssistant:
    """Provide the Home Assistant attributes used by synchronous setup."""

    def __init__(self, clients=None) -> None:
        self.data = {}
        if clients is not None:
            self.data[DOMAIN] = {
                "tcp_client": clients,
                "ip": [],
                "known_ips": set(),
                "client_callbacks": [],
                "lock": threading.RLock(),
                "discovery_lock": threading.RLock(),
                "stopped": False,
            }
        self.loop = RecordingLoop()
        self.bus = RecordingBus()
        self.created_tasks = []
        self.executor_thread_ids = []
        self.is_stopping = False

    def async_create_task(self, task) -> None:
        """Record scheduled platform loads without starting an event loop."""
        self.created_tasks.append(task)

    async def async_add_executor_job(self, target, *args):
        """Execute discovery in a worker like Home Assistant does."""
        def run_target():
            self.executor_thread_ids.append(threading.get_ident())
            return target(*args)

        with ThreadPoolExecutor(max_workers=1) as executor:
            return await asyncio.get_running_loop().run_in_executor(
                executor, run_target
            )


class StartupReadinessTest(unittest.TestCase):
    """Verify startup never depends on a fixed connection delay."""

    def test_setup_loads_platforms_without_fixed_sleep(self) -> None:
        """Integration setup schedules platforms immediately after clients."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )

        hass = RecordingHomeAssistant()
        platform_loads = []

        async def platform_load(platform: str) -> str:
            """Represent one Home Assistant asynchronous platform load."""
            return platform

        def create_platform_load(hass, platform, *args):
            load = platform_load(platform)
            platform_loads.append(load)
            self.addCleanup(load.close)
            return load

        load_platform = Mock(side_effect=create_platform_load)
        with patch.object(integration, "get_ip", return_value=["192.0.2.1"]), patch.object(
            integration, "tcp_client", return_value=DelayedDeviceClient()
        ), patch.object(
            integration, "async_load_platform", load_platform
        ), patch.object(
            integration,
            "async_track_time_interval",
            return_value=lambda: None,
        ), patch.object(
            integration, "time", create=True
        ) as time_module:
            setup_succeeded = integration.setup(hass, {DOMAIN: {}})

        self.assertTrue(setup_succeeded)
        time_module.sleep.assert_not_called()
        self.assertEqual(
            [call.args[1] for call in load_platform.call_args_list],
            ["light", "switch"],
        )
        self.assertTrue(all(inspect.isawaitable(load) for load in platform_loads))
        self.assertEqual(hass.loop.scheduled, [()])
        self.assertEqual(hass.created_tasks, platform_loads)

    def test_deferred_setup_skips_platforms_and_interval_after_stop(self) -> None:
        """Queued setup work rechecks both integration and Home Assistant stop."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )

        for stop_source in ("integration", "home_assistant"):
            with self.subTest(stop_source=stop_source):
                hass = RecordingHomeAssistant()
                hass.loop = DeferredRecordingLoop()
                load_platform = Mock(return_value=None)
                track_interval = Mock(return_value=lambda: None)

                with patch.object(
                    integration, "get_ip", return_value=[]
                ), patch.object(
                    integration, "async_load_platform", load_platform
                ), patch.object(
                    integration,
                    "async_track_time_interval",
                    track_interval,
                ):
                    self.assertTrue(integration.setup(hass, {DOMAIN: {}}))
                    if stop_source == "integration":
                        _, stop_listener = hass.bus.listeners[0]
                        stop_listener(None)
                    else:
                        hass.is_stopping = True
                    hass.loop.run_scheduled()

                load_platform.assert_not_called()
                track_interval.assert_not_called()
                self.assertEqual(hass.created_tasks, [])

    def test_setup_handles_stop_during_initial_client_construction(self) -> None:
        """Shutdown during initial discovery closes its late client."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant()
        created_clients = []
        load_platform = Mock(return_value=None)
        track_interval = Mock(return_value=lambda: None)

        def create_client(*args, **kwargs) -> DelayedDeviceClient:
            client = DelayedDeviceClient()
            created_clients.append(client)
            if hass.bus.listeners:
                _, stop_listener = hass.bus.listeners[0]
                stop_listener(None)
            return client

        with patch.object(
            integration, "get_ip", return_value=["192.0.2.10"]
        ), patch.object(
            integration, "tcp_client", side_effect=create_client
        ), patch.object(
            integration, "async_load_platform", load_platform
        ), patch.object(
            integration,
            "async_track_time_interval",
            track_interval,
        ):
            setup_succeeded = integration.setup(hass, {DOMAIN: {}})

        self.assertTrue(setup_succeeded)
        self.assertEqual(len(created_clients), 1)
        self.assertTrue(created_clients[0].closed)
        self.assertTrue(hass.data[DOMAIN]["stopped"])
        self.assertEqual(hass.data[DOMAIN]["tcp_client"], [])
        load_platform.assert_not_called()
        track_interval.assert_not_called()

    def test_late_client_close_failure_does_not_escape_discovery(self) -> None:
        """A late client cleanup failure cannot escape discovery."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([])

        class FailingCloseClient(DelayedDeviceClient):
            def close(self) -> None:
                """Simulate a late client cleanup failure."""
                raise RuntimeError("Injected late client close failure")

        client = FailingCloseClient()

        def create_client(*args, **kwargs) -> FailingCloseClient:
            integration._close_clients(hass)
            return client

        try:
            with self.assertLogs(integration.__name__, level="ERROR") as logs:
                with patch.object(
                    integration, "tcp_client", side_effect=create_client
                ):
                    integration._add_new_clients(hass, ["192.0.2.10"], "en")
        except RuntimeError as err:
            self.fail(f"Late client close failure escaped discovery: {err}")

        self.assertTrue(hass.data[DOMAIN]["stopped"])
        self.assertEqual(hass.data[DOMAIN]["tcp_client"], [])
        self.assertEqual(hass.data[DOMAIN]["ip"], [])
        self.assertTrue(any("192.0.2.10" in message for message in logs.output))

    def test_periodic_discovery_adds_each_new_client_once(self) -> None:
        """An initially empty integration discovers later devices without duplicates."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant()
        event_loop_thread_id = threading.get_ident()
        scheduled_intervals = []
        platform_loads = []
        created_clients = []

        async def platform_load() -> None:
            pass

        def create_platform_load(*args):
            load = platform_load()
            platform_loads.append(load)
            self.addCleanup(load.close)
            return load

        def create_client(ip: str, lang: str = "en") -> DelayedDeviceClient:
            client = DelayedDeviceClient()
            created_clients.append((ip, lang, client))
            return client

        def track_interval(hass, action, interval, **kwargs):
            scheduled_intervals.append((action, interval, kwargs))
            return lambda: None

        with patch.object(
            integration,
            "get_ip",
            side_effect=[[], ["192.0.2.10"], ["192.0.2.10"]],
        ), patch.object(
            integration, "tcp_client", side_effect=create_client
        ), patch.object(
            integration,
            "async_load_platform",
            new=Mock(side_effect=create_platform_load),
        ), patch.object(
            integration,
            "async_track_time_interval",
            side_effect=track_interval,
            create=True,
        ):
            self.assertTrue(integration.setup(hass, {DOMAIN: {"lang": "zh"}}))
            self.assertIn(DOMAIN, hass.data)
            self.assertEqual(hass.data[DOMAIN]["tcp_client"], [])
            self.assertEqual(len(scheduled_intervals), 1)
            action, interval, options = scheduled_intervals[0]
            self.assertEqual(interval, timedelta(seconds=60))
            self.assertTrue(options["cancel_on_shutdown"])

            subscriber = getattr(integration, "register_client_callback", None)
            self.assertTrue(callable(subscriber))
            notified_clients = []
            subscriber(hass, notified_clients.append)

            asyncio.run(action(datetime.now(UTC)))
            asyncio.run(action(datetime.now(UTC)))

        self.assertEqual(
            [(ip, lang) for ip, lang, _ in created_clients],
            [("192.0.2.10", "zh")],
        )
        self.assertEqual(notified_clients, [created_clients[0][2]])
        self.assertEqual(hass.data[DOMAIN]["ip"], ["192.0.2.10"])
        self.assertEqual(len(hass.executor_thread_ids), 2)
        self.assertTrue(
            all(
                thread_id != event_loop_thread_id
                for thread_id in hass.executor_thread_ids
            )
        )

        self.assertEqual(len(hass.bus.listeners), 1)
        _, stop_listener = hass.bus.listeners[0]
        stop_listener(None)
        self.assertTrue(created_clients[0][2].closed)

    def test_concurrent_discovery_serializes_duplicate_address(self) -> None:
        """Concurrent discovery tasks create one client for one address."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([])
        timeout = 5
        first_discovery_started = threading.Event()
        allow_first_discovery = threading.Event()
        second_discovery_waiting = threading.Event()
        get_ip_lock = threading.Lock()
        get_ip_calls = 0
        created_clients = []
        notified_clients = []
        thread_errors: queue.Queue[BaseException] = queue.Queue()
        hass.data[DOMAIN]["discovery_lock"] = ObservedRLock(
            second_discovery_waiting
        )
        integration.register_client_callback(hass, notified_clients.append)

        def discover_address() -> list[str]:
            nonlocal get_ip_calls
            with get_ip_lock:
                get_ip_calls += 1
                call = get_ip_calls
            if call == 1:
                first_discovery_started.set()
                if not allow_first_discovery.wait(timeout):
                    raise TimeoutError(
                        "Timed out waiting to release first discovery"
                    )
            return ["192.0.2.10"]

        def create_client(ip: str, lang: str = "en") -> DelayedDeviceClient:
            client = DelayedDeviceClient()
            created_clients.append((ip, lang, client))
            return client

        def discover_client() -> None:
            try:
                integration._discover_new_clients(hass, "en", ())
            except BaseException as err:
                thread_errors.put(err)

        first_thread = threading.Thread(target=discover_client, daemon=True)
        second_thread = threading.Thread(target=discover_client, daemon=True)
        with patch.object(
            integration, "get_ip", side_effect=discover_address
        ), patch.object(
            integration, "tcp_client", side_effect=create_client
        ):
            first_thread.start()
            try:
                self.assertTrue(first_discovery_started.wait(timeout))
                second_thread.start()
                self.assertTrue(second_discovery_waiting.wait(timeout))
            finally:
                allow_first_discovery.set()
                first_thread.join(timeout)
                if second_thread.ident is not None:
                    second_thread.join(timeout)

        self.assertTrue(thread_errors.empty())
        self.assertFalse(first_thread.is_alive())
        self.assertFalse(second_thread.is_alive())
        self.assertEqual(get_ip_calls, 2)
        self.assertEqual(
            [(ip, lang) for ip, lang, _ in created_clients],
            [("192.0.2.10", "en")],
        )
        self.assertEqual(
            hass.data[DOMAIN]["tcp_client"], [created_clients[0][2]]
        )
        self.assertEqual(notified_clients, [created_clients[0][2]])
        self.assertEqual(hass.data[DOMAIN]["known_ips"], {"192.0.2.10"})

    def test_one_discovery_batch_deduplicates_addresses(self) -> None:
        """Duplicate configured and broadcast addresses create one client."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([])
        created_clients = []

        def create_client(ip: str, lang: str = "en") -> DelayedDeviceClient:
            client = DelayedDeviceClient()
            created_clients.append((ip, lang, client))
            return client

        with patch.object(integration, "tcp_client", side_effect=create_client):
            integration._add_new_clients(
                hass,
                ["192.0.2.10", "192.0.2.10", "192.0.2.10"],
                "en",
            )

        self.assertEqual(len(created_clients), 1)
        self.assertEqual(hass.data[DOMAIN]["ip"], ["192.0.2.10"])

    def test_client_construction_failure_does_not_block_or_reserve_ips(self) -> None:
        """A failed address remains retryable without blocking later addresses."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([])
        attempts = []
        created_clients = []
        notified_clients = []
        integration.register_client_callback(hass, notified_clients.append)

        def create_client(ip: str, lang: str = "en") -> DelayedDeviceClient:
            attempts.append(ip)
            if ip == "192.0.2.10" and attempts.count(ip) == 1:
                raise RuntimeError("Injected client construction failure")
            client = DelayedDeviceClient()
            created_clients.append((ip, lang, client))
            return client

        try:
            with self.assertLogs(integration.__name__, level="ERROR"):
                with patch.object(
                    integration, "tcp_client", side_effect=create_client
                ):
                    integration._add_new_clients(
                        hass, ["192.0.2.10", "192.0.2.11"], "zh"
                    )
                    integration._add_new_clients(hass, ["192.0.2.10"], "zh")
        except RuntimeError as err:
            self.fail(f"Client construction failure escaped discovery: {err}")

        self.assertEqual(
            attempts,
            ["192.0.2.10", "192.0.2.11", "192.0.2.10"],
        )
        self.assertEqual(
            [(ip, lang) for ip, lang, _ in created_clients],
            [("192.0.2.11", "zh"), ("192.0.2.10", "zh")],
        )
        self.assertEqual(
            notified_clients,
            [created_clients[0][2], created_clients[1][2]],
        )
        self.assertEqual(
            hass.data[DOMAIN]["known_ips"],
            {"192.0.2.10", "192.0.2.11"},
        )
        self.assertEqual(
            hass.data[DOMAIN]["ip"], ["192.0.2.11", "192.0.2.10"]
        )

    def test_callback_replay_failure_does_not_block_later_clients(self) -> None:
        """One replay failure cannot escape or hide remaining clients."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        first = DelayedDeviceClient()
        second = DelayedDeviceClient()
        hass = RecordingHomeAssistant([first, second])
        replayed_clients = []

        def replay_client(client: DelayedDeviceClient) -> None:
            if client is first:
                raise RuntimeError("Injected callback replay failure")
            replayed_clients.append(client)

        try:
            with self.assertLogs(integration.__name__, level="ERROR"):
                integration.register_client_callback(hass, replay_client)
        except RuntimeError as err:
            self.fail(f"Client callback failure escaped replay: {err}")

        self.assertEqual(replayed_clients, [second])
        self.assertIn(
            replay_client, hass.data[DOMAIN]["client_callbacks"]
        )

    def test_callback_replay_does_not_hold_integration_lock(self) -> None:
        """Shutdown can proceed while an existing-client callback is busy."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([DelayedDeviceClient()])
        callback_started = threading.Event()
        allow_callback = threading.Event()
        stop_finished = threading.Event()
        thread_errors: queue.Queue[BaseException] = queue.Queue()
        timeout = 5

        def blocking_callback(client: DelayedDeviceClient) -> None:
            callback_started.set()
            allow_callback.wait()

        def register_callback() -> None:
            try:
                integration.register_client_callback(hass, blocking_callback)
            except BaseException as err:
                thread_errors.put(err)

        def stop_integration() -> None:
            try:
                integration._close_clients(hass)
            except BaseException as err:
                thread_errors.put(err)
            finally:
                stop_finished.set()

        registration_thread = threading.Thread(
            target=register_callback, daemon=True
        )
        stop_thread = threading.Thread(target=stop_integration, daemon=True)
        registration_thread.start()
        try:
            self.assertTrue(callback_started.wait(timeout))
            stop_thread.start()
            self.assertTrue(stop_finished.wait(timeout))
            self.assertTrue(hass.data[DOMAIN]["stopped"])
        finally:
            allow_callback.set()
            registration_thread.join(timeout)
            if stop_thread.ident is not None:
                stop_thread.join(timeout)

        self.assertTrue(thread_errors.empty())
        self.assertFalse(registration_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())

    def test_stop_drains_ready_callback_without_integration_lock(self) -> None:
        """A readiness callback can access integration state during shutdown."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        client = DelayedDeviceClient(LIGHT_TYPE_CODE)
        hass = RecordingHomeAssistant([client])
        callback_started = threading.Event()
        stop_finished = threading.Event()
        lock_results = []
        thread_errors: queue.Queue[BaseException] = queue.Queue()
        timeout = 5

        def ready_callback(_client: DelayedDeviceClient) -> None:
            callback_started.set()
            if not client.stop_signaled.wait(timeout):
                raise AssertionError("Client stop was not signaled")
            state_lock = hass.data[DOMAIN]["lock"]
            acquired = state_lock.acquire(timeout=timeout)
            lock_results.append(acquired)
            if acquired:
                state_lock.release()

        def publish_ready() -> None:
            try:
                client.add_ready_callback(ready_callback)
            except BaseException as err:
                thread_errors.put(err)

        def stop_integration() -> None:
            try:
                integration._close_clients(hass)
            except BaseException as err:
                thread_errors.put(err)
            finally:
                stop_finished.set()

        callback_thread = threading.Thread(target=publish_ready, daemon=True)
        stop_thread = threading.Thread(target=stop_integration, daemon=True)
        try:
            callback_thread.start()
            self.assertTrue(callback_started.wait(timeout))
            stop_thread.start()
            self.assertTrue(stop_finished.wait(timeout))
        finally:
            client.signal_stop()
            if callback_thread.ident is not None:
                callback_thread.join(timeout)
            if stop_thread.ident is not None:
                stop_thread.join(timeout)

        self.assertTrue(thread_errors.empty())
        self.assertFalse(callback_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertEqual(lock_results, [True])

    def test_stop_signals_all_clients_before_draining_callbacks(self) -> None:
        """A blocked callback cannot delay another client's stop signal."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        first = DelayedDeviceClient(LIGHT_TYPE_CODE)
        second = DelayedDeviceClient()
        hass = RecordingHomeAssistant([first, second])
        callback_started = threading.Event()
        allow_callback = threading.Event()
        stop_finished = threading.Event()
        thread_errors: queue.Queue[BaseException] = queue.Queue()
        timeout = 5

        def blocking_callback(_client: DelayedDeviceClient) -> None:
            callback_started.set()
            if not allow_callback.wait(timeout):
                raise TimeoutError("Timed out waiting to release callback")

        def publish_ready() -> None:
            try:
                first.add_ready_callback(blocking_callback)
            except BaseException as err:
                thread_errors.put(err)

        def stop_integration() -> None:
            try:
                integration._close_clients(hass)
            except BaseException as err:
                thread_errors.put(err)
            finally:
                stop_finished.set()

        callback_thread = threading.Thread(target=publish_ready, daemon=True)
        stop_thread = threading.Thread(target=stop_integration, daemon=True)
        callback_thread.start()
        try:
            self.assertTrue(callback_started.wait(timeout))
            stop_thread.start()
            self.assertTrue(second.stop_signaled.wait(timeout))
        finally:
            allow_callback.set()
            callback_thread.join(timeout)
            if stop_thread.ident is not None:
                stop_thread.join(timeout)

        self.assertTrue(thread_errors.empty())
        self.assertTrue(stop_finished.is_set())
        self.assertFalse(callback_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())

    def test_stop_marks_replayed_client_before_callback_dispatch(self) -> None:
        """Shutdown rejects a replay callback that passed the state check."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        timeout = 5
        close_started = threading.Event()
        allow_close = threading.Event()
        client = BlockingCloseDeviceClient(
            close_started, allow_close, timeout
        )
        hass = RecordingHomeAssistant([client])
        callback_started = threading.Event()
        allow_callback = threading.Event()
        callback_release_timed_out = threading.Event()
        added_clients = []
        thread_errors: queue.Queue[BaseException] = queue.Queue()

        def add_ready_client(replayed_client: DelayedDeviceClient) -> None:
            callback_started.set()
            if not allow_callback.wait(timeout):
                callback_release_timed_out.set()
                raise TimeoutError("Timed out waiting to release replay callback")
            replayed_client.add_ready_callback(added_clients.append)

        def register_callback() -> None:
            try:
                integration.register_client_callback(hass, add_ready_client)
            except BaseException as err:
                thread_errors.put(err)

        def stop_integration() -> None:
            try:
                integration._close_clients(hass)
            except BaseException as err:
                thread_errors.put(err)

        registration_thread = threading.Thread(
            target=register_callback, daemon=True
        )
        stop_thread = threading.Thread(target=stop_integration, daemon=True)
        registration_thread.start()
        try:
            self.assertTrue(callback_started.wait(timeout))
            stop_thread.start()
            self.assertTrue(close_started.wait(timeout))
            allow_callback.set()
            registration_thread.join(timeout)
        finally:
            allow_callback.set()
            allow_close.set()
            registration_thread.join(timeout)
            if stop_thread.ident is not None:
                stop_thread.join(timeout)

        self.assertTrue(thread_errors.empty())
        self.assertFalse(registration_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertFalse(callback_release_timed_out.is_set())
        self.assertFalse(client.release_timed_out.is_set())
        self.assertEqual(added_clients, [])

    def test_notification_failure_does_not_block_callbacks_or_addresses(self) -> None:
        """One platform failure cannot stop the discovery notification batch."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([])
        successful_notifications = []
        created_clients = []

        def fail_notification(client: DelayedDeviceClient) -> None:
            raise RuntimeError("Injected client notification failure")

        def create_client(ip: str, lang: str = "en") -> DelayedDeviceClient:
            client = DelayedDeviceClient()
            created_clients.append((ip, client))
            return client

        integration.register_client_callback(hass, fail_notification)
        integration.register_client_callback(
            hass, successful_notifications.append
        )

        try:
            with self.assertLogs(integration.__name__, level="ERROR"):
                with patch.object(
                    integration, "tcp_client", side_effect=create_client
                ):
                    integration._add_new_clients(
                        hass, ["192.0.2.10", "192.0.2.11"], "en"
                    )
        except RuntimeError as err:
            self.fail(f"Client callback failure escaped notification: {err}")

        self.assertEqual(
            successful_notifications,
            [created_clients[0][1], created_clients[1][1]],
        )
        self.assertEqual(
            hass.data[DOMAIN]["ip"], ["192.0.2.10", "192.0.2.11"]
        )

    def test_client_notification_does_not_hold_integration_lock(self) -> None:
        """Shutdown can proceed while a new-client callback is busy."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([])
        client = DelayedDeviceClient()
        callback_started = threading.Event()
        allow_callback = threading.Event()
        stop_finished = threading.Event()
        thread_errors: queue.Queue[BaseException] = queue.Queue()
        timeout = 5

        def blocking_callback(ready_client: DelayedDeviceClient) -> None:
            callback_started.set()
            allow_callback.wait()

        def discover_client() -> None:
            try:
                integration._add_new_clients(
                    hass, ["192.0.2.10"], "en"
                )
            except BaseException as err:
                thread_errors.put(err)

        def stop_integration() -> None:
            try:
                integration._close_clients(hass)
            except BaseException as err:
                thread_errors.put(err)
            finally:
                stop_finished.set()

        integration.register_client_callback(hass, blocking_callback)
        discovery_thread = threading.Thread(target=discover_client, daemon=True)
        stop_thread = threading.Thread(target=stop_integration, daemon=True)
        with patch.object(integration, "tcp_client", return_value=client):
            discovery_thread.start()
            try:
                self.assertTrue(callback_started.wait(timeout))
                stop_thread.start()
                self.assertTrue(stop_finished.wait(timeout))
                self.assertTrue(hass.data[DOMAIN]["stopped"])
            finally:
                allow_callback.set()
                discovery_thread.join(timeout)
                if stop_thread.ident is not None:
                    stop_thread.join(timeout)

        self.assertTrue(thread_errors.empty())
        self.assertFalse(discovery_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertTrue(client.closed)

    def test_stop_marks_new_client_before_callback_dispatch(self) -> None:
        """Shutdown rejects a new-client callback past its state check."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        timeout = 5
        close_started = threading.Event()
        allow_close = threading.Event()
        client = BlockingCloseDeviceClient(
            close_started, allow_close, timeout
        )
        hass = RecordingHomeAssistant([])
        callback_started = threading.Event()
        allow_callback = threading.Event()
        callback_release_timed_out = threading.Event()
        added_clients = []
        thread_errors: queue.Queue[BaseException] = queue.Queue()

        def add_ready_client(ready_client: DelayedDeviceClient) -> None:
            callback_started.set()
            if not allow_callback.wait(timeout):
                callback_release_timed_out.set()
                raise TimeoutError("Timed out waiting to release client callback")
            ready_client.add_ready_callback(added_clients.append)

        integration.register_client_callback(hass, add_ready_client)

        def discover_client() -> None:
            try:
                integration._add_new_clients(
                    hass, ["192.0.2.10"], "en"
                )
            except BaseException as err:
                thread_errors.put(err)

        def stop_integration() -> None:
            try:
                integration._close_clients(hass)
            except BaseException as err:
                thread_errors.put(err)

        discovery_thread = threading.Thread(target=discover_client, daemon=True)
        stop_thread = threading.Thread(target=stop_integration, daemon=True)
        with patch.object(integration, "tcp_client", return_value=client):
            discovery_thread.start()
            try:
                self.assertTrue(callback_started.wait(timeout))
                stop_thread.start()
                self.assertTrue(close_started.wait(timeout))
                allow_callback.set()
                discovery_thread.join(timeout)
            finally:
                allow_callback.set()
                allow_close.set()
                discovery_thread.join(timeout)
                if stop_thread.ident is not None:
                    stop_thread.join(timeout)

        self.assertTrue(thread_errors.empty())
        self.assertFalse(discovery_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertFalse(callback_release_timed_out.is_set())
        self.assertFalse(client.release_timed_out.is_set())
        self.assertEqual(added_clients, [])

    def test_reentrant_stop_skips_remaining_client_callbacks(self) -> None:
        """A callback-triggered stop prevents later platform notification."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([])
        later_notifications = []
        client = DelayedDeviceClient()

        integration.register_client_callback(
            hass, lambda _client: integration._close_clients(hass)
        )
        integration.register_client_callback(hass, later_notifications.append)

        with patch.object(integration, "tcp_client", return_value=client):
            integration._add_new_clients(hass, ["192.0.2.10"], "en")

        self.assertTrue(client.closed)
        self.assertEqual(later_notifications, [])

    def test_stopped_integration_rejects_callback_registration(self) -> None:
        """Shutdown prevents retaining or replaying new platform callbacks."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        client = DelayedDeviceClient()
        hass = RecordingHomeAssistant([client])
        replayed_clients = []

        integration._close_clients(hass)
        integration.register_client_callback(hass, replayed_clients.append)

        self.assertEqual(replayed_clients, [])
        self.assertEqual(hass.data[DOMAIN]["client_callbacks"], [])

    def test_close_failure_does_not_block_remaining_clients(self) -> None:
        """One client cleanup failure cannot abort integration shutdown."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )

        class FailingCloseClient(DelayedDeviceClient):
            def close(self) -> None:
                raise RuntimeError("Injected client close failure")

        later_client = DelayedDeviceClient()
        hass = RecordingHomeAssistant(
            [FailingCloseClient(), later_client]
        )

        try:
            with self.assertLogs(integration.__name__, level="ERROR"):
                integration._close_clients(hass)
        except RuntimeError as err:
            self.fail(f"Client close failure escaped shutdown: {err}")

        self.assertTrue(later_client.closed)

    def test_stopped_integration_rejects_late_discovery_results(self) -> None:
        """An in-flight discovery cannot add clients after lifecycle cleanup."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([])
        created_clients = []

        integration._close_clients(hass)
        with patch.object(
            integration,
            "tcp_client",
            side_effect=lambda *args, **kwargs: created_clients.append(
                DelayedDeviceClient()
            ),
        ):
            integration._add_new_clients(hass, ["192.0.2.10"], "en")

        self.assertEqual(created_clients, [])
        self.assertEqual(hass.data[DOMAIN]["tcp_client"], [])

    def test_stop_waits_for_client_finishing_during_discovery(self) -> None:
        """Shutdown waits for a client constructed by admitted discovery."""
        integration = importlib.import_module(
            "custom_components.hass_cozylife_local_pull"
        )
        hass = RecordingHomeAssistant([])
        client = DelayedDeviceClient()
        construction_started = threading.Event()
        allow_construction = threading.Event()
        construction_timed_out = threading.Event()
        stop_waiting = threading.Event()
        stop_finished = threading.Event()
        thread_errors: queue.Queue[BaseException] = queue.Queue()
        timeout = 5
        hass.data[DOMAIN]["discovery_lock"] = ObservedRLock(stop_waiting)

        def create_client(*args, **kwargs) -> DelayedDeviceClient:
            construction_started.set()
            if not allow_construction.wait(timeout * 2):
                construction_timed_out.set()
                raise TimeoutError(
                    "Timed out waiting to release client construction"
                )
            return client

        def discover_client() -> None:
            try:
                integration._discover_new_clients(
                    hass, "en", ("192.0.2.10",)
                )
            except BaseException as err:
                thread_errors.put(err)

        def stop_integration() -> None:
            try:
                integration._close_clients(hass)
            except BaseException as err:
                thread_errors.put(err)
            finally:
                stop_finished.set()

        with patch.object(
            integration, "get_ip", return_value=[]
        ), patch.object(
            integration, "tcp_client", side_effect=create_client
        ):
            discovery_thread = threading.Thread(
                target=discover_client, daemon=True
            )
            stop_thread = threading.Thread(
                target=stop_integration, daemon=True
            )
            discovery_thread.start()
            try:
                self.assertTrue(construction_started.wait(timeout))
                stop_thread.start()
                self.assertTrue(stop_waiting.wait(timeout))
                self.assertFalse(stop_finished.is_set())
            finally:
                allow_construction.set()
                discovery_thread.join(timeout)
                if stop_thread.ident is not None:
                    stop_thread.join(timeout)

        self.assertFalse(discovery_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertFalse(construction_timed_out.is_set())
        self.assertTrue(thread_errors.empty())
        self.assertTrue(stop_finished.is_set())
        self.assertTrue(client.closed)
        self.assertEqual(hass.data[DOMAIN]["tcp_client"], [])

    def test_platforms_add_clients_that_become_ready_later(self) -> None:
        """A delayed handshake adds the entity without reloading its platform."""
        cases = (
            (light, LIGHT_TYPE_CODE, light.CozyLifeLight),
            (switch, SWITCH_TYPE_CODE, switch.CozyLifeSwitch),
        )

        for platform, device_type_code, entity_type in cases:
            with self.subTest(platform=platform.__name__):
                client = DelayedDeviceClient()
                hass = RecordingHomeAssistant([client])
                added_entities = []

                platform.setup_platform(
                    hass, {}, added_entities.extend, discovery_info={}
                )
                self.assertEqual(added_entities, [])

                client.become_ready(device_type_code)

                self.assertEqual(len(added_entities), 1)
                self.assertIsInstance(added_entities[0], entity_type)

    def test_platforms_register_callbacks_for_already_ready_clients(self) -> None:
        """A handshake completed before platform setup cannot be missed."""
        cases = (
            (light, LIGHT_TYPE_CODE, light.CozyLifeLight),
            (switch, SWITCH_TYPE_CODE, switch.CozyLifeSwitch),
        )

        for platform, device_type_code, entity_type in cases:
            with self.subTest(platform=platform.__name__):
                client = DelayedDeviceClient(device_type_code)
                hass = RecordingHomeAssistant([client])
                added_entities = []

                platform.setup_platform(
                    hass, {}, added_entities.extend, discovery_info={}
                )

                self.assertEqual(client.registration_count, 1)
                self.assertEqual(len(added_entities), 1)
                self.assertIsInstance(added_entities[0], entity_type)

    def test_platforms_subscribe_to_clients_discovered_later(self) -> None:
        """Loaded platforms attach readiness callbacks to future clients."""
        cases = (
            (light, LIGHT_TYPE_CODE, light.CozyLifeLight),
            (switch, SWITCH_TYPE_CODE, switch.CozyLifeSwitch),
        )

        for platform, device_type_code, entity_type in cases:
            with self.subTest(platform=platform.__name__):
                hass = RecordingHomeAssistant([])
                added_entities = []

                platform.setup_platform(
                    hass, {}, added_entities.extend, discovery_info={}
                )

                callbacks = hass.data[DOMAIN]["client_callbacks"]
                self.assertEqual(len(callbacks), 1)
                client = DelayedDeviceClient()
                callbacks[0](client)
                client.become_ready(device_type_code)

                self.assertEqual(len(added_entities), 1)
                self.assertIsInstance(added_entities[0], entity_type)

    def test_platforms_ignore_ready_clients_for_another_device_type(self) -> None:
        """A readiness callback preserves each platform's type boundary."""
        cases = (
            (light, SWITCH_TYPE_CODE),
            (switch, LIGHT_TYPE_CODE),
        )

        for platform, device_type_code in cases:
            with self.subTest(platform=platform.__name__):
                client = DelayedDeviceClient(device_type_code)
                hass = RecordingHomeAssistant([client])
                added_entities = []

                platform.setup_platform(
                    hass, {}, added_entities.extend, discovery_info={}
                )

                self.assertEqual(client.registration_count, 1)
                self.assertEqual(added_entities, [])
