"""Regression tests for CozyLife device countdown number entities."""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
from datetime import UTC, datetime, timedelta
import threading
import unittest
from unittest.mock import Mock, patch

from homeassistant.components.number import (
    NumberDeviceClass,
    NumberEntity,
    NumberMode,
)
from homeassistant.const import UnitOfTime
from homeassistant.core import HomeAssistant, is_callback
from homeassistant.exceptions import HomeAssistantError

from custom_components.hass_cozylife_local_pull.tcp_client import (
    DeviceCommandRejectedError,
)


class FakeTcpClient:
    """Provide deterministic capabilities and state to a number entity."""

    device_id = "device-1234"
    device_model_name = "Test Light"
    device_type_code = "01"

    def __init__(
        self,
        dpid: list[int],
        state: dict[str, int],
        control_result: bool = True,
    ) -> None:
        self.dpid = dpid
        self.state = state
        self.control_result = control_result
        self.query_count = 0
        self.last_query_attributes: list[int] | None = None
        self.last_payload: dict[str, int] | None = None
        self.state_callbacks = []
        self.last_state_sequence_number = 1699999999999

    def query(self, attributes: list[int] | None = None) -> dict[str, int]:
        """Return the configured device state."""
        self.query_count += 1
        self.last_query_attributes = attributes
        state = self.state.copy()
        if state and self.state_callbacks:
            self.report(state)
        return state

    def control(self, payload: dict[str, int]) -> bool:
        """Record the latest control payload."""
        self.last_payload = payload
        if self.control_result:
            self.report(payload)
        return self.control_result

    def add_state_callback(self, callback):
        """Subscribe one entity to active device reports."""
        self.state_callbacks.append(callback)

        def remove_callback() -> None:
            self.state_callbacks.remove(callback)

        return remove_callback

    def report(
        self,
        state: dict[str, int],
        sequence_number: int | None = None,
    ) -> None:
        """Deliver one active report to all subscribed entities."""
        if sequence_number is None:
            sequence_number = self.last_state_sequence_number + 1
        if sequence_number < self.last_state_sequence_number:
            return
        self.last_state_sequence_number = sequence_number
        for callback in tuple(self.state_callbacks):
            callback(state, sequence_number)


class TimestampedCountdownClient(FakeTcpClient):
    """Publish timestamped control replies to a countdown entity."""

    def __init__(
        self,
        dpid: list[int],
        state: dict[str, int],
        control_reply: dict[str, int],
    ) -> None:
        super().__init__(dpid, state)
        self.control_reply = control_reply

    def control(self, payload: dict[str, int]) -> bool:
        """Publish the device reply while retaining the requested payload."""
        self.last_payload = payload
        self.report(self.control_reply, 1700000000000)
        return True


class ImmediateLoop:
    """Run thread-safe callbacks immediately in entity unit tests."""

    def call_soon_threadsafe(self, callback, *args) -> None:
        """Execute one event-loop callback without another thread."""
        callback(*args)


class QueuedLoop:
    """Queue thread-safe callbacks until the test drains them."""

    def __init__(self) -> None:
        self.callbacks = []

    def call_soon_threadsafe(self, callback, *args) -> None:
        """Record one callback without running it on the caller thread."""
        self.callbacks.append((callback, args))

    def drain(self) -> None:
        """Run every queued callback in scheduling order."""
        callbacks, self.callbacks = self.callbacks, []
        for callback, args in callbacks:
            callback(*args)


class FakeHomeAssistant:
    """Provide the event-loop boundary used by countdown entities."""

    def __init__(self, loop=None) -> None:
        self.loop = loop or ImmediateLoop()

    async def async_add_executor_job(self, target, *args):
        """Run one synchronous device operation for entity unit tests."""
        result = target(*args)
        await asyncio.sleep(0)
        return result


class ThreadedHomeAssistant:
    """Run device input and output in a worker while retaining the real loop."""

    def __init__(self, loop) -> None:
        self.loop = loop

    async def async_add_executor_job(self, target, *args):
        """Execute one blocking device operation in a worker thread."""
        result = await asyncio.to_thread(target, *args)
        await asyncio.sleep(0)
        return result


def _light_countdown(number_module, client):
    """Create the existing light countdown variant for shared state tests."""
    return number_module.CozyLifeCountdown(client, "13", "Countdown")


class NumberImportTest(unittest.TestCase):
    """Verify the countdown platform is available to Home Assistant."""

    def test_number_module_is_available(self) -> None:
        """The integration exposes a number platform module."""
        module = importlib.util.find_spec(
            "custom_components.hass_cozylife_local_pull.number"
        )

        self.assertIsNotNone(module)


class CountdownDeviceVariantTest(unittest.IsolatedAsyncioTestCase):
    """Verify switch and motor countdowns use their configured identity and DPID."""

    CASES = (
        ("2", "Countdown 1", "Test Switch", "device-1234_countdown_1"),
        ("6", "Countdown", "Test Motor", "device-1234_countdown"),
    )

    def test_variants_expose_expected_identity(self) -> None:
        """Each variant derives its name and unique ID from its label suffix."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )

        for dp_id, label_suffix, model_name, unique_id in self.CASES:
            with self.subTest(dp_id=dp_id):
                client = FakeTcpClient([1, int(dp_id)], {dp_id: 0})
                client.device_model_name = model_name

                entity = number.CozyLifeCountdown(
                    client, dp_id, label_suffix
                )

                self.assertEqual(
                    entity.name,
                    f"{model_name} 1234 {label_suffix}",
                )
                self.assertEqual(entity.unique_id, unique_id)

    async def test_variants_query_configured_dpid(self) -> None:
        """Initial state queries target the variant's own DPID."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )

        for dp_id, label_suffix, _model_name, _unique_id in self.CASES:
            with self.subTest(dp_id=dp_id):
                client = FakeTcpClient([1, int(dp_id)], {dp_id: 60})
                entity = number.CozyLifeCountdown(
                    client, dp_id, label_suffix
                )
                entity.hass = FakeHomeAssistant()

                with patch.object(
                    number, "async_track_time_interval", return_value=Mock()
                ):
                    await entity.async_added_to_hass()

                self.assertEqual(client.last_query_attributes, [int(dp_id)])
                self.assertEqual(entity.native_value, 60)

    async def test_variants_apply_configured_active_report(self) -> None:
        """Active reports update only the variant's own DPID state."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )

        for dp_id, label_suffix, _model_name, _unique_id in self.CASES:
            with self.subTest(dp_id=dp_id):
                client = FakeTcpClient([1, int(dp_id)], {dp_id: 0})
                entity = number.CozyLifeCountdown(
                    client, dp_id, label_suffix
                )
                entity.hass = FakeHomeAssistant()

                with patch.object(
                    number, "async_track_time_interval", return_value=Mock()
                ), patch.object(entity, "async_write_ha_state"):
                    await entity.async_added_to_hass()
                    client.report({dp_id: 45})

                self.assertEqual(entity.native_value, 45)

    async def test_variants_control_configured_dpid(self) -> None:
        """Number commands write the variant's own DPID."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )

        for dp_id, label_suffix, _model_name, _unique_id in self.CASES:
            with self.subTest(dp_id=dp_id):
                client = FakeTcpClient([1, int(dp_id)], {dp_id: 0})
                entity = number.CozyLifeCountdown(
                    client, dp_id, label_suffix
                )
                entity.hass = FakeHomeAssistant()

                with patch.object(
                    number, "async_track_time_interval", return_value=Mock()
                ), patch.object(entity, "async_write_ha_state"):
                    await entity.async_added_to_hass()
                    await entity.async_set_native_value(30)

                self.assertEqual(client.last_payload, {dp_id: 30})
                self.assertEqual(entity.native_value, 30)


class LightCountdownStateTest(unittest.IsolatedAsyncioTestCase):
    """Verify the entity reads countdown state from the device."""

    async def test_control_applies_authoritative_countdown_reply(self) -> None:
        """Countdown state comes from the reply instead of the requested value."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = TimestampedCountdownClient(
            [1, 13], {"13": 0}, {"13": 5}
        )
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state") as write_state:
            await entity.async_added_to_hass()
            await entity.async_set_native_value(30)

        self.assertEqual(entity.native_value, 5)
        write_state.assert_called_once_with()

    async def test_entity_reads_current_countdown_seconds(self) -> None:
        """DPID 13 supplies the initial native value."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        self.assertTrue(hasattr(number, "CozyLifeCountdown"))

        client = FakeTcpClient([1, 13], {"1": 1, "13": 60})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()
        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ):
            await entity.async_added_to_hass()

        self.assertEqual(entity.native_value, 60)
        self.assertTrue(entity.available)
        self.assertEqual(client.query_count, 1)
        self.assertEqual(client.last_query_attributes, [13])

    def test_entity_exposes_countdown_number_contract(self) -> None:
        """The entity advertises the device protocol's numeric limits."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"1": 1, "13": 60})

        entity = _light_countdown(number, client)

        self.assertEqual(entity.native_min_value, 0)
        self.assertEqual(entity.native_max_value, 86400)
        self.assertEqual(entity.native_step, 1)
        self.assertEqual(entity.mode, NumberMode.BOX)
        self.assertEqual(entity.device_class, NumberDeviceClass.DURATION)
        self.assertEqual(entity.native_unit_of_measurement, UnitOfTime.SECONDS)
        self.assertEqual(entity.name, "Test Light 1234 Countdown")
        self.assertEqual(entity.unique_id, "device-1234_countdown")

    async def test_async_update_refreshes_countdown_from_device(self) -> None:
        """A forced refresh replaces the cached value with the device value."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"1": 1, "13": 60})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()
        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state"):
            await entity.async_added_to_hass()
            client.state["13"] = 30

            self.assertTrue(hasattr(entity, "async_update"))
            await entity.async_update()

        self.assertEqual(entity.native_value, 30)
        self.assertTrue(entity.available)
        self.assertEqual(client.query_count, 2)

    async def test_missing_countdown_marks_entity_unavailable(self) -> None:
        """A failed query preserves the last reported countdown value."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"1": 1, "13": 60})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()
        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ):
            await entity.async_added_to_hass()
        client.state = {}

        self.assertTrue(hasattr(entity, "async_update"))
        await entity.async_update()

        self.assertEqual(entity.native_value, 60)
        self.assertFalse(entity.available)

    def test_invalid_device_countdown_preserves_last_valid_value(self) -> None:
        """Malformed or out-of-range query data is treated as unavailable."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )

        for invalid_value in (None, "invalid", -1, 86401, 1.5, 10**1000):
            with self.subTest(value=invalid_value):
                client = FakeTcpClient([1, 13], {"13": invalid_value})
                entity = _light_countdown(number, client)
                entity._apply_countdown_query({"13": 60})

                try:
                    entity._apply_countdown_query({"13": invalid_value})
                except (TypeError, ValueError) as err:
                    self.fail(f"Invalid device countdown escaped: {err}")

                self.assertEqual(entity.native_value, 60)
                self.assertFalse(entity.available)
                self.assertIsNone(entity._countdown_deadline)

    async def test_async_update_applies_query_on_event_loop(self) -> None:
        """A forced refresh queries in a worker and applies on the event loop."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        entity = _light_countdown(number, client)
        event_loop_thread = threading.get_ident()
        entity.hass = ThreadedHomeAssistant(asyncio.get_running_loop())
        self.assertTrue(hasattr(entity, "async_update"))
        query_threads = []
        apply_threads = []
        query = client.query
        set_countdown_state = entity._set_countdown_state

        def record_query(attributes=None):
            query_threads.append(threading.get_ident())
            return query(attributes)

        def record_state_application(value):
            apply_threads.append(threading.get_ident())
            set_countdown_state(value)

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(
            client, "query", side_effect=record_query
        ), patch.object(
            entity, "_set_countdown_state", side_effect=record_state_application
        ), patch.object(entity, "async_write_ha_state"):
            await entity.async_added_to_hass()
            query_threads.clear()
            apply_threads.clear()
            client.state["13"] = 20
            await entity.async_update()

        self.assertEqual(len(query_threads), 1)
        self.assertNotIn(event_loop_thread, query_threads)
        self.assertEqual(apply_threads, [event_loop_thread])


class LightCountdownInitialQueryTest(unittest.IsolatedAsyncioTestCase):
    """Verify initial device input and output does not block entity creation."""

    async def test_initial_query_runs_in_executor_after_entity_is_added(
        self,
    ) -> None:
        """Construction is local and the first DPID 13 query uses a worker."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 60})
        event_loop_thread = threading.get_ident()
        query_threads = []
        query = client.query

        def record_query(attributes=None):
            query_threads.append(threading.get_ident())
            return query(attributes)

        with patch.object(client, "query", side_effect=record_query):
            entity = _light_countdown(number, client)
            self.assertEqual(query_threads, [])
            entity.hass = ThreadedHomeAssistant(asyncio.get_running_loop())
            await entity.async_added_to_hass()

        self.assertEqual(len(query_threads), 1)
        self.assertNotIn(event_loop_thread, query_threads)
        self.assertEqual(entity.native_value, 60)
        self.assertTrue(entity.available)

    async def test_initial_query_subscribes_before_interleaved_report(
        self,
    ) -> None:
        """The first query cannot miss a newer DPID 13 active report."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()
        subscribed_during_query = []

        def report_then_fail(_attributes=None) -> dict[str, int]:
            subscribed_during_query.append(bool(client.state_callbacks))
            client.report({"13": 60}, 1700000000000)
            return {}

        with patch.object(
            client, "query", side_effect=report_then_fail
        ), patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state") as write_state:
            await entity.async_added_to_hass()

        self.assertEqual(subscribed_during_query, [True])
        self.assertEqual(entity.native_value, 60)
        self.assertTrue(entity.available)
        write_state.assert_not_called()

    async def test_invalid_initial_query_does_not_start_updates(self) -> None:
        """Invalid initial DPID 13 data leaves both update tasks stopped."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": "invalid"})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()

        with patch.object(
            number, "async_track_time_interval"
        ) as track_interval, patch.object(entity, "async_write_ha_state"):
            await entity.async_added_to_hass()

        self.assertFalse(entity.available)
        self.assertIsNone(entity._cancel_countdown_tick)
        self.assertIsNone(entity._cancel_countdown_calibration)
        track_interval.assert_not_called()


class LightCountdownControlTest(unittest.IsolatedAsyncioTestCase):
    """Verify Home Assistant writes countdown values to the device."""

    async def test_set_native_value_starts_countdown(self) -> None:
        """A positive number is sent as DPID 13 seconds."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        self.assertIsNot(
            number.CozyLifeCountdown.async_set_native_value,
            NumberEntity.async_set_native_value,
        )
        client = FakeTcpClient([1, 13], {"1": 1, "13": 0})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state"):
            await entity.async_added_to_hass()
            await entity.async_set_native_value(60)

        self.assertEqual(client.last_payload, {"13": 60})
        self.assertEqual(entity.native_value, 60)
        self.assertTrue(entity.available)
        self.assertEqual(client.query_count, 1)

    async def test_set_native_value_zero_cancels_countdown(self) -> None:
        """Zero is sent to cancel the active device countdown."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        self.assertIsNot(
            number.CozyLifeCountdown.async_set_native_value,
            NumberEntity.async_set_native_value,
        )
        client = FakeTcpClient([1, 13], {"1": 1, "13": 60})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state"):
            await entity.async_added_to_hass()
            await entity.async_set_native_value(0)

        self.assertEqual(client.last_payload, {"13": 0})
        self.assertEqual(entity.native_value, 0)
        self.assertTrue(entity.available)

    async def test_invalid_countdown_is_rejected_before_device_io(self) -> None:
        """Only whole seconds inside the protocol range may be sent."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )

        for invalid_value in (-1, 86401, 1.5, float("inf"), float("nan")):
            with self.subTest(value=invalid_value):
                client = FakeTcpClient([1, 13], {"1": 1, "13": 60})
                entity = _light_countdown(number, client)
                entity._apply_countdown_query(client.query([13]))

                with self.assertRaises(HomeAssistantError):
                    await entity.async_set_native_value(invalid_value)

                self.assertIsNone(client.last_payload)
                self.assertEqual(entity.native_value, 60)
                self.assertTrue(entity.available)

    async def test_device_rejection_preserves_countdown_state(self) -> None:
        """A valid device rejection is not treated as an offline device."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"1": 1, "13": 60})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        error = None

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(
            client,
            "control",
            side_effect=DeviceCommandRejectedError("Device rejected command"),
        ), patch.object(entity, "async_write_ha_state") as write_state:
            await entity.async_added_to_hass()
            try:
                await entity.async_set_native_value(30)
            except Exception as err:
                error = err

        self.assertIsInstance(error, HomeAssistantError)
        self.assertIn("rejected", str(error).lower())
        self.assertEqual(entity.native_value, 60)
        self.assertTrue(entity.available)
        write_state.assert_not_called()

    async def test_control_failure_marks_countdown_unavailable(self) -> None:
        """A transport failure preserves the value and marks it unavailable."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient(
            [1, 13], {"1": 1, "13": 60}, control_result=False
        )
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())
        error = None

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state") as write_state:
            await entity.async_added_to_hass()
            try:
                await entity.async_set_native_value(30)
            except Exception as err:
                error = err

        self.assertIsInstance(error, HomeAssistantError)
        self.assertEqual(entity.native_value, 60)
        self.assertFalse(entity.available)
        write_state.assert_called_once_with()

    async def test_successful_retry_recovers_countdown_availability(self) -> None:
        """A successful command after transport failure restores availability."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient(
            [1, 13], {"1": 1, "13": 60}, control_result=False
        )
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state"):
            await entity.async_added_to_hass()
            try:
                await entity.async_set_native_value(30)
            except HomeAssistantError:
                pass
        self.assertFalse(entity.available)

        client.control_result = True
        with patch.object(entity, "async_write_ha_state"):
            await entity.async_set_native_value(30)

        self.assertTrue(entity.available)
        self.assertEqual(entity.native_value, 30)

    async def test_async_control_applies_state_on_event_loop(self) -> None:
        """Only device input and output runs outside the event loop."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        entity = _light_countdown(number, client)
        event_loop_thread = threading.get_ident()
        entity.hass = ThreadedHomeAssistant(asyncio.get_running_loop())
        control_threads = []
        apply_threads = []
        control = client.control
        set_countdown_state = entity._set_countdown_state

        def record_control(payload):
            control_threads.append(threading.get_ident())
            return control(payload)

        def record_state_application(value):
            apply_threads.append(threading.get_ident())
            set_countdown_state(value)

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(
            client, "control", side_effect=record_control
        ), patch.object(
            entity,
            "_set_countdown_state",
            side_effect=record_state_application,
        ), patch.object(entity, "async_write_ha_state"):
            await entity.async_added_to_hass()
            apply_threads.clear()
            await entity.async_set_native_value(30)

        self.assertEqual(len(control_threads), 1)
        self.assertNotIn(event_loop_thread, control_threads)
        self.assertEqual(apply_threads, [event_loop_thread])

    async def test_successful_control_writes_state_immediately(self) -> None:
        """A successful command publishes its new value before returning."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant(asyncio.get_running_loop())

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state") as write_state:
            await entity.async_added_to_hass()
            await entity.async_set_native_value(30)

        self.assertEqual(entity.native_value, 30)
        write_state.assert_called_once_with()

class LightCountdownTimerTest(unittest.IsolatedAsyncioTestCase):
    """Verify local countdown ticks and device calibration lifecycle."""

    def test_local_tick_runs_on_home_assistant_event_loop(self) -> None:
        """State writes must never run from a worker thread."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 30})
        entity = _light_countdown(number, client)

        self.assertTrue(is_callback(entity._handle_countdown_tick))

    def test_platform_uses_explicit_countdown_calibration(self) -> None:
        """The entity owns calibration instead of platform polling."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        entity = _light_countdown(number, client)

        self.assertFalse(entity.should_poll)
        self.assertEqual(
            number.COUNTDOWN_CALIBRATION_INTERVAL,
            timedelta(seconds=10),
        )

    async def test_zero_countdown_keeps_calibration_for_recovery(self) -> None:
        """An idle countdown keeps probing until the device is available."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        callbacks = {}
        cancel_calibration = Mock()
        cancel_tick = Mock()

        def track_interval(_hass, callback, interval, **_kwargs):
            callbacks[interval] = callback
            return {
                number.COUNTDOWN_CALIBRATION_INTERVAL: cancel_calibration,
                number.COUNTDOWN_TICK_INTERVAL: cancel_tick,
            }[interval]

        with patch.object(
            number,
            "async_track_time_interval",
            side_effect=track_interval,
        ):
            client = FakeTcpClient([1, 13], {"13": 0})
            entity = _light_countdown(number, client)
            entity.hass = FakeHomeAssistant()
            await entity.async_added_to_hass()

            self.assertEqual(
                set(callbacks), {number.COUNTDOWN_CALIBRATION_INTERVAL}
            )
            calibration = callbacks[number.COUNTDOWN_CALIBRATION_INTERVAL]

            client.state = {}
            with patch.object(entity, "async_write_ha_state"):
                await calibration(datetime.now(UTC))

            self.assertFalse(entity.available)
            self.assertIs(
                entity._cancel_countdown_calibration, cancel_calibration
            )
            cancel_calibration.assert_not_called()

            client.state = {"13": 5}
            with patch.object(entity, "async_write_ha_state"):
                await calibration(datetime.now(UTC))

        self.assertTrue(entity.available)
        self.assertEqual(entity.native_value, 5)

    async def test_active_report_replaces_countdown_without_query(self) -> None:
        """DPID 13 reports immediately reset seconds and the local deadline."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()

        with patch.object(
            number,
            "async_track_time_interval",
            side_effect=[Mock(), Mock()],
        ), patch.object(number, "monotonic", return_value=100):
            await entity.async_added_to_hass()
            with patch.object(entity, "async_write_ha_state") as write_state:
                client.report({"13": 20})

        self.assertEqual(entity.native_value, 20)
        self.assertEqual(entity._countdown_deadline, 120)
        self.assertTrue(entity.available)
        self.assertEqual(client.query_count, 1)
        write_state.assert_called_once_with()

    async def test_queued_older_countdown_report_is_discarded(self) -> None:
        """Only the latest queued DPID 13 report reaches entity state."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        loop = QueuedLoop()
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant(loop)

        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state") as write_state:
            await entity.async_added_to_hass()
            client.report({"13": 10}, 1700000000000)
            client.report({"13": 20}, 1700000000001)
            loop.drain()

        self.assertEqual(entity.native_value, 20)
        self.assertTrue(entity.available)
        write_state.assert_called_once_with()

    async def test_invalid_active_report_marks_countdown_unavailable(self) -> None:
        """Invalid DPID 13 data stops local state while preserving its value."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()
        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ):
            await entity.async_added_to_hass()

        for invalid_value in (None, "invalid", -1, 86401, 1.5, 10**1000):
            with self.subTest(value=invalid_value), patch.object(
                entity, "async_write_ha_state"
            ) as write_state:
                try:
                    client.report({"13": invalid_value})
                except (TypeError, ValueError) as err:
                    self.fail(f"Invalid active report escaped: {err}")

                self.assertEqual(entity.native_value, 0)
                self.assertFalse(entity.available)
                self.assertIsNone(entity._countdown_deadline)
                write_state.assert_called_once_with()

    async def test_valid_report_restarts_updates_after_invalid_report(self) -> None:
        """A later valid DPID 13 report restarts both stopped update tasks."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        tracked_intervals = []
        cancel_callbacks = []

        def track_interval(_hass, _callback, interval, **_kwargs):
            tracked_intervals.append(interval)
            cancel_callback = Mock()
            cancel_callbacks.append(cancel_callback)
            return cancel_callback

        client = FakeTcpClient([1, 13], {"13": 60})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()
        with patch.object(
            number,
            "async_track_time_interval",
            side_effect=track_interval,
        ), patch.object(number, "monotonic", return_value=100), patch.object(
            entity, "async_write_ha_state"
        ):
            await entity.async_added_to_hass()
            client.report({"13": "invalid"})
            client.report({"13": 20})

        self.assertTrue(entity.available)
        self.assertEqual(entity.native_value, 20)
        self.assertEqual(entity._countdown_deadline, 120)
        self.assertEqual(
            tracked_intervals,
            [
                number.COUNTDOWN_TICK_INTERVAL,
                number.COUNTDOWN_CALIBRATION_INTERVAL,
                number.COUNTDOWN_TICK_INTERVAL,
                number.COUNTDOWN_CALIBRATION_INTERVAL,
            ],
        )
        cancel_callbacks[0].assert_called_once_with()
        cancel_callbacks[1].assert_called_once_with()
        cancel_callbacks[2].assert_not_called()
        cancel_callbacks[3].assert_not_called()

    async def test_invalid_query_paths_mark_countdown_unavailable(self) -> None:
        """Refresh and calibration apply invalid DPID 13 availability state."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )

        for query_source in ("refresh", "calibration"):
            with self.subTest(query_source=query_source):
                client = FakeTcpClient([1, 13], {"13": 60})
                entity = _light_countdown(number, client)
                entity.hass = FakeHomeAssistant()
                cancel_tick = Mock()
                cancel_calibration = Mock()

                def track_interval(_hass, _callback, interval, **_kwargs):
                    return {
                        number.COUNTDOWN_TICK_INTERVAL: cancel_tick,
                        number.COUNTDOWN_CALIBRATION_INTERVAL: cancel_calibration,
                    }[interval]

                with patch.object(
                    number,
                    "async_track_time_interval",
                    side_effect=track_interval,
                ):
                    await entity.async_added_to_hass()
                client.state = {"13": "invalid"}

                with patch.object(
                    entity, "async_write_ha_state"
                ) as write_state:
                    if query_source == "refresh":
                        await entity.async_update()
                    else:
                        await entity._async_calibrate_countdown(
                            datetime.now(UTC)
                        )

                self.assertEqual(entity.native_value, 60)
                self.assertFalse(entity.available)
                self.assertIsNone(entity._countdown_deadline)
                self.assertIsNone(entity._cancel_countdown_tick)
                self.assertIsNone(entity._cancel_countdown_calibration)
                cancel_tick.assert_called_once_with()
                cancel_calibration.assert_called_once_with()
                write_state.assert_called_once_with()

    async def test_removal_unsubscribes_countdown_reports(self) -> None:
        """A removed countdown entity no longer receives DPID 13 reports."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        entity = _light_countdown(number, client)
        entity.hass = FakeHomeAssistant()
        with patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ):
            await entity.async_added_to_hass()
        await entity.async_will_remove_from_hass()

        with patch.object(entity, "async_write_ha_state") as write_state:
            client.report({"13": 20})

        self.assertEqual(entity.native_value, 0)
        write_state.assert_not_called()

    async def test_equal_device_report_refreshes_last_updated(self) -> None:
        """Receiving unchanged DPID 13 still refreshes UI update time."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        entity = _light_countdown(
            number,
            FakeTcpClient([1, 13], {"13": 0})
        )
        hass = HomeAssistant("/tmp")
        entity.hass = hass
        entity.entity_id = "number.test_light_countdown"
        entity._verified_state_writable = True
        await entity.async_added_to_hass()
        entity.async_write_ha_state()
        first_last_updated = hass.states.get(entity.entity_id).last_updated

        await entity._async_calibrate_countdown(datetime.now(UTC))
        await asyncio.sleep(0)

        current_state = hass.states.get(entity.entity_id)
        self.assertEqual(current_state.state, "0")
        self.assertGreater(current_state.last_updated, first_last_updated)

    async def test_equal_active_report_refreshes_last_updated(self) -> None:
        """An unchanged active DPID 13 report refreshes Home Assistant time."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        entity = _light_countdown(number, client)
        hass = HomeAssistant("/tmp")
        entity.hass = hass
        entity.entity_id = "number.test_active_countdown"
        entity._verified_state_writable = True
        await entity.async_added_to_hass()
        entity.async_write_ha_state()
        first_last_updated = hass.states.get(entity.entity_id).last_updated

        report_thread = threading.Thread(
            target=client.report,
            args=({"13": 0}, client.last_state_sequence_number),
        )
        report_thread.start()
        await asyncio.to_thread(report_thread.join, 5)
        self.assertFalse(report_thread.is_alive())
        await asyncio.sleep(0)

        current_state = hass.states.get(entity.entity_id)
        self.assertEqual(current_state.state, "0")
        self.assertGreater(current_state.last_updated, first_last_updated)

    async def test_active_countdown_ticks_locally_without_device_query(self) -> None:
        """One-second ticks update memory and stop when zero is reached."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        callbacks = {}
        cancel_tasks = {
            timedelta(seconds=1): Mock(),
            timedelta(seconds=10): Mock(),
        }

        def track_interval(_hass, callback, interval, **_kwargs):
            callbacks[interval] = callback
            return cancel_tasks[interval]

        with patch.object(
            number,
            "async_track_time_interval",
            track_interval,
            create=True,
        ), patch.object(
            number, "monotonic", return_value=100, create=True
        ):
            client = FakeTcpClient([1, 13], {"13": 3})
            entity = _light_countdown(number, client)
            entity.hass = FakeHomeAssistant()
            await entity.async_added_to_hass()

        self.assertTrue(hasattr(entity, "_countdown_deadline"))
        self.assertEqual(
            set(callbacks),
            {timedelta(seconds=1), timedelta(seconds=10)},
        )
        with patch.object(entity, "async_write_ha_state") as write_state, patch.object(
            number, "monotonic", return_value=101.2, create=True
        ):
            callbacks[timedelta(seconds=1)](None)

        self.assertEqual(entity.native_value, 2)
        self.assertEqual(client.query_count, 1)
        write_state.assert_called_once_with()

        with patch.object(entity, "async_write_ha_state") as write_state, patch.object(
            number, "monotonic", return_value=103, create=True
        ):
            callbacks[timedelta(seconds=1)](None)

        self.assertEqual(entity.native_value, 0)
        self.assertEqual(client.query_count, 1)
        write_state.assert_called_once_with()
        cancel_tasks[timedelta(seconds=1)].assert_called_once_with()
        cancel_tasks[timedelta(seconds=10)].assert_not_called()

    async def test_device_query_recalibrates_local_deadline(self) -> None:
        """A device poll replaces local drift without extra tick queries."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        callbacks = {}
        cancel_tasks = {
            timedelta(seconds=1): Mock(),
            timedelta(seconds=10): Mock(),
        }

        def track_interval(_hass, callback, interval, **_kwargs):
            callbacks[interval] = callback
            return cancel_tasks[interval]

        with patch.object(
            number,
            "async_track_time_interval",
            side_effect=track_interval,
            create=True,
        ), patch.object(number, "monotonic", return_value=100, create=True):
            client = FakeTcpClient([1, 13], {"13": 5})
            entity = _light_countdown(number, client)
            entity.hass = FakeHomeAssistant()
            await entity.async_added_to_hass()

        self.assertEqual(
            set(callbacks),
            {timedelta(seconds=1), timedelta(seconds=10)},
        )
        with patch.object(entity, "async_write_ha_state"), patch.object(
            number, "monotonic", return_value=102, create=True
        ):
            callbacks[timedelta(seconds=1)](None)
        self.assertEqual(entity.native_value, 3)

        client.state["13"] = 8
        with patch.object(entity, "async_write_ha_state") as write_state, patch.object(
            number, "monotonic", return_value=103, create=True
        ):
            await callbacks[timedelta(seconds=10)](None)
        self.assertEqual(entity.native_value, 8)
        self.assertEqual(client.query_count, 2)
        self.assertEqual(client.last_query_attributes, [13])
        write_state.assert_called_once_with()

        with patch.object(entity, "async_write_ha_state"), patch.object(
            number, "monotonic", return_value=104, create=True
        ):
            callbacks[timedelta(seconds=1)](None)
        self.assertEqual(entity.native_value, 7)

    async def test_calibration_applies_query_result_on_event_loop(self) -> None:
        """Worker queries return before countdown attributes are changed."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        client = FakeTcpClient([1, 13], {"13": 0})
        entity = _light_countdown(number, client)
        event_loop_thread = threading.get_ident()
        entity.hass = ThreadedHomeAssistant(asyncio.get_running_loop())
        await entity.async_added_to_hass()
        client.state["13"] = 20
        query_threads = []
        apply_threads = []
        query = client.query
        set_countdown_state = entity._set_countdown_state

        def record_query(attributes=None):
            query_threads.append(threading.get_ident())
            return query(attributes)

        def record_state_application(value):
            apply_threads.append(threading.get_ident())
            set_countdown_state(value)

        with patch.object(
            client, "query", side_effect=record_query
        ), patch.object(
            entity, "_set_countdown_state", side_effect=record_state_application
        ), patch.object(
            number, "async_track_time_interval", return_value=Mock()
        ), patch.object(entity, "async_write_ha_state"):
            await entity._async_calibrate_countdown(datetime.now(UTC))

        self.assertEqual(len(query_threads), 1)
        self.assertNotIn(event_loop_thread, query_threads)
        self.assertEqual(apply_threads, [event_loop_thread])

    async def test_stale_tick_keeps_unavailable_recovery_query(self) -> None:
        """A cancelled tick already queued cannot stop device recovery queries."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        callbacks = {}
        cancel_tick = Mock()
        cancel_calibration = Mock()

        def track_interval(_hass, callback, interval, **_kwargs):
            callbacks[interval] = callback
            return {
                timedelta(seconds=1): cancel_tick,
                timedelta(seconds=10): cancel_calibration,
            }[interval]

        with patch.object(
            number, "async_track_time_interval", side_effect=track_interval
        ), patch.object(number, "monotonic", return_value=100):
            client = FakeTcpClient([1, 13], {"13": 30})
            entity = _light_countdown(number, client)
            entity.hass = FakeHomeAssistant()
            await entity.async_added_to_hass()
            stale_tick = callbacks[timedelta(seconds=1)]
            calibration = callbacks[timedelta(seconds=10)]
            client.state = {}
            with patch.object(entity, "async_write_ha_state"):
                await calibration(datetime.now(UTC))
                stale_tick(datetime.now(UTC))

        self.assertFalse(entity.available)
        self.assertIs(
            entity._cancel_countdown_calibration, cancel_calibration
        )
        cancel_tick.assert_called_once_with()
        cancel_calibration.assert_not_called()

    async def test_failed_calibration_keeps_recovery_query_running(self) -> None:
        """An unavailable countdown continues its ten-second device probe."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        callbacks = {}
        cancel_first_tick = Mock()
        cancel_calibration = Mock()
        cancel_second_tick = Mock()
        scheduled_tasks = iter(
            [cancel_first_tick, cancel_calibration, cancel_second_tick]
        )

        def track_interval(_hass, callback, interval, **_kwargs):
            callbacks[interval] = callback
            return next(scheduled_tasks)

        with patch.object(
            number, "async_track_time_interval", side_effect=track_interval
        ), patch.object(number, "monotonic", return_value=100):
            client = FakeTcpClient([1, 13], {"13": 30})
            entity = _light_countdown(number, client)
            entity.hass = FakeHomeAssistant()
            await entity.async_added_to_hass()
            calibration = callbacks[timedelta(seconds=10)]
            client.state = {}
            with patch.object(entity, "async_write_ha_state"):
                await calibration(datetime.now(UTC))

            self.assertFalse(entity.available)
            self.assertIsNone(entity._cancel_countdown_tick)
            self.assertIs(
                entity._cancel_countdown_calibration, cancel_calibration
            )
            cancel_first_tick.assert_called_once_with()
            cancel_calibration.assert_not_called()

            client.state = {"13": 5}
            with patch.object(entity, "async_write_ha_state"):
                await calibration(datetime.now(UTC))

        self.assertTrue(entity.available)
        self.assertEqual(entity.native_value, 5)
        self.assertIs(entity._cancel_countdown_tick, cancel_second_tick)
        self.assertIs(entity._cancel_countdown_calibration, cancel_calibration)

    async def test_zero_offline_and_removal_stop_local_updates(self) -> None:
        """Zero stops local ticks while removal stops every update task."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )

        for stop_source in ("zero", "remove"):
            with self.subTest(stop_source=stop_source):
                cancel_tasks = [Mock(), Mock()]
                with patch.object(
                    number,
                    "async_track_time_interval",
                    side_effect=cancel_tasks,
                    create=True,
                ), patch.object(
                    number, "monotonic", return_value=100, create=True
                ):
                    client = FakeTcpClient([1, 13], {"13": 30})
                    entity = _light_countdown(number, client)
                    entity.hass = FakeHomeAssistant()
                    await entity.async_added_to_hass()

                if stop_source == "zero":
                    with patch.object(entity, "async_write_ha_state"):
                        await entity.async_set_native_value(0)
                    cancel_tasks[0].assert_called_once_with()
                    cancel_tasks[1].assert_not_called()
                else:
                    await entity.async_will_remove_from_hass()
                    for cancel_task in cancel_tasks:
                        cancel_task.assert_called_once_with()

    async def test_positive_value_starts_local_updates(self) -> None:
        """Setting a stopped countdown starts one local tick schedule."""
        number = importlib.import_module(
            "custom_components.hass_cozylife_local_pull.number"
        )
        cancel_tasks = [Mock(), Mock()]
        with patch.object(
            number,
            "async_track_time_interval",
            side_effect=cancel_tasks,
            create=True,
        ) as track_interval, patch.object(
            number, "monotonic", return_value=100, create=True
        ):
            client = FakeTcpClient([1, 13], {"13": 0})
            entity = _light_countdown(number, client)
            entity.hass = FakeHomeAssistant()
            await entity.async_added_to_hass()
            track_interval.assert_called_once_with(
                entity.hass,
                entity._async_calibrate_countdown,
                number.COUNTDOWN_CALIBRATION_INTERVAL,
            )

            with patch.object(entity, "async_write_ha_state"):
                await entity.async_set_native_value(5)

        self.assertEqual(track_interval.call_count, 2)
        for cancel_task in cancel_tasks:
            cancel_task.assert_not_called()
