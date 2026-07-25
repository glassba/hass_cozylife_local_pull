"""Regression tests for resilient CozyLife product metadata loading."""

from __future__ import annotations

import json
import queue
import threading
import unittest
from unittest.mock import Mock, patch

import requests

from custom_components.hass_cozylife_local_pull import utils


TEST_TIMEOUT_SECONDS = 5


class ProductMetadataTest(unittest.TestCase):
    """Verify external metadata failures preserve local integration startup."""

    def setUp(self) -> None:
        """Isolate module caches while preserving their original objects."""
        cache_patch = patch.object(utils, "_CACHE_PID", {})
        cache_patch.start()
        self.addCleanup(cache_patch.stop)
        load_patch = patch.object(utils, "_CACHE_PID_LOAD", {})
        load_patch.start()
        self.addCleanup(load_patch.stop)

    def test_connection_failure_returns_empty_metadata(self) -> None:
        """A remote connection failure must not escape into integration setup."""
        with patch.object(
            utils.requests,
            "get",
            side_effect=requests.ConnectionError("metadata service offline"),
        ):
            result = utils.get_pid_list()

        self.assertEqual(result, [])

    def test_non_object_json_returns_empty_metadata(self) -> None:
        """Valid JSON with the wrong top-level type is treated as unavailable."""
        for content in (b"null", b"[]"):
            with self.subTest(content=content), patch.object(
                utils.requests,
                "get",
                return_value=Mock(status_code=200, content=content),
            ):
                self.assertEqual(utils.get_pid_list(), [])

    def test_invalid_json_encoding_returns_empty_metadata(self) -> None:
        """Invalid response bytes follow the same unavailable-data contract."""
        response = Mock(status_code=200, content=b"\xff")

        with patch.object(utils.requests, "get", return_value=response):
            self.assertEqual(utils.get_pid_list(), [])

    def test_successful_empty_metadata_is_cached(self) -> None:
        """A valid empty product list is a successful cacheable response."""
        response = Mock(
            status_code=200,
            content=json.dumps({"ret": "1", "info": {"list": []}}).encode(),
        )

        with patch.object(
            utils.requests, "get", return_value=response
        ) as request:
            self.assertEqual(utils.get_pid_list(), [])
            self.assertEqual(utils.get_pid_list(), [])

        request.assert_called_once()

    def test_invalid_nested_metadata_is_not_cached(self) -> None:
        """Malformed categories and models cannot poison the shared cache."""
        invalid_lists = [
            [None],
            [{"m": []}],
            [{"c": 1, "m": []}],
            [{"c": "01", "m": None}],
            [{"c": "01", "m": [None]}],
            [{"c": "01", "m": [{"i": "icon", "n": "name", "dpid": [1]}]}],
            [{"c": "01", "m": [{"pid": 1, "i": "icon", "n": "name", "dpid": [1]}]}],
            [{"c": "01", "m": [{"pid": "pid", "i": None, "n": "name", "dpid": [1]}]}],
            [{"c": "01", "m": [{"pid": "pid", "i": "icon", "n": None, "dpid": [1]}]}],
            [{"c": "01", "m": [{"pid": "pid", "i": "icon", "n": "name", "dpid": None}]}],
            [{"c": "01", "m": [{"pid": "pid", "i": "icon", "n": "name", "dpid": [True]}]}],
        ]

        for metadata in invalid_lists:
            with self.subTest(metadata=metadata):
                utils._CACHE_PID.clear()
                utils._CACHE_PID_LOAD.clear()
                content = json.dumps(
                    {"ret": "1", "info": {"list": metadata}}
                ).encode()
                with patch.object(
                    utils.requests,
                    "get",
                    return_value=Mock(status_code=200, content=content),
                ):
                    result = utils.get_pid_list()

                self.assertEqual(result, [])
                self.assertEqual(utils._CACHE_PID, {})
                self.assertEqual(utils._CACHE_PID_LOAD, {})

    def test_concurrent_initial_load_uses_one_request(self) -> None:
        """Concurrent first callers share one complete metadata request."""
        metadata = [
            {
                "c": "01",
                "m": [
                    {
                        "pid": "product-1234",
                        "i": "mdi:lightbulb",
                        "n": "Test Device",
                        "dpid": [1, 4],
                    }
                ],
            }
        ]
        response = Mock(
            status_code=200,
            content=json.dumps(
                {"ret": "1", "info": {"list": metadata}}
            ).encode(),
        )
        callers_ready = threading.Barrier(3)
        request_started = threading.Event()
        waiter_attached = threading.Event()
        allow_response = threading.Event()
        results: queue.Queue[list] = queue.Queue()
        errors: queue.Queue[BaseException] = queue.Queue()
        timeout = 5

        class ObservedFuture(utils.Future):
            """Signal when a caller starts waiting for the active load."""

            def result(self, timeout=None):
                waiter_attached.set()
                return super().result(
                    timeout
                    if timeout is not None
                    else TEST_TIMEOUT_SECONDS
                )

        def get_metadata(*args, **kwargs):
            request_started.set()
            if not allow_response.wait(timeout):
                raise TimeoutError("Timed out waiting to release metadata response")
            return response

        def load_metadata() -> None:
            try:
                callers_ready.wait(timeout)
                results.put(utils.get_pid_list())
            except BaseException as err:
                errors.put(err)

        threads = [
            threading.Thread(target=load_metadata, daemon=True)
            for _ in range(2)
        ]
        with patch.object(utils, "Future", ObservedFuture), patch.object(
            utils.requests, "get", side_effect=get_metadata
        ) as request:
            for thread in threads:
                thread.start()

            try:
                callers_ready.wait(timeout)
                self.assertTrue(request_started.wait(timeout))
                self.assertTrue(waiter_attached.wait(timeout))
                allow_response.set()
                for thread in threads:
                    thread.join(timeout)
            finally:
                allow_response.set()
                for thread in threads:
                    thread.join(timeout)

        self.assertTrue(errors.empty())
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(request.call_count, 1)
        self.assertEqual([results.get(), results.get()], [metadata, metadata])

    def test_concurrent_failure_is_shared_before_a_later_retry(self) -> None:
        """One failed load is shared while the next caller can retry."""
        metadata = [
            {
                "c": "01",
                "m": [
                    {
                        "pid": "product-1234",
                        "i": "mdi:lightbulb",
                        "n": "Test Device",
                        "dpid": [1, 4],
                    }
                ],
            }
        ]
        response = Mock(
            status_code=200,
            content=json.dumps(
                {"ret": "1", "info": {"list": metadata}}
            ).encode(),
        )
        callers_ready = threading.Barrier(3)
        first_request_started = threading.Event()
        waiter_attached = threading.Event()
        allow_failure = threading.Event()
        attempt_lock = threading.Lock()
        attempts = 0
        results: queue.Queue[list] = queue.Queue()
        errors: queue.Queue[BaseException] = queue.Queue()
        timeout = 5

        class ObservedFuture(utils.Future):
            """Signal when a caller begins waiting for the active load."""

            def result(self, timeout=None):
                waiter_attached.set()
                return super().result(
                    timeout
                    if timeout is not None
                    else TEST_TIMEOUT_SECONDS
                )

        def get_metadata(*args, **kwargs):
            nonlocal attempts
            with attempt_lock:
                attempts += 1
                attempt = attempts
            if attempt == 1:
                first_request_started.set()
                if not allow_failure.wait(timeout):
                    raise TimeoutError(
                        "Timed out waiting to release metadata failure"
                    )
                raise requests.ConnectionError("metadata service offline")
            return response

        def load_metadata() -> None:
            try:
                callers_ready.wait(timeout)
                results.put(utils.get_pid_list())
            except BaseException as err:
                errors.put(err)

        threads = [
            threading.Thread(target=load_metadata, daemon=True)
            for _ in range(2)
        ]
        with patch.object(utils, "Future", ObservedFuture), patch.object(
            utils.requests, "get", side_effect=get_metadata
        ) as request:
            for thread in threads:
                thread.start()

            try:
                callers_ready.wait(timeout)
                self.assertTrue(first_request_started.wait(timeout))
                self.assertTrue(waiter_attached.wait(timeout))
                allow_failure.set()
                for thread in threads:
                    thread.join(timeout)
            finally:
                allow_failure.set()
                for thread in threads:
                    thread.join(timeout)

            self.assertTrue(errors.empty())
            self.assertTrue(
                all(not thread.is_alive() for thread in threads)
            )
            self.assertEqual(request.call_count, 1)
            self.assertEqual([results.get(), results.get()], [[], []])

            self.assertEqual(utils.get_pid_list(), metadata)
            self.assertEqual(request.call_count, 2)

    def test_waiter_retry_starts_new_request_before_failed_owner_returns(
        self,
    ) -> None:
        """A waiter can retry as soon as a failed load is published."""
        metadata = [
            {
                "c": "01",
                "m": [
                    {
                        "pid": "product-1234",
                        "i": "mdi:lightbulb",
                        "n": "Test Device",
                        "dpid": [1, 4],
                    }
                ],
            }
        ]
        response = Mock(
            status_code=200,
            content=json.dumps(
                {"ret": "1", "info": {"list": metadata}}
            ).encode(),
        )
        first_request_started = threading.Event()
        waiter_attached = threading.Event()
        allow_failure = threading.Event()
        result_published = threading.Event()
        allow_owner_return = threading.Event()
        waiter_finished = threading.Event()
        attempt_lock = threading.Lock()
        attempts = 0
        owner_results: queue.Queue[list] = queue.Queue()
        waiter_results = []
        errors: queue.Queue[BaseException] = queue.Queue()
        timeout = 5

        class PausingFirstFuture(utils.Future):
            """Pause the first load immediately after waking its waiters."""

            created = 0

            def __init__(self) -> None:
                super().__init__()
                self._pause_publication = self.created == 0
                type(self).created += 1

            def result(self, timeout=None):
                if self._pause_publication:
                    waiter_attached.set()
                return super().result(
                    timeout
                    if timeout is not None
                    else TEST_TIMEOUT_SECONDS
                )

            def set_result(self, result) -> None:
                super().set_result(result)
                if not self._pause_publication:
                    return
                result_published.set()
                if not allow_owner_return.wait(timeout):
                    raise TimeoutError(
                        "Timed out waiting to release metadata owner"
                    )

        def get_metadata(*args, **kwargs):
            nonlocal attempts
            with attempt_lock:
                attempts += 1
                attempt = attempts
            if attempt == 1:
                first_request_started.set()
                if not allow_failure.wait(timeout):
                    raise TimeoutError(
                        "Timed out waiting to release metadata failure"
                    )
                raise requests.ConnectionError("metadata service offline")
            return response

        def load_owner() -> None:
            try:
                owner_results.put(utils.get_pid_list())
            except BaseException as err:
                errors.put(err)

        def wait_and_retry() -> None:
            try:
                waiter_results.append(utils.get_pid_list())
                waiter_results.append(utils.get_pid_list())
            except BaseException as err:
                errors.put(err)
            finally:
                waiter_finished.set()

        owner = threading.Thread(target=load_owner, daemon=True)
        waiter = threading.Thread(target=wait_and_retry, daemon=True)
        with patch.object(utils, "Future", PausingFirstFuture), patch.object(
            utils.requests, "get", side_effect=get_metadata
        ) as request:
            owner.start()
            try:
                self.assertTrue(first_request_started.wait(timeout))
                waiter.start()
                self.assertTrue(waiter_attached.wait(timeout))
                allow_failure.set()
                self.assertTrue(result_published.wait(timeout))
                self.assertTrue(waiter_finished.wait(timeout))
            finally:
                allow_failure.set()
                allow_owner_return.set()
                owner.join(timeout)
                if waiter.ident is not None:
                    waiter.join(timeout)

        self.assertTrue(errors.empty())
        self.assertFalse(owner.is_alive())
        self.assertFalse(waiter.is_alive())
        self.assertEqual(owner_results.get(), [])
        self.assertEqual(waiter_results, [[], metadata])
        self.assertEqual(request.call_count, 2)

    def test_waiter_retry_starts_new_load_before_exception_owner_returns(
        self,
    ) -> None:
        """A waiter can retry as soon as an exceptional load is published."""
        metadata = [
            {
                "c": "01",
                "m": [
                    {
                        "pid": "product-1234",
                        "i": "mdi:lightbulb",
                        "n": "Test Device",
                        "dpid": [1, 4],
                    }
                ],
            }
        ]
        first_fetch_started = threading.Event()
        waiter_attached = threading.Event()
        allow_exception = threading.Event()
        exception_published = threading.Event()
        allow_owner_return = threading.Event()
        waiter_finished = threading.Event()
        attempt_lock = threading.Lock()
        attempts = 0
        owner_errors: queue.Queue[BaseException] = queue.Queue()
        waiter_errors: queue.Queue[BaseException] = queue.Queue()
        retry_results = []
        timeout = 5

        class PausingFirstExceptionFuture(utils.Future):
            """Pause the first owner immediately after waking its waiters."""

            created = 0

            def __init__(self) -> None:
                super().__init__()
                self._pause_publication = self.created == 0
                type(self).created += 1

            def result(self, timeout=None):
                if self._pause_publication:
                    waiter_attached.set()
                return super().result(
                    timeout
                    if timeout is not None
                    else TEST_TIMEOUT_SECONDS
                )

            def set_exception(self, exception) -> None:
                super().set_exception(exception)
                if not self._pause_publication:
                    return
                exception_published.set()
                if not allow_owner_return.wait(timeout):
                    raise TimeoutError(
                        "Timed out waiting to release metadata exception owner"
                    )

        def fetch_metadata(_lang: str):
            nonlocal attempts
            with attempt_lock:
                attempts += 1
                attempt = attempts
            if attempt == 1:
                first_fetch_started.set()
                if not allow_exception.wait(timeout):
                    raise TimeoutError(
                        "Timed out waiting to release metadata exception"
                    )
                raise RuntimeError("Injected metadata loader failure")
            return metadata

        def load_owner() -> None:
            try:
                utils.get_pid_list()
            except BaseException as err:
                owner_errors.put(err)

        def wait_and_retry() -> None:
            try:
                with self.assertRaisesRegex(
                    RuntimeError, "Injected metadata loader failure"
                ):
                    utils.get_pid_list()
                retry_results.append(utils.get_pid_list())
            except BaseException as err:
                waiter_errors.put(err)
            finally:
                waiter_finished.set()

        owner = threading.Thread(target=load_owner, daemon=True)
        waiter = threading.Thread(target=wait_and_retry, daemon=True)
        with patch.object(
            utils, "Future", PausingFirstExceptionFuture
        ), patch.object(
            utils, "_fetch_pid_list", side_effect=fetch_metadata
        ):
            owner.start()
            try:
                self.assertTrue(first_fetch_started.wait(timeout))
                waiter.start()
                self.assertTrue(waiter_attached.wait(timeout))
                allow_exception.set()
                self.assertTrue(exception_published.wait(timeout))
                self.assertTrue(waiter_finished.wait(timeout))
            finally:
                allow_exception.set()
                allow_owner_return.set()
                owner.join(timeout)
                if waiter.ident is not None:
                    waiter.join(timeout)

        self.assertFalse(owner.is_alive())
        self.assertFalse(waiter.is_alive())
        self.assertTrue(waiter_errors.empty())
        self.assertEqual(owner_errors.qsize(), 1)
        self.assertIsInstance(owner_errors.get(), RuntimeError)
        self.assertEqual(retry_results, [metadata])
        self.assertEqual(attempts, 2)

    def test_concurrent_languages_use_independent_loads(self) -> None:
        """Concurrent language requests do not share localized metadata."""
        metadata_by_language = {
            language: [
                {
                    "c": "01",
                    "m": [
                        {
                            "pid": "product-1234",
                            "i": "mdi:lightbulb",
                            "n": name,
                            "dpid": [1, 4],
                        }
                    ],
                }
            ]
            for language, name in (("zh", "测试设备"), ("en", "Test Device"))
        }
        request_lock = threading.Lock()
        requested_languages = []
        both_requests_started = threading.Event()
        allow_responses = threading.Event()
        results = {}
        errors: queue.Queue[BaseException] = queue.Queue()
        timeout = 5

        def get_metadata(_url, params, timeout):
            language = params["lang"]
            with request_lock:
                requested_languages.append(language)
                if len(requested_languages) == 2:
                    both_requests_started.set()
            if not allow_responses.wait(timeout):
                raise TimeoutError(
                    "Timed out waiting to release localized metadata responses"
                )
            return Mock(
                status_code=200,
                content=json.dumps(
                    {
                        "ret": "1",
                        "info": {"list": metadata_by_language[language]},
                    }
                ).encode(),
            )

        def load_metadata(language: str) -> None:
            try:
                results[language] = utils.get_pid_list(language)
            except BaseException as err:
                errors.put(err)

        threads = [
            threading.Thread(target=load_metadata, args=(language,), daemon=True)
            for language in ("zh", "en")
        ]
        with patch.object(
            utils.requests, "get", side_effect=get_metadata
        ) as request:
            for thread in threads:
                thread.start()

            try:
                self.assertTrue(both_requests_started.wait(timeout))
            finally:
                allow_responses.set()
                for thread in threads:
                    thread.join(timeout)

            self.assertTrue(errors.empty())
            self.assertTrue(all(not thread.is_alive() for thread in threads))
            cached_results = {
                language: utils.get_pid_list(language)
                for language in ("zh", "en")
            }

        self.assertCountEqual(requested_languages, ["zh", "en"])
        self.assertEqual(request.call_count, 2)
        self.assertEqual(results, metadata_by_language)
        self.assertEqual(cached_results, metadata_by_language)
