# -*- coding: utf-8 -*-
from collections import deque
import json
import select
import socket
import time
from typing import Callable, Optional, Union, Any
import logging
from .utils import get_pid_list, get_sn
from .const import LANG
import threading

CMD_INFO = 0
CMD_QUERY = 2
CMD_SET = 3
CMD_REPORT = 10
STATE_COMMANDS = (CMD_QUERY, CMD_SET, CMD_REPORT)
CMD_LIST = [CMD_INFO, CMD_QUERY, CMD_SET]
RESPONSE_TIMEOUT_SECONDS = 3
STATE_LISTENER_WAIT_SECONDS = 0.25
MAX_FRAME_SIZE_BYTES = 64 * 1024
_LOGGER = logging.getLogger(__name__)


class DeviceCommandRejectedError(Exception):
    """Report a valid device rejection without invalidating its connection."""


class tcp_client(object):
    """
    Represents a device
    send:{"cmd":0,"pv":0,"sn":"1636463553873","msg":{}}
    receiver:{"cmd":0,"pv":0,"sn":"1636463553873","msg":{"did":"629168597cb94c4c1d8f","dtp":"02","pid":"e2s64v",
    "mac":"7cb94c4c1d8f","ip":"192.168.123.57","rssi":-33,"sv":"1.0.0","hv":"0.0.1"},"res":0}

    send:{"cmd":2,"pv":0,"sn":"1636463611798","msg":{"attr":[0]}}
    receiver:{"cmd":2,"pv":0,"sn":"1636463611798","msg":{"attr":[1,2,3,4,5,6],"data":{"1":0,"2":0,"3":1000,"4":1000,
    "5":65535,"6":65535}},"res":0}
    
    send:{"cmd":3,"pv":0,"sn":"1636463662455","msg":{"attr":[1],"data":{"1":0}}}
    receiver:{"cmd":3,"pv":0,"sn":"1636463662455","msg":{"attr":[1],"data":{"1":0}},"res":0}
    receiver:{"cmd":10,"pv":0,"sn":"1636463664000","msg":{"attr":[1,2,3,4,5,6],"data":{"1":0,"2":0,"3":1000,
    "4":1000,"5":65535,"6":65535}}}
    """
    _ip = str
    _port = 5555
    _connect = socket
    
    _device_id = str
    # _device_key = str
    _pid = str
    _device_type_code = str
    _icon = str
    _device_model_name = str
    _dpid = []
    _last_sequence_number: Optional[int] = None
    
    def __init__(self, ip, lang: str = LANG):
        self._ip = ip
        self._lang = lang
        self._connect = None  # Initialize _connect as None
        self._connecting_socket = None
        self._receive_buffer = b''
        self._connect_phase_lock = threading.Lock()
        self._io_lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._ready_callback_lock = threading.RLock()
        self._state_callback_lock = threading.RLock()
        self._state_report_queue_lock = threading.Lock()
        self._state_report_dispatch_lock = threading.RLock()
        self._reconnect_thread = None
        self._state_listener_thread = None
        self._stop_event = threading.Event()
        self._last_sequence_number = None
        self._last_state_sequence_number: int | None = None
        self._ready = False
        self._ready_callbacks: list[Callable[["tcp_client"], None]] = []
        self._state_callbacks: list[Callable[[dict, int], None]] = []
        self._state_report_queue: deque[tuple[dict, int]] = deque()
        self._close_connection() 
        self._reconnect()
    
    def _interrupt_socket(self, connection) -> None:
        """Interrupt blocking socket work before releasing its resources."""
        if connection is None:
            return
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except (AttributeError, OSError):
            pass
        try:
            connection.close()
        except Exception as err:
            _LOGGER.error('Error while closing the connection: %s', err)

    def _close_connection(self):
        """Close the active socket and discard connection-specific fragments."""
        with self._io_lock:
            with self._lifecycle_lock:
                connection = self._connect
                self._connect = None
            if connection:
                self._interrupt_socket(connection)
            self._receive_buffer = b''

    def signal_stop(self) -> None:
        """Prevent new work without waiting for admitted operations."""
        self._stop_event.set()

    def request_stop(self) -> None:
        """Prevent new work and wait for admitted operations to finish."""
        self.signal_stop()
        # Wait for a socket connection admitted before stop publication.
        with self._connect_phase_lock:
            pass
        # Wait for network work admitted under the lifecycle lock to finish.
        with self._lifecycle_lock:
            pass
        # Wait for the complete send/receive transaction to leave the I/O lock.
        with self._io_lock:
            pass
        # Do not return while a callback that passed its stop check can run.
        with self._ready_callback_lock:
            pass

    def close(self) -> None:
        """Stop connection recovery and release the active socket."""
        self.signal_stop()
        with self._lifecycle_lock:
            connecting_socket = self._connecting_socket
            active_socket = self._connect
            worker = self._reconnect_thread
            state_listener = self._state_listener_thread

        # Interrupt sockets before taking the transaction lock so blocked I/O
        # can release the worker that owns it.
        self._interrupt_socket(connecting_socket)
        if active_socket is not connecting_socket:
            self._interrupt_socket(active_socket)

        self.request_stop()

        if worker is not None and worker is not threading.current_thread():
            worker.join(RESPONSE_TIMEOUT_SECONDS)
            if worker.is_alive():
                _LOGGER.warning(
                    'Reconnect worker did not stop for %s within timeout',
                    self._ip,
                )
        if (
            state_listener is not None
            and state_listener is not threading.current_thread()
        ):
            state_listener.join(RESPONSE_TIMEOUT_SECONDS)
            if state_listener.is_alive():
                _LOGGER.warning(
                    'State listener did not stop for %s within timeout',
                    self._ip,
                )

        with self._io_lock:
            with self._lifecycle_lock:
                self._connecting_socket = None
            self._ready_callbacks = []
            self._close_connection()
        with self._state_callback_lock:
            self._state_callbacks = []
        with self._state_report_queue_lock:
            self._state_report_queue.clear()

    def add_ready_callback(
        self, callback: Callable[["tcp_client"], None]
    ) -> None:
        """Run a callback once the first device handshake has completed."""
        with self._io_lock:
            if self._stop_event.is_set():
                return
            if not self._ready:
                self._ready_callbacks.append(callback)
                return

        self._run_ready_callback(callback)

    def _publish_ready(self) -> None:
        """Publish first readiness outside the network transaction lock."""
        with self._io_lock:
            if self._stop_event.is_set() or self._ready:
                return
            self._ready = True
            callbacks, self._ready_callbacks = self._ready_callbacks, []

        for callback in callbacks:
            self._run_ready_callback(callback)

    def _run_ready_callback(
        self, callback: Callable[["tcp_client"], None]
    ) -> None:
        """Keep platform callback failures from invalidating the connection."""
        with self._ready_callback_lock:
            if self._stop_event.is_set():
                return
            try:
                callback(self)
            except Exception:
                _LOGGER.exception('Ready callback failed for %s', self._ip)

    def add_state_callback(
        self, callback: Callable[[dict, int], None]
    ) -> Callable[[], None]:
        """Subscribe to validated device state messages."""
        with self._state_callback_lock:
            if not self._stop_event.is_set():
                self._state_callbacks.append(callback)

        def remove_callback() -> None:
            with self._state_callback_lock:
                if callback in self._state_callbacks:
                    self._state_callbacks.remove(callback)

        return remove_callback

    def _state_report_data(
        self, response: dict
    ) -> tuple[dict, int] | None:
        """Return ordered state data from one valid device response."""
        command = response['cmd']
        if command in (CMD_QUERY, CMD_SET):
            result = response.get('res')
            if type(result) is not int:
                _LOGGER.info(
                    'Device state response result is invalid: cmd=%s sn=%s',
                    command,
                    response.get('sn'),
                )
                return None
            if result != 0:
                _LOGGER.info(
                    'Discarding device state payload: cmd=%s sn=%s res=%s',
                    command,
                    response.get('sn'),
                    result,
                )
                return None

        message = response.get('msg')
        if not isinstance(message, dict) or not isinstance(
            message.get('data'), dict
        ):
            _LOGGER.info('Device state response data is invalid')
            return None

        sequence_number = self._parse_sequence_number(response.get('sn'))
        if sequence_number is None:
            _LOGGER.info('Device state sequence number is invalid')
            return None
        if (
            self._last_state_sequence_number is not None
            and sequence_number < self._last_state_sequence_number
        ):
            return None
        if (
            self._last_state_sequence_number is None
            or sequence_number > self._last_state_sequence_number
        ):
            self._last_state_sequence_number = sequence_number
        return message['data'].copy(), sequence_number

    def _dispatch_state_reports(
        self, reports: list[tuple[dict, int]]
    ) -> None:
        """Run state callbacks after the network transaction lock is released."""
        for state, sequence_number in reports:
            with self._state_callback_lock:
                if self._stop_event.is_set():
                    return
                callbacks = tuple(self._state_callbacks)
                for callback in callbacks:
                    if callback not in self._state_callbacks:
                        continue
                    try:
                        callback(state, sequence_number)
                    except Exception:
                        _LOGGER.exception(
                            'State callback failed for %s', self._ip
                        )

    def _queue_state_report(self, state: dict, sequence_number: int) -> None:
        """Preserve device state messages in socket receive order."""
        with self._state_report_queue_lock:
            if (
                self._state_report_queue
                and self._state_report_queue[-1][1] == sequence_number
            ):
                queued_state, _ = self._state_report_queue[-1]
                queued_state.update(state)
                return
            self._state_report_queue.append((state, sequence_number))

    def _queue_state_response(self, response: dict) -> bool:
        """Queue one valid, non-stale device state response."""
        command = response.get('cmd')
        if type(command) is not int or command not in STATE_COMMANDS:
            return False
        state_report = self._state_report_data(response)
        if state_report is None:
            return False
        self._queue_state_report(*state_report)
        return True

    @staticmethod
    def _parse_sequence_number(value: object) -> int | None:
        """Normalize one Unix millisecond sequence number for comparisons."""
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            return None
        try:
            sequence_number = int(value)
        except ValueError:
            return None
        return sequence_number if sequence_number >= 0 else None

    def _drain_state_reports(self) -> None:
        """Serialize queued state callbacks outside the network lock."""
        with self._state_report_dispatch_lock:
            while True:
                with self._state_report_queue_lock:
                    if not self._state_report_queue:
                        return
                    state_report = self._state_report_queue.popleft()
                _, sequence_number = state_report
                if (
                    self._last_state_sequence_number is not None
                    and sequence_number < self._last_state_sequence_number
                ):
                    continue
                self._dispatch_state_reports([state_report])

    def _start_state_listener(self, connection) -> None:
        """Listen for active reports while no request owns the connection."""
        try:
            if connection.fileno() < 0:
                return
        except (AttributeError, OSError, TypeError, ValueError):
            return

        def listen_for_state() -> None:
            try:
                while not self._stop_event.is_set():
                    with self._io_lock:
                        if self._connect is not connection:
                            return
                        has_buffered_frame = b'\r\n' in self._receive_buffer

                    if not has_buffered_frame:
                        readable, _, _ = select.select(
                            [connection], [], [], STATE_LISTENER_WAIT_SECONDS
                        )
                        if not readable:
                            continue

                    report_queued = False
                    try:
                        with self._io_lock:
                            if self._connect is not connection:
                                return
                            while True:
                                frame = self._pop_received_frame()
                                if frame is None:
                                    readable, _, _ = select.select(
                                        [connection], [], [], 0
                                    )
                                    if not readable:
                                        break
                                    chunk = connection.recv(1024)
                                    if not chunk:
                                        raise ConnectionError(
                                            'Device closed the connection'
                                        )
                                    self._receive_buffer += chunk
                                    continue
                                response = json.loads(frame)
                                command = response.get('cmd')
                                if (
                                    type(command) is int
                                    and command in STATE_COMMANDS
                                ):
                                    if self._queue_state_response(response):
                                        report_queued = True
                                else:
                                    _LOGGER.info(
                                        'Ignoring unexpected idle response from %s',
                                        self._ip,
                                    )
                    finally:
                        if report_queued:
                            self._drain_state_reports()
            except Exception as err:
                should_reconnect = False
                with self._io_lock:
                    if self._connect is connection:
                        _LOGGER.info('State listener error: %s', err)
                        self._close_connection()
                        should_reconnect = not self._stop_event.is_set()
                if should_reconnect:
                    self._reconnect()
            finally:
                with self._lifecycle_lock:
                    if self._state_listener_thread is listener:
                        self._state_listener_thread = None

        listener = threading.Thread(target=listen_for_state)
        listener.daemon = True
        with self._lifecycle_lock:
            if self._stop_event.is_set() or self._connect is not connection:
                return
            self._state_listener_thread = listener
            listener.start()
        
    def _reconnect(self):
        """Start one connection recovery worker for this device."""
        def reconnect_thread():
            try:
                while not self._stop_event.is_set():
                    retry_error = None
                    publish_ready = False
                    with self._connect_phase_lock:
                        s = None
                        try:
                            with self._lifecycle_lock:
                                if self._stop_event.is_set():
                                    return
                                s = socket.socket(
                                    socket.AF_INET,
                                    socket.SOCK_STREAM,
                                )
                                s.settimeout(RESPONSE_TIMEOUT_SECONDS)
                                self._connecting_socket = s
                            s.connect((self._ip, self._port))
                            with self._io_lock:
                                self._close_connection()
                                with self._lifecycle_lock:
                                    stopped = self._stop_event.is_set()
                                    if self._connecting_socket is s:
                                        self._connecting_socket = None
                                    if not stopped:
                                        self._connect = s
                                        self._receive_buffer = b''

                                if stopped:
                                    self._interrupt_socket(s)
                                    return
                                # Keep the admitted handshake inside the stop
                                # operation's complete I/O barrier.
                                if not self._device_info():
                                    raise ConnectionError(
                                        'Device information response is invalid'
                                    )
                            with self._lifecycle_lock:
                                if self._reconnect_thread is thread:
                                    self._reconnect_thread = None
                            publish_ready = True
                        except Exception as err:
                            with self._io_lock:
                                with self._lifecycle_lock:
                                    if self._connecting_socket is s:
                                        self._connecting_socket = None
                                    active_socket = self._connect is s
                                if active_socket:
                                    self._close_connection()
                                elif s is not None:
                                    self._interrupt_socket(s)
                            retry_error = err

                    # Readiness callbacks may perform I/O from this or another
                    # thread, so release reconnect admission before dispatch.
                    if publish_ready:
                        self._publish_ready()
                        self._start_state_listener(s)
                        return

                    if retry_error is not None:
                        _LOGGER.info(f'Reconnection failed: {retry_error}')
                        if self._stop_event.wait(60):
                            return
            finally:
                with self._lifecycle_lock:
                    if self._reconnect_thread is thread:
                        self._reconnect_thread = None

        with self._lifecycle_lock:
            if self._stop_event.is_set():
                return
            worker = self._reconnect_thread
            if worker is not None and worker.is_alive():
                return

            thread = threading.Thread(target=reconnect_thread)
            thread.daemon = True  # This makes the thread exit when the main program exits
            thread.start()
            self._reconnect_thread = thread


    @property
    def check(self) -> bool:
        """
        Determine whether the device is filtered
        :return:
        """
        return True
    
    @property
    def dpid(self):
        return self._dpid
    
    @property
    def device_model_name(self):
        return self._device_model_name
    
    @property
    def icon(self):
        return self._icon

    @property
    def last_state_sequence_number(self) -> int | None:
        """Return the latest accepted device state timestamp."""
        return self._last_state_sequence_number
    
    @property
    def device_type_code(self) -> str:
        return self._device_type_code
    
    @property
    def device_id(self):
        return self._device_id

    def _set_timeout_for_deadline(self, deadline: float) -> None:
        """Limit the next blocking socket operation to the transaction budget."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Response deadline expired')
        self._connect.settimeout(remaining)

    def _pop_received_frame(self) -> bytes | None:
        """Remove one complete delimited frame from the receive buffer."""
        if b'\r\n' not in self._receive_buffer:
            if len(self._receive_buffer) > MAX_FRAME_SIZE_BYTES:
                raise ValueError('CozyLife protocol frame exceeds size limit')
            return None

        frame, self._receive_buffer = self._receive_buffer.split(b'\r\n', 1)
        if len(frame) > MAX_FRAME_SIZE_BYTES:
            raise ValueError('CozyLife protocol frame exceeds size limit')
        return frame

    def _receive_message(self, deadline: float | None = None) -> dict:
        """Receive one delimited JavaScript Object Notation message."""
        with self._io_lock:
            while (frame := self._pop_received_frame()) is None:
                if deadline is not None:
                    self._set_timeout_for_deadline(deadline)
                chunk = self._connect.recv(1024)
                if not chunk:
                    raise ConnectionError('Device closed the connection')
                self._receive_buffer += chunk

            return json.loads(frame)
    
    def _device_info(self) -> bool:
        """Request device information and report whether the handshake is valid."""
        resp_json = None
        with self._io_lock:
            response_deadline = time.monotonic() + RESPONSE_TIMEOUT_SECONDS
            try:
                request_sequence_number = self._only_send(
                    CMD_INFO, {}, response_deadline
                )
                while True:
                    response = self._receive_message(response_deadline)
                    response_command = response.get('cmd')
                    if (
                        type(response_command) is int
                        and response_command in STATE_COMMANDS
                    ):
                        self._queue_state_response(response)
                        continue
                    response_sequence_number = self._parse_sequence_number(
                        response.get('sn')
                    )
                    if response_sequence_number == request_sequence_number:
                        resp_json = response
                        break
            except Exception:
                _LOGGER.info('_device_info.recv.error')
            finally:
                if self._connect:
                    try:
                        self._connect.settimeout(RESPONSE_TIMEOUT_SECONDS)
                    except OSError:
                        pass

        self._drain_state_reports()
        if resp_json is None:
            return False
        
        if (
            type(resp_json.get('cmd')) is not int
            or resp_json['cmd'] != CMD_INFO
            or type(resp_json.get('res')) is not int
            or resp_json['res'] != 0
        ):
            _LOGGER.info('_device_info.recv.protocol_error')
            return False

        message = resp_json.get('msg')
        if not isinstance(message, dict):
            _LOGGER.info('_device_info.recv.error1')
            return False

        device_id = message.get('did')
        if not isinstance(device_id, str) or not device_id:
            _LOGGER.info('_device_info.recv.error2')
            return False

        product_id = message.get('pid')
        if not isinstance(product_id, str) or not product_id:
            _LOGGER.info('_device_info.recv.error3')
            return False

        pid_list = get_pid_list(self._lang)
        if not pid_list:
            _LOGGER.info('_device_info.product_metadata.empty')
            return False

        product_metadata = None
        device_type_code = None
        for item in pid_list:
            for item1 in item['m']:
                if item1['pid'] == product_id:
                    product_metadata = item1
                    device_type_code = item['c']
                    break

            if product_metadata is not None:
                break

        if product_metadata is None:
            _LOGGER.info('_device_info.product_metadata.unmapped')
            return False

        self._device_id = device_id
        self._pid = product_id
        self._device_type_code = device_type_code
        self._icon = product_metadata['i']
        self._device_model_name = product_metadata['n']
        self._dpid = product_metadata['dpid']
        
        # _LOGGER.info(pid_list)
        _LOGGER.info(self._device_id)
        _LOGGER.info(self._device_type_code)
        _LOGGER.info(self._pid)
        _LOGGER.info(self._device_model_name)
        _LOGGER.info(self._icon)
        return True
    
    def _get_package(self, cmd: int, payload: dict) -> tuple[bytes, int]:
        """Build a frame with a sequence not reused by an unread response."""
        last_request_sequence_number = (
            self._last_sequence_number
            if self._last_sequence_number is not None
            else -1
        )
        last_state_sequence_number = (
            self._last_state_sequence_number
            if self._last_state_sequence_number is not None
            else -1
        )
        sequence_number = max(
            int(get_sn()),
            last_request_sequence_number + 1,
            last_state_sequence_number + 1,
        )
        self._last_sequence_number = sequence_number
        request_sequence_number = sequence_number

        if CMD_SET == cmd:
            message = {
                'pv': 0,
                'cmd': cmd,
                'sn': str(request_sequence_number),
                'msg': {
                    'attr': [int(item) for item in payload.keys()],
                    'data': payload,
                }
            }
        elif CMD_QUERY == cmd:
            message = {
                'pv': 0,
                'cmd': cmd,
                'sn': str(request_sequence_number),
                'msg': {
                    'attr': [
                        int(item) for item in payload.get('attr', [0])
                    ],
                }
            }
        elif CMD_INFO == cmd:
            message = {
                'pv': 0,
                'cmd': cmd,
                'sn': str(request_sequence_number),
                'msg': {}
            }
        else:
            raise Exception('CMD is not valid')
        
        payload_str = json.dumps(message, separators=(',', ':',))
        _LOGGER.info(f'_package={payload_str}')
        return bytes(payload_str + "\r\n", encoding='utf8'), request_sequence_number
    
    def _send_receiver(self, cmd: int, payload: dict) -> Union[dict, Any]:
        """Send a query, returning empty data while connection recovery starts."""
        try:
            with self._io_lock:
                if self._stop_event.is_set():
                    return {}
                try:
                    # Synchronize the final stop check with request_stop before I/O.
                    with self._lifecycle_lock:
                        if self._stop_event.is_set():
                            return {}
                        response_deadline = (
                            time.monotonic() + RESPONSE_TIMEOUT_SECONDS
                        )
                        request_sequence_number = self._only_send(
                            cmd, payload, response_deadline
                        )
                    while time.monotonic() < response_deadline:
                        response = self._receive_message(response_deadline)
                        response_command = response.get('cmd')
                        state_response_queued = False
                        if (
                            type(response_command) is int
                            and response_command in STATE_COMMANDS
                        ):
                            state_response_queued = (
                                self._queue_state_response(response)
                            )
                            if response_command != CMD_QUERY:
                                continue
                        # Only accept the response for this request.
                        response_sequence_number = self._parse_sequence_number(
                            response.get('sn')
                        )
                        if response_sequence_number != request_sequence_number:
                            continue

                        if (
                            type(response.get('cmd')) is not int
                            or response['cmd'] != CMD_QUERY
                            or type(response.get('res')) is not int
                        ):
                            raise ValueError('Query response is invalid')

                        if response['res'] != 0:
                            return {}

                        message = response.get('msg')
                        if (
                            not isinstance(message, dict)
                            or not isinstance(message.get('data'), dict)
                        ):
                            raise ValueError('Query response is invalid')

                        if not state_response_queued:
                            return {}
                        return message['data']

                    raise TimeoutError('Timed out waiting for the query response')

                except Exception as e:
                    _LOGGER.info(f'_send_receiver.error: {e}')
                    self._close_connection()
                    self._reconnect()  # Reconnect on exception
                    return {}
                finally:
                    if self._connect:
                        try:
                            self._connect.settimeout(RESPONSE_TIMEOUT_SECONDS)
                        except OSError:
                            pass
        finally:
            self._drain_state_reports()
    
    def _only_send(
        self, cmd: int, payload: dict, deadline: float | None = None
    ) -> int:
        """Send one complete protocol frame while preserving request order."""
        with self._io_lock:
            frame, request_sequence_number = self._get_package(cmd, payload)
            if deadline is not None:
                self._set_timeout_for_deadline(deadline)
            self._connect.sendall(frame)
            return request_sequence_number
    
    def control(self, payload: dict) -> bool:
        """Send control data and require a matching device acknowledgement."""
        try:
            with self._io_lock:
                if self._stop_event.is_set():
                    return False
                try:
                    # Synchronize the final stop check with request_stop before I/O.
                    with self._lifecycle_lock:
                        if self._stop_event.is_set():
                            return False
                        response_deadline = (
                            time.monotonic() + RESPONSE_TIMEOUT_SECONDS
                        )
                        request_sequence_number = self._only_send(
                            CMD_SET, payload, response_deadline
                        )
                    while time.monotonic() < response_deadline:
                        response = self._receive_message(response_deadline)
                        response_command = response.get('cmd')
                        if (
                            type(response_command) is int
                            and response_command in STATE_COMMANDS
                        ):
                            self._queue_state_response(response)
                            if response_command != CMD_SET:
                                continue
                        response_sequence_number = self._parse_sequence_number(
                            response.get('sn')
                        )
                        if response_sequence_number != request_sequence_number:
                            continue

                        if (
                            type(response.get('cmd')) is not int
                            or response['cmd'] != CMD_SET
                            or type(response.get('res')) is not int
                        ):
                            raise ValueError(
                                'Control acknowledgement is invalid'
                            )

                        if response['res'] != 0:
                            raise DeviceCommandRejectedError(
                                "Device rejected command with result "
                                f"{response['res']}"
                            )

                        message = response.get('msg')
                        if (
                            not isinstance(message, dict)
                            or not isinstance(message.get('data'), dict)
                            or message['data'] != payload
                        ):
                            raise ValueError(
                                'Control acknowledgement is invalid'
                            )

                        return True

                    raise TimeoutError('Timed out waiting for the control response')
                except DeviceCommandRejectedError:
                    raise
                except Exception as err:
                    _LOGGER.info(f'control.error: {err}')
                    self._close_connection()
                    self._reconnect()
                    return False
                finally:
                    if self._connect:
                        try:
                            self._connect.settimeout(RESPONSE_TIMEOUT_SECONDS)
                        except OSError:
                            pass
        finally:
            self._drain_state_reports()

        return False
    
    def query(self, attributes: list[int] | None = None) -> dict:
        """
        query device state
        :return:
        """
        return self._send_receiver(
            CMD_QUERY,
            {'attr': [0] if attributes is None else attributes},
        )
