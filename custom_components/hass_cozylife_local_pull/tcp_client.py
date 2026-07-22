# -*- coding: utf-8 -*-
import json
import socket
import time
from typing import Optional, Union, Any
import logging
from .utils import get_pid_list, get_sn
import threading

CMD_INFO = 0
CMD_QUERY = 2
CMD_SET = 3
CMD_LIST = [CMD_INFO, CMD_QUERY, CMD_SET]
RESPONSE_TIMEOUT_SECONDS = 3
MAX_FRAME_SIZE_BYTES = 64 * 1024
_LOGGER = logging.getLogger(__name__)


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
    receiver:{"cmd":10,"pv":0,"sn":"1636463664000","res":0,"msg":{"attr":[1,2,3,4,5,6],"data":{"1":0,"2":0,"3":1000,
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
    
    def __init__(self, ip):
        self._ip = ip
        self._connect = None  # Initialize _connect as None
        self._receive_buffer = b''
        self._io_lock = threading.RLock()
        self._reconnect_thread = None
        self._last_sequence_number = None
        self._close_connection() 
        self._reconnect()
    
    def _close_connection(self):
        """Close the active socket and discard connection-specific fragments."""
        with self._io_lock:
            if self._connect:
                try:
                    self._connect.close()
                except Exception as e:
                    _LOGGER.error(f'Error while closing the connection: {e}')
                self._connect = None
            self._receive_buffer = b''
        
    def _reconnect(self):
        """Start one connection recovery worker for this device."""
        def reconnect_thread():
            while True:
                s = None
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(RESPONSE_TIMEOUT_SECONDS)
                    s.connect((self._ip, self._port))
                    with self._io_lock:
                        self._close_connection()
                        self._connect = s
                        self._receive_buffer = b''
                        if not self._device_info():
                            raise ConnectionError(
                                'Device information response is invalid'
                            )
                        if self._reconnect_thread is thread:
                            self._reconnect_thread = None
                    return
                except Exception as e:
                    with self._io_lock:
                        if self._connect is s:
                            self._close_connection()
                        elif s is not None:
                            try:
                                s.close()
                            except Exception as close_error:
                                _LOGGER.error(
                                    f'Error while closing reconnect socket: {close_error}'
                                )
                    _LOGGER.info(f'Reconnection failed: {e}')
                    time.sleep(60)  # Wait for 60 seconds before trying to reconnect

        with self._io_lock:
            if (
                self._reconnect_thread is not None
                and self._reconnect_thread.is_alive()
            ):
                return

            thread = threading.Thread(target=reconnect_thread)
            thread.daemon = True  # This makes the thread exit when the main program exits
            self._reconnect_thread = thread
            thread.start()


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

    def _receive_message(self, deadline: float | None = None) -> dict:
        """Receive one delimited JavaScript Object Notation message."""
        with self._io_lock:
            while b'\r\n' not in self._receive_buffer:
                if len(self._receive_buffer) > MAX_FRAME_SIZE_BYTES:
                    raise ValueError('CozyLife protocol frame exceeds size limit')
                if deadline is not None:
                    self._set_timeout_for_deadline(deadline)
                chunk = self._connect.recv(1024)
                if not chunk:
                    raise ConnectionError('Device closed the connection')
                self._receive_buffer += chunk

            frame, self._receive_buffer = self._receive_buffer.split(b'\r\n', 1)
            if len(frame) > MAX_FRAME_SIZE_BYTES:
                raise ValueError('CozyLife protocol frame exceeds size limit')
            return json.loads(frame)
    
    def _device_info(self) -> bool:
        """Request device information and report whether the handshake is valid."""
        with self._io_lock:
            response_deadline = time.monotonic() + RESPONSE_TIMEOUT_SECONDS
            try:
                request_sequence_number = self._only_send(
                    CMD_INFO, {}, response_deadline
                )
                while True:
                    resp_json = self._receive_message(response_deadline)
                    if resp_json.get('sn') == request_sequence_number:
                        break
            except Exception:
                _LOGGER.info('_device_info.recv.error')
                return False
            finally:
                if self._connect:
                    try:
                        self._connect.settimeout(RESPONSE_TIMEOUT_SECONDS)
                    except OSError:
                        pass
        
        if resp_json.get('msg') is None or type(resp_json['msg']) is not dict:
            _LOGGER.info('_device_info.recv.error1')
            
            return False
        
        if resp_json['msg'].get('did') is None:
            _LOGGER.info('_device_info.recv.error2')
            
            return False

        self._device_id = resp_json['msg']['did']
        
        if resp_json['msg'].get('pid') is None:
            _LOGGER.info('_device_info.recv.error3')
            return False
        
        self._pid = resp_json['msg']['pid']        
        pid_list = get_pid_list()

        for item in pid_list:
            match = False
            for item1 in item['m']:
                if item1['pid'] == self._pid:
                    match = True
                    self._icon = item1['i']
                    self._device_model_name = item1['n']
                    self._dpid = item1['dpid']
                    break
            
            if match:
                self._device_type_code = item['c']                
                break
        
        # _LOGGER.info(pid_list)
        _LOGGER.info(self._device_id)
        _LOGGER.info(self._device_type_code)
        _LOGGER.info(self._pid)
        _LOGGER.info(self._device_model_name)
        _LOGGER.info(self._icon)
        return True
    
    def _get_package(self, cmd: int, payload: dict) -> tuple[bytes, str]:
        """Build a frame with a sequence not reused by an unread response."""
        sequence_number = int(get_sn())
        if (
            self._last_sequence_number is not None
            and sequence_number <= self._last_sequence_number
        ):
            sequence_number = self._last_sequence_number + 1
        self._last_sequence_number = sequence_number
        request_sequence_number = str(sequence_number)

        if CMD_SET == cmd:
            message = {
                'pv': 0,
                'cmd': cmd,
                'sn': request_sequence_number,
                'msg': {
                    'attr': [int(item) for item in payload.keys()],
                    'data': payload,
                }
            }
        elif CMD_QUERY == cmd:
            message = {
                'pv': 0,
                'cmd': cmd,
                'sn': request_sequence_number,
                'msg': {
                    'attr': [0],
                }
            }
        elif CMD_INFO == cmd:
            message = {
                'pv': 0,
                'cmd': cmd,
                'sn': request_sequence_number,
                'msg': {}
            }
        else:
            raise Exception('CMD is not valid')
        
        payload_str = json.dumps(message, separators=(',', ':',))
        _LOGGER.info(f'_package={payload_str}')
        return bytes(payload_str + "\r\n", encoding='utf8'), request_sequence_number
    
    def _send_receiver(self, cmd: int, payload: dict) -> Union[dict, Any]:
        """Send a query, returning empty data while connection recovery starts."""
        with self._io_lock:
            try:
                response_deadline = time.monotonic() + RESPONSE_TIMEOUT_SECONDS
                request_sequence_number = self._only_send(
                    cmd, payload, response_deadline
                )
                while time.monotonic() < response_deadline:
                    response = self._receive_message(response_deadline)
                    # Only accept the response for this request.
                    if response.get('sn') != request_sequence_number:
                        continue

                    if len(response) == 0:
                        return {}

                    if response.get('msg') is None or type(response['msg']) is not dict:
                        return {}

                    if response['msg'].get('data') is None or type(response['msg']['data']) is not dict:
                        return {}

                    return response['msg']['data']

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
    
    def _only_send(
        self, cmd: int, payload: dict, deadline: float | None = None
    ) -> str:
        """Send one complete protocol frame while preserving request order."""
        with self._io_lock:
            frame, request_sequence_number = self._get_package(cmd, payload)
            if deadline is not None:
                self._set_timeout_for_deadline(deadline)
            self._connect.sendall(frame)
            return request_sequence_number
    
    def control(self, payload: dict) -> bool:
        """Send control data, returning False while connection recovery starts."""
        with self._io_lock:
            try:
                self._only_send(CMD_SET, payload)
            except (AttributeError, OSError) as err:
                _LOGGER.info(f'control.send.error: {err}')
                self._close_connection()
                self._reconnect()
                return False

        return True
    
    def query(self) -> dict:
        """
        query device state
        :return:
        """
        return self._send_receiver(CMD_QUERY, {})
