import json
import socket
import time
from .utils import get_sn
import logging


_LOGGER = logging.getLogger(__name__)
DISCOVERY_COLLECTION_TIMEOUT_SECONDS = 1.0
MAX_DISCOVERED_DEVICES = 255

"""
discover device
"""


def get_ip() -> list:
    """
    get device ip
    :return: list
    """
    server = None
    ip = []
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # server.bind(('192.168.123.1', 0))
        # Enable broadcasting mode
        server.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Set a timeout so the socket does not block
        # indefinitely when trying to receive data.
        server.settimeout(0.1)
        request_sequence_number = get_sn()
        request_payload = json.dumps(
            {
                'cmd': 0,
                'pv': 0,
                'sn': request_sequence_number,
                'msg': {},
            },
            separators=(',', ':'),
        )

        i = 0
        while i < 3:
            # server.sendto(bytes(request_payload, encoding='utf-8'), ('<broadcast>', 6095))
            server.sendto(
                bytes(request_payload, encoding='utf-8'),
                ('255.255.255.255', 6095),
            )
            time.sleep(0.03)
            i += 1

        # max tries before first data received
        max = 5
        i = 0
        while i < max:
            i += 1
            try:
                data, addr = server.recvfrom(1024, socket.MSG_PEEK)
            except socket.timeout:
                _LOGGER.info(f'{i}/{max} try, udp timeout')
                continue
            _LOGGER.info(f'first udp.receiver:{addr[0]}')
            break
        else:
            _LOGGER.warning('cannot find any device')
            return []

        collection_deadline = (
            time.monotonic() + DISCOVERY_COLLECTION_TIMEOUT_SECONDS
        )
        while len(ip) < MAX_DISCOVERED_DEVICES:
            remaining = collection_deadline - time.monotonic()
            if remaining <= 0:
                break
            server.settimeout(remaining)
            try:
                data, addr = server.recvfrom(1024)
            except socket.timeout:
                _LOGGER.info('udp timeout')
                break
            try:
                response = json.loads(data)
            except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                _LOGGER.info('Ignoring invalid discovery response')
                continue

            if not isinstance(response, dict):
                _LOGGER.info('Ignoring non-object discovery response')
                continue

            message = response.get('msg')
            if (
                type(response.get('cmd')) is not int
                or response['cmd'] != 0
                or type(response.get('pv')) is not int
                or response['pv'] != 0
                or not isinstance(response.get('sn'), str)
                or response['sn'] != request_sequence_number
                or type(response.get('res')) is not int
                or response['res'] != 0
                or not isinstance(message, dict)
                or not isinstance(message.get('did'), str)
                or not message['did']
                or not isinstance(message.get('pid'), str)
                or not message['pid']
            ):
                _LOGGER.info('Ignoring unexpected discovery response')
                continue

            _LOGGER.info(f'udp.receiver:{addr[0]}')
            if addr[0] not in ip: ip.append(addr[0])

        return ip
    except OSError as err:
        _LOGGER.warning('UDP discovery failed: %s', err)
        return ip
    finally:
        if server is not None:
            try:
                server.close()
            except OSError as err:
                _LOGGER.warning(
                    'Failed to close UDP discovery socket: %s', err
                )
