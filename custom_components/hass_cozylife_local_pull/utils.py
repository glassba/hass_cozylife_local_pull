# -*- coding: utf-8 -*-
from concurrent.futures import Future
import json
import threading
import time
import requests
import logging
from .const import (
    API_DOMAIN,
    LANG
)
_LOGGER = logging.getLogger(__name__)


def get_sn() -> str:
    """
    message sn
    :return: str
    """
    return str(int(round(time.time() * 1000)))


# cache get_pid_list result for many calls
_SUPPORTED_LANGUAGES = ('zh', 'en', 'es', 'pt', 'ja', 'ru', 'nl', 'ko', 'fr', 'de')
_CACHE_PID: dict[str, list] = {}
_CACHE_PID_LOCK = threading.Lock()
_CACHE_PID_LOAD: dict[str, Future[list]] = {}


def get_pid_list(lang='en') -> list:
    """Return cached product metadata or share one in-flight load."""
    if lang not in _SUPPORTED_LANGUAGES:
        _LOGGER.info(f'not support lang={lang}, will set lang={LANG}')
        lang = LANG

    with _CACHE_PID_LOCK:
        if lang in _CACHE_PID:
            return _CACHE_PID[lang]
        load = _CACHE_PID_LOAD.get(lang)
        if load is None:
            load = Future()
            _CACHE_PID_LOAD[lang] = load
            should_load = True
        else:
            should_load = False

    if not should_load:
        return load.result()

    try:
        fetched = _fetch_pid_list(lang)
    except BaseException as err:
        with _CACHE_PID_LOCK:
            if _CACHE_PID_LOAD.get(lang) is load:
                del _CACHE_PID_LOAD[lang]
        load.set_exception(err)
        raise

    result = [] if fetched is None else fetched
    with _CACHE_PID_LOCK:
        if fetched is not None:
            _CACHE_PID[lang] = fetched
        if _CACHE_PID_LOAD.get(lang) is load:
            del _CACHE_PID_LOAD[lang]
    load.set_result(result)
    return result


def _fetch_pid_list(lang: str) -> list | None:
    """
    http://doc.doit/project-12/doc-95/
    :param lang:
    :return:
    """
    try:
        res = requests.get(f'http://{API_DOMAIN}/api/v2/device_product/model', {
            'lang': lang
        }, timeout=3)
    except requests.RequestException:
        _LOGGER.exception('Failed to retrieve product metadata')
        return None

    if 200 != res.status_code:
        _LOGGER.info('get_pid_list.result is none')
        return None
    try:
        pid_list = json.loads(res.content)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        _LOGGER.info('get_pid_list.result is not json')
        return None

    if not isinstance(pid_list, dict):
        _LOGGER.info('get_pid_list.result has an invalid shape')
        return None

    if pid_list.get('ret') is None:
        return None

    if '1' != pid_list['ret']:
        return None

    if pid_list.get('info') is None or type(pid_list.get('info')) is not dict:
        return None

    if pid_list['info'].get('list') is None or type(pid_list['info']['list']) is not list:
        return None

    product_list = pid_list['info']['list']
    for category in product_list:
        if (
            not isinstance(category, dict)
            or not isinstance(category.get('c'), str)
            or not category['c']
            or not isinstance(category.get('m'), list)
        ):
            _LOGGER.info('get_pid_list.result has an invalid category')
            return None

        for model in category['m']:
            if not isinstance(model, dict):
                _LOGGER.info('get_pid_list.result has an invalid model')
                return None
            if (
                not isinstance(model.get('pid'), str)
                or not model['pid']
                or not isinstance(model.get('i'), str)
                or not model['i']
                or not isinstance(model.get('n'), str)
                or not model['n']
                or not isinstance(model.get('dpid'), list)
                or any(type(dpid) is not int for dpid in model['dpid'])
            ):
                _LOGGER.info('get_pid_list.result has an invalid model')
                return None

    return product_list
