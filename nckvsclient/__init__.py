# -*- coding: utf-8 -*-

import json
import urllib2
from contextlib import closing

_config = {}
_system_param = {}


class RPCError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return 'RPCError: {} {}'.format(self.code, self.message)


def setup(base_url, login_name, login_pass, **kwargs):
    _config.clear()
    _system_param.clear()

    _config['base_url'] = base_url
    _config['login_name'] = login_name
    _config['login_pass'] = login_pass
    _config.update(kwargs)

    for key in ('login_name', 'login_pass', 'app_servername', 'app_username',
                'timezone'):
        _system_param[key] = _config.get(key, '')


def set(items):
    url = _config['base_url'] + '/data/set/'
    param = {
        'system': _system_param,
        'query': {
            'datalist': items,
            'datatypename': 'commonstest1',
            'datatypeversion': 1
        }
    }
    return _request(url, param)


def search(query):
    url = _config['base_url'] + '/data/search/'
    param = {
        'system': _system_param,
        'query': {
            'datatypename': 'commonstest1',
            'dataversion': '*',
            'limit': 0,
            'sortorder': [],
            'matching': query
        }
    }
    return _request(url, param)


def _request(url, param):
    data = json.dumps(param)
    headers = {
        'Content-type': 'application/json',
        'Content-length': len(data)
    }
    req = urllib2.Request(url, data, headers)
    with closing(urllib2.urlopen(req)) as res:
        result = json.load(res)
        if result['code'] == '200':
            return result
        raise RPCError(result['code'], result['message'])
