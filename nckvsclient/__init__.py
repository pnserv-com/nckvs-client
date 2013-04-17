# -*- coding: utf-8 -*-

import json
from contextlib import closing

try:
    from urllib import request
except ImportError:
    import urllib2 as request

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser as ConfigParser


class RPCError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return 'RPCError: {} {}'.format(self.code, self.message)


class KVSClient(object):
    def __init__(self, base_url, login_name, login_pass, datatypename,
                 datatypeversion=1, **kwargs):
        self.config = dict(base_url=base_url,
                           login_name=login_name,
                           login_pass=login_pass,
                           datatypename=datatypename,
                           datatypeversion=datatypeversion,
                           **kwargs)
        self.system_param = {}

        for key in ('login_name', 'login_pass', 'app_servername',
                    'app_username', 'timezone'):
            self.system_param[key] = self.config.get(key, '')

    @classmethod
    def from_file(cls, filename, section='nckvs'):
        parser = ConfigParser()
        parser.read(filename)
        config = dict(parser.items(section))
        config['datatypeversion'] = int(config.get('datatypeversion', '1'))
        return cls(**config)

    def set(self, items):
        url = self.config['base_url'] + '/data/set/'
        param = {
            'system': self.system_param,
            'query': {
                'datalist': items,
                'datatypename': self.config['datatypename'],
                'datatypeversion': self.config['datatypeversion']
            }
        }
        return self._request(url, param)

    def search(self, query):
        url = self.config['base_url'] + '/data/search/'
        param = {
            'system': self.system_param,
            'query': {
                'datatypename': self.config['datatypename'],
                'dataversion': '*',
                'limit': 0,
                'sortorder': [],
                'matching': query
            }
        }
        return self._request(url, param)

    def _request(self, url, param):
        data = json.dumps(param, ensure_ascii=False)
        headers = {
            'Content-type': 'application/json',
            'Content-length': len(data)
        }
        req = request.Request(url, data, headers)
        with closing(request.urlopen(req)) as res:
            result = json.load(res)
            if result['code'] == '200':
                return result
            raise RPCError(result['code'], result['message'])
