# -*- coding: utf-8 -*-

import os

import pytest
from mock import patch, call

import nckvsclient as nckvs

try:
    from urllib import request
except ImportError:
    import urllib2 as request

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

BASE_URL = 'http://example.com'
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def pytest_funcarg__system_param(request):
    return {
        'login_name': 'user',
        'login_pass': 'pass',
        'app_servername': 'appname',
        'app_username': 'appuser',
        'timezone': ''
    }


def pytest_funcarg__client(request):
    return nckvs.KVSClient(BASE_URL, 'user', 'pass', 'testtype',
                           datatypeversion=2, app_servername='appname',
                           app_username='appuser', timezone='',
                           json_items=['list'])


class TestKVSClient(object):
    def test_init(self, client, system_param):
        assert client.config == {
            'base_url': BASE_URL,
            'login_name': 'user',
            'login_pass': 'pass',
            'app_servername': 'appname',
            'app_username': 'appuser',
            'timezone': '',
            'datatypename': 'testtype',
            'datatypeversion': 2,
            'json_items': ['list']
        }
        assert client.system_param == system_param

    def test_init2(self):
        client = nckvs.KVSClient(BASE_URL, 'user', 'pass', 'testtype')
        assert client.config['datatypeversion'] == 1

    def test_from_file(self):
        filename = os.path.join(DATA_DIR, 'config.test.ini')
        client = nckvs.KVSClient.from_file(filename)
        assert client.config == {
            'base_url': 'http://example.com/weatherinfo/nckvsrpc/api/rest',
            'login_name': 'user',
            'login_pass': 'pass',
            'app_servername': 'appname',
            'app_username': 'appuser',
            'timezone': '',
            'datatypename': 'testtype',
            'datatypeversion': 1,
            'json_items': []
        }

    def test_from_file2(self):
        filename = os.path.join(DATA_DIR, 'config.test.ini')
        client = nckvs.KVSClient.from_file(filename, 'other')
        assert client.config['datatypeversion'] == 2
        assert client.config['json_items'] == ['list', 'dict']

    @patch('nckvsclient.KVSClient._request')
    def test_set(self, _request, client, system_param):
        client.set([{'key1': 'value1'}])
        assert _request.call_args == call(BASE_URL + '/data/set/', {
            'system': system_param,
            'query': {
                'datalist': [{'key1': 'value1'}],
                'datatypename': 'testtype',
                'datatypeversion': 2
            }
        })

    @patch('nckvsclient.KVSClient._request')
    def test_search(self, _request, client, system_param):
        client.search([{'key': 'key1', 'value': 'valu1', 'pattern': 'cmp'}])
        assert _request.call_args == call(BASE_URL + '/data/search/', {
            'system': system_param,
            'query': {
                'datatypename': 'testtype',
                'dataversion': '*',
                'limit': 0,
                'sortorder': [],
                'matching': [{'key': 'key1', 'value': 'valu1', 'pattern': 'cmp'}]
            }
        })

    @patch.object(request, 'urlopen')
    def test_request(self, urlopen, client):
        urlopen.return_value = StringIO('{"code":"200"}')
        res = client._request(BASE_URL, {'key': 'value'})
        assert res == {'code': '200'}
        req = urlopen.call_args[0][0]
        assert req.data == '{"key": "value"}'
        assert req.headers == {
            'Content-type': 'application/json',
            'Content-length': 16
        }

    @patch.object(request, 'urlopen')
    def test_request_error(self, urlopen, client):
        urlopen.return_value = StringIO('{"code":"400","message":"invalid"}')
        with pytest.raises(nckvs.RPCError):
            try:
                client._request(BASE_URL, {'key': 'value'})
            except nckvs.RPCError as e:
                assert e.code == '400'
                assert e.message == 'invalid'
                assert str(e) == 'RPCError: 400 invalid'
                raise

    @patch.object(request, 'urlopen')
    def test_request_multibyte(self, urlopen, client):
        urlopen.return_value = StringIO('{"code":"200"}')
        client._request(BASE_URL, {'key': '日本語'})
        req = urlopen.call_args[0][0]
        assert req.data == '{"key": "日本語"}'

    def test_jsonify(self, client):
        param = {'normal': 'value', 'list': ['v1', '日本語']}
        data = client._jsonify(param)
        assert r'"list": "[\"v1\", \"日本語\"]"' in data
