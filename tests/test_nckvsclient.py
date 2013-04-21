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
                           app_username='appuser', timezone='')


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
            'datatypeversion': 2
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
            'datatypeversion': 1
        }

    def test_from_file2(self):
        filename = os.path.join(DATA_DIR, 'config.test.ini')
        client = nckvs.KVSClient.from_file(filename, 'other')
        assert client.config['datatypeversion'] == 2

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

    @patch.object(request, 'urlopen')
    def test_set_request(self, urlopen, client):
        urlopen.return_value = StringIO('{"code":"200"}')
        d = {'id': '-1', 'key': '日本語', 'list': [1, 2], 'hash': {'k': 'v'}}
        client.set([d])
        req = urlopen.call_args[0][0]
        assert '"query": {"datalist": [{' in req.data
        assert r'"hash": "{\"k\": \"v\"}"' in req.data
        assert '"list": "[1, 2]"' in req.data

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

    @patch('nckvsclient.KVSClient._request')
    def test_delete(self, _request, client, system_param):
        client.delete([1, 2])
        assert _request.call_args == call(BASE_URL + '/data/delete/', {
            'system': system_param,
            'query': {
                'datatypename': 'testtype',
                'datatypeversion': 2,
                'idlist': [1, 2]
            }
        })

    @patch('nckvsclient.KVSClient.set')
    @patch('nckvsclient.KVSClient.search')
    def test_upsert(self, search, set_, client, system_param):
        item = {'doc_id': '0001', 'rev': '1', 'value': 'first'}

        search.return_value = {'datalist': []}
        client.upsert(item, 'doc_id')
        assert search.call_args == call([{
            'key': 'doc_id', 'value': '0001', 'pattern': 'cmp'
        }])
        assert set_.call_args == call([dict(id='-1', **item)])

        search.return_value = {'datalist': [dict(id='1', **item)]}
        client.upsert(item, 'doc_id')
        assert set_.call_args == call([dict(id='1', **item)])

        client.upsert(item, 'doc_id', 'fcmp')
        assert search.call_args == call([{
            'key': 'doc_id', 'value': '0001', 'pattern': 'fcmp'
        }])

        set_.reset_mock()
        client.upsert(item, 'doc_id', cmp=lambda x, y: False)
        assert set_.call_count == 0

        search.return_value = {'datalist': [dict(id='1', **item),
                                            dict(id='2', **item)]}
        with pytest.raises(nckvs.NotUniqueError):
            client.upsert(item, 'doc_id')

    @patch.object(request, 'urlopen')
    def test_request(self, urlopen, client):
        urlopen.return_value = StringIO('{"code":"200","datalist":[]}')
        res = client._request(BASE_URL, {'key': 'value'})
        assert res == {'code': '200', 'datalist': []}
        req = urlopen.call_args[0][0]
        assert req.data == '{"key": "value"}'
        assert req.headers == {
            'Content-type': 'application/json',
            'Content-length': 16
        }

    @patch.object(request, 'urlopen')
    def test_request_multibyte(self, urlopen, client):
        urlopen.return_value = StringIO('{"code":"200"}')
        client._request(BASE_URL, {'key': '日本語'})
        req = urlopen.call_args[0][0]
        assert req.data == '{"key": "日本語"}'

    def test_error_response(self, client):
        res = '{"code":"400","message":"invalid"}'
        with pytest.raises(nckvs.RPCError):
            try:
                client._parse_response(res)
            except nckvs.RPCError as e:
                assert e.code == '400'
                assert e.message == 'invalid'
                assert str(e) == 'RPCError: 400 invalid'
                raise

    def test_parse_result(self, client):
        res = (r'{"code":"200","datalist":['
               r'{"k1":"v1","list":"[\"v2\",\"v3\"]","num":1},'
               r'{"k4":"v4","list":"[\"v5\",\"v6\"]"}]}')
        assert client._parse_response(res) == {
            'code': '200', 'datalist': [
                {'k1': 'v1', 'list': ['v2', 'v3'], 'num': 1},
                {'k4': 'v4', 'list': ['v5', 'v6']}
            ]
        }
