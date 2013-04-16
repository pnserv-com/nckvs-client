# -*- coding: utf-8 -*-

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


def setup_module(module):
    nckvs.setup(BASE_URL, 'user', 'pass', app_servername='appname',
                app_username='appuser', timezone='')


def teardown_module(module):
    nckvs._config.clear()
    nckvs._system_param.clear()


def pytest_funcarg__system_param(request):
    return {
        'login_name': 'user',
        'login_pass': 'pass',
        'app_servername': 'appname',
        'app_username': 'appuser',
        'timezone': ''
    }


def test_setup(system_param):
    assert nckvs._config == {
        'base_url': BASE_URL,
        'login_name': 'user',
        'login_pass': 'pass',
        'app_servername': 'appname',
        'app_username': 'appuser',
        'timezone': ''
    }
    assert nckvs._system_param == system_param


@patch('nckvsclient._request')
def test_set(_request, system_param):
    nckvs.set([{'key1': 'value1'}])
    assert _request.call_args == call(BASE_URL + '/data/set/', {
        'system': system_param,
        'query': {
            'datalist': [{'key1': 'value1'}],
            'datatypename': 'commonstest1',
            'datatypeversion': 1
        }
    })


@patch('nckvsclient._request')
def test_search(_request, system_param):
    nckvs.search([{'key': 'key1', 'value': 'valu1', 'pattern': 'cmp'}])
    assert _request.call_args == call(BASE_URL + '/data/search/', {
        'system': system_param,
        'query': {
            'datatypename': 'commonstest1',
            'dataversion': '*',
            'limit': 0,
            'sortorder': [],
            'matching': [{'key': 'key1', 'value': 'valu1', 'pattern': 'cmp'}]
        }
    })


@patch.object(request, 'urlopen')
def test_request(urlopen):
    urlopen.return_value = StringIO('{"code":"200"}')
    res = nckvs._request(BASE_URL, {'key': 'value'})
    assert res == {'code': '200'}
    req = urlopen.call_args[0][0]
    assert req.data == '{"key": "value"}'
    assert req.headers == {
        'Content-type': 'application/json',
        'Content-length': 16
    }


@patch.object(request, 'urlopen')
def test_request_error(urlopen):
    urlopen.return_value = StringIO('{"code":"400","message":"invalid"}')
    with pytest.raises(nckvs.RPCError):
        try:
            nckvs._request(BASE_URL, {'key': 'value'})
        except nckvs.RPCError as e:
            assert e.code == '400'
            assert e.message == 'invalid'
            assert str(e) == 'RPCError: 400 invalid'
            raise
