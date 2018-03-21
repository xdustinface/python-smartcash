#
# Part of `libsmartcash`
#
# Copyright 2018 dustinface
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import re
import subprocess
import json
import logging
import re
import copy
import base64
import http.client as http
from urllib.parse import urlparse

logger = logging.getLogger("smartcash-rpc")

class RPCException(Exception):
    def __init__(self, code = None, message = None):
        super(RPCException, self).__init__()
        self.error = RPCError(code,message)

    def __str__(self):
        return '{} => {}'.format(self.error.code, self.error.message)

class RPCError(object):
    def __init__(self, code = None, message = None):
        self.code = code if code else "?"
        self.message = message if message else "?"

class RPCResponse(object):
    def __init__(self, data = None, error = None):
        self.data = data
        self.error = error

class RPCConfig(object):
    def __init__(self, user, password, url = "http://127.0.0.1", port = 9679, timeout = 20):

        self.port = port

        self.url = urlparse(url)

        try:
            self.user = user.encode('utf8')
        except:
            self.user = user

        try:
            self.password = password.encode('utf8')
        except:
            self.password = password

        self.authHeader = b'Basic ' + base64.b64encode(self.user + b':' + self.password)
        self.timeout = timeout

class SmartCashRPC(object):

    def __init__(self, config):

        self.config = copy.deepcopy(config)
        self.connection = None


    def request(self, method, args = None):

        self.connection = http.HTTPConnection(self.config.url.hostname, self.config.port,
                                             timeout=self.config.timeout)

        post = json.dumps({'version': '1.1',
                               'method': method,
                               'params': args})

        try:
            self.connection.request('POST', self.config.url.path, post,
                                {'Host': self.config.url.hostname,
                                 'Authorization': self.config.authHeader,
                                 'Content-type': 'application/json'})

            self.connection.sock.settimeout(self.config.timeout)

            response = self.connection.getresponse()

        except Exception as e:
            raise RPCException(10,'Request error - {}'.format(e))
        else:

            if response is None:
                raise RPCException(11,'No response from server')

            if response.getheader('Content-Type') != 'application/json':
                raise RPCException(12, 'Non JSON response: {}, {}'.format(response.status, response.reason))

            try:
                data = response.read().decode('utf8')
                response = json.loads(data)
            except:
                response = None

            if not response:
                raise RPCException(13, 'JSON response parse error')

            error = response['error'] if 'error' in response else None
            result = response['result'] if 'result' in response else None

            if error:
                raise RPCException(response['error']['code'],response['error']['message'])

            if not result:
                raise RPCException(14,' RPC result missing')

        return result

    def validateAddress(self, address):

        cleanAddress = re.sub('[^A-Za-z0-9]+', '', address)

        try:

            cliResult = subprocess.check_output(['smartcash-cli', 'validateaddress',cleanAddress])
            output = cliResult.decode('utf-8')

        except RPCException as e:
            response.error = e.error
            logging.error('validateAddress', exc_info=e)
        else:

            validate = self.parseResponse(output)

            if not validate:
                logging.error("validateAddress no valid response")
                return False

            if "isvalid" in validate and validate["isvalid"] == True:
                return True

        return False

    def getInfo(self):

        response = RPCResponse()

        try:
            response.data = self.request('getinfo')
        except RPCException as e:
            response.error = e.error
            logging.debug('getInfo', exc_info=e)

        return response

    def getBlockByHash(self, blockHash):

        response = RPCResponse()

        try:
            response.data = self.request('getblock', [blockHash])
        except RPCException as e:
            response.error = e.error
            logging.debug('getBlockByHash', exc_info=e)

        return response

    def getBlockByNumber(self, number):

        response = RPCResponse()

        try:
            response.data = self.request('getblockhash', [number])
        except RPCException as e:
            response.error = e.error
            logging.debug('getBlockByNumber', exc_info=e)
        else:

            if response.data:
                return self.getBlockByHash(response.data)

        return response

    def getRawTransaction(self, txhash):

        response = RPCResponse()

        try:
            response.data = self.request('getrawtransaction', [txhash, 1])
        except RPCException as e:
            response.error = e.error
            logging.debug('getRawTransaction', exc_info=e)

        return response

    def getSyncStatus(self):

        response = RPCResponse()

        try:
            response.data = self.request('snsync', ['status'])
        except RPCException as e:
            response.error = e.error
            logging.debug('snsync', exc_info=e)
        else:

            # {
            #   "AssetID": 999,
            #   "AssetName": "SMARTNODE_SYNC_FINISHED",
            #   "Attempt": 0,
            #   "IsBlockchainSynced": true,
            #   "IsMasternodeListSynced": true,
            #   "IsWinnersListSynced": true,
            #   "IsSynced": true,
            #   "IsFailed": false
            # }
            if not response.data:
                logging.debug('getSyncStatus no status')
            elif not 'IsBlockchainSynced' in response.data:
                err = 'getSyncStatus no IsBlockchainSynced'
                response.data = None
                response.error = RPCError(16,err)
                logging.debug(err)
            elif not 'IsMasternodeListSynced' in response.data:
                err = 'getSyncStatus no IsMasternodeListSynced'
                response.data = None
                response.error = RPCError(16,err)
                logging.debug(err)
            elif not 'IsWinnersListSynced' in response.data:
                err = 'getSyncStatus no IsWinnersListSynced'
                response.data = None
                response.error = RPCError(16,err)
                logging.debug(err)

        return response

    def getSmartNodeList(self, mode):

        response = RPCResponse()

        try:
            response.data = self.request('smartnode', ['list', mode ])
        except RPCException as e:
            response.error = e.error
            logging.debug('snsync', exc_info=e)

        return response
