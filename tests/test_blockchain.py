#!/usr/bin/env python3
#####
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
#####

import os, logging
from smartcash.rpc import RPCConfig
from smartcash.blockchain import SmartCashBlockchain

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)

if __name__ == '__main__':

    directory = os.path.dirname(os.path.realpath(__file__))

    rpcConfig = RPCConfig('smart','cash')

    # Test SQLITE
    #chain = SmartCashBlockchain('sqlite:////' + directory + '/tt.db', rpcConfig)

    # Test MYSQL - You need to create the database before!
    chain = SmartCashBlockchain('mysql+mysqlconnector://root:smartcash@localhost/smartcash')

    chain.run()

    while True:
        address = input("Address: ")
        print("Outputs: {}".format(chain.getNumerOfOutputs(address)))
        print("Balance {}".format(chain.getBalance(address)))

    # Test POSTGRESS
    #chain = SmartCashBlockchain('sqlite:////' + directory + '/tt.db', rpcConfig)
