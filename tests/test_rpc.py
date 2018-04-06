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

import logging
import time
import sys
from smartcash.rpc import SmartCashRPC, RPCConfig

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

logger = logging.getLogger("rpctest")

def test(success, msg, *margs):

    text = msg.format(*margs)

    if success:
        logger.info("[PASSED] {}".format(text))
    else:
        logger.error("[FAILED] {}".format(text))
        raise Exception("Test stopped")

if __name__ == '__main__':

    config = RPCConfig('someusername','somepassword')

    rpc = SmartCashRPC(config)

    sync = rpc.getSyncStatus()

    test(sync.data, "get sync state")
    test(not sync.error, "get sync error")
    test(sync['IsBlockchainSynced'], "blockchain synced")
    test(sync['IsMasternodeListSynced'], "nodelist synced")
    test(sync['IsWinnersListSynced'], "winnerslist synced")

    info = rpc.getInfo()
    test(info.data, "get info")

    test('blocks' in info, "get current block => {}".format(info['blocks'] if 'blocks' in info else None))

    blockInvalid = rpc.getBlockByHash("0000000000INVALIDINVALIDINVALIDINVALIDINVALIDINVALIDINVALIDINVALID")
    test(blockInvalid.data == None, "get block with invalid hash")

    nodelist = rpc.getSmartNodeList('full')

    test(nodelist.data != None, "get nodelist full")
    test(len(nodelist), "nodes available {}", len(nodelist.data))

    lastBlock = 0
    testedBlocks = 0

    while testedBlocks != 3:

        logger.info("[SLEEP] Wait for a new block")

        time.sleep(30)

        info = rpc.getInfo()
        test(info.data, "get info")

        test('blocks' in info.data, "get current block => {}".format(info.data['blocks'] if 'blocks' in info.data else None))

        if lastBlock != info.data['blocks']:
            lastBlock = info.data['blocks']
        else:
            continue

        blockI = rpc.getBlockByNumber(lastBlock)
        test(blockI.data != None, "get block {} by number", lastBlock)
        test('hash' in blockI.data and blockI.data['hash'] != "", "hash available? => {}", blockI.data['hash'] if 'hash' in blockI.data else None)

        blockH = rpc.getBlockByHash(blockI.data['hash'])
        test(blockH.data != None, "get block {} by hash".format(lastBlock))
        test(int(blockH.data['height']) == lastBlock, "check block {} height by hash",lastBlock )

        for tx in blockH.data['tx']:
            rawTx = rpc.getRawTransaction(tx)
            test( rawTx.data != None, "get rawTx {}".format(tx))
            test(rawTx.data['blockhash'] == blockH.data['hash'], "compare block of rawTx {}".format(tx) )

        testedBlocks += 1
