#####
# Part of `libsmartcash`
#
# Class to maintain a database of the SmartCash blockchain.
#  * Creates the database
#  * Keeps the blocks/transactions synced
#  * Callacks for new blocks
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

import os, sys
import time
import logging
import threading
from smartcash.util import ThreadedSQLite
from smartcash.rpc import SmartCashRPC, RPCConfig

logger = logging.getLogger("_chain_")

class SNReward(object):

    def __init__(self, block, txtime, destination, amount):
        self.block = block
        self.txtime = txtime
        self.destination = destination
        self.amount = amount

    def __str__(self):
        return '[{0.destination}] {0.block} - {0.amount}'.format(self)

    def __eq__(self, other):
        return self.block == other.block

    def __hash__(self):
        return hash(self.block)

class SmartCashBlockchain(object):

    def __init__(self, dbPath, rpcConfig, newBlockCB = None):
        self.db = SmartCashBlockchainDB(dbPath)
        self.newBlockCB = newBlockCB
        self.rpc = SmartCashRPC(rpcConfig)

    def run(self):

        currentHeight = self.db.getCurrentHeight()

        if not currentHeight:
            currentHeight = 0
        else:
            currentHeight = currentHeight['height'] + 1

        logger.info("Start block {}".format(currentHeight))

        while True:

            block = self.rpc.getBlockByNumber(currentHeight)

            if not block.data:
                sys.exit("Could not fetch block {}".format(block.error))

            if block.data['confirmations'] < 3:
                logger.info("Wait for confirmations: {}".format(block.data['hash']))
                time.sleep(30)
                continue

            if currentHeight % 100 == 0:
                logger.warning("{}".format(currentHeight))

            currentHeight += 1

            block = block.data
            height = block['height']

            if not self.db.addBlock(block):
                sys.exit("[{}] Error: {}".format(height,block))

            logger.info("[{}] Added: {}".format(height, str(block['hash'])))

            for tx in block['tx']:

                if height == 0:

                    if self.db.addTransaction(height, {
                    'hash' : tx,
                    'size' : 0,
                    'version' : 0
                    }):
                        continue
                    else:
                        sys.exit("Could not add genesis transaction.")

                rawTx = self.rpc.getRawTransaction(tx)

                if not rawTx.data:
                    sys.exit("Could not fetch raw transaction {}".format(rawTx.error))

                rawTx = rawTx.data
                txId = rawTx['hash']

                if not self.db.addTransaction(height, rawTx):
                    sys.exit("  => error add tx: {}".format(rawTx))

                logger.info("  => added TX: {}".format(txId))

                for output in rawTx['vout']:

                    if not self.db.addOutput(txId, output):
                        sys.exit("  => error add output: {}".format(output))

                    address = 'unknown'

                    if 'pub' in output['scriptPubKey']['type']:
                        address = output['scriptPubKey']['addresses'][0]

                    logger.info("    => added output: {} - {}".format(address,output['value']))

                for input in rawTx['vin']:

                    if not self.db.addInput(txId, input):
                        sys.exit("  => error add input: {}".format(input))

                    outTxId = None

                    if 'coinbase' in input:
                        outTxId = input['coinbase']
                    else:
                        outTxId = input['txid']

                    logger.info("    => added input: {}".format(outTxId))


    def getRewardForBlock(self, block):

        if not block or 'tx' not in block:
            return None

        blockHeight = block['height']
        expectedPayout = 5000  * ( 143500 / int(blockHeight) ) * 0.1
        expectedUpper = expectedPayout * 1.01
        expectedLower = expectedPayout * 0.99

        # Search the new coin transaction of the block
        for tx in block['tx']:
            rawTx = self.rpc.getRawTransaction(tx)

            if not rawTx.data:
                return None

            rawTx = rawTx.data

            # We found the new coin transaction of the block
            if len(rawTx['vin']) == 1 and 'coinbase' in rawTx['vin'][0]:

                for out in rawTx['vout']:

                    amount = int(out['value'])

                    if amount <= expectedUpper and amount >= expectedLower:
                       #We found the node payout for this block!

                       txtime = rawTx['time']
                       payee = out['scriptPubKey']['addresses'][0]

                       return SNReward(blockHeight, txtime, payee, amount)

        return None

#####
#
# Wrapper for the node database where all the nodes from the
# global nodelist are stored.
#
#####

class SmartCashBlockchainDB(object):

    def __init__(self, dburi):

        self.connection = ThreadedSQLite(dburi)

        if self.isEmpty():
            self.reset()

    def isEmpty(self):

        tables = []

        with self.connection as db:

            db.cursor.execute("SELECT name FROM sqlite_master")

            tables = db.cursor.fetchall()

        return len(tables) == 0

    def raw(self, query):

        with self.connection as db:
            db.cursor.execute(query)
            return db.cursor.fetchall()

        return None


    def addBlock(self, block):
        	# `height` INTEGER NOT NULL PRIMARY KEY,\
            # `hash` TEXT,\
            # `size` INTEGER,\
            # `time` INTEGER,\
            # `mediantime` INTEGER,\
            # `version` INTEGER,\
        	# `merkleroot` TEXT\
        try:

            with self.connection as db:
                query = "INSERT INTO block(\
                        height,\
                        hash,\
                        size,\
                        blocktime,\
                        mediantime,\
                        version,\
                        merkleroot) \
                        values( ?, ?, ?, ?, ?, ?, ? )"

                db.cursor.execute(query, (
                                  block['height'],
                                  block['hash'],
                                  block['size'],
                                  block['time'],
                                  block['mediantime'],
                                  block['version'],
                                  block['merkleroot']))

                return True

        except Exception as e:
            logger.debug("Duplicate?!", exc_info=e)

        return False

    def addTransaction(self, height, tx):
        	# `id` INTEGER PRIMARY KEY AUTOINCREMENT,\
            # `hash` TEXT NOT NULL,\
            # `height` INTEGER,\
        	# `size` INTEGER,\
            # `version` INTEGER\
        try:

            with self.connection as db:
                query = "INSERT INTO tx(\
                        hash,\
                        height,\
                        size,\
                        version) \
                        values( ?, ?, ?, ? )"

                db.cursor.execute(query, (
                                  tx['hash'],
                                  height,
                                  tx['size'],
                                  tx['version']))

                return True

        except Exception as e:
            logger.debug("Duplicate?!", exc_info=e)

        return False

    def addOutput(self, txid, output):
        	# `txid` INTEGER NOT NULL PRIMARY KEY,\
        	# `n` INTEGER NOT NULL,\
        	# `value` REAL NOT NULL,\
            # `address` TEXT NOT NULL\
        try:

            with self.connection as db:
                query = "INSERT INTO txout(\
                        txid,\
                        n,\
                        type,\
                        value,\
                        address) \
                        values( ?, ?, ?, ?, ? )"

                address = "unknown"
                t = output['scriptPubKey']['type']

                if 'pub' in t:
                    address = output['scriptPubKey']['addresses'][0]

                db.cursor.execute(query, (
                                  txid,
                                  output['n'],
                                  t,
                                  output['value'],
                                  address))

                return True

        except Exception as e:
            logger.debug("Duplicate?!", exc_info=e)

        return False

    def addInput(self, txid, input):
            # `txid` TEXT NOT NULL,\
            # `out_txid` TEXT NOT NULL,\
        	# `n` INTEGER NOT NULL,\
        	# `sequence` TEXT\
        try:

            with self.connection as db:
                query = "INSERT INTO txin(\
                        txid,\
                        out_txid,\
                        n,\
                        sequence) \
                        values( ?, ?, ?, ?)"

                outTxId = None

                if 'coinbase' in input:
                    outTxId = 'coinabse'
                    n = -1
                else:
                    outTxId = input['txid']
                    n = input['vout']


                db.cursor.execute(query, (
                                  txid,
                                  outTxId,
                                  n,
                                  input['sequence']))

                return True

        except Exception as e:
            logger.debug("Duplicate?!", exc_info=e)

        return False

    def getCurrentHeight(self):

        currentHeight = None

        with self.connection as db:

            db.cursor.execute("SELECT * FROM block WHERE height=(SELECT max(height) FROM block)")

            currentHeight = db.cursor.fetchone()

        return currentHeight

    def reset(self):

        sql = '\
        BEGIN TRANSACTION;\
        CREATE TABLE "block" (\
        	`height` INTEGER NOT NULL PRIMARY KEY,\
            `hash` TEXT NOT NULL,\
            `blocktime` INTEGER,\
            `mediantime` INTEGER,\
            `version` INTEGER,\
        	`size` INTEGER,\
        	`merkleroot` TEXT\
        );\
        CREATE TABLE "tx" (\
            `hash` TEXT NOT NULL,\
            `height` INTEGER,\
        	`size` INTEGER,\
            `version` INTEGER\
        );\
        CREATE TABLE "txin" (\
            `txid` TEXT NOT NULL,\
            `out_txid` TEXT NOT NULL,\
        	`n` INTEGER NOT NULL,\
        	`sequence` TEXT\
        );\
        CREATE TABLE "txout" (\
        	`txid` TEXT NOT NULL,\
        	`n` INTEGER NOT NULL,\
            `type` TEXT,\
        	`value` REAL NOT NULL,\
            `address` TEXT NOT NULL\
        );\
        COMMIT;'

        with self.connection as db:
            db.cursor.executescript(sql)
