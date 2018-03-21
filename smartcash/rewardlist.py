#
# Part of `libsmartcash`
#
# Simple class to create and maintain a database
# with all paid SmartNode rewards.
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

import os, sys
import time
import logging
from smartcash.util import ThreadedSQLite
from smartcash.rpc import SmartCashRPC, RPCConfig

logger = logging.getLogger("_rewards_")

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

class SNRewardList(object):

    def __init__(self, dbPath, rpcConfig, rewardCB = None):
        self.db = SNRewardDatabase(dbPath)
        self.rewardCB = rewardCB
        self.rpc = SmartCashRPC(rpcConfig)

    def run(self):

        abusedSmart = 0
        currentHeight = 300000

        lastReward = self.db.getLastReward()

        if lastReward:
            currentHeight = int(lastReward['block']) + 1

        logger.info("Start block {}".format(currentHeight))

        while True:

            currentBlock = self.rpc.getBlockByNumber(currentHeight)

            if not currentBlock.data:
                logger.error("Could not fetch block {}".format(block.error))
                return

            if currentBlock.data['confirmations'] < 3:
                logger.info("[{}] Wait for confirmations ({}): {}".format(currentHeight, currentBlock.data['confirmations'], currentBlock.data['hash']))
                logger.debug("BLOCK: {}".format(currentBlock.data))
                time.sleep(30)
                continue

            block = currentBlock.data

            currentHeight += 1

            reward = self.getRewardForBlock(block)

            if reward:

                if self.db.addReward(reward,2):
                    logger.debug("Added: {}".format(str(reward)))

                if self.rewardCB:
                    self.rewardCB(reward)

            else:

                expectedSmart = 5000  * ( 143500 / int(block['height']) ) * 0.1

                if self.db.addReward(SNReward(block['height'],0,"error",expectedSmart),2):
                    logger.error("Could not fetch reward! {} - missing payout {}".format(block['height'],expectedSmart))

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

class SNRewardDatabase(object):

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


    def addReward(self, reward,  source):

        try:

            with self.connection as db:
                query = "INSERT INTO rewards(\
                        block,\
                        txtime,\
                        payee,\
                        amount,\
                        source) \
                        values( ?, ?, ?,  ?, ? )"

                db.cursor.execute(query, (
                                  reward.block,
                                  reward.txtime,
                                  reward.destination,
                                  reward.amount,
                                  source))

                return True

        except Exception as e:
            logger.debug("addReward", exc_info=e)

        return False

    def getLastReward(self):

        lastReward = None

        with self.connection as db:

            db.cursor.execute("SELECT * FROM rewards WHERE block=(SELECT max(block) FROM rewards)")

            lastReward = db.cursor.fetchone()

        return lastReward

    def getPayoutsForPayee(self, payee):

        payouts = None

        with self.connection as db:

            db.cursor.execute("SELECT * FROM rewards WHERE payee=?",[payee])

            payouts = db.cursor.fetchall()

        return payouts

    def reset(self):

        sql = '\
        BEGIN TRANSACTION;\
        CREATE TABLE "rewards" (\
        	`block` INTEGER NOT NULL PRIMARY KEY,\
        	`txtime` INTEGER,\
        	`payee` TEXT,\
            `amount` REAL,\
            `source` INTEGER\
        );\
        COMMIT;'

        with self.connection as db:
            db.cursor.executescript(sql)
