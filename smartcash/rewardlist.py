#
# Part of `python-smartcash`
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
from threading import Thread, Lock
import time
import logging
from smartcash.util import ThreadedSQLite
from smartcash.rpc import SmartCashRPC, RPCConfig
from sqlalchemy import *

logger = logging.getLogger("smartcash.rewardlist")

def reward_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]

    return SNReward(**d)

class SNRewardError(object):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return "{} - {}".format(self.code, self.message)


class SNReward(object):

    def __init__(self, **kwargs):
        attributes = ['block', 'txtime', 'payee', 'amount', 'source', 'meta', 'verified']

        for attribute in attributes:
            if attribute in kwargs:
                 setattr(self, attribute, kwargs[attribute])

        if not hasattr(self, 'block'):
            raise Exception("SNReward - Missing block")

        if not self.block or self.block < 300000:
            self.block = 0

        if not hasattr(self, 'amount'):
            if self.block:
                self.amount = round(5000.0  * ( 143500.0 / int(self.block) ) * 0.1, 1)
            else:
                self.amount = 0

        if not hasattr(self, 'source'):
            self.source = 0

        if not hasattr(self, 'meta'):
            self.meta = 0

        if not hasattr(self, 'verified'):
            self.verified = 0

    def __str__(self):
        return '[{0.payee}] {0.block} - {0.amount}'.format(self)

    def __eq__(self, other):
        return self.block == other.block

    def __hash__(self):
        return hash(self.block)

class SNRewardList(Thread):

    def __init__(self, dbPath, rpcConfig, rewardCB = None, errorCB = None):

        Thread.__init__(self)

        self.running = False
        self.paused = False
        self.daemon = True

        self.rewardCB = rewardCB
        self.errorCB = errorCB
        self.rpc = SmartCashRPC(rpcConfig)

        self.chainHeight = None
        self.currentHeight = None
        self.synced = False

        self.db = SNRewardDatabase(dbPath)

    def start(self):

        if not self.is_alive():
            logger.info("Starting!")
            Thread.start(self)

    def stop(self):
        self.running = False

    def pause(self):
        logger.info("pause")
        self.paused = True

    def resume(self):
        logger.info("resume")
        self.paused = False

    def run(self):

        self.currentHeight = 300000

        lastReward = self.getLastReward()

        if lastReward:
            self.currentHeight = lastReward.block + 1

        logger.info("Start block {}".format(self.currentHeight))

        lastInfoCheck = 0

        self.running = True

        while self.running:

            while self.paused:
                logger.info("paused!")
                time.sleep(5)

            if not lastInfoCheck or (time.time() - lastInfoCheck) > 50:

                info = self.rpc.getInfo()

                if info.error:
                    self.chainHeight = None
                    logger.error("getInfo failed {}".format(str(info.error)))
                else:
                    lastInfoCheck = time.time()
                    self.chainHeight = info['blocks']
                    logger.info("Current chain height: {}".format(self.chainHeight))

            block = self.rpc.getBlockByNumber(self.currentHeight)

            if block.error:
                logger.error("Could not fetch block {}".format(block.error))
                time.sleep(30)
                continue

            if block['confirmations'] < 3:

                logger.info("[{}] Wait for confirmations ({}): {}".format(self.currentHeight, block['confirmations'], block['hash']))
                logger.debug("BLOCK: {}".format(block.data))
                time.sleep(10)
                continue

            if not 'tx' in block:

                reward = SNReward(block=block['height'],
                                           txtime=0,
                                           payee="error",
                                           source=0,
                                           meta=-1,
                                           verified=1)

                if self.addReward(reward):
                    logger.error("No transactions in block! {} - missing payout {}".format(reward.block,reward.amount))
                    self.currentHeight += 1

                    if self.errorCB:
                        self.errorCB(SNRewardError(1, "No transactions " + str(reward)))

                    continue

            blockHeight = block['height']
            expectedPayout = 5000.0  * ( 143500.0 / blockHeight ) * 0.1
            expectedUpper = expectedPayout * 1.01
            expectedLower = expectedPayout * 0.99

            reward = None
            error = False

            # Search the new coin transaction of the block
            for tx in block['tx']:

                rawTx = self.rpc.getRawTransaction(tx)

                if rawTx.error:
                    error = True

                    if self.errorCB:
                        self.errorCB(SNRewardError(2, "getRawTransaction" + str(rawTx.error)))

                    break

                # We found the new coin transaction of the block
                if len(rawTx['vin']) == 1 and 'coinbase' in rawTx['vin'][0]:

                    for out in rawTx['vout']:

                        amount = float(out['value'])

                        if amount <= expectedUpper and amount >= expectedLower:
                           #We found the node payout for this block!

                           txtime = rawTx['time']
                           payee = out['scriptPubKey']['addresses'][0]

                           reward = SNReward(block=blockHeight,
                                           txtime=txtime,
                                           payee=payee,
                                           amount=amount,
                                           source=0,
                                           meta=0,
                                           verified=1)

            if self.paused:
                continue

            if reward:

                if self.addReward(reward):
                    self.currentHeight += 1
                    logger.debug("Added: {}".format(str(reward)))
                else:
                    if self.verifyReward(reward):
                        self.currentHeight += 1
                        logger.debug("Verified: {}".format(str(reward)))
                    else:
                        logger.warning("Could not verify reward - {}".format(str(reward)))

                        if self.errorCB:
                            self.errorCB(SNRewardError(-1, "Could not verify reward {}".format(str(reward))))

                        time.sleep(300)

                if self.rewardCB:
                    self.rewardCB(reward, self.blockDistance())

            elif not error:

                if self.errorCB:
                    self.errorCB(SNRewardError(3, "Could not find reward in transactions!"))

                reward = SNReward(block=block['height'],
                                           txtime=0,
                                           payee="error",
                                           source=0,
                                           meta=-2,
                                           verified=1)
                if self.addReward(reward):
                    self.currentHeight += 1
                    logger.error("Could not fetch reward! {} - missing payout {}".format(reward.block,reward.amount))

            else:
                time.sleep(60)
                logger.debug("Unexpected error occured. Sleep a bit..")

    def blockDistance(self):
        return self.chainHeight - self.currentHeight if self.chainHeight else sys.maxsize

    def addReward(self, reward):

        try:

            with self.db.connection as db:
                query = "INSERT INTO rewards(\
                        block,\
                        txtime,\
                        payee,\
                        amount,\
                        source,\
                        meta,\
                        verified) \
                        values( ?, ?, ?, ?, ?, ?, ? )"

                db.cursor.execute(query, (
                                  reward.block,
                                  reward.txtime,
                                  reward.payee,
                                  reward.amount,
                                  reward.source,
                                  reward.meta,
                                  reward.verified))

                return True

        except Exception as e:
            pass
            #logger.info("addReward", exc_info=e)

        return False

    def getLastReward(self):

        lastReward = None

        try:

            with self.db.connection as db:
                db.cursor.row_factory = reward_factory
                db.cursor.execute("SELECT * FROM rewards WHERE verified=1 ORDER BY block DESC LIMIT 1")
                lastReward = db.cursor.fetchone()

        except Exception as e:
            logger.error("getLastReward", exc_info=e)

        return lastReward

    def getRewardsForPayee(self, payee, fromTime = None):

        payouts = None
        query = "SELECT * FROM rewards WHERE payee=? "

        if fromTime:
            query += "AND txtime >= {}".format(int(fromTime))

        try:

            with self.db.connection as db:
                db.cursor.row_factory = reward_factory
                db.cursor.execute(query,[payee])
                payouts = db.cursor.fetchall()

        except Exception as e:
            logger.error("getRewardsForPayee - " + query, exc_info=e)

        return payouts

    def verifyReward(self, reward):

        updated = False

        query = "UPDATE rewards SET verified=1 WHERE block=?"

        with self.db.connection as db:

            db.cursor.execute(query,[reward.block]).rowcount
            updated = db.cursor.rowcount

        return updated

    def getNextReward(self, fromTime=None):

        nextReward = None

        query = "SELECT * FROM rewards WHERE txtime>={} LIMIT 1".format(int(fromTime))

        try:

            with self.db.connection as db:
                db.cursor.row_factory = reward_factory
                db.cursor.execute(query)
                nextReward = db.cursor.fetchone()

        except Exception as e:
            logger.error("getNextReward - " + query, exc_info=e)

        return nextReward

    def updateSource(self, reward):

        updated = False

        query = "UPDATE rewards SET source=? WHERE block=?"

        with self.db.connection as db:
            db.cursor.execute(query,(reward.source, reward.block))
            updated = db.cursor.rowcount

        return updated

    def updateMeta(self, reward):

        updated = False

        query = "UPDATE rewards SET meta=? WHERE block=?"

        with self.db.connection as db:
            db.cursor.execute(query,(reward.meta, reward.block))
            updated = db.cursor.rowcount

        return updated

    def getRewardCount(self, start = None, meta = None, source = None):

        query = "SELECT count(*) as c FROM rewards "

        if start != None:
            query += "WHERE txtime>={} ".format(int(start))

        if source != None:
            query += "{} source={} ".format("WHERE" if not "WHERE" in query else "AND", int(source))

        if meta != None:
            query += "{} meta={} ".format("WHERE" if not "WHERE" in query else "AND", int(meta))

        with self.db.connection as db:

            db.cursor.execute(query)
            rewards = db.cursor.fetchone()


        return rewards['c']

    def getReward(self, block):

        reward = None

        query = "SELECT * FROM rewards WHERE block={}".format(block)

        try:

            with self.db.connection as db:

                db.cursor.row_factory = reward_factory
                db.cursor.execute(query)
                reward = db.cursor.fetchone()

        except Exception as e:
            logger.info("getReward - " + query, exc_info=e)

        return reward

    def getRewards(self, payee, start = None):

        rewards = []
        query = "SELECT * FROM rewards WHERE payee='{}' ".format(payee)

        if start:
            query += "AND txtime >= {}".format(int(start))

        try:

            with self.db.connection as db:
                db.cursor.row_factory = reward_factory
                db.cursor.execute(query)
                rewards = db.cursor.fetchall()

        except Exception as e:
            logger.info("getRewards - " + query, exc_info=e)

        return rewards

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

        result = None

        with self.connection as db:
            db.cursor.execute(query)
            result = db.cursor.fetchall()

        return result

    def reset(self):

        sql = '\
        BEGIN TRANSACTION;\
        CREATE TABLE "rewards" (\
        	`block` INTEGER NOT NULL PRIMARY KEY,\
        	`txtime` INTEGER,\
        	`payee` TEXT,\
            `amount` REAL,\
            `source` INTEGER,\
            `meta` INTEGER,\
            `verified` INTEGER\
        );\
        COMMIT;'

        with self.connection as db:
            db.cursor.executescript(sql)
