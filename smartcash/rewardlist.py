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
from threading import Thread, Lock
import time
import logging
from smartcash.util import ThreadedSQLite
from smartcash.rpc import SmartCashRPC, RPCConfig
from sqlalchemy import *

logger = logging.getLogger("smartcash.rewardlist")

class SNReward(object):

    def __init__(self, **kwargs):
        attributes = ['block', 'txtime', 'payee', 'amount', 'source', 'meta']

        for attribute in attributes:
            if attribute in kwargs:
                 setattr(self, attribute, kwargs[attribute])

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

    def __str__(self):
        return '[{0.payee}] {0.block} - {0.amount}'.format(self)

    def __eq__(self, other):
        return self.block == other.block

    def __hash__(self):
        return hash(self.block)

class SNRewardList(Thread):

    def __init__(self, sessionPath, rpcConfig, rewardCB = None):

        Thread.__init__(self)

        self.running = False
        self.daemon = True

        self.conn = None
        self.rewardCB = rewardCB
        self.rpc = SmartCashRPC(rpcConfig)
        self.sessionPath = sessionPath

        self.chainHeight = None
        self.currentHeight = None
        self.synced = False

        self.lock = Lock()

        self.metadata = MetaData()

        self.rewards = Table('rewards', self.metadata,
            Column('block', Integer, primary_key=True, autoincrement=False),
            Column('txtime', Integer),
            Column('payee', Text),
            Column('amount', Float),
            Column('source', Integer),
            Column('meta', Integer),
        )

    def start(self):

        if not self.is_alive():
            logger.info("Starting!")
            Thread.start(self)

    def stop(self):
        self.running = False

    def run(self):

        self.lock.acquire()

        self.engine = create_engine(self.sessionPath, connect_args={'check_same_thread': False}, echo=False)

        self.metadata.create_all(self.engine)

        self.conn = self.engine.connect()

        self.lock.release()

        abusedSmart = 0
        self.currentHeight = 300000

        lastReward = self.getLastReward()

        if lastReward:
            self.currentHeight = int(lastReward.block) + 1

        logger.info("Start block {}".format(self.currentHeight))

        lastInfoCheck = 0

        self.running = True

        while self.running:

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

            self.currentHeight += 1

            reward = self.getRewardForBlock(block)

            if reward:

                if self.addReward(reward):
                    logger.debug("Added: {}".format(str(reward)))

                if self.rewardCB:
                    self.rewardCB(reward, self.isSynced())

            else:

                reward = SNReward(block=block['height'],
                                           txtime=0,
                                           payee="error",
                                           source=0,
                                           meta=-1)
                if self.addReward(reward):

                    logger.error("Could not fetch reward! {} - missing payout {}".format(reward.block,reward.amount))

    def isSynced(self):
        return self.chainHeight and self.currentHeight >= ( self.chainHeight - 4 )

    def getRewardForBlock(self, block):

        if not block or 'tx' not in block:
            return None

        blockHeight = block['height']
        expectedPayout = 5000.0  * ( 143500.0 / blockHeight ) * 0.1
        expectedUpper = expectedPayout * 1.01
        expectedLower = expectedPayout * 0.99

        # Search the new coin transaction of the block
        for tx in block['tx']:

            rawTx = self.rpc.getRawTransaction(tx)

            if rawTx.error:
                return None

            # We found the new coin transaction of the block
            if len(rawTx['vin']) == 1 and 'coinbase' in rawTx['vin'][0]:

                for out in rawTx['vout']:

                    amount = float(out['value'])

                    if amount <= expectedUpper and amount >= expectedLower:
                       #We found the node payout for this block!

                       txtime = rawTx['time']
                       payee = out['scriptPubKey']['addresses'][0]

                       return SNReward(block=blockHeight,
                                       txtime=txtime,
                                       payee=payee,
                                       amount=amount,
                                       source=0,
                                       meta=0)

        return None

    def execute(self, query):

        self.lock.acquire()

        result = self.conn.execute(query)

        self.lock.release()

        return result

    def addReward(self, reward):

        try:

            newReward = self.rewards.insert()\
                           .values(block=reward.block,
                                   txtime=reward.txtime,
                                   payee=reward.payee,
                                   amount=reward.amount,
                                   source=reward.source,
                                   meta=reward.meta)

            if self.execute(newReward):
                return True

        except Exception as e:
            logger.debug("addReward", exc_info=e)
            self.lock.release()

        return False

    def getNextReward(self, fromTime=None):

        query = select([self.rewards]).where(self.rewards.c.txtime >= fromTime).limit(1)

        nextReward = self.execute(query).fetchone()

        return nextReward if nextReward else None

    def getLastReward(self):

        query = select([self.rewards]).order_by(self.rewards.c.block.desc()).limit(1)

        lastReward = self.execute(query).fetchone()

        return lastReward if lastReward else None

    def updateSource(self, reward):

        query = self.rewards.update().\
                    values(source=reward.source).\
                    where(self.rewards.c.block == reward.block)

        updated = self.execute(query).rowcount

        if not updated:
            updated = self.addReward(reward)
            logger.info("updateSource not found, add {}".format(updated))
        else:
            logger.info("updateSource updated - {}".format(str(reward)))

        return updated

    def updateMeta(self, reward):

        query = self.rewards.update().\
                    values(meta=reward.meta).\
                    where(self.rewards.c.block == reward.block)

        updated = self.execute(query).rowcount

        if not updated:
            updated = self.addReward(reward)
            logger.info("updateMeta not found, add {}".format(updated))
        else:
            logger.info("updateMeta updated - {}".format(str(reward)))

        return updated

    def getRewardsForPayee(self, payee, fromTime = None):

        query = select([self.rewards]).where(self.rewards.c.payee == payee)

        if fromTime:
            query = query.where(self.rewards.c.txtime >= fromTime)

        rewards = self.execute(query).fetchall()

        return rewards

    def getRewardCount(self, start = None, meta = None, source = None):

        query = select([func.count(self.rewards.c.block)])

        if start != None:
            query = query.where(self.rewards.c.txtime >= start)

        if source != None:
            query = query.where(self.rewards.c.source == source)

        if meta != None:
            query = query.where(self.rewards.c.meta == meta)

        rewards = self.execute(query).scalar()

        return rewards

    def getReward(self, block):

        query = select([self.rewards]).where(self.rewards.c.block == block)

        rewards = self.execute(query).fetchone()

        return rewards

    def getRewards(self, payee, start = None):

        query = select([self.rewards]).where(self.rewards.c.payee == payee)

        if start:
            query = query.where(self.rewards.c.txtime >= start)

        rewards = self.execute(query).fetchall()

        return rewards
