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

from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, Integer, Float, String, MetaData, ForeignKey
from sqlalchemy import *

from smartcash.rpc import SmartCashRPC

logger = logging.getLogger("smartcash.chaindatabase")

DEFAULT_MODE = 0
SYNC_MODE = 1

class SmartCashBlockchain(object):

    def __init__(self, sessionPath, rpcConfig = None, newBlockCB = None):

        self.newBlockCB = newBlockCB

        if not rpcConfig:
            self.mode = DEFAULT_MODE
        else:
            self.mode = SYNC_MODE
            self.rpc = SmartCashRPC(rpcConfig)

        self.engine = create_engine(sessionPath, echo=False)

        self.metadata = MetaData()

        self.blocks = Table('blocks', self.metadata,
            Column('height', Integer, primary_key=True, autoincrement=False),
            Column('blockhash', String(64), nullable=False),
            Column('blocktime', Integer),
            Column('mediantime', Integer),
            Column('version', Integer),
            Column('size', Integer),
            Column('merkleroot', String(64), nullable=False),
        )

        self.transactions = Table('transactions', self.metadata,
            Column('txid', String(64), primary_key=True, autoincrement=False),
            Column('height', Integer, ForeignKey('blocks.height')),
            Column('size', Integer),
            Column('version', Integer)
        )

        self.outputs = Table('outputs',self.metadata,
            Column('txid', String(64), ForeignKey('transactions.txid'), primary_key=True, autoincrement=False),
            Column('n', Integer, primary_key=True, autoincrement=False),
            Column('type', String(64)),
            Column('value', Float),
            Column('address', String(64)),
        )

        self.inputs = Table('inputs', self.metadata,
            Column('txid', String(64), primary_key=True, autoincrement=False),
            Column('output_txid', String(64), primary_key=True, autoincrement=False),
            Column('output_n', Integer, primary_key=True, autoincrement=False),
            Column('sequence', Integer),
            ForeignKeyConstraint(['output_txid', 'output_n'], ['outputs.txid', 'outputs.n'])
        )

        self.metadata.create_all(self.engine)

        self.conn = self.engine.connect()

    def run(self):

        if self.mode == DEFAULT_MODE:
            return

        nextBlock = self.getCurrentHeight() + 1

        logger.info("Start block {}".format(nextBlock))

        while True:

            block = self.rpc.getBlockByNumber(nextBlock)

            if not block.data:
                logger.error("Could not fetch block: {}".format(str(block.error)))
                time.sleep(30)
                continue

            if block.data['confirmations'] < 2:
                logger.info("Wait for confirmations: {}".format(block.data['hash']))
                time.sleep(30)
                continue

            if nextBlock % 100 == 0:
                logger.info("Block {}".format(nextBlock))

            block = block.data
            height = block['height']

            if nextBlock != height:
                sys.exit("Invalid block height received?!")

            transactions = []

            for tx in block['tx']:

                if height == 0:
                    # Genesis transaction
                    transactions.append({'txid':tx,
                                         'height':0,
                                         'size':0,
                                         'version':0,
                                         'vin':[],
                                         'vout':[]})
                    continue

                rawTx = self.rpc.getRawTransaction(tx)

                if not rawTx.data:
                    sys.exit("Could not fetch raw transaction {}".format(rawTx.error))

                transactions.append(rawTx.data)

            if self.addBlockdata(block, transactions):
                nextBlock += 1
            else:
                sys.exit("Could not add block {}".format(block))

    def addBlockdata(self,block, transactions):

        try:

            newBlock = self.blocks.insert()\
                           .values(height=block['height'],
                                   blockhash=block['hash'],
                                   blocktime=block['time'],
                                   mediantime=block['mediantime'],
                                   version=block['version'],
                                   size=block['size'],
                                   merkleroot=block['merkleroot'] )

            result = self.conn.execute(newBlock)

            if result:
                logger.debug("[{}] Add: {}".format(block['height'], str(block['hash'])))
            else:
                logger.error("newBlock error")
                return

            for transaction in transactions:

                newTx = self.transactions.insert()\
                             .values(txid=transaction['txid'],
                                     height=block['height'],
                                     size=transaction['size'],
                                     version=transaction['version'])

                result = self.conn.execute(newTx)

                if result:
                    logger.debug("  => add TX: {}".format(transaction['txid']))
                else:
                    logger.error("newTx error")
                    return

                for input in transaction['vin']:

                    outputTxid = None

                    if 'coinbase' in input:
                        outputTxid = input['coinbase']
                        n = -1
                    else:
                        outputTxid = input['txid']
                        n = input['vout']

                    newInput = self.inputs.insert()\
                                    .values(txid=transaction['txid'],
                                            output_txid=outputTxid,
                                            output_n=n,
                                            sequence=input['sequence'])

                    if self.conn.execute(newInput):

                        logger.debug("    => add input: {}".format(outputTxid))
                    else:
                        logger.error("newInput error")

                for output in transaction['vout']:

                    address = "unknown"
                    type = output['scriptPubKey']['type']

                    if 'pub' in type:
                        address = output['scriptPubKey']['addresses'][0]

                    newOutput = self.outputs.insert()\
                                    .values(txid=transaction['txid'],
                                            n=output['n'],
                                            type=type,
                                            value=output['value'],
                                            address=address)

                    if self.conn.execute(newOutput):
                        logger.debug("    => add output: {} - {}".format(address,output['value']))
                    else:
                        logger.error("newOutput error")

            return True

        except IntegrityError as e:
            logger.error("TODO: Handle double spendings?: ", exc_info=e)
            # db.rollback()
            #
            # doubleTx = None
            #
            # for transaction in transactions:
            #
            #     try:
            #         doubleTx = db.query(Transaction).filter(Transaction.txid == transaction['txid']).one()
            #     except:
            #         doubleTx = None
            #     else:
            #         doubleTx = transaction
            #         break
            #
            # if doubleTx:
            #
            #     logger.warning("Block {} found double tx => {}".format(block['height'], transaction['txid']))
            #
            #     transactions.remove(doubleTx)
            #
            #     db.add(DoubleSpending(txid=transaction['txid'],
            #                           height=block['height']))
            #     db.commit()
            #
            #     return self.addBlockdata(block, transactions)
            # else:
                # logger.error("IntegrityError: ", exc_info=e)

        except Exception as e:
            logger.error("addBlockdata: ", exc_info=e)

        return False

    def getCurrentHeight(self):

        height = self.conn.execute(select([func.max(self.blocks.c.height)])).scalar()

        return height if height else -1

    def getNumerOfOutputs(self, address):

        count = self.conn.execute(select([func.count(self.outputs.c.address)]).where(self.outputs.c.address == address)).scalar()

        return count if count else -1

    def getTransactionsForAddress(self, address):

        try:

            transactions = select()

            sent = self.conn.execute(
                        select([func.sum(self.outputs.c.value)])
                        .select_from(self.inputs.join(self.outputs))
                        .where(self.outputs.c.address.like(address))
                    ).scalar()

            received = self.conn.execute(
                         select([func.sum(self.outputs.c.value)])
                         .where(self.outputs.c.address == address)
                       ).scalar()

            sent = sent if sent else 0
            received = received if received else 0

            return received - sent

        except Exception as e:
            logger.error("getBalance", exc_info=e)
            pass

        return None

    def getBalance(self, address):

        try:
            self.conn = self.engine.connect()

            sent = self.conn.execute(
                        select([func.sum(self.outputs.c.value)])
                        .select_from(self.inputs.join(self.outputs))
                        .where(self.outputs.c.address.like(address))
                    ).scalar()

            received = self.conn.execute(
                         select([func.sum(self.outputs.c.value)])
                         .where(self.outputs.c.address == address)
                       ).scalar()

            sent = sent if sent else 0
            received = received if received else 0

            return received - sent

        except Exception as e:
            logger.error("getBalance", exc_info=e)
            pass

        return None
