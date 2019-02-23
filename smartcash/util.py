#
# Part of `python-smartcash`
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

import threading
import sqlite3 as sql

HF_1_2_MULTINODE_PAYMENTS = 545005
HF_1_2_8_COLLATERAL_CHANGE = 910000

class ThreadedSQLite(object):
    def __init__(self, dburi):
        self.lock = threading.Lock()
        self.connection = sql.connect(dburi, check_same_thread=False)
        self.connection.row_factory = sql.Row
        self.cursor = None
    def __enter__(self):
        self.lock.acquire()
        self.cursor = self.connection.cursor()
        return self
    def __exit__(self, type, value, traceback):
        self.connection.commit()

        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None

        self.lock.release()

def getPayeesPerBlock(nHeight):

    if nHeight >= HF_1_2_MULTINODE_PAYMENTS and nHeight < HF_1_2_8_COLLATERAL_CHANGE:
        return 10
    elif nHeight >= HF_1_2_8_COLLATERAL_CHANGE:
        return 1

    return 1

def getPayoutInterval(nHeight):

    if nHeight >= HF_1_2_MULTINODE_PAYMENTS:
        return 2

    return 1

def getBlockReward(nHeight):
    return 5000.0  * ( 143500.0 / nHeight ) * 0.1
