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

import os, logging, time
from smartcash.rpc import RPCConfig
from smartcash.rewardlist import SNRewardList, SNReward

if not input:
    input=raw_input

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger("rewardtest")

def rewardCB(reward, isSynced):
    logger.info("rewardCB: {}".format(str(reward)))

if __name__ == '__main__':

    test = 1

    directory = os.path.dirname(os.path.realpath(__file__))

    rpcConfig = RPCConfig('someusername','somepassword')

    rewardList = SNRewardList('sqlite:////' + directory + '/tt.db', rpcConfig, rewardCB)

    rewardList.start()

    while test == 1:

        if rewardList.isSynced():

            address = raw_input("Address to lookup: ")

            rewards = rewardList.getRewardsForPayee(address)

            if rewards:
                print("Rewards: {}".format(rewards))
                print("Profit: {}".format(sum(map(lambda x: x.amount,rewards))))
            else:
                print("{} did't receive any rewards yet.".format(address))
        else:
            print("Syncing... {}/{}".format(rewardList.currentHeight, rewardList.chainHeight))
            time.sleep(2)

    if test == 2:

        reward = SNReward(block=201,
                          txtime=123,
                          payee="test",
                          source=1,
                          meta=0)

        rewardList.updateSource(reward)
