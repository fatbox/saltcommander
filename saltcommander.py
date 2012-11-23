#!/usr/bin/env python
#
# Scheduling daemon for applying highstate in a consistent and
# even schedule
#
# Author: Evan Borgstrom <evan/at/fatbox/dot/ca>
# Copyright (c) 2012 - FatBox Inc.
# 

import salt.client
import logging
import time

# how often we want minions to reapply state
RUN_INTERVAL = 43200

# rediscover_interval is how often we look for new minions
REDISCOVER_INTERVAL = 3600

class SaltCommander(object):
    # this will hold a list of known minions
    minions = []
    
    # this will hold our salt client
    client = None

    # the last time we ran
    last_discovery = 0

    # this will hold the calculated minion interval
    minion_interval = 0

    def __init__(self, debug=False):
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        logging.info("Salt commander - Starting up")
        self.client = salt.client.LocalClient()

    def discover_minions(self):
        logging.debug("Discovering minions")
        ret = self.client.cmd('*', 'test.ping')
        self.minions = ret.keys()
        logging.debug("Found %d minions:" % len(self.minions))
        self.last_discovery = time.time()

        # calculate our minion interval
        self.minion_interval = RUN_INTERVAL / len(self.minions)
        logging.debug("Minion interval is %d" % self.minion_interval)

    def run(self):
        try:
            last_minion = None
            minion_idx = 0
            while True:
                # update our minion list every rediscover_interval
                if (time.time() - self.last_discovery) > REDISCOVER_INTERVAL:
                    self.discover_minions()
                    if last_minion:
                        try:
                            minion_idx = self.minions.index(last_minion)
                        except ValueError:
                            minion_idx = 0

                # apply the next host
                last_minion = self.minions[minion_idx]
                logging.info("Applying state for %s" % last_minion)
                self.client.cmd(last_minion, 'state.highstate')

                # sleep for our interval
                logging.debug("Sleeping for %d seconds" % self.minion_interval)
                time.sleep(self.minion_interval)

                # increase the minion_idx
                minion_idx += 1
                if minion_idx > len(self.minions):
                    minion_idx = 0

        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    commander = SaltCommander(debug=True)
    commander.run()
