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
RUN_INTERVAL = 3600

# rediscover_interval is how often we look for new minions
REDISCOVER_INTERVAL = 600

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
            minion_idx = 0
            while True:
                # update our minion list every rediscover_interval
                if (time.time() - self.last_discovery) > REDISCOVER_INTERVAL:
                    self.discover_minions()
                    minion_idx = 0

                # apply the next host
                logging.info("Applying state for %s" % self.minions[minion_idx])
                self.client.cmd(self.minions[minion_idx], 'state.highstate')

                # sleep for our interval
                logging.debug("Sleeping for %d seconds" % self.minion_interval)
                time.sleep(self.minion_interval)

        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    commander = SaltCommander(debug=True)
    commander.run()
