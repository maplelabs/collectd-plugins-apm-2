"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""Python plugin for collectd to get socket connection info using netstat command"""

# !/usr/bin/python
import signal
import json
import time
import collectd

# user imports
import utils
from constants import *


class SocketStats(object):
    """Plugin object will be created only once and collects utils
       and available socket connection info every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL
        self.prev_data = {}

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]

    def netstats_command(self):
        """
        Returns dictionary with values of available socket connections using netstat command.
        """
        # cmnd = "top -b -o +" + self.usage_parameter +" -n 1 | head -17 | sed -n '8,20p' | awk '{print $1, $2, $9, $10, $12}'"
        cmnd = "netstat -aenp | egrep 'tcp|udp'"

        process, err = utils.get_cmd_output(cmnd)
        result = []
        process_order = 1
        for line in process.splitlines():
            netstats_res = {}
            # line = process.stdout.readline()
            if line != b'':
                response = line.split()
                command = " "
                
                if response[0].startswith('udp'):
                    netstats_res['connState'] = "STATELESS"
                else:
                    netstats_res['connState'] = response[5]

                netstats_res['connProtocol'] = response[0]
                netstats_res['recvQ'] = response[1]
                netstats_res['sendQ'] = response[2]
                netstats_res['localAddress'] = response[3]
                netstats_res['foreignAddress'] = response[4]
                netstats_res[PLUGINTYPE] = "netstats"
                # os.write(1, line)
                process_order += 1
                result.append(netstats_res)
            else:
                break
        return result

    def add_common_params(self, netstats_res):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = int(round(time.time() * 1000))
        for result in netstats_res:
            result[TIMESTAMP] = timestamp
            result[PLUGIN] = "socketstats"
            result[ACTUALPLUGINTYPE] = "socketstats"
        # netstats_res[PLUGIN_INS] = P_INS_ALL
        collectd.info("Plugin socketstats: Added common parameters successfully")

    def collect_data(self):
        """Validates if dictionary is not null.If null then returns None."""
        netstats_res = self.netstats_command()
        if not netstats_res:
            collectd.error("Plugin socketstats: Unable to fetch Socket Summary")
            return None

        collectd.info("Plugin socketstats: Added socket information successfully")
        self.add_common_params(netstats_res)

        return netstats_res

    def dispatch_data(self, netstats_res):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin socketstats: Successfully sent to collectd.")
        collectd.debug("Plugin socketstats: Values dispatched = " +
                       json.dumps(netstats_res))
        for result in netstats_res:
            utils.dispatch(result)

    def read(self):
        """Collects all data."""
        netstats_res = self.collect_data()
        if not netstats_res:
            return

        # dispatch data to collectd
        self.dispatch_data(netstats_res)

    def read_temp(self):
        """
        Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback.
        """
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init():
    """When new process is formed, action to SIGCHLD is reset to default behavior."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)


OBJ = SocketStats()
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)

