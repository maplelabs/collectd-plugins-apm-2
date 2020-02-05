"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""Python plugin for collectd to get list of open files and related stats using lsof command"""

# !/usr/bin/python
import signal
import json
import time
import collectd

# user imports
import utils
from constants import *


class LsofStats(object):
    """Plugin object will be created only once and collects available info from lsof command every interval."""

    def __init__(self):
        """Initializes interval and previous dictionary variable."""
        self.interval = DEFAULT_INTERVAL

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]

    def conv_b_to_kb(self, input_bytes):
        if input_bytes is None:
            input_bytes = 0
        if input_bytes == 'None':
            input_bytes = 0
        convert_kb = input_bytes / 1024
        return convert_kb


    def lsof_command(self):
        """
        Returns dictionary with information regarding open files using lsof command
        """

        cmnd = "lsof -nPs -Ki | sed -n '2,$p'"

        process, err = utils.get_cmd_output(cmnd)
        result = []
        process_order = 1
        for line in process.splitlines():
            lsof_res = {}
            # line = process.stdout.readline()
            if line != b'':
                try:
                    response = line.split()
                    first_half = response[0:5]
                    second_half = response[5:]
                    brackets = "[]{}()"
                    name = " "
                    lsof_res["processCommand"] = first_half[0]
                    lsof_res["pid"] = long(first_half[1])
                    lsof_res["user"] = first_half[2]
                    lsof_res["fileDescriptor"] = first_half[3]
                    lsof_res["fileType"] = first_half[4]
                    if lsof_res["fileType"] in ["netlink"]:
                        lsof_res["device"] = "None"
                        lsof_res["node"] = long(second_half[0])
                        lsof_res["size"] = 0
                        name = name.join(second_half[1:])
                    elif lsof_res["fileType"] in ["unknown"]:
                        lsof_res["device"] = "None"
                        lsof_res["node"] = "None"
                        lsof_res["size"] = 0
                        name = name.join(second_half[0:])
                    else:
                        lsof_res["device"] = second_half[0]
                        second_half = second_half[1:]
                        try:
                            node = long(second_half[1])
                            lsof_res["node"] = second_half[1]
                            lsof_res["size"] = self.conv_b_to_kb(int(second_half[0]))
                            name = name.join(second_half[2:])
 
                        except ValueError:
                            lsof_res["node"] = second_half[0]
                            lsof_res["size"] = 0
                            name = name.join(second_half[1:])

                    for c in brackets:
                        name = name.replace(c, "")
                    lsof_res["name"] = name
                
                except Exception as err:
                    collectd.error("Plugin lsofstats: Could not parse one or more lines due to: %s" % err)
                    continue

                result.append(lsof_res)
            else:
                break
        return result

    def add_common_params(self, lsof_res):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = int(round(time.time() * 1000))
        for result in lsof_res:
            result[TIMESTAMP] = timestamp
            result[PLUGIN] = "lsofstats"
            result[ACTUALPLUGINTYPE] = "lsofstats"
            result[PLUGINTYPE] = "lsof_stats"
        # lsof_res[PLUGIN_INS] = P_INS_ALL
        collectd.info("Plugin lsofstats: Added common parameters successfully")


    def collect_data(self):
        """Validates if dictionary is not null.If null then returns None."""
        lsof_res = self.lsof_command()
        if not lsof_res:
            collectd.error("Plugin lsofstats: Unable to fetch lsof Summary")
            return None

        collectd.info("Plugin lsofstats: Added file information successfully")
        self.add_common_params(lsof_res)

        return lsof_res


    def dispatch_data(self, lsof_res):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin lsofstats: Successfully sent to collectd.")
        collectd.info("Plugin lsofstats: Values dispatched = " +
                       json.dumps(lsof_res))
        for result in lsof_res:
            collectd.info("Dump: %s" % result)
            utils.dispatch(result)


    def read(self):
        """Collects all data."""
        lsof_res = self.collect_data()
        if not lsof_res:
            return

        # dispatch data to collectd
        self.dispatch_data(lsof_res)


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


OBJ = LsofStats()
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)

