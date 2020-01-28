"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
"""Python plugin for collectd to get highest CPU/Memory usage process using top command"""

# !/usr/bin/python
import signal
import json
import time
import collectd
import subprocess
import re

# user imports
import utils
from utils import PlatformOS, PlatformVersion
from constants import *



SORT_BY = {'CPU': 'pcpu', 'MEM': 'pmem'}


class PSStats(object):
    """Plugin object will be created only once and collects utils
       and available CPU/RAM usage info every interval."""

    def __init__(self, interval=1, num_processes='*', sort_by='CPU'):
        """Initializes interval."""
        self.interval = DEFAULT_INTERVAL
        self.num_processes = num_processes
        self.sort_by = sort_by

    def config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == "num_processes":
                self.num_processes = children.values[0]

    def bytesConv(self, data):
        """
        Convert the memory to kb based on its unit
            1 mb = 1024 kilobytes
            1 gb = 1024 * 1024 kilobytes = 1048576 kb
            1 tb = 1024 * 1024 * 1024 kilobytes = 1073741824 kb
        """
        data = str(data)
        convVal = {'k': 1, 'm': 1024, 'g': 1048576, 't': 1073741824}

        splitVal = re.search('(\d*\.?\d*)', data).group()
        splitUnit = re.search('([a-z])', data)

        splitUnit = 'k' if not splitUnit else splitUnit.group()

        if splitUnit in convVal:
            data = float(splitVal) * convVal[splitUnit]
        return data

    def ps_command(self):
        """
        Returns dictionary with values of available and top SPU and memory usage summary of teh process.
        """
        # cmnd = "top -b -o +" + self.usage_parameter +" -n 1 | head -17 | sed -n '8,20p' | awk '{print $1, $2, $9, $10, $12}'"
        if self.num_processes == '*':
            cmnd = "ps -wweo uname,pid,psr,pcpu,cputime,pmem,rsz,vsz,tty,s,etime,args --sort=-" + SORT_BY[self.sort_by] + " | sed -n '2,$p'"
        else:
            head_value = 1 + int(self.num_processes)
            cmnd = "ps -wweo uname,pid,psr,pcpu,cputime,pmem,rsz,vsz,tty,s,etime,args --sort=-" + SORT_BY[self.sort_by] + " | head -" + str(head_value) + " | sed -n '2," + str(head_value) + "p'"
        collectd.info("#### CMD: %s" % cmnd)
        process, err = utils.get_cmd_output(cmnd)
        result = []
        process_order = 1
        for line in process.splitlines():
            ps_stats_res = {}
            # line = process.stdout.readline()
            if line != b'':
                response = line.split()
                command = " "
                command = command.join(response[11:]).strip("[]{}()")

                # Memory usage Conversion
                virt_mem = self.bytesConv(response[7])
                resi_mem = self.bytesConv(response[6])

                ps_stats_res['order'] = process_order
                ps_stats_res['pid'] = long(response[1])
                ps_stats_res['process_user'] = response[0]
                ps_stats_res['virt_memory'] = virt_mem
                ps_stats_res['res_memory'] = resi_mem
                ps_stats_res['processor'] = response[2]
                ps_stats_res['controlling_terminal'] = response[8]
                ps_stats_res['status_code'] = response[9]
                ps_stats_res['cpu_percent'] = float(response[3])
                ps_stats_res['cpu_time'] = str(response[4])
                ps_stats_res['memory_percent'] = float(response[5])
                ps_stats_res['process_command'] = command
                ps_stats_res['elapsed_time'] = str(response[10])

                # os.write(1, line)
                process_order += 1
                result.append(ps_stats_res)
            else:
                break
        return result

    def add_common_params(self, ps_stats_res):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        timestamp = int(round(time.time() * 1000))
        for result in ps_stats_res:
            result[TIMESTAMP] = timestamp
            result[PLUGIN] = "psstats"
            result[PLUGINTYPE] = "process_stats"
            result[ACTUALPLUGINTYPE] = "psstats"
        # ps_stats_res[PLUGIN_INS] = P_INS_ALL
        collectd.info("Plugin ps_stats: Added common parameters successfully")

    def collect_data(self):
        """Validates if dictionary is not null.If null then returns None."""
        ps_stats_res = self.ps_command()
        if not ps_stats_res:
            collectd.error("Plugin ps_stats: Unable to fetch Top Usage Summary")
            return None

        collectd.info("Plugin ps_stats: Added ram information successfully")
        self.add_common_params(ps_stats_res)

        return ps_stats_res

    def dispatch_data(self, ps_stats_res):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin ps_stats: Successfully sent to collectd.")
        collectd.debug("Plugin ps_stats: Values dispatched = " +
                       json.dumps(ps_stats_res))
        for result in ps_stats_res:
            collectd.info("Dump : %s" % result)
            utils.dispatch(result)

    def read(self):
        """Collects all data."""
        ps_stats_res = self.collect_data()
        if not ps_stats_res:
            return

        # dispatch data to collectd
        self.dispatch_data(ps_stats_res)

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


OBJ = PSStats()
collectd.register_config(OBJ.config)
collectd.register_read(OBJ.read_temp)

