"""
*******************
*Copyright 2017, MapleLabs, All Rights Reserved.
*
********************
"""
""" A collectd-python plugin for retrieving
    metrics from Redis Database server.
    """

import collectd
import signal
import time
import json
import redis as red
import traceback
import subprocess

# user imports
from constants import *
from utils import *
from copy import deepcopy


class RedisStats:
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.host = 'localhost'
        self.user = None
        self.password = None
        self.port = None
        self.redis_client = None
        self.pollCounter = 0
        self.documentsTypes = []
        self.previousData = {}

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
            if children.key == USER:
                self.user = children.values[0]
            if children.key == PASSWORD:
                self.password = children.values[0]
            if children.key == DOCUMENTSTYPES:
                self.documentsTypes = children.values[0]

    def connect_redis(self):
        try:
            retry_flag = True
            retry_count = 0
            password_flag = False
            while retry_flag and retry_count < 3:
                try:
                    if password_flag:
                        self.redis_client = red.StrictRedis(host=self.host, port=self.port, password=self.password, db=0)
                    else:
                        self.redis_client = red.StrictRedis(host=self.host, port = self.port, db=0)
                    server_details = self.redis_client.info(section="server")
                    retry_flag = False
                    collectd.info("Connection to Redis successfull in attempt %s" % (retry_count))
                except Exception as e:
                    collectd.error("Retry after 5 sec as connection to Redis failed in attempt %s" % (retry_count))
                    retry_count += 1
                    if (not password_flag) and retry_count == 3:
                        retry_count = 0
                        password_flag = True
                    time.sleep(5)
        except Exception as e:
            collectd.error("Exception in the connect_redis due to %s" % e)
            return

    def get_redis_details(self):
        details_dict={}
        stats_dict={}
        final_redis_dict={}
        try:
            server_details = self.redis_client.info(section="server")
            if server_details:
                details_dict["version"] = server_details.get("redis_version",None)
                details_dict["buildId"] = server_details.get("redis_build_id",None)
                details_dict["mode"] = server_details.get("redis_mode",None)
                details_dict["os"] = server_details.get("os")
                details_dict["upTime"] = server_details.get("uptime_in_seconds",None)
            server_conn_details = self.redis_client.info("clients")
            if server_conn_details:
                stats_dict["blockedClients"] = server_conn_details.get("blocked_clients",0)
                stats_dict["connectedClients"] = server_conn_details.get("connected_clients",0)
            server_stats = self.redis_client.info(section="stats")
            if server_stats:
                input_bytes = None
                try:
                    input_bytes = round(server_stats.get("total_net_input_bytes",0) / (1024.0 * 1024.0), 2)
                except Exception as e:
                    collectd.error("Error in getting total input bytes due to %s" % str(e))
                output_bytes = None
                try:
                    output_bytes = round(server_stats.get("total_net_output_bytes",0) / (1024.0 * 1024.0), 2)
                except Exception as e:
                    collectd.error("Error in getting total input bytes due to %s" % str(e))

                stats_dict["instantaneousopspersec"] = server_stats.get("instantaneous_ops_per_sec",0)
                if self.pollCounter <= 1:
                    self.previousData["totalConnReceived"] = server_stats.get("total_connections_received",0)
                    self.previousData["totalCommandsProcessed"] = server_stats.get("total_commands_processed",0)
                    self.previousData["totalNetInputBytes"] = input_bytes
                    self.previousData["totalNetOuputBytes"] = output_bytes
                    self.previousData["keyspaceHits"] = server_stats.get("keyspace_hits",0)
                    self.previousData["keyspaceMisses"] = server_stats.get("keyspace_misses",0)
                    self.previousData["expiredKeys"] = server_stats.get("expired_keys",0)
                    self.previousData["evictedKeys"] = server_stats.get("evicted_keys",0)
                    self.previousData["rejectedConn"] = server_stats.get("rejected_connections",0)
                    stats_dict["totalConnReceived"] = 0
                    stats_dict["totalCommandsProcessed"] = 0
                    stats_dict["totalNetInputBytes"] = 0
                    stats_dict["totalNetOutputBytes"] = 0
                    stats_dict["keyspaceHits"] = 0
                    stats_dict["keyspaceMisses"] = 0
                    stats_dict["keyspaceHitRate"] = 0.0
                    stats_dict["keyspaceMissRate"] = 0.0
                    stats_dict["writeThroughput"] = 0.0
                    stats_dict["readThroughput"] = 0.0
                else:
                    stats_dict["rejectedConn"] = server_stats.get("rejected_connections",0) - self.previousData["rejectedConn"]
                    stats_dict["expiredKeys"] = server_stats.get("expired_keys",0) - self.previousData["expiredKeys"]
                    stats_dict["evictedKeys"] = server_stats.get("evicted_keys",0) - self.previousData["evictedKeys"]
                    stats_dict["totalConnReceived"] = server_stats.get("total_connections_received",0) - self.previousData["totalConnReceived"]
                    stats_dict["totalCommandsProcessed"] = server_stats.get("total_commands_processed",0) - self.previousData["totalCommandsProcessed"]
                    stats_dict["totalNetInputBytes"] = input_bytes - self.previousData["totalNetInputBytes"]
                    stats_dict["totalNetOutputBytes"] = output_bytes - self.previousData["totalNetOuputBytes"]
                    stats_dict["keyspaceHits"] = server_stats.get("keyspace_hits",0) - self.previousData["keyspaceHits"]
                    stats_dict["keyspaceMisses"] = server_stats.get("keyspace_misses",0) - self.previousData["keyspaceMisses"]
                    if ((stats_dict["keyspaceHits"] > 0) or (stats_dict["keyspaceMisses"] > 0)):
                        stats_dict["keyspaceHitRate"] = round(float(stats_dict["keyspaceHits"] / (stats_dict["keyspaceHits"] + stats_dict["keyspaceMisses"])), 2)
                        stats_dict["keyspaceMissRate"] = round(float(stats_dict["keyspaceMisses"] / (stats_dict["keyspaceHits"] + stats_dict["keyspaceMisses"])), 2)
                    else:
                        stats_dict["keyspaceHitRate"] = 0
                        stats_dict["keyspaceMissRate"] = 0
                    stats_dict["readThroughput"] = round(float(float(stats_dict["totalNetInputBytes"]) / int(self.interval)), 2)
                    stats_dict["writeThroughput"] = round(float(float(stats_dict["totalNetOutputBytes"]) / int(self.interval)), 2)
                    self.previousData["totalConnReceived"] = server_stats.get("total_connections_received",0)
                    self.previousData["totalCommandsProcessed"] = server_stats.get("total_commands_processed",0)
                    self.previousData["totalNetInputBytes"] = input_bytes
                    self.previousData["totalNetOuputBytes"] = output_bytes
                    self.previousData["keyspaceHits"] = server_stats.get("keyspace_hits",0)
                    self.previousData["keyspaceMisses"] = server_stats.get("keyspace_misses",0)
                    self.previousData["expiredKeys"] = server_stats.get("expired_keys",0)
                    self.previousData["evictedKeys"] = server_stats.get("evicted_keys",0)
                    self.previousData["rejectedConn"] = server_stats.get("rejected_connections",0)
                keyspace_details = self.redis_client.info("keyspace")
                if keyspace_details:
                    totalk = 0
                    dbcount = 0
                    for k, v in keyspace_details.items():
                        totalk += int(v["keys"])
                        dbcount += 1
                    stats_dict["totKeys"] = totalk
                    stats_dict["totalDatabases"] = dbcount
                else:
                    collectd.error("No Key details found")
                    stats_dict["totKeys"] = 0
                    stats_dict["totalDatabases"] = 0
                outlis = subprocess.check_output(["redis-cli", "--intrinsic-latency", "1"]).split()
                if len(outlis) > 0:
                    try:
                        stats_dict["latency"] = float(outlis[-16])
                    except ValueError:
                        collectd.error("No latency details found")
                        stats_dict["latency"] = 0
            memory_stats = self.redis_client.info(section="memory")
            if memory_stats:
                #if self.pollCounter <= 1:
                stats_dict["memoryUsed"] = round(memory_stats.get("used_memory",0) / (1024.0 * 1024.0), 2)
                #else:
                    #stats_dict["memoryUsed"] = round(memory_stats.get("used_memory",0)/(1024.0*1024.0), 2) - self.previousData["memoryUsed"]
                stats_dict["used_memory_peak"] = round(memory_stats.get("used_memory_peak",0) / (1024.0 * 1024.0), 2)
                details_dict["totalSystemMemory"] = round(memory_stats.get("total_system_memory",0) / (1024.0 * 1024.0), 2)
                stats_dict["memFragmentationRatio"] = memory_stats.get("mem_fragmentation_ratio",0)
                details_dict["memoryAllocator"] = memory_stats.get("mem_allocator",None)
                #self.previousData["memoryUsed"] = round(memory_stats.get("used_memory",0) / (1024.0 * 1024.0), 2)
            else:
                collectd.error("No memory stats found")
            persistence_stats = self.redis_client.info(section="persistence")
            if persistence_stats:
                stats_dict["lastSaveTime"] = persistence_stats.get("rdb_last_save_time",0)
                stats_dict["lastSaveChanges"] = persistence_stats.get("rdb_changes_since_last_save",0)
            rep_stats = self.redis_client.info(section="replication")
            if rep_stats:
                details_dict["role"] = rep_stats.get("role",None)
                stats_dict["connectedSlaves"] = rep_stats.get("connected_slaves",0)
                details_dict["masterLinkStatus"] = rep_stats.get("master_link_status", None)
                stats_dict["masterLastIOSecsAgo"] = rep_stats.get("master_last_io_seconds_ago", None)
                details_dict["masterLinkDownSinceSecs"] = rep_stats.get("master_link_down_since_seconds", None)
            details_dict[PLUGINTYPE] = "redisDetails"
            stats_dict[PLUGINTYPE] = "redisStat"
            final_redis_dict["redisDetails"] = details_dict
            final_redis_dict["redisStat"] = stats_dict
            self.add_common_params(final_redis_dict)
            collectd.error(json.dumps(final_redis_dict))
            return final_redis_dict
        except Exception as err:
            collectd.error("Unable to the details due to %s" % str(err))
            return final_redis_dict

    def get_keyspace_details(self):
            key_dict = []
            try:
                key_stats = self.redis_client.info("keyspace")
                if key_stats:
                    for ky,val in key_stats.items():
                        final_redis_dict={}
                        val['name'] = ky
                        val[PLUGINTYPE] = "keyspaceStat"
                        final_redis_dict["keyspaceStat"] = val
                        self.add_common_params(final_redis_dict)
                        key_dict.append(final_redis_dict)
                else:
                    return key_dict
            except Exception as e:
                collectd.error("Unable to Keyspace details due to %s" % str(e))
                return key_dict
            return key_dict

    @staticmethod
    def add_common_params(redis_dict):
        hostname = gethostname()
        timestamp = int(round(time.time() * 1000))
        
        for details_type, details in redis_dict.items():
            details[HOSTNAME] = hostname
            details[TIMESTAMP] = timestamp
            details[PLUGIN] = "redisdb"
            details[ACTUALPLUGINTYPE] = "redisdb"
            details[PLUGINTYPE] = details_type

    @staticmethod
    def dispatch_data(dict_disks_copy):
        collectd.debug(json.dumps(dict_disks_copy.keys()))
        for details_type, details in dict_disks_copy.items():
            collectd.debug("Plugin Redis: Values: " + json.dumps(details))
            collectd.info("final details are : %s" % details)
            dispatch(details)

    def read(self):
        try:
            self.pollCounter += 1
            self.connect_redis()
            # collect data
            dict_redis = self.get_redis_details()
            if not dict_redis:
                collectd.error("Plugin Redis: Unable to fetch data for Redis.")
                return
            else:
                # Deleteing documentsTypes which were not requetsed
                for doc in dict_redis.keys():
                    if dict_redis[doc]['_documentType'] not in self.documentsTypes:
                        collectd.error('you are in')
                        del dict_redis[doc]
            # dispatch data to collectd, copying by value
            #self.dispatch_data(deepcopy(dict_redis))
            self.dispatch_data(dict_redis)
            collectd.error('after this')
            collectd.error(json.dumps(dict_redis))
            for keyDetails in self.get_keyspace_details():
                self.dispatch_data(keyDetails)
        except Exception as e:
            collectd.debug(traceback.format_exc())
            collectd.error("Couldn't read and gather the SQL metrics due to theexception :%s" % e)
            return

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


def init():
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)

obj = RedisStats()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
