""" A collectd-python plugin for retrieving
    metrics from nginx status module. """

import collectd
import re
import time
import requests
import platform
import json
from constants import *
from utils import *

SERVER_STATS = "serverStats"
SERVER_DETAILS = "serverDetails"

docs = [SERVER_STATS, SERVER_DETAILS]
#docs = ["nginxDetails"]

class Nginx(object):
    def __init__(self):
        self.interval = DEFAULT_INTERVAL
        self.port = 80
        self.location = DEFAULT_LOCATION
        self.host = 'localhost'
        self.secure = False
        self.pollCounter = 0
        self.previousData = {'previousTotal':0}
        self.api = self.get_api_version()
        self.prev_req = 0

    def read_config(self, cfg):
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            if children.key == LOCATION:
                self.location = children.values[0]
            if children.key == PORT:
                self.port = children.values[0]
            if children.key == SECURE:
                self.secure = children.values[0]

    def get_api_version(self):
        try:
            url='http://'+self.host+'/api/'
            resp = requests.get(url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                return str(max(content))
        except Exception as ex:
            return
            
    def get_server_details(self):
        # Get the version of the NGINX server running
        server_details = {}
        try:
            url='http://'+self.host+'/api/'+self.api+'/nginx'
            resp = requests.get(url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                server_details['nginxVersion'] = content['version']
                server_details['nginxBuild'] = content['build']
        except Exception as ex:
            raise ex
        # Get the status of the NGINX server
        running = False
        try:
            stdout, stderr = get_cmd_output("ps -ef | grep nginx")
            if stdout:
                tokens = str(stdout).strip().split('\n')
                for token in tokens:
                   attributes =  token.strip().split()
                   if len(attributes) >= 8:
                       if 'nginx' in attributes[7]:
                           running = True
                           break
        except Exception as ex:
            raise ex
        # Get the uptime of the NGINX Plus server
        try:
            uptime_v = 0.0
            #t =0
            stdout, stder = get_cmd_output('ps -eo comm,etime,user | grep nginx | grep root')
            #for val in stdout.split():
            #    print(val)
            if re.search("-", stdout):
                data = re.findall('([0-9]+\-[0-9]+\:[^-].[^\s]*)', stdout)
                if data:
                    d = data[0].split('-')
                    d_to_m = 1440 * int(d[0])
                    t = d[1].split(':')
            else:
                data = re.findall('([0-9]+\:[^-].[^\s]*)', stdout)
                d_to_m = 0
                t = data[0].split(':')

            if data:
                if len(t) == 2:
                    m = int(t[0])
                    s_to_m = round(float(t[1]) / 60, 4)
                    uptime_v = m + s_to_m
                else:
                    h_to_m = 60 * int(t[0])
                    m = int(t[1])
                    s_to_m = round(float(t[2]) / 60, 4)
                    uptime_v = d_to_m + h_to_m + m + s_to_m

        except Exception as err:
            raise err
        server_details.update({'processRunning': running, 'upTime': uptime_v, 'nginxOS' : platform.dist()[0]})
        return server_details

    def get_rps_diff(self, total):
        if self.pollCounter == 1:
            self.prev_req = total
            return 0.0
        else:
            diff_total = total - self.prev_req
            rps = round(diff_total / float(self.interval), 2)
            self.prev_req = total
            return rps

    def get_server_stats(self):
        data_dict={}
        try:
            url='http://'+self.host+'/api/'+self.api+'/'
            con_url='connections'
            resp = requests.get(url+con_url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                data_dict['activeConnections'] = content['active']
                data_dict['acceptedConnections'] = content['accepted']
                data_dict['activeWaiting'] = content['idle']
                data_dict['droppedConnections'] = content['dropped']
            con_url='http/requests'
            resp = requests.get(url+con_url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                data_dict['currentRequests'] = content['current']
                data_dict['totalRequests'] = content['total']
                #data_dict["requestsPerSecond"] = round(content['total'] / float(self.interval), 2)
                data_dict['requestsPerSecond'] = self.get_rps_diff(content['total'])
                #self.previousData['previousTotal'] = content['total']
            con_url='ssl'
            resp = requests.get(url+con_url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                data_dict['handshakes'] = content['handshakes']
                data_dict['handshakesFailed'] = content['handshakes_failed']
                data_dict["sessionReuses"] = content['session_reuses']
            con_url='processes'
            resp = requests.get(url+con_url, verify=False)
            if resp and resp.status_code == 200:
                content = json.loads(resp.content)
                data_dict['processRespawned'] = content['respawned']
            return data_dict
        except Exception as ex:
            raise ex


    def poll(self, type):
        
        nginx_doc = {}
        
        if type == SERVER_STATS:
            server_stats = None
            try:
                server_stats = self.get_server_stats()
            except Exception as ex:
                err = 'Error collecting nginx server stats : {0}'.format(ex.message)
                collectd.error(err)
            if server_stats:
                nginx_doc = server_stats
                collectd.info('nginx doc updated with server stats')
        else:
            server_details = None
            try:
                server_details = self.get_server_details()
            except Exception as ex:
                err = 'Error collecting nginx server details : {0}'.format(ex.message)
                collectd.error(err)
            if server_details:
                nginx_doc = server_details
                collectd.info('nginx doc updated with server details')
        return nginx_doc

    @staticmethod
    def add_common_params(result_dict, doc_type):
        hostname = gethostname()
        timestamp = int(round(time.time() * 1000))
        result_dict[PLUGIN] = "nginxplus"
        result_dict[HOSTNAME] = hostname
        result_dict[TIMESTAMP] = timestamp
        result_dict[PLUGINTYPE] = doc_type
        result_dict[ACTUALPLUGINTYPE] = "nginxplus"
        collectd.info("Plugin nginx: Added common parameters successfully")

    @staticmethod
    def dispatch_data(result_dict):
        collectd.info("Plugin nginx: Values dispatched = " + json.dumps(result_dict))
        dispatch(result_dict)

    def read(self):
        self.pollCounter += 1
        # collect data
        try:
            for doc in docs:
                result_dict = self.poll(doc)
                #result_dict = self.nginxplusstats()
                if not result_dict:
                    collectd.error("Plugin nginx: Unable to fetch information of nginx for document Type: " + doc)
                else:
                    collectd.info("Plugin nginx:Success fetching information of nginx for document Type: " + doc)
                    self.add_common_params(result_dict, doc)
                    # dispatch data to collectd
                    self.dispatch_data(result_dict)
        except Exception as ex:
            collectd.error("Plugin nginx: Unable to fetch information due to exception" +ex)

    def read_temp(self):
        collectd.unregister_read(self.read_temp)
        collectd.register_read(self.read, interval=int(self.interval))


obj = Nginx()
collectd.register_config(obj.read_config)
collectd.register_read(obj.read_temp)
