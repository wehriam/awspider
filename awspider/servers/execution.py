import cPickle
import time
import pprint
import re
from twisted.web.client import _parse
from uuid import uuid5, NAMESPACE_DNS
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred
from twisted.internet import task
from twisted.internet import reactor
from twisted.web import server
from .base import BaseServer, LOGGER
from ..aws import sdb_now, sdb_now_add
from ..resources import ExecutionResource
from ..networkaddress import getNetworkAddress

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class ExecutionServer(BaseServer):
    
    peers = {}
    peer_uuids = []
    reportjobspeedloop = None
    jobsloop = None
    queryloop = None
    coordinateloop = None  
    uuid_limits = {'start':None, 'end':None}
    public_ip = None
    local_ip = None
    network_information = {}
    queued_jobs = {}
    job_queue = []
    job_count = 0
    query_start_time = None
    simultaneous_jobs = 50
    querying_for_jobs = False
    reservation_update_queue = []
    current_sql = ""
    last_job_query_count = 0
    def __init__(self,
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_sdb_reservation_domain, 
            aws_s3_http_cache_bucket=None, 
            aws_s3_storage_bucket=None,
            aws_sdb_coordination_domain=None,
            aws_s3_reservation_cache_bucket=None,
            max_simultaneous_requests=100,
            max_requests_per_host_per_second=0,
            max_simultaneous_requests_per_host=0,
            port=5001, 
            log_file='executionserver.log',
            log_directory=None,
            log_level="debug",
            name=None,
            time_offset=None,
            peer_check_interval=60,
            reservation_check_interval=60,
            hammer_prevention=False):
        if name == None:
            name = "AWSpider Execution Server UUID: %s" % self.uuid
        self.network_information["port"] = port
        self.hammer_prevention = hammer_prevention
        self.peer_check_interval = int(peer_check_interval)
        self.reservation_check_interval = int(reservation_check_interval)
        resource = ExecutionResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        BaseServer.__init__(
            self,
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_s3_reservation_cache_bucket=aws_s3_reservation_cache_bucket,
            aws_s3_http_cache_bucket=aws_s3_http_cache_bucket, 
            aws_sdb_reservation_domain=aws_sdb_reservation_domain, 
            aws_s3_storage_bucket=aws_s3_storage_bucket,
            aws_sdb_coordination_domain=aws_sdb_coordination_domain,
            max_simultaneous_requests=max_simultaneous_requests,
            max_requests_per_host_per_second=max_requests_per_host_per_second,
            max_simultaneous_requests_per_host=max_simultaneous_requests_per_host,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level,
            name=name,
            time_offset=time_offset,
            port=port)

    def start(self):
        reactor.callWhenRunning(self._start)
        return self.start_deferred

    def _start(self):
        deferreds = []
        deferreds.append(self.getNetworkAddress())
        if self.time_offset is None:
            deferreds.append(self.getTimeOffset())
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._startCallback)

    def _startCallback(self, data):
        for row in data:
            if row[0] == False:
                d = self.shutdown()
                d.addCallback(self._startHandleError, row[1])
                return d
        d = BaseServer.start(self)   
        d.addCallback(self._startCallback2)

    def _startCallback2(self, data):
        if self.shutdown_trigger_id is not None:
            self.reportjobspeedloop = task.LoopingCall(self.reportJobSpeed)
            self.reportjobspeedloop.start(60)
            self.jobsloop = task.LoopingCall(self.executeJobs)
            self.jobsloop.start(1)
            if self.aws_sdb_coordination_domain is not None:
                self.peerCheckRequest()  
                d = maybeDeferred(self.coordinate)
                d.addCallback(self._startCallback3)
            else:
                self.queryloop = task.LoopingCall(self.query)
                self.queryloop.start(self.reservation_check_interval)
    
    def _startCallback3(self, data):
        self.coordinateloop = task.LoopingCall(self.coordinate)
        self.coordinateloop.start(self.peer_check_interval)
        self.queryloop = task.LoopingCall(self.query)
        self.queryloop.start(self.reservation_check_interval)
    
    def shutdown(self):
        LOGGER.critical("Shutting down.")
        self.job_queue = []
        self.queued_jobs = {}
        deferreds = []
        LOGGER.debug("%s stopping on main HTTP interface." % self.name)
        d = self.site_port.stopListening()
        if isinstance(d, Deferred):
            deferreds.append(d)
        if self.reportjobspeedloop is not None:
            LOGGER.debug("Stopping report job speed loop.")
            d = self.reportjobspeedloop.stop()
            if isinstance(d, Deferred):
                deferreds.append(d)            
        if self.jobsloop is not None:
            LOGGER.debug("Stopping jobs loop.")
            d = self.jobsloop.stop()
            if isinstance(d, Deferred):
                deferreds.append(d)           
        if self.queryloop is not None:
            LOGGER.debug("Stopping query loop.")
            d = self.queryloop.stop()
            if isinstance(d, Deferred):
                deferreds.append(d)
        if self.coordinateloop is not None:
            LOGGER.debug("Stopping coordinating loop.")
            d = self.coordinateloop.stop()
            if isinstance(d, Deferred):
                deferreds.append(d)
            LOGGER.debug("Removing data from SDB coordination domain.")
            d = self.sdb.delete(self.aws_sdb_coordination_domain, self.uuid)
            d.addCallback(self.peerCheckRequest)
            deferreds.append(d)
        if len(deferreds) > 0:
            d = DeferredList(deferreds)
            d.addCallback(self._shutdownCallback)
            return d
        else:
            return self._shutdownCallback(None)
    
    def _shutdownCallback(self, data):
        return BaseServer.shutdown(self)

    def peerCheckRequest(self, data=None):
        LOGGER.debug("Signaling peers.")
        deferreds = []
        for uuid in self.peers:
            if uuid != self.uuid and self.peers[uuid]["active"]:
                LOGGER.debug("Signaling %s to check peers." % self.peers[uuid]["uri"])
                d = self.rq.getPage(
                    self.peers[uuid]["uri"] + "/coordinate", 
                    prioritize=True)
                d.addCallback(
                    self._peerCheckRequestCallback, 
                    self.peers[uuid]["uri"])
                d.addErrback(
                    self._peerCheckRequestErrback, 
                    self.peers[uuid]["uri"])
                deferreds.append(d)
        if len(deferreds) > 0:
            LOGGER.debug("Combinining shutdown signal deferreds.")
            return DeferredList(deferreds, consumeErrors=True)
        return True
    
    def _peerCheckRequestErrback(self, error, uri):
        LOGGER.debug("Could not get %s/coordinate: %s" % (uri, str(error)))
        
    def _peerCheckRequestCallback(self, data, uri):
        LOGGER.debug("Got %s/coordinate." % uri)

    def getNetworkAddress(self):
        d = getNetworkAddress()
        d.addCallback(self._getNetworkAddressCallback)
        d.addErrback(self._getNetworkAddressErrback)
        return d

    def _getNetworkAddressCallback(self, data):   
        if "public_ip" in data:
            self.public_ip = data["public_ip"]
            self.network_information["public_ip"] = self.public_ip
        if "local_ip" in data:
            self.local_ip = data["local_ip"]  
            self.network_information["local_ip"] = self.local_ip

    def _getNetworkAddressErrback(self, error):
        message = "Could not get network address." 
        LOGGER.error(message)
        raise Exception(message)

    def coordinate(self):
        server_data = self.getServerData()
        attributes = {
            "created":sdb_now(offset=self.time_offset),
            "load_avg":server_data["load_avg"],
            "running_time":server_data["running_time"],
            "cost":server_data["cost"],
            "active_requests":server_data["active_requests"],
            "pending_requests":server_data["pending_requests"],
            "current_timestamp":server_data["current_timestamp"],
            "job_queue":len(self.job_queue),
            "active_jobs":len(self.active_jobs),
            "queued_jobs":len(self.queued_jobs),
            "current_sql":self.current_sql.replace("\n", ""),
            "last_job_query_count":self.last_job_query_count}
        if self.uuid_limits["start"] is None and self.uuid_limits["end"] is not None:
            attributes["range"] = "Start - %s" % self.uuid_limits["end"]
        elif self.uuid_limits["start"] is not None and self.uuid_limits["end"] is None:
            attributes["range"] = "%s - End" % self.uuid_limits["start"]
        elif self.uuid_limits["start"] is None and self.uuid_limits["end"] is None:
            attributes["range"] = "Full range"
        else:
            attributes["range"] = "%s - %s" % (self.uuid_limits["start"], self.uuid_limits["end"])
        attributes.update(self.network_information)
        d = self.sdb.putAttributes(
            self.aws_sdb_coordination_domain, 
            self.uuid, 
            attributes, 
            replace=attributes.keys())
        d.addCallback(self._coordinateCallback)
        d.addErrback(self._coordinateErrback)
        return d
        
    def _coordinateCallback(self, data):
        sql = "SELECT public_ip, local_ip, port FROM `%s` WHERE created > '%s'" % (
            self.aws_sdb_coordination_domain, 
            sdb_now_add(self.peer_check_interval * -2, 
            offset=self.time_offset))
        LOGGER.debug("Querying SimpleDB, \"%s\"" % sql)
        d = self.sdb.select(sql)
        d.addCallback(self._coordinateCallback2)
        d.addErrback(self._coordinateErrback)
        return d
        
    def _coordinateCallback2(self, discovered):
        existing_peers = set(self.peers.keys())
        discovered_peers = set(discovered.keys())
        new_peers = discovered_peers - existing_peers
        old_peers = existing_peers - discovered_peers
        for uuid in old_peers:
            LOGGER.debug("Removing peer %s" % uuid)
            if uuid in self.peers:
                del self.peers[uuid]
        deferreds = []
        for uuid in new_peers:
            if uuid == self.uuid:
                self.peers[uuid] = {
                    "uri":"http://127.0.0.1:%s" % self.port,
                    "local_ip":"127.0.0.1",
                    "port":self.port,
                    "active":True
                }
            else:
                deferreds.append(self.verifyPeer(uuid, discovered[uuid]))
        if len(new_peers) > 0:
            if len(deferreds) > 0:
                d = DeferredList(deferreds, consumeErrors=True)
                d.addCallback(self._coordinateCallback3)
                return d
            else:
                self._coordinateCallback3(None) #Just found ourself.
        elif len(old_peers) > 0:
            self._coordinateCallback3(None)
        else:
            pass # No old, no new.

    def _coordinateCallback3(self, data):
        LOGGER.debug("Re-organizing peers.")
        for uuid in self.peers:
            if "local_ip" in self.peers[uuid]:
                self.peers[uuid]["uri"] = "http://%s:%s" % (
                    self.peers[uuid]["local_ip"], 
                    self.peers[uuid]["port"])
                self.peers[uuid]["active"] = True
                self.rq.setHostMaxRequestsPerSecond(
                    self.peers[uuid]["local_ip"], 
                    0)
                self.rq.setHostMaxSimultaneousRequests(
                    self.peers[uuid]["local_ip"], 
                    0)
            elif "public_ip" in self.peers[uuid]:
                self.peers[uuid]["uri"] = "http://%s:%s" % (
                    self.peers[uuid]["public_ip"], 
                    self.peers[uuid]["port"])
                self.peers[uuid]["active"] = True
                self.rq.setHostMaxRequestsPerSecond(
                    self.peers[uuid]["public_ip"], 
                    0)
                self.rq.setHostMaxSimultaneousRequests(
                    self.peers[uuid]["public_ip"],
                    0)
            else:
                LOGGER.error("Peer %s has no local or public IP. This should not happen." % uuid)
        self.peer_uuids = self.peers.keys()
        self.peer_uuids.sort()
        LOGGER.debug("Peers updated to: %s" % self.peers)
        # Set UUID peer limits by splitting up lexicographical namespace using hex values.
        peer_count = len(self.peers)
        splits = [hex(4096/peer_count * x)[2:] for x in range(1, peer_count)]
        splits = zip([None] + splits, splits + [None])
        splits = [{"start":x[0], "end":x[1]} for x in splits]
        if self.uuid in self.peer_uuids:
            self.uuid_limits = splits[self.peer_uuids.index(self.uuid)]
        else:
            self.uuid_limits = {"start":None, "end":None}
        job_queue_length = len(self.job_queue)
        if self.uuid_limits["start"] is None and self.uuid_limits["end"] is not None:
            self.job_queue = filter(self.testJobByEnd, self.job_queue)
        elif self.uuid_limits["start"] is not None and self.uuid_limits["end"] is None:
            self.job_queue = filter(self.testJobByStart, self.job_queue)
        elif self.uuid_limits["start"] is not None and self.uuid_limits["end"] is not None:
            self.job_queue = filter(self.testJobByStartAndEnd, self.job_queue)
        self.queued_jobs = dict((x["uuid"], True) for x in self.job_queue)
        LOGGER.info("Abandoned %s jobs that were out of range." % (job_queue_length - len(self.job_queue)))
        LOGGER.debug("Updated UUID limits to: %s" % self.uuid_limits)
    
    def _coordinateErrback(self, error):
        LOGGER.error("Could not query SimpleDB for peers: %s" % str(error))

    def testJobByEnd(self, job):
        return job["uuid"] < self.uuid_limits["end"]

    def testJobByStart(self, job):
        return job["uuid"] > self.uuid_limits["start"]

    def testJobByStartAndEnd(self, job):
        return (job["uuid"] < self.uuid_limits["end"] and job["uuid"] > self.uuid_limits["start"])
        
    def verifyPeer(self, uuid, peer):
        LOGGER.debug("Verifying peer %s" % uuid)
        deferreds = []
        if "port" in peer:
            port = int(peer["port"][0])
        else:
            port = self.port
        if uuid not in self.peers:
            self.peers[uuid] = {}
        self.peers[uuid]["active"] = False
        self.peers[uuid]["port"] = port
        if "local_ip" in peer:
            local_ip = peer["local_ip"][0]
            local_url = "http://%s:%s/server" % (local_ip, port)
            d = self.rq.getPage(local_url, timeout=5, prioritize=True)
            d.addCallback(self._verifyPeerLocalIPCallback, uuid, local_ip, port)
            deferreds.append(d)
        if "public_ip" in peer:
            public_ip = peer["public_ip"][0]
            public_url = "http://%s:%s/server" % (public_ip, port)
            d = self.rq.getPage(public_url, timeout=5, prioritize=True)
            d.addCallback(self._verifyPeerPublicIPCallback, uuid, public_ip, port)
            deferreds.append(d)
        if len(deferreds) > 0:
            d = DeferredList(deferreds, consumeErrors=True)
            return d
        else:
            return None

    def _verifyPeerLocalIPCallback(self, data, uuid, local_ip, port):
        LOGGER.debug("Verified local IP for %s" % uuid)
        self.peers[uuid]["local_ip"] = local_ip

    def _verifyPeerPublicIPCallback(self, data, uuid, public_ip, port):
        LOGGER.debug("Verified public IP for %s" % uuid)
        self.peers[uuid]["public_ip"] = public_ip

    def getPage(self, *args, **kwargs):
        if not self.hammer_prevention or len(self.peer_uuids) == 0:
            return self.pg.getPage(*args, **kwargs)
        else:
            scheme, host, port, path = _parse(args[0])
            peer_key = int(uuid5(NAMESPACE_DNS, host).int % len(self.peer_uuids))
            peer_uuid = self.peer_uuids[peer_key]
            if peer_uuid == self.uuid or self.peers[peer_uuid]["active"] == False:
                return self.pg.getPage(*args, **kwargs)
            else:
                parameters = {}
                parameters["url"] = args[0]
                if "method" in kwargs:
                    parameters["method"] = kwargs["method"]   
                if "postdata" in kwargs: 
                    parameters["postdata"] = urllib.urlencode(kwargs["postdata"])
                if "headers" in kwargs: 
                    parameters["headers"] = urllib.urlencode(kwargs["headers"])
                if "cookies" in kwargs: 
                    parameters["cookies"] = urllib.urlencode(kwargs["cookies"])         
                if "agent" in kwargs:
                    parameters["agent"] = kwargs["agent"]
                if "timeout" in kwargs:
                    parameters["timeout"] = kwargs["timeout"]
                if "followRedirect" in kwargs:
                    parameters["followRedirect"] = kwargs["followRedirect"]
                if "url_hash" in kwargs: 
                    parameters["url_hash"] = kwargs["url_hash"]
                if "cache" in kwargs: 
                    parameters["cache"] = kwargs["cache"]
                if "prioritize" in kwargs: 
                    parameters["prioritize"] = kwargs["prioritize"]
                url = "%s/getpage?%s" % (
                    self.peers[peer_uuid]["uri"], 
                    urllib.urlencode(parameters))
                LOGGER.debug("Rerouting request for %s to %s" % (args[0], url))
                d = self.rq.getPage(url, prioritize=True)
                d.addErrback(self._getPageErrback, args, kwargs) 
                return d

    def _getPageErrback(self, error, args, kwargs):
        LOGGER.error(args[0] + ":" + str(error))
        return self.pg.getPage(*args, **kwargs)

    def queryByUUID(self, uuid):
        sql = "SELECT * FROM `%s` WHERE itemName() = '%s'" % (
            self.aws_sdb_reservation_domain, 
            uuid)
        LOGGER.debug("Querying SimpleDB, \"%s\"" % sql)
        d = self.sdb.select(sql)
        d.addCallback(self._queryCallback2)
        d.addErrback(self._queryErrback)        
        return d

    def query(self, data=None):
        if len(self.job_queue) > 1000:
            LOGGER.debug("Skipping query. %s jobs already active." % len(self.job_queue))
            return
        if self.querying_for_jobs:
            LOGGER.debug("Skipping query. Already querying for jobs.")
            return
        self.querying_for_jobs = True
        if self.uuid_limits["start"] is None and self.uuid_limits["end"] is not None:
            uuid_limit_clause = "AND itemName() < '%s'" % self.uuid_limits["end"]
        elif self.uuid_limits["start"] is not None and self.uuid_limits["end"] is None:
            uuid_limit_clause = "AND itemName() > '%s'" % self.uuid_limits["start"]
        elif self.uuid_limits["start"] is None and self.uuid_limits["end"] is None:
            uuid_limit_clause = ""
        else:
            uuid_limit_clause = "AND itemName() BETWEEN '%s' AND '%s'" % (
                self.uuid_limits["start"], 
                self.uuid_limits["end"])
        sql = """SELECT * 
                FROM `%s` 
                WHERE
                reservation_next_request < '%s' %s
                LIMIT 2500""" % (
                self.aws_sdb_reservation_domain, 
                sdb_now(offset=self.time_offset),
                uuid_limit_clause)
        sql = re.sub(r"\s\s*", " ", sql);
        self.current_sql = sql
        LOGGER.debug("Querying SimpleDB, \"%s\"" % sql)
        d = self.sdb.select(sql, max_results=5000)
        d.addCallback(self._queryCallback)
        d.addErrback(self._queryErrback)

    def _queryErrback(self, error):
        self.querying_for_jobs = False
        LOGGER.error("Unable to query SimpleDB.\n%s" % error)
        
    def _queryCallback(self, data):
        LOGGER.info("Fetched %s jobs." % len(data))
        self.querying_for_jobs = False
        # Iterate through the reservation data returned from SimpleDB
        self.last_job_query_count = len(data)
        for uuid in data:
            if uuid in self.active_jobs or uuid in self.queued_jobs:
                continue
            kwargs_raw = {}
            reserved_arguments = {}
            # Load attributes into dicts for use by the system or custom functions.
            for key in data[uuid]:
                if key in self.reserved_arguments:
                    reserved_arguments[key] = data[uuid][key][0]
                else:
                    kwargs_raw[key] = data[uuid][key][0]
            # Check for the presence of all required system attributes.
            if "reservation_function_name" not in reserved_arguments:
                LOGGER.error("Reservation %s does not have a function name." % uuid)
                self.deleteReservation(uuid)
                continue
            function_name = reserved_arguments["reservation_function_name"]
            if function_name not in self.functions:
                LOGGER.error("Unable to process function %s for UUID: %s" % (function_name, uuid))
                continue
            if "reservation_created" not in reserved_arguments:
                LOGGER.error("Reservation %s, %s does not have a created time." % (function_name, uuid))
                self.deleteReservation(uuid, function_name=function_name)
                continue
            if "reservation_next_request" not in reserved_arguments:
                LOGGER.error("Reservation %s, %s does not have a next request time." % (function_name, uuid))
                self.deleteReservation(uuid, function_name=function_name)
                continue                
            if "reservation_error" not in reserved_arguments:
                LOGGER.error("Reservation %s, %s does not have an error flag." % (function_name, uuid))
                self.deleteReservation(uuid, function_name=function_name)
                continue
            # Load custom function.
            if function_name in self.functions:
                exposed_function = self.functions[function_name]
            else:
                LOGGER.error("Could not find function %s." % function_name)
                continue
            # Check for required / optional arguments.
            kwargs = {}
            for key in kwargs_raw:
                if key in exposed_function["required_arguments"]:
                    kwargs[key] = kwargs_raw[key]
                if key in exposed_function["optional_arguments"]:
                    kwargs[key] = kwargs_raw[key]
            has_required_arguments = True
            for key in exposed_function["required_arguments"]:
                if key not in kwargs:
                    has_required_arguments = False
                    LOGGER.error("%s, %s does not have required argument %s." % (function_name, uuid, key))
            if not has_required_arguments:
                continue
            self.queued_jobs[uuid] = True
            job = {"exposed_function":exposed_function,
                "kwargs":kwargs,
                "function_name":function_name,
                "uuid":uuid}
            if "reservation_cache" in reserved_arguments:
                LOGGER.debug("Using reservation fast cache for %s, %s on on SimpleDB." % (function_name, uuid))
                job["reservation_cache"] = reserved_arguments["reservation_cache"]
            else:
                job["reservation_cache"] = None
            self.job_queue.append(job)
        self.job_count = 0
        self.query_start_time = time.time()
        self.executeJobs()            

    def reportJobSpeed(self):
        if self.query_start_time is not None and self.job_count > 0:
            seconds_per_job = (time.time() - self.query_start_time) / self.job_count
            LOGGER.info("Average execution time: %s, %s active." % (seconds_per_job, len(self.active_jobs)))
        else:
            LOGGER.info("No average speed to report yet.")
            
    def executeJobs(self, data=None):           
        while len(self.job_queue) > 0 and len(self.active_jobs) < self.simultaneous_jobs:
            job = self.job_queue.pop(0)
            exposed_function = job["exposed_function"]
            kwargs = job["kwargs"]
            function_name = job["function_name"]
            uuid = job["uuid"]
            del self.queued_jobs[uuid]
            LOGGER.debug("Calling %s with args %s" % (function_name, kwargs))
            d = self.callExposedFunction(
                exposed_function["function"], 
                kwargs, 
                function_name, 
                uuid=uuid,
                reservation_fast_cache=job["reservation_cache"])
            d.addCallback(self._jobCountCallback)
            d.addErrback(self._jobCountErrback)
            d.addCallback(self._setNextRequest, uuid, exposed_function["interval"], function_name)
        
    def _jobCountCallback(self, data=None):
        self.job_count += 1
        
    def _jobCountErrback(self, error):
        self.job_count += 1
    
    def _setNextRequest(self, data, uuid, exposed_function_interval, function_name):
        reservation_next_request_parameters = {
            "reservation_next_request":sdb_now_add(
                exposed_function_interval, 
                offset=self.time_offset)}
        if uuid in self.reservation_fast_caches:
            LOGGER.debug("Set reservation fast cache for %s, %s on on SimpleDB." % (function_name, uuid))
            reservation_next_request_parameters["reservation_cache"] = self.reservation_fast_caches[uuid]
            del self.reservation_fast_caches[uuid]
        self.reservation_update_queue.append((
            uuid,
            reservation_next_request_parameters))
        if len(self.reservation_update_queue) > 25:
            self._sendReservationUpdateQueue()
    
    def _sendReservationUpdateQueue(self, data=None):
        if len(self.reservation_update_queue) < 25 and (len(self.active_jobs) > 0 or len(self.job_queue) > 0):
            return
        if len(self.reservation_update_queue) == 0:
            return
        LOGGER.debug("Sending reservation queue. Current length is %s" % (
            len(self.reservation_update_queue)))
        reservation_updates = self.reservation_update_queue[0:25]
        replace=["reservation_next_request", "reservation_cache"]
        reservation_replaces = [(x[0], replace) for x in reservation_updates]
        reservation_updates = dict(reservation_updates)
        reservation_replaces = dict(reservation_replaces)
        self.reservation_update_queue = self.reservation_update_queue[25:]
        d = self.sdb.batchPutAttributes(
            self.aws_sdb_reservation_domain,
            reservation_updates,
            replace_by_item_name=reservation_replaces)
        d.addCallback(
            self._sendReservationUpdateQueueCallback, 
            reservation_updates.keys())
        d.addErrback(
            self._sendReservationUpdateQueueErrback, 
            reservation_updates.keys(),
            reservation_updates)
        if len(self.reservation_update_queue) > 0:
            reactor.callLater(0, self._sendReservationUpdateQueue)

    def _sendReservationUpdateQueueCallback(self, data, uuids):
        LOGGER.debug("Set next request for %s on on SimpleDB." % uuids)
    
    def _sendReservationUpdateQueueErrback(self, error, uuids, reservation_updates): 
        LOGGER.error("Unable to set next request for %s on SimpleDB. Adding back to update queue.\n%s" % (uuids, error.value))
        self.reservation_update_queue.extend(reservation_updates.items())
