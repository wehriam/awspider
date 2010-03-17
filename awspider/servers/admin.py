from twisted.internet.defer import Deferred, DeferredList, maybeDeferred
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.internet import task
from twisted.web import server
from .base import BaseServer, LOGGER, PRETTYPRINTER
from ..resources import AdminResource
from ..aws import sdb_now_add


class AdminServer(BaseServer):
    
    peercheckloop = None 
    exposed_functions = []
    exposed_function_resources = {}
    
    def __init__(self,
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_sdb_reservation_domain, 
            aws_s3_http_cache_bucket=None,
            aws_s3_storage_bucket=None,
            aws_sdb_coordination_domain=None,
            port=5003, 
            log_file='adminserver.log',
            log_directory=None,
            log_level="debug",
            name=None,
            time_offset=None,
            peer_check_interval=60):
        if name == None:
            name = "AWSpider Admin Server UUID: %s" % self.uuid
        resource = AdminResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        self.peer_check_interval = int(peer_check_interval)
        BaseServer.__init__(
            self,
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_s3_http_cache_bucket=aws_s3_http_cache_bucket, 
            aws_sdb_reservation_domain=aws_sdb_reservation_domain, 
            aws_s3_storage_bucket=aws_s3_storage_bucket,
            aws_sdb_coordination_domain=aws_sdb_coordination_domain,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level,
            name=name,
            port=port,
            time_offset=time_offset)
        
    def start(self):
        reactor.callWhenRunning(self._start)
        return self.start_deferred

    def _start(self):
        deferreds = []
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
        LOGGER.debug("At callback.")
        if self.shutdown_trigger_id is not None:
            if self.aws_sdb_coordination_domain is not None:
                self.peercheckloop = task.LoopingCall(self.peerCheck)
                self.peercheckloop.start(self.peer_check_interval)
        
    def shutdown(self):
        deferreds = []
        LOGGER.debug("%s stopping on main HTTP interface." % self.name)
        d = self.site_port.stopListening()
        if isinstance(d, Deferred):
            deferreds.append(d)
        if len(deferreds) > 0:
            d = DeferredList(deferreds)
            d.addCallback(self._shutdownCallback)
            return d
        else:
            return self._shutdownCallback(None)

    def _shutdownCallback(self, data):
        return BaseServer.shutdown(self)

    def peerCheck(self):
        sql = "SELECT * FROM `%s` WHERE created > '%s'" % (
            self.aws_sdb_coordination_domain, 
            sdb_now_add(self.peer_check_interval * -2, 
            offset=self.time_offset))
        LOGGER.debug("Querying SimpleDB, \"%s\"" % sql)
        d = self.sdb.select(sql)
        d.addCallback(self._peerCheckCallback)
        d.addErrback(self._peerCheckErrback)
        return d
    
    def _peerCheckCallback(self, data):
        LOGGER.info("Got server data:\n%s" % PRETTYPRINTER.pformat(data.values()))
        ip_addresses = [x['public_ip'][0] for x in data.values()]
        LOGGER.info("%s machines responding. %s" % (len(data), ip_addresses))
        
    def _peerCheckErrback(self, error):
        LOGGER.error("Could not query SimpleDB for peers: %s" % str(error))
    
    def clearHTTPCache(self):
        return self.s3.emptyBucket(self.aws_s3_http_cache_bucket)

    
