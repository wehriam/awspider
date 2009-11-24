import time
from uuid import uuid4
import logging
import logging.handlers
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from ..aws import AmazonS3, AmazonSDB
from ..requestqueuer import RequestQueuer
from ..pagegetter import PageGetter
from ..timeoffset import getTimeOffset
from ..networkaddress import getNetworkAddress

LOGGER = logging.getLogger("main")

class AWSpiderBaseServer(object):
    
    logging_handler = None
    shutdown_trigger_id = None
    uuid = uuid4().hex
    public_ip = None
    local_ip = None
    network_information = {}
    start_time = time.time()
    
    def __init__(self,
                 aws_access_key_id, 
                 aws_secret_access_key, 
                 aws_s3_cache_bucket=None, 
                 aws_sdb_reservation_domain=None, 
                 aws_s3_storage_bucket=None,
                 aws_sdb_coordination_domain=None,
                 max_simultaneous_requests=0,
                 max_requests_per_host_per_second=0,
                 max_simultaneous_requests_per_host=0,
                 log_file=None,
                 log_directory=None,
                 log_level="debug",
                 name=None,
                 time_offset=None,
                 port=8080):
        self.network_information["port"] = port
        self.time_offset = time_offset
        self.name = name
        self.start_deferred = Deferred()
        self.rq = RequestQueuer( 
            max_simultaneous_requests=int(max_simultaneous_requests), 
            max_requests_per_host_per_second=int(max_requests_per_host_per_second), 
            max_simultaneous_requests_per_host=int(max_simultaneous_requests_per_host))
        self.rq.setHostMaxRequestsPerSecond("127.0.0.1", 0)
        self.rq.setHostMaxSimultaneousRequests("127.0.0.1", 0)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_s3_cache_bucket = aws_s3_cache_bucket
        self.aws_s3_storage_bucket = aws_s3_storage_bucket
        self.aws_sdb_reservation_domain = aws_sdb_reservation_domain
        self.aws_sdb_coordination_domain = aws_sdb_coordination_domain
        self.s3 = AmazonS3(self.aws_access_key_id, self.aws_secret_access_key, self.rq)
        self.sdb = AmazonSDB(self.aws_access_key_id, self.aws_secret_access_key, self.rq)
        self.pg = PageGetter(self.s3, self.aws_s3_cache_bucket, rq=self.rq)
        self._setupLogging(log_file, log_directory, log_level)
        if self.name is not None:
            LOGGER.info("Successfully loaded %s configuration." % self.name)

    def _setupLogging(self, log_file, log_directory, log_level):
        if log_directory is None:
            self.logging_handler = logging.StreamHandler()
        else:
            self.logging_handler = logging.handlers.TimedRotatingFileHandler(
                os.path.join(log_directory, log_file), 
                when='D', 
                interval=1)
        log_format = "%(levelname)s: %(message)s %(pathname)s:%(lineno)d"
        self.logging_handler.setFormatter(logging.Formatter(log_format))
        LOGGER.addHandler(self.logging_handler)
        log_level = log_level.lower()
        log_levels = {
            "debug":logging.DEBUG, 
            "info":logging.INFO, 
            "warning":logging.WARNING, 
            "error":logging.ERROR,
            "critical":logging.CRITICAL
        }
        if log_level in log_levels:
            LOGGER.setLevel(log_levels[log_level])
        else:
            LOGGER.setLevel(logging.DEBUG)     
            def start(self):
                reactor.callWhenRunning(self._start)
                return self.start_deferred

    def start(self):
        reactor.callWhenRunning(self._baseStart)
        return self.start_deferred

    def _baseStart(self):
        LOGGER.critical("Checking S3 and SDB setup.")
        deferreds = []
        if self.aws_s3_cache_bucket is not None:
            deferreds.append(
                self.s3.checkAndCreateBucket(self.aws_s3_cache_bucket))   
        if self.aws_sdb_reservation_domain is not None:
            deferreds.append(
                self.sdb.checkAndCreateDomain(self.aws_sdb_reservation_domain))
        if self.aws_s3_storage_bucket is not None:
            deferreds.append(
                self.s3.checkAndCreateBucket(self.aws_s3_storage_bucket))
        if self.aws_sdb_coordination_domain is not None:
            deferreds.append(
                self.sdb.checkAndCreateDomain(self.aws_sdb_coordination_domain))
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._baseStartCallback)

    def _baseStartCallback(self, data):
        for row in data:
            if row[0] == False:
                d = self.shutdown()
                d.addCallback(self._startHandleError, row[1])
                return d
        self.shutdown_trigger_id = reactor.addSystemEventTrigger(
            'before', 
            'shutdown', 
            self.shutdown)
        LOGGER.critical("Starting %s" % self.name)
        self._baseStartCallback2(None)

    def _baseStartCallback2(self, data):
        self.start_deferred.callback(True)

    def _startHandleError(self, data, error):
        self.start_deferred.errback(error)
        
    def shutdown(self):
        LOGGER.debug("%s waiting for shutdown." % self.name)
        d = Deferred()
        reactor.callLater(0, self._waitForShutdown, d)
        return d

    def _waitForShutdown(self, shutdown_deferred):          
        if self.rq.getPending() > 0 or self.rq.getActive() > 0:
            LOGGER.debug("%s waiting for shutdown." % self.name)
            reactor.callLater(1, self._waitForShutdown, shutdown_deferred)
            return
        self.shutdown_trigger_id = None
        LOGGER.debug("%s shut down." % self.name)
        LOGGER.removeHandler(self.logging_handler)
        shutdown_deferred.callback(True)
    
    def getTimeOffset(self):
        d = getTimeOffset()
        d.addCallback(self._getTimeOffsetCallback)
        d.addErrback(self._getTimeOffsetErrback)
        return d

    def _getTimeOffsetCallback(self, time_offset):
        self.time_offset = time_offset
        LOGGER.info( "Got time offset for sync: %s" % self.time_offset )

    def _getTimeOffsetErrback(self, error):
        if self.time_offset is None:
            message = "Could not get time offset for sync."
            LOGGER.critical(message)
            raise Exception(message)
            
    def getNetworkAddress(self):
        d = getNetworkAddress()
        d.addCallback(self._getNetworkAddressCallback)
        d.addErrback(self._getNetworkAddressErrback)
        return d

    def _getNetworkAddressCallback( self, data  ):   
        if "public_ip" in data:
            self.public_ip = data["public_ip"]
            self.network_information["public_ip"] = self.public_ip
        if "local_ip" in data:
            self.local_ip = data["local_ip"]  
            self.network_information["local_ip"] = self.local_ip

    def _getNetworkAddressErrback(self, error):
        message = "Could not get network address." 
        LOGGER.critical(message)
        raise Exception(message)
