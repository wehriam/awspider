import os
import time
from uuid import uuid4
import inspect
import logging
import logging.handlers
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred
from ..aws import AmazonS3, AmazonSDB
from ..requestqueuer import RequestQueuer
from ..pagegetter import PageGetter
from ..timeoffset import getTimeOffset
from ..exceptions import DeleteReservationException
import cPickle

LOGGER = logging.getLogger("main")

class BaseServer(object):
    
    logging_handler = None
    shutdown_trigger_id = None
    uuid = uuid4().hex
    start_time = time.time()
    active_jobs = {}
    reserved_arguments = [
        "reservation_function_name", 
        "reservation_created", 
        "reservation_next_request", 
        "reservation_error"]
    functions = {}
    
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
        if name == None:
            name = "AWSpider Server UUID: %s" % self.uuid
        self.port = port
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
    
    def callExposedFunction(self, func, kwargs, function_name, uuid=None):
        d = maybeDeferred(func, **kwargs)
        d.addCallback(self._callExposedFunctionCallback, function_name, uuid)
        d.addErrback(self._callExposedFunctionErrback, function_name, uuid)
        return d

    def _callExposedFunctionErrback(self, error, function_name, uuid):
        if uuid is not None:
            del self.active_jobs[uuid]
        try:
            error.raiseException()
        except DeleteReservationException, e:
            if uuid is not None:
                self.deleteReservation(uuid)
            message = """Error with %s, %s.\n%s            
            Reservation deleted at request of the function.""" % (
                function_name,
                uuid,
                error)
            LOGGER.error(message)
            return
        except:
            pass
        if uuid is None:
            LOGGER.error("Error with %s, %s.\n%s" % (function_name, uuid, error))
        else:
            LOGGER.error("Error with %s.\n%s" % (function_name, error))
        return error

    def _callExposedFunctionCallback(self, data, function_name, uuid):
        LOGGER.debug("Function %s returned successfully." % (function_name))
        # If the UUID is None, this is a one-off type of thing.
        if uuid is None:
            return data
        # If the data is None, there's nothing to store.
        if data is None:
            del self.active_jobs[uuid]
            return None
        # If we have an place to store the response on S3, do it.
        if self.aws_s3_storage_bucket is not None:
            LOGGER.debug("Putting result for %s, %s on S3." % (function_name, uuid))
            pickled_data = cPickle.dumps(data)
            d = self.s3.putObject(
                self.aws_s3_storage_bucket, 
                uuid, 
                pickled_data, 
                content_type="text/plain", 
                gzip=True)
            d.addCallback(self._exposedFunctionCallback2, data, uuid)
            d.addErrback(self._exposedFunctionErrback2, data, function_name, uuid)
            return d
        return data

    def _exposedFunctionErrback2(self, error, data, function_name, uuid):
        del self.active_jobs[uuid]
        LOGGER.error("Could not put results of %s, %s on S3.\n%s" % (function_name, uuid, error))
        return data
        
    def _exposedFunctionCallback2(self, s3_callback_data, data, uuid):
        del self.active_jobs[uuid]
        return data    
        
    def expose(self, *args, **kwargs):
        return self.makeCallable(expose=True, *args, **kwargs)

    def makeCallable(self, func, interval=0, name=None, expose=False):
        argspec = inspect.getargspec(func)
        # Get required / optional arguments
        arguments = argspec[0]
        if len(arguments) > 0 and arguments[0:1][0] == 'self':
            arguments.pop(0)
        kwarg_defaults = argspec[3]
        if kwarg_defaults is None:
            kwarg_defaults = []
        required_arguments = arguments[0:len(arguments) - len(kwarg_defaults)]
        optional_arguments = arguments[len(arguments) - len(kwarg_defaults):]
        # Get function name, usually class/method
        if name is not None:
            function_name = name
        elif hasattr(func, "im_class"):
            function_name = "%s/%s" % (func.im_class.__name__, func.__name__)
        else:
            function_name = func.__name__
        function_name = function_name.lower()
        # Make sure the function isn't using any reserved arguments.
        for key in required_arguments:
            if key in self.reserved_arguments:
                message = "Required argument name '%s' used in function %s is reserved." % (key, function_name)
                LOGGER.error(message)
                raise Exception(message)
        for key in optional_arguments:
            if key in self.reserved_arguments:
                message = "Optional argument name '%s' used in function %s is reserved." % (key, function_name)
                LOGGER.error(message)
                raise Exception(message)
        # Make sure we don't already have a function with the same name.
        if function_name in self.functions:
            raise Exception("A method or function with the name %s is already callable." % function_name)
        # Add it to our list of callable functions.
        self.functions[function_name] = {
            "function":func,
            "interval":interval,
            "required_arguments":required_arguments,
            "optional_arguments":optional_arguments
        }
        LOGGER.info("Function %s is now callable." % function_name)
        return function_name

    def getPage(self, *args, **kwargs):
        return self.pg.getPage(*args, **kwargs)
        
    def setHostMaxRequestsPerSecond(self, *args, **kwargs):
        return self.rq.setHostMaxRequestsPerSecond(*args, **kwargs)

    def setHostMaxSimultaneousRequests(self, *args, **kwargs):
        return self.rq.setHostMaxSimultaneousRequests(*args, **kwargs)

    def deleteReservation(self, uuid, function_name="Unknown"):
        LOGGER.info("Deleting reservation %s, %s." % (function_name, uuid))
        deferreds = []
        deferreds.append(self.sdb.delete(self.aws_sdb_reservation_domain, uuid))
        deferreds.append(self.s3.deleteObject(self.aws_s3_storage_bucket, uuid))
        d = DeferredList(deferreds)
        d.addCallback(self._deleteReservationCallback, function_name, uuid)
        d.addErrback(self._deleteReservationErrback, function_name, uuid)
        return d

    def _deleteReservationCallback(self, data, function_name, uuid):
        LOGGER.info("Reservation %s, %s successfully deleted." % (function_name, uuid))
        return True

    def _deleteReservationErrback(self, error, function_name, uuid ):
        LOGGER.error("Error deleting reservation %s, %s.\n%s" % (function_name, uuid, error))
        return False
        
