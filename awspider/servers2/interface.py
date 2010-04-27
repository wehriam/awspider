import pprint
from uuid import uuid4
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.web import server
from .base import BaseServer, LOGGER
from ..resources import InterfaceResource, ExposedResource
from ..aws import sdb_now
from ..evaluateboolean import evaluateBoolean

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class InterfaceServer(BaseServer):
    
    exposed_functions = []
    exposed_function_resources = {}
    name = "AWSpider Interface Server UUID: %s" % str(uuid4())
    
    def __init__(self,
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_s3_http_cache_bucket=None,
            aws_s3_storage_bucket=None,
            max_simultaneous_requests=50,
            max_requests_per_host_per_second=1,
            max_simultaneous_requests_per_host=5,
            port=5000, 
            log_file='interfaceserver.log',
            log_directory=None,
            log_level="debug"):
        resource = Resource()
        interface_resource = InterfaceResource(self)
        resource.putChild("interface", interface_resource)
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        BaseServer.__init__(
            self,
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_s3_http_cache_bucket=aws_s3_http_cache_bucket, 
            aws_s3_storage_bucket=aws_s3_storage_bucket,
            max_simultaneous_requests=max_simultaneous_requests,
            max_requests_per_host_per_second=max_requests_per_host_per_second,
            max_simultaneous_requests_per_host=max_simultaneous_requests_per_host,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level,
            port=port)
        
    def start(self):
        reactor.callWhenRunning(self._start)
        return self.start_deferred

    def _start(self):
        deferreds = []
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._startCallback)

    def _startCallback(self, data):
        for row in data:
            if row[0] == False:
                d = self.shutdown()
                d.addCallback(self._startHandleError, row[1])
                return d
        d = BaseServer.start(self)

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
    
    def makeCallable(self, func, interval=0, name=None, expose=False):
        function_name = BaseServer.makeCallable(
            self, 
            func, 
            interval=interval, 
            name=name, 
            expose=expose)
        if expose:
            self.exposed_functions.append(function_name)
            er = ExposedResource(self, function_name)
            function_name_parts = function_name.split("/")
            if len(function_name_parts) > 1:
                if function_name_parts[0] in self.exposed_function_resources:
                    r = self.exposed_function_resources[function_name_parts[0]]
                else:
                    r = Resource()
                    self.exposed_function_resources[function_name_parts[0]] = r
                self.function_resource.putChild(function_name_parts[0], r)
                r.putChild(function_name_parts[1], er)
            else:
                self.function_resource.putChild(function_name_parts[0], er)
            LOGGER.info("Function %s is now available via the HTTP interface." % function_name)
            
    def createReservation(self, function_name, **kwargs):
        uuid = uuid4().hex
        if not isinstance(function_name, str):
            for key in self.functions:
                if self.functions[key]["function"] == function_name:
                    function_name = key
                    break
        if function_name not in self.functions:
            raise Exception("Function %s does not exist." % function_name)
        d = self.callExposedFunction(
            self.functions[function_name]["function"], 
            kwargs, 
            function_name, 
            uuid=uuid)
        d.addCallback(self._createReservationCallback, function_name, uuid)
        d.addErrback(self._createReservationErrback, function_name, uuid)
        return d

    def _createReservationCallback(self, data, function_name, uuid):
        return data

    def _createReservationErrback(self, error, function_name, uuid):
        LOGGER.error("Unable to create reservation for %s:%s, %s.\n" % (function_name, uuid, error))
        return {}