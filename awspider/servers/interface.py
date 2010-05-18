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
    
    def __init__(self,
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_sdb_reservation_domain, 
            aws_s3_reservation_cache_bucket=None,
            aws_s3_http_cache_bucket=None,
            aws_s3_storage_bucket=None,
            aws_sdb_coordination_domain=None,
            max_simultaneous_requests=50,
            max_requests_per_host_per_second=1,
            max_simultaneous_requests_per_host=5,
            port=5000, 
            log_file='interfaceserver.log',
            log_directory=None,
            log_level="debug",
            name=None,
            time_offset=None):
        if name == None:
            name = "AWSpider Interface Server UUID: %s" % self.uuid
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
        if not isinstance(function_name, str):
            for key in self.functions:
                if self.functions[key]["function"] == function_name:
                    function_name = key
                    break
        if function_name not in self.functions:
            raise Exception("Function %s does not exist." % function_name)
        function = self.functions[function_name]
        filtered_kwargs = {}
        for key in function["required_arguments"]:
            if key in kwargs:
                #filtered_kwargs[key] = convertToUTF8(kwargs[key])
                filtered_kwargs[key] = kwargs[key]
            else:
                raise Exception("Required parameter '%s' not found. Required parameters are %s. Optional parameters are %s." % (key, function["required_arguments"], function["optional_arguments"]))
        for key in function["optional_arguments"]:
            if key in kwargs:
                #filtered_kwargs[key] = convertToUTF8(kwargs[key])
                filtered_kwargs[key] = kwargs[key]
        if function["interval"] > 0:
            reserved_arguments = {}
            reserved_arguments["reservation_function_name"] = function_name
            reserved_arguments["reservation_created"] = sdb_now(offset=self.time_offset)
            reserved_arguments["reservation_next_request"] = reserved_arguments["reservation_created"]
            reserved_arguments["reservation_error"] = "0"
            arguments = {}
            arguments.update(reserved_arguments)
            arguments.update(filtered_kwargs)
            uuid = uuid4().hex
            LOGGER.debug("Creating reservation on SimpleDB for %s, %s." % (function_name, uuid))
            a = self.sdb.putAttributes(self.aws_sdb_reservation_domain, uuid, arguments)
            a.addCallback(self._createReservationCallback, function_name, uuid)
            a.addErrback(self._createReservationErrback, function_name, uuid)
            if "call_immediately" in kwargs and not evaluateBoolean(kwargs["call_immediately"]):    
                d = DeferredList([a], consumeErrors=True)
            else:
                LOGGER.debug("Calling %s immediately with arguments:\n%s" % (function_name, PRETTYPRINTER.pformat(filtered_kwargs)))
                self.active_jobs[uuid] = True
                b = self.callExposedFunction(function["function"], filtered_kwargs, function_name, uuid=uuid)
                d = DeferredList([a,b], consumeErrors=True)
            d.addCallback(self._createReservationCallback2, function_name, uuid)
            d.addErrback(self._createReservationErrback2, function_name, uuid)
            return d
        else:
            LOGGER.debug("Calling %s with arguments:\n%s" % (function_name, PRETTYPRINTER.pformat(filtered_kwargs)))
            d = self.callExposedFunction(function["function"], filtered_kwargs, function_name)
            return d
            
    def _createReservationCallback(self, data, function_name, uuid):
        LOGGER.error(data)
        LOGGER.debug("Created reservation on SimpleDB for %s, %s." % (function_name, uuid))
        return uuid

    def _createReservationErrback(self, error, function_name, uuid):
        LOGGER.error("Unable to create reservation on SimpleDB for %s:%s, %s.\n" % (function_name, uuid, error))
        return error

    def _createReservationCallback2(self, data, function_name, uuid):
        for row in data:
            if row[0] == False:
                raise row[1]
        if len(data) == 1:
            return {data[0][1]:{}}
        else:
            return {data[0][1]:data[1][1]}

    def _createReservationErrback2(self, error, function_name, uuid):
        LOGGER.error("Unable to create reservation for %s:%s, %s.\n" % (function_name, uuid, error))
        return error
    
    def showReservation(self, uuid):
        d = self.sdb.getAttributes(self.aws_sdb_reservation_domain, uuid)
        return d
    
    def executeReservation(self, uuid):
        sql = "SELECT * FROM `%s` WHERE itemName() = '%s'" % (self.aws_sdb_reservation_domain, uuid)
        LOGGER.debug("Querying SimpleDB, \"%s\"" % sql)
        d = self.sdb.select(sql)
        d.addCallback(self._executeReservationCallback)
        d.addErrback(self._executeReservationErrback)        
        return d
    
    def _executeReservationCallback(self, data):
        if len(data) == 0:
            raise Exception("Could not find reservation.")
        uuid = data.keys()[0]
        kwargs_raw = {}
        reserved_arguments = {}
        # Load attributes into dicts for use by the system or custom functions.
        for key in data[uuid]:
            if key in self.reserved_arguments:
                reserved_arguments[key] = data[uuid][key][0]
            else:
                kwargs_raw[key] = data[uuid][key][0]
        # Check to make sure the custom function is present.
        function_name = reserved_arguments["reservation_function_name"]
        if function_name not in self.functions:
            raise Exception("Unable to process function %s for UUID: %s" % (function_name, uuid))
            return
        # Check for the presence of all required system attributes.
        if "reservation_function_name" not in reserved_arguments:
            self.deleteReservation(uuid)
            raise Exception("Reservation %s does not have a function name." % uuid)
        if "reservation_created" not in reserved_arguments:
            self.deleteReservation(uuid, function_name=function_name)
            raise Exception("Reservation %s, %s does not have a created time." % (function_name, uuid))
        if "reservation_next_request" not in reserved_arguments:
            self.deleteReservation(uuid, function_name=function_name)
            raise Exception("Reservation %s, %s does not have a next request time." % (function_name, uuid))
        if "reservation_error" not in reserved_arguments:
            self.deleteReservation(uuid, function_name=function_name)
            raise Exception("Reservation %s, %s does not have an error flag." % (function_name, uuid))
        # Load custom function.
        if function_name in self.functions:
            exposed_function = self.functions[function_name]
        else:
            raise Exception("Could not find function %s." % function_name)
            return
        # Check for required / optional arguments.
        kwargs = {}
        for key in kwargs_raw:
            if key in exposed_function["required_arguments"]:
                kwargs[key] = kwargs_raw[key]
            if key in exposed_function["optional_arguments"]:
                kwargs[key] = kwargs_raw[key]
        has_reqiured_arguments = True
        for key in exposed_function["required_arguments"]:
            if key not in kwargs:
                has_reqiured_arguments = False
                raise Exception("%s, %s does not have required argument %s." % (function_name, uuid, key))
        LOGGER.debug("Executing function.\n%s" % function_name)
        return self.callExposedFunction(exposed_function["function"], kwargs, function_name, uuid=uuid)
        
    def _executeReservationErrback(self, error):
        LOGGER.error("Unable to query SimpleDB.\n%s" % error)
    
    
