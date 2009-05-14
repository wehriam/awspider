# To do:
# Hammering prevention
#  - Peer finding
#  - Request trading
# Negative cache
# Max-age cache

from twisted.python.failure import Failure

from uuid import uuid4
from twisted.internet import task
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList

from pagegetter import RequestQueuer, PageGetter
from unicodeconverter import convertToUTF8

from aws import AmazonS3, AmazonSDB

from twisted.web import server
from twisted.web.resource import Resource
import time

import inspect

from twisted.internet import reactor

from twisted.web import static

import re
import os

from aws import sdb_now, sdb_now_add

from resources.template import TemplateResource
from resources.exposed import ExposedResource
from resources.data import DataResource
from resources.control import ControlResource
from resources.peer import PeerResource

from twisted.internet import threads

import cPickle
import sys


import pprint
PrettyPrinter = pprint.PrettyPrinter(indent=4)

import logging
import logging.handlers

from timeoffset import getTimeOffset
from networkaddress import getNetworkAddress

logger = logging.getLogger("main")

def evaluateBoolean( b ):
    if b == "false" or b == "False":
        return False
    elif b == "true" or b == "True":
        return True
    elif b == "no" or b == "No":
        return False
    elif b == "yes" or b == "Yes":
        return True
    else:
        try:
            return bool( int(b) )
        except:
            return False

class AWSpider:

    """
    Amazon S3, Amazon EC2 based web spider.
    """
    
    paused = False
    plugins = []
    start_time = time.time()
    
    public_ip = None
    local_ip = None
    network_information = {}
    
    uuid = uuid4().hex
    reserved_arguments = ["reservation_function_name", "reservation_created", "reservation_next_request", "reservation_error"]
    exposed_function_resources = {}
    functions = {}
    exposed_functions = []
    peers = {}
    peer_uuids = []
    
    def __init__( self, 
                aws_access_key_id, 
                aws_secret_access_key, 
                aws_s3_cache_bucket, 
                aws_sdb_reservation_domain, 
                aws_s3_storage_bucket=None,
                aws_sdb_coordination_domain=None, 
                port=8080, 
                max_requests=50, 
                log_directory=None, 
                log_level="debug",
                web_admin=True,
                web_admin_port=None,
                time_offset=None,
                peer_check_interval=5,
                reservation_check_interval=5):
        
        self.time_offset = time_offset
        self.peer_check_interval = int(peer_check_interval)
        self.reservation_check_interval = int(reservation_check_interval)
        
        self.port = int(port)
        self.network_information["port"] = self.port
        
        self.rq = RequestQueuer( max_requests=int(max_requests) )
        
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_s3_cache_bucket = aws_s3_cache_bucket
        self.aws_s3_storage_bucket = aws_s3_storage_bucket
        self.aws_sdb_reservation_domain = aws_sdb_reservation_domain
        self.aws_sdb_coordination_domain = aws_sdb_coordination_domain
        
        self.resource = Resource()
        
        if web_admin_port is None:
            web_admin_port = self.port
        else:
            web_admin_port = int(web_admin_port)

        if web_admin:
            self.resource.putChild('', TemplateResource("index" ) )
            self.resource.putChild('static', static.File( os.path.join(os.path.dirname(__file__), 'web_static') ) )
            self.resource.putChild('control', ControlResource( self ) )
          
        self.resource.putChild('data', DataResource( self ) )
        self.resource.putChild('peer', PeerResource( self ) )
        self.function_resource = Resource()
        self.resource.putChild( "function", self.function_resource )
        self.site = server.Site( self.resource )
        reactor.listenTCP(self.port, self.site)

        if web_admin and web_admin_port != self.port:
            self.web_admin_site = server.Site( self.resource )
            reactor.listenTCP( web_admin_port, self.web_admin_site )

        self.s3 = AmazonS3( self.aws_access_key_id, self.aws_secret_access_key, self.rq )
        self.sdb = AmazonSDB( self.aws_access_key_id, self.aws_secret_access_key, self.rq )
        
        self.pg = PageGetter( self.s3, self.aws_s3_cache_bucket, rq=self.rq )

        if log_directory is None:
            handler = logging.StreamHandler()
        else:
            handler = logging.handlers.TimedRotatingFileHandler(os.path.join(log_directory, 'spider.log'), when='D', interval=1)
            
        formatter = logging.Formatter("%(levelname)s: %(message)s %(pathname)s:%(lineno)d")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        log_level = log_level.lower()
        log_levels = {
            "debug":logging.DEBUG, 
            "info":logging.INFO, 
            "warning":logging.WARNING, 
            "error":logging.ERROR,
            "critical":logging.CRITICAL
        }
        if log_level in log_levels:
            logger.setLevel(log_levels[log_level])
        else:
            logger.setLevel(logging.DEBUG)
    
        logger.info("Successfully loaded the Spider's configuration.")

    def showReservation( self, uuid ):
        d = self.sdb.getAttributes( self.aws_sdb_reservation_domain, uuid )
        return d

    def getServerData( self ):    
        
        running_time = time.time() - self.start_time
        cost = (self.sdb.box_usage * .14) * (60*60*24*30.4) / (running_time)
        
        data = {
            "paused":self.paused,
            "running_time":running_time,
            "cost":cost
        }
        
        logger.debug("Got server data:\n%s" % PrettyPrinter.pformat(data) )

        return data

    def getPage( self, *args, **kwargs ):
        return self.pg.getPage( *args, **kwargs )
    
    def getExposedFunctionDetails( self ):
        functions = []
        for key in self.exposed_functions:
            function = [key, {}]
            function[1]["interval"] = self.functions[key]["interval"]
            function[1]["required_arguments"] = self.functions[key]["required_arguments"]
            function[1]["optional_arguments"] = self.functions[key]["optional_arguments"]
            functions.append( function )
        
        functions.sort(lambda x,y:cmp(x[0], y[0]))
        
        logger.info("Got exposed function details:\n%s" % PrettyPrinter.pformat(functions) )
        
        return functions

    
    def createReservation( self, function_name, **kwargs ):
        
        if not isinstance( function_name, str ):
            for key in self.functions:
                if self.functions[key]["function"] == function_name:
                    function_name = key
                    break            
        
        if function_name not in self.functions:
            raise Exception("Function %s does not exist." % function_name )
    
        function = self.functions[ function_name ]
    
        filtered_kwargs = {}
        for key in function["required_arguments"]:
            if key in kwargs:
                filtered_kwargs[key] = convertToUTF8( kwargs[key] )
            else:
                raise Exception("Required parameter '%s' not found. Required parameters are %s. Optional parameters are %s." % (key, function["required_arguments"], function["optional_arguments"] ))

        for key in function["optional_arguments"]:
            if key in kwargs:
                filtered_kwargs[key] = convertToUTF8( kwargs[key] )
        
        if function["interval"] > 0:
            reserved_arguments = {}
            reserved_arguments["reservation_function_name"] = function_name
            reserved_arguments["reservation_created"] = sdb_now(offset=self.time_offset)
            reserved_arguments["reservation_next_request"] = reserved_arguments["reservation_created"]
            reserved_arguments["reservation_error"] = "0"
            
            all_arguments = {}
            all_arguments.update( reserved_arguments )
            all_arguments.update( filtered_kwargs )
            
            uuid = uuid4().hex
            logger.debug( "Creating reservation on SimpleDB for %s, %s." % (function_name, uuid))

            a = self.sdb.putAttributes( self.aws_sdb_reservation_domain, uuid, all_arguments )
            a.addCallback( self._createReservationCallback, function_name, uuid )
            a.addErrback( self._createReservationErrback, function_name, uuid )
                        
            if "call_immediately" in kwargs and not evaluateBoolean( kwargs["call_immediately"] ):    
                d = DeferredList([a], consumeErrors=True)
            else:
                logger.debug( "Calling %s in a thread immediately with arguments:\n%s" % (function_name, PrettyPrinter.pformat(filtered_kwargs) ) )
                b = Deferred()
                b.addCallback(self._callExposedFunctionWrapper, function["function"], filtered_kwargs, function_name, uuid)
                reactor.callInThread( b.callback, None )
                d = DeferredList([a,b], consumeErrors=True)
            
            d.addCallback( self._createReservationCallback2, function_name, uuid )
            d.addErrback( self._createReservationErrback2, function_name, uuid )
            return d
            
        else:
            logger.debug( "Calling %s in a thread immediately with arguments:\n%s" % (function_name, PrettyPrinter.pformat(filtered_kwargs) ) )
            d = Deferred()
            d.addCallback(self.callExposedFunctionImmediately, function["function"], filtered_kwargs, function_name)
            reactor.callInThread( d.callback, None )
            return d

    def _callExposedFunctionWrapper( self, data, func, kwargs, function_name, uuid ):
        return self.callExposedFunction( func, kwargs, function_name, uuid )

    def _createReservationCallback( self, data, function_name, uuid ):
        logger.debug( "Created reservation on SimpleDB for %s, %s." % (function_name, uuid))
        return uuid
    
    def _createReservationErrback( self, error, function_name, uuid ):
        logger.error( "Unable to create reservation on SimpleDB for %s:%s, %s.\n" % (function_name, uuid, error) )
        return error
    
    def _createReservationCallback2( self, data, function_name, uuid ):
        
        for row in data:
            if row[0] == False:
                raise row[1]
                
        if len(data) == 1:
            return {data[0][1]:{}}
        else:
            return {data[0][1]:data[1][1]}
            
    def _createReservationErrback2( self, error, function_name, uuid ):
        logger.error( "Unable to create reservation for %s:%s, %s.\n" % (function_name, uuid, error) )
        return error
    
    def callExposedFunctionImmediately( self, data, func, kwargs, function_name ):

        try:
            d = func( **kwargs )
        except Exception, e:
            return self._callExposedFunctionImmediatelyErrback( Failure(exc_value=sys.exc_value, exc_type=sys.exc_type, exc_tb=sys.exc_traceback), function_name )
            
        if isinstance( d, Deferred ):            
            d.addCallback( self._callExposedFunctionImmediatelyCallback, function_name )
            d.addErrback( self._callExposedFunctionImmediatelyErrback, function_name )
            return d
        else:
            return self._callExposedFunctionImmediatelyCallback( d, function_name )
        
    def _callExposedFunctionImmediatelyCallback( self, data, function_name ):
        logger.info("Function %s returned successfully:%s" % (function_name, data) )
        return data
        
    def _callExposedFunctionImmediatelyErrback( self, error, function_name ):
        logger.error( "Error with immediate callback of %s.\n%s\n%s" % (function_name, error, error.value.__dict__) )

        return error
            
    def expose( self, *args, **kwargs ):
        return self.makeCallable( expose=True, *args, **kwargs )
        
    def makeCallable( self, func, interval=0, name=None, expose=False ):
            
        argspec = inspect.getargspec( func )
        arguments = argspec[0]
        if len(arguments) > 0 and arguments[0:1][0] == 'self':
            arguments.pop(0)
        
        kwarg_defaults = argspec[3]
        if kwarg_defaults is None:
            kwarg_defaults = []
        
        required_arguments = arguments[ 0:len(arguments) - len(kwarg_defaults) ]
        optional_arguments = arguments[ len(arguments) - len(kwarg_defaults):]

        if hasattr(func, "im_class") and name is None:
            function_name = "%s/%s" % ( func.im_class.__name__, func.__name__)
        elif name is not None:
            function_name = name
        else:
            function_name = func.__name__
        
        function_name = function_name.lower()
        
        for key in required_arguments:
            if key in self.reserved_arguments:
                message = "Required argument name '%s' used in function %s is reserved." % (key, function_name)
                logger.error( message )
                raise Exception( message )
                
        for key in optional_arguments:
            if key in self.reserved_arguments:
                message = "Optional argument name '%s' used in function %s is reserved." % (key, function_name)
                logger.error( message )
                raise Exception( message )
        
        self.functions[ function_name ] = {
            "function":func,
            "interval":interval,
            "required_arguments":required_arguments,
            "optional_arguments":optional_arguments
        }
        
        logger.info( "Function %s is now callable by the Spider." % function_name )
        
        if expose:
            
            self.exposed_functions.append( function_name )
            
            er = ExposedResource( self, function_name )
        
            function_name_parts = function_name.split("/")
            if len( function_name_parts ) > 1:
                if function_name_parts[0] in self.exposed_function_resources:
                    r = self.exposed_function_resources[ function_name_parts[0] ]
                else:
                    r = Resource()
                    self.exposed_function_resources[ function_name_parts[0] ] = r
                self.function_resource.putChild(function_name_parts[0], r )
                r.putChild( function_name_parts[1], er)
            else:
                self.function_resource.putChild(function_name_parts[0], er )
            
            logger.info( "Function %s is now available via the Spider's web interface." % function_name )
            
        return function_name
        
    def start( self ):
        reactor.callWhenRunning( self._start )
        
    def _start( self ):
        logger.critical( "Checking S3 and SimpleDB setup, getting time offset for sync." )
        
        deferreds = []
        
        # Make sure the specified S3 cache bucket works.
        
        deferreds.append( self._checkAndCreateS3Bucket( self.aws_s3_cache_bucket ) )

        if self.aws_s3_storage_bucket is not None:
            deferreds.append( self._checkAndCreateS3Bucket( self.aws_s3_storage_bucket ) )

        deferreds.append( self._checkAndCreateSDBDomain( self.aws_sdb_reservation_domain ) )    

        if self.aws_sdb_coordination_domain is not None:
            deferreds.append( self._checkAndCreateSDBDomain( self.aws_sdb_coordination_domain ) )
            deferreds.append( self.getNetworkAddress() )
        
        if self.time_offset is None:
            deferreds.append( self.getTimeOffset() )
        
        d = DeferredList(deferreds, consumeErrors=True )
        d.addCallback( self._startCallback )

    def _startCallback( self, data ):   
        reactor.addSystemEventTrigger('before', 'shutdown', self._cleanupBeforeReactorShutdown)     
        logger.critical( "Starting Spider UUID: %s" % self.uuid)
        self.queryloop = task.LoopingCall( self.query )
        self.queryloop.start( self.reservation_check_interval )
        if self.aws_sdb_coordination_domain is not None:
            self.coordinateloop = task.LoopingCall( self.coordinate )
            self.coordinateloop.start( self.peer_check_interval )  
            reactor.callLater( 10, self.peerCheckRequest )          
        #self.paused = True
        pass
   
    def getNetworkAddress( self ):
        d = getNetworkAddress()
        d.addCallback( self._getNetworkAddressCallback )
        d.addErrback( self._getNetworkAddressErrback )
        return d
                
    def _getNetworkAddressCallback( self, data  ):   
        if "public_ip" in data:
            self.public_ip = data["public_ip"]
            self.network_information["public_ip"] = self.public_ip
            
        if "local_ip" in data:
            self.local_ip = data["local_ip"]  
            self.network_information["local_ip"] = self.local_ip
         
    def _getNetworkAddressErrback( self, error ):
        logger.critical( "Could not get network address." )
        self.shutdown()
            
    def getTimeOffset( self ):
        d = getTimeOffset()
        d.addCallback( self._getTimeOffsetCallback )
        d.addErrback( self._getTimeOffsetErrback )
        return d
        
    def _getTimeOffsetCallback( self, time_offset ):
        logger.info( "Got time offset for sync." )
        self.time_offset = time_offset
    
    def _getTimeOffsetErrback( self, error ):
        if self.time_offset is None:
            logger.critical( "Could not get time offset for sync." )
            self.shutdown()
    
    def _checkAndCreateS3Bucket( self, bucket_name ):
        d = self.s3.getBucket( bucket_name )
        d.addErrback( self._checkAndCreateS3BucketErrback, bucket_name ) 
        return d
        
    def _checkAndCreateS3BucketErrback( self, error, bucket_name ):
        if int(error.value.status) == 404:
            logger.info( "Bucket %s does not exist, creating." % bucket_name )     
            d = self.s3.putBucket( bucket_name )
            d.addErrback( self._checkAndCreateS3BucketErrback2, bucket_name )
            return d
            
        logger.critical( "Could not find or create S3 bucket '%s'." % bucket_name )
        self.shutdown()
        
    def _checkAndCreateS3BucketErrback2( self, error, bucket_name ):
        logger.critical( "Could not create S3 bucket '%s'" % bucket_name )
        self.shutdown()

    def _checkAndCreateSDBDomain( self, domain_name ):    
        d = self.sdb.domainMetadata( domain_name )
        d.addErrback( self._checkAndCreateSDBDomainErrback, domain_name )     
        return d

    def _checkAndCreateSDBDomainErrback( self, error, domain_name ):
        
        if int(error.value.status) == 400:  
            logger.info( "Domain %s does not exist, creating." % domain_name )
            d = self.sdb.createDomain( domain_name  )
            d.addErrback( self._checkAndCreateSDBDomainErrback2, domain_name )
            return d
        
        logger.critical( "Could not find or create SDB domain '%s'." % domain_name )
        self.shutdown()
        
    def _checkAndCreateSDBDomainErrback2( self, error, domain_name ):
        logger.critical( "Could not create SDB domain '%s'" % domain_name )
        self.shutdown()
    
    def pause(self):
        logger.critical( "Pausing Spider." )
        self.paused = True
        self.queryloop.stop() 
        self.coordinateloop.stop()
        
    def resume(self):
        logger.critical( "Resuming Spider." )
        self.paused = False
        self.queryloop.start( self.reservation_check_interval ) 
        if self.aws_sdb_coordination_domain is not None:
            self.coordinateloop.start( self.peer_check_interval)
    
    def setReservationError( self, uuid, function_name="Unknown" ):
        logger.error( "Setting error on reservation %s, %s." % (function_name, uuid) )
        d = self.sdb.putAttributes( self.aws_sdb_reservation_domain, uuid, {"reservation_error":"1"}, replace=["reservation_error"] )
        d.addCallback( self._setReservationErrorCallback, uuid )
        d.addErrback( self._setReservationErrorErrback, uuid )
        return d
        
    def _setReservationErrorCallback( self, data, function_name, uuid ):
        logger.error( "Error set on reservation %s, %s." % (function_name, uuid ) )
        
    def _setReservationErrorErrback( self, error, function_name, uuid ):
        logger.error( "Unable to set error on reservation %s, %s.\n%s" % (function_name, uuid, error) )
    
    def deleteFunctionReservations( self, function_name ):
        logger.info( "Deleting reservations for function %s." % (function_name) )
        d = self.sdb.select( "SELECT itemName() FROM %s WHERE reservation_function_name='%s'" % ( self.aws_sdb_reservation_domain, function_name) )
        d.addCallback( self._deleteFunctionReservationsCallback, function_name )
        d.addErrback( self._deleteFunctionReservationsErrback, function_name )
        return d
    
    def _deleteFunctionReservationsCallback( self, data, function_name ):
        logger.debug( "Found reservations for function %s: %s" % (function_name, data) )
        deferreds = []
        for uuid in data:
            deferreds.append( self.sdb.delete(self.aws_sdb_reservation_domain, uuid) )
        d = DeferredList(deferreds, consumeErrors = True )
        d.addCallback( self._deleteFunctionReservationsCallback2, function_name )
        return d
    
    def _deleteFunctionReservationsCallback2( self, data, function_name ):
        try:
            for row in data:
                if row[0] == False:
                    raise row[1] 
        except:
            return self._deleteFunctionReservationsErrback( Failure(exc_value=sys.exc_value, exc_type=sys.exc_type, exc_tb=sys.exc_traceback), function_name )
        
        logger.debug( "Deleted reservations for function %s." % (function_name) )
        
    def _deleteFunctionReservationsErrback( self, error, function_name ):
        logger.error( "Error deleting reservations for %s.\n%s" ) % (function_name, error)
        
    def deleteReservation( self, uuid, function_name="Unknown" ):
        logger.info( "Deleting reservation %s, %s." % (function_name, uuid) )
        d = self.sdb.delete( self.aws_sdb_reservation_domain, uuid )
        d.addCallback( self._deleteReservationCallback, function_name, uuid )
        d.addErrback( self._deleteReservationErrback, function_name, uuid )
        return d
        
    def _deleteReservationCallback( self, data, function_name, uuid ):
        logger.info( "Reservation %s, %s successfully deleted." % (function_name, uuid) )
        
    def _deleteReservationErrback( self, error, function_name, uuid ):
        logger.error( "Error deleting reservation %s, %s.\n%s" ) % (function_name, uuid, error)

    def query( self ):
        
        if len(self.functions) > 0:
            reservation_function_names = []
            for function_name in self.functions.keys():
                 reservation_function_names.append( "reservation_function_name='%s'" % function_name )
            reservation_function_names =  "INTERSECTION " + " OR ".join( reservation_function_names )
        else:
            reservation_function_names = ""
        
        sql = "SELECT * FROM %s WHERE reservation_next_request < '%s' INTERSECTION reservation_error != '1' %s" % ( self.aws_sdb_reservation_domain, sdb_now(offset=self.time_offset), reservation_function_names )
        
        #logger.debug( "Querying SimpleDB, \"%s\"" % sql )
        
        d = self.sdb.select( sql )
        d.addCallback( self._queryCallback )
        d.addErrback( self._queryErrback )
    
    def _queryErrback( self, error ):
        logger.error( "Unable to query SimpleDB.\n%s" % error )
        
    def _queryCallback( self, data ):
        
        for uuid in data:
            
            kwargs_raw = {}
            reserved_arguments = {}
            
            for key in data[ uuid ]:
                if key in self.reserved_arguments:
                    reserved_arguments[ key ] = data[ uuid ][key][0]
                else:
                    kwargs_raw[key] = data[ uuid ][key][0]
            
            if "reservation_function_name" not in reserved_arguments:
                logger.error( "Reservation %s does not have a function name." % uuid )
                self.deleteReservation( uuid )
                continue

            function_name = reserved_arguments["reservation_function_name"]

            if "reservation_created" not in reserved_arguments:
                logger.error( "Reservation %s, %s does not have a created time." % (function_name, uuid) )
                self.deleteReservation( uuid, function_name=function_name )
                continue            
            
            if "reservation_next_request" not in reserved_arguments:
                logger.error( "Reservation %s, %s does not have a next request time." % (function_name, uuid) )
                self.deleteReservation( uuid, function_name=function_name )
                continue                

            if "reservation_error" not in reserved_arguments:
                logger.error( "Reservation %s, %s does not have an error flag." % (function_name, uuid) )
                self.deleteReservation( uuid, function_name=function_name )
                continue
            
            if reserved_arguments["reservation_function_name"] in self.functions:
                exposed_function = self.functions[ function_name  ]
            else:
                logger.error( "Could not find resource %s." % function_name )
                continue
            
            d = self.sdb.putAttributes( self.aws_sdb_reservation_domain, uuid, {"reservation_next_request":sdb_now_add(exposed_function["interval"], offset=self.time_offset) }, replace=["reservation_next_request"] )
            d.addCallback( self._setNextRequestCallback, function_name, uuid)
            d.addErrback( self._setNextRequestErrback, function_name, uuid )
            
            kwargs = {}
            for key in kwargs_raw:
                if key in exposed_function["required_arguments"]:
                    kwargs[ key ] = kwargs_raw[ key ]
                if key in exposed_function["optional_arguments"]:
                    kwargs[ key ] = kwargs_raw[ key ]
            
            has_reqiured_arguments = True
            for key in exposed_function["required_arguments"]:
                if key not in kwargs:
                    has_reqiured_arguments = False
                    logger.error( "%s, %s does not have required argument %s." % (function_name, uuid, key) ) 
                    
            if not has_reqiured_arguments:
                self.setReservationError( uuid )
                continue
            
            reactor.callInThread(self.callExposedFunction, exposed_function["function"], kwargs, function_name, uuid  )

    def callExposedFunction( self, func, kwargs, function_name, uuid ):
        
        logger.debug( "Calling %s, %s." % (function_name, uuid) )
        
        try:
            d = func( **kwargs )
        except:         
            return self._exposedFunctionErrback( Failure(exc_value=sys.exc_value, exc_type=sys.exc_type, exc_tb=sys.exc_traceback), function_name, uuid )
        
        if isinstance(d, Deferred ):
            d.addCallback( self._exposedFunctionCallback, function_name, uuid )
            d.addErrback( self._exposedFunctionErrback, function_name, uuid )
            return d
        else:
            return self._exposedFunctionCallback( d, function_name, uuid )
            
    def _setNextRequestCallback(self, data, function_name, uuid ):
        logger.debug( "Set next request for %s, %s on on SimpleDB." % (function_name, uuid) )
        
    def _setNextRequestErrback(self, error, function_name, uuid ):
        logger.error( "Unable to set next request for %s, %s on SimpleDB.\n%s" % (function_name, uuid, error.value) )
    
    def get( self, uuid ):
        logger.debug( "Getting %s from S3." % uuid )
        d = self.s3.getObject( self.aws_s3_storage_bucket, uuid )
        d.addCallback( self._getCallback, uuid )
        d.addErrback( self._getErrback, uuid )
        return d
        
    def _getCallback( self, data, uuid ):
        logger.debug( "Got %s from S3." % (uuid) ) 
        return cPickle.loads( data["response"] )
    
    def _getErrback( self, error, uuid ):
        logger.error( "Could not get %s from S3.\n%s" % (uuid, error) ) 
        return error
    
    def _exposedFunctionCallback( self, data, function_name, uuid ):
        
        if self.aws_s3_storage_bucket is not None:
            if data is None:
                logger.debug( "Recieved None for %s, %s." % (function_name, uuid) )
                return None
            else:    
                logger.debug( "Putting result for %s, %s on S3." % (function_name, uuid) )
                pickled_data = cPickle.dumps( data )
                d = self.s3.putObject( self.aws_s3_storage_bucket, uuid, pickled_data, contentType="text/plain", gzip=True )
                d.addErrback( self._exposedFunctionErrback2, function_name, uuid )
                d.addCallback( self._exposedFunctionCallback2, data )
                return d
    
    def _exposedFunctionCallback2( self, s3_callback_data, data ):
        return data
    
    def _exposedFunctionErrback( self, error, function_name, uuid ):
        logger.error( "Error with %s, %s.\n%s" % (function_name, uuid, error) )
    
    def _exposedFunctionErrback2( self, error, function_name, uuid ):
        logger.error( "Could not put results of %s, %s on S3.\n%s" % (function_name, uuid, error) )

    
    def _cleanupBeforeReactorShutdown( self ):
        logger.debug( "Cleaning up before reactor shutdown." )
        
        deferreds = []
        
        if self.aws_sdb_coordination_domain is not None:
            d = self.sdb.delete( self.aws_sdb_coordination_domain, self.uuid )
            d.addCallback( self._cleanupBeforeReactorShutdownCallback )
            return d
    
    def _cleanupBeforeReactorShutdownCallback( self, data  ):
        logger.debug( "Deleted uuid from %s" % self.aws_sdb_coordination_domain )
        return self.peerCheckRequest()
    
    def peerCheckRequest( self ):

        logger.debug( "Signaling peers." )

        deferreds = []

        for uuid in self.peers:
            if uuid != self.uuid:
                logger.debug( "Signaling %s to check peers." % self.peers[uuid]["uri"] )
                d = self.getPage( self.peers[uuid]["uri"] + "/peer/check", cache= -1 )
                d.addCallback( self._peerCheckRequestCallback, self.peers[uuid]["uri"] )
                deferreds.append( d )

        if len(deferreds) > 0:
            return DeferredList(deferreds)

    def _peerCheckRequestCallback( self, data, uri ):
        logger.debug( "Got %s/peer/check." % uri )
    
    def shutdown( self ):
        logger.critical( "Stopping server." )
        reactor.callLater( 1, reactor.stop )
    
    def coordinate( self ):
        print "Peers: %s" % self.peers
        attributes = { "created":sdb_now(offset=self.time_offset) }
        attributes.update( self.network_information )
        self.sdb.putAttributes( self.aws_sdb_coordination_domain, self.uuid, attributes, replace=attributes.keys() )
        reactor.callLater( 5, self.checkPeers )
    
    def checkPeers( self ):
        sql = "SELECT public_ip, local_ip, port FROM %s WHERE created > '%s'" % ( self.aws_sdb_coordination_domain, sdb_now_add( self.peer_check_interval * -2, offset=self.time_offset) )
        #logger.debug( "Querying SimpleDB, \"%s\"" % sql )
        d = self.sdb.select( sql )
        d.addCallback( self._checkPeersCallback )
        d.addErrback( self._checkPeersErrback )
        
    def _checkPeersCallback( self, discovered ):

        existing_peers = set( self.peers.keys() )
        discovered_peers = set( discovered.keys() )
        new_peers = discovered_peers - existing_peers
        old_peers = existing_peers - discovered_peers
        
        deferreds = []
        
        for uuid in new_peers:
            if uuid == self.uuid:
                self.peers[uuid] = {
                    "local_ip":"127.0.0.1",
                    "port":self.port
                }
            else:
                deferreds.append( self.verifyPeer( uuid, discovered[uuid] ) )
        
        for uuid in old_peers:
            logger.debug( "Removing peer %s" % uuid )
            del self.peers[ uuid ]
        
        if len( new_peers ) > 0:
            if len(deferreds) > 0:
                d = DeferredList(deferreds, consumeErrors=True)
                d.addCallback( self._checkPeersCallback2 )
            else:
                self._checkPeersCallback2( None ) #Just found ourself.
        elif len( old_peers ) > 0:
            self._checkPeersCallback2( None )
        else:
            pass # No old, no new.
        
    def _checkPeersCallback2( self, data ):
        logger.debug( "Re-organizing peers." )
        for uuid in self.peers:
            if "local_ip" in self.peers[uuid]:
                self.peers[uuid]["uri"] = "http://%s:%s" % (self.peers[uuid]["local_ip"], self.peers[uuid]["port"] )
            elif "public_ip" in self.peers[uuid]:
                self.peers[uuid]["uri"] = "http://%s:%s" % (self.peers[uuid]["public_ip"], self.peers[uuid]["port"] )
            else:
                logger.error("Peer %s has no local or public IP. This should not happen." % uuid )
        
        self.peer_uuids = self.peers.keys()
        self.peer_uuids.sort()
        
        print self.peers 
        print self.peer_uuids
        
        # Go through the peers and reorganize the group.
        pass
        
    def _checkPeersErrback( self, error ):
        print error

    def verifyPeer( self, uuid, peer ):
        
        logger.debug( "Verifying peer %s" % uuid )
        
        deferreds = []
        
        if "port" in peer:
            port = int(peer["port"][0])
        else:
            port = self.port
            
        if "local_ip" in peer:
            local_ip = peer["local_ip"][0]
            local_url = "http://%s:%s/data/server" % (local_ip, port)
            d = self.getPage( local_url, timeout=5, cache=-1 )
            d.addCallback( self._verifyPeerLocalIPCallback, uuid, local_ip, port )
            deferreds.append( d )
            
        if "public_ip" in peer:
            public_ip = peer["public_ip"][0]
            public_url = "http://%s:%s/data/server" % (public_ip, port)
            d = self.getPage( public_url, timeout=5, cache=-1  )         
            d.addCallback( self._verifyPeerPublicIPCallback, uuid, public_ip, port )
            deferreds.append( d )

        if len(deferreds) > 0:
            d = DeferredList( deferreds, consumeErrors=True )
            return d
        else:
            return None
    
    def _verifyPeerLocalIPCallback( self, data, uuid, local_ip, port ):
        logger.debug( "Verified local IP for %s" % uuid )
        if uuid not in self.peers:
            self.peers[ uuid ] = {}
        self.peers[ uuid ]["local_ip"] = local_ip
        self.peers[ uuid ]["port"] = port
    
    def _verifyPeerPublicIPCallback( self, data, uuid, public_ip, port ):
        logger.debug( "Verified public IP for %s" % uuid )
        if uuid not in self.peers:
            self.peers[ uuid ] = {}
        self.peers[ uuid ]["public_ip"] = public_ip
        self.peers[ uuid ]["port"] = port
        
