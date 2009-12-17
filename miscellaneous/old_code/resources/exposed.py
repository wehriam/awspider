from twisted.python.failure import Failure
from twisted.web.resource import Resource
from awspider.aws import sdb_now
from uuid import uuid4
import simplejson
from twisted.web import server
import traceback
import sys
import cStringIO, gzip

class ExposedResource(Resource):
    
    isLeaf = True
    
    def __init__( self, spider, function_name ):
        self.spider = spider
        self.function_name = function_name
        Resource.__init__(self)
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        
        kwargs = {}
        for key in request.args:
            kwargs[key] = request.args[key][0]
            
        try:
            d = self.spider.createReservation( self.function_name, **kwargs )
        except:
            return self._errorResponse(Failure(exc_value=sys.exc_value, exc_type=sys.exc_type, exc_tb=sys.exc_traceback))
            
        d.addCallback( self._successResponse )
        d.addErrback( self._errorResponse )
        d.addCallback( self._immediateResponse, request )
        return server.NOT_DONE_YET

    def _successResponse( self, data ):
        return simplejson.dumps(data)
        
    def _errorResponse( self, error ):

        reason = str(error.value)
        tb = traceback.format_exc( traceback.extract_tb(error.tb) )

        return simplejson.dumps( {"error":reason, "traceback":tb} )
        
    def _immediateResponse( self, data, request ):    
        
        encoding = request.getHeader("accept-encoding")
        
        if encoding and "gzip" in encoding:

            zbuf = cStringIO.StringIO()
            zfile = gzip.GzipFile(None, 'wb', 9, zbuf)
            if isinstance( data, unicode ):
                zfile.write( unicode(data).encode("utf-8") )
            elif isinstance( data, str ):
                zfile.write( unicode(data, 'utf-8' ).encode("utf-8") )
            else:
                zfile.write( unicode(data).encode("utf-8") )

            zfile.close()
            request.setHeader("Content-encoding","gzip")
            request.write( zbuf.getvalue() )
        else:
            request.write( data )
        request.finish()

    
