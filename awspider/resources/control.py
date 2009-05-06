from twisted.web.resource import Resource
from twisted.web import server
import traceback
import simplejson
from twisted.python.failure import Failure
class ControlResource(Resource):
    
    isLeaf = True
    
    def __init__( self, spider ):
        
        self.spider = spider
        Resource.__init__(self)
    
    def render(self, request):
        
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        
        if len(request.postpath) > 0:
            if request.postpath[0] == "query":
                self.spider.query()
                return simplejson.dumps( True )
            elif request.postpath[0] == "shutdown":
                self.spider.shutdown()
                return simplejson.dumps( True )
            elif request.postpath[0] == "pause":
                self.spider.pause()
                return simplejson.dumps( True )
            elif request.postpath[0] == "resume":
                self.spider.resume()    
                return simplejson.dumps( True )
            elif request.postpath[0] == "delete_reservation":
                if "uuid" in request.args:
                    d = self.spider.deleteReservation( request.args["uuid"][0] )
                    d.addCallback( self._successResponse )
                    d.addErrback( self._errorResponse )
                    d.addCallback( self._immediateResponse, request )
                    return server.NOT_DONE_YET
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required.")))
            elif request.postpath[0] == "delete_function_reservations": 
                if "function_name" in request.args:
                    d = self.spider.deleteFunctionReservations( request.args["function_name"][0] )
                    d.addCallback( self._successResponse )
                    d.addErrback( self._errorResponse )
                    d.addCallback( self._immediateResponse, request )
                    return server.NOT_DONE_YET           
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter function_name is required.")))         
                    
    def _successResponse( self, data ):
        return simplejson.dumps( True )

    def _errorResponse( self, error ):

        reason = str(error.value)
        tb = traceback.format_exc( traceback.extract_tb(error.tb) )

        return simplejson.dumps( {"error":reason, "traceback":tb} )

    def _immediateResponse(self, data, request ):           
        request.write( data )
        request.finish()