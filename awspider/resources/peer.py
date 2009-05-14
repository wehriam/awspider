from twisted.web.resource import Resource
from twisted.web import server
import traceback
import simplejson
from twisted.python.failure import Failure
from twisted.internet import reactor

class PeerResource(Resource):
    
    isLeaf = True
    
    def __init__( self, spider ):
        
        self.spider = spider
        Resource.__init__(self)
    
    def render(self, request):
        
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        
        if len(request.postpath) > 0:
            if request.postpath[0] == "check":
                reactor.callLater( 5, self.spider.checkPeers )
                return simplejson.dumps( True )         
                    
    def _successResponse( self, data ):
        return simplejson.dumps( True )

    def _errorResponse( self, error ):

        reason = str(error.value)
        tb = traceback.format_exc( traceback.extract_tb(error.tb) )

        return simplejson.dumps( {"error":reason, "traceback":tb} )

    def _immediateResponse(self, data, request ):           
        request.write( data )
        request.finish()