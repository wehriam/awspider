from twisted.web.resource import Resource
from twisted.web import server
import traceback
import simplejson
from twisted.python.failure import Failure
from twisted.internet import reactor
from awspider import evaluateboolean
import urlparse

class PeerResource(Resource):
    
    isLeaf = True
    
    def __init__( self, spider ):
        
        self.spider = spider
        Resource.__init__(self)
    
    def render(self, request):
        
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        
        if len(request.postpath) > 0:
            if request.postpath[0] == "check":
                reactor.callLater( 5, self.spider.coordinate )
                return simplejson.dumps( True )  
            if request.postpath[0] == "getpage" and "url" in request.args:
                #print "Recieving peering request for %s" % request.args["url"][0]
                kwargs = {}
                if "method" in request.args:
                    kwargs["method"] = request.args["method"][0]     
                if "postdata" in request.args: 
                    kwargs["postdata"] = urlparse.parse_qs( request.args["postdata"][0] )
                if "headers" in request.args: 
                    kwargs["headers"] = urlparse.parse_qs( request.args["headers"][0] )  
                if "cookies" in request.args: 
                    kwargs["cookies"] = urlparse.parse_qs( request.args["cookies"][0] )           
                if "agent" in request.args:
                    kwargs["agent"] = request.args["agent"][0]
                if "timeout" in request.args:
                    kwargs["timeout"] = int( request.args["timeout"][0] )
                if "followRedirect" in request.args:
                    kwargs["followRedirect"] = evaluateboolean( request.args["followRedirect"][0] )
                if "hash_url" in request.args: 
                    kwargs["hash_url"] = request.args["hash_url"][0] 
                if "cache" in request.args: 
                    kwargs["cache"] = int( request.args["cache"][0] )
                if "prioritize" in request.args: 
                    kwargs["prioritize"] = evaluateboolean( request.args["prioritize"][0] )
    
                d = self.spider.pg.getPage( request.args["url"][0], **kwargs  )              
                d.addCallback( self._getpageCallback, request )         
                d.addErrback( self._getpageErrback, request )     
                return server.NOT_DONE_YET
    
    def _getpageCallback( self, data, request ):

        request.setResponseCode( data["status"], data["message"] )

        for header in data["headers"]:
            request.setHeader(header, data["headers"][header][0])
        request.write( data["response"] )
        request.finish()
        
    def _getpageErrback( self, error, request ):
        reason = str(error.value)
        tb = traceback.format_exc( traceback.extract_tb(error.tb) )
        request.write( simplejson.dumps( {"error":reason, "traceback":tb} ) )    
        request.finish()
        
    def _successResponse( self, data ):
        return simplejson.dumps( True )

    def _errorResponse( self, error ):

        reason = str(error.value)
        tb = traceback.format_exc( traceback.extract_tb(error.tb) )

        return simplejson.dumps( {"error":reason, "traceback":tb} )

    def _immediateResponse(self, data, request ):           
        request.write( data )
        request.finish()