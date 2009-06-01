from twisted.web.resource import Resource
from twisted.web import server
from twisted.internet import reactor

class MiniWebServer:
    def __init__( self ):
        self.resource = Resource()
        self.resource.putChild('', MiniResource() )
        self.site = server.Site( self.resource )
        self.port = reactor.listenTCP( 8080, self.site )
        
    def shutdown( self ):
        return self.port.stopListening()
        
class MiniResource:
        isLeaf = True
        def render(self, request):
            
            request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
            
            #if "etag" in request.args:
            #    request.setHeader('Etag', 'hItPEjEaBzlhWc5DKKdB3MneWm0')
            #
            #if "last-modified" in request.args:
            #    request.setHeader('Last-Modified', 'Thu, 28 May 2009 08:57:03 GMT')
            #
            #if "expires" in request.args:
            #    request.setHeader('Expires', 'Thu, 28 May 2019 09:03:36 GMT')
            
            return "true"