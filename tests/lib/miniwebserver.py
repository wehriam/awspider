from twisted.web.resource import Resource
from twisted.web import server
from twisted.internet import reactor
import uuid

class MiniWebServer:
    def __init__(self):
        self.resource = Resource()
        self.resource.putChild('helloworld', TrueResource())
        self.resource.putChild('expires', ExpiresResource())
        self.resource.putChild('random', RandomResource())
        self.site = server.Site(self.resource)
        self.port = reactor.listenTCP(8080, self.site)
        
    def shutdown(self):
        return self.port.stopListening()

class RandomResource:
    
    isLeaf = True
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        return uuid.uuid4().hex

class ExpiresResource(object):
    
    isLeaf = True
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        request.setHeader('Expires', 'Thu, 28 May 2019 09:03:36 GMT')
        return "Hello World, this won't expire for a long time!"
        
class TrueResource:
    
        isLeaf = True
        
        def render(self, request):
            request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
            return "Hello World!"