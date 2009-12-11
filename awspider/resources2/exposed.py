import sys
from twisted.web import server
from twisted.internet.defer import maybeDeferred 
from .base import BaseResource

class ExposedResource(BaseResource):
    
    isLeaf = True
    
    def __init__(self, interfaceserver, function_name):
        self.interfaceserver = interfaceserver
        self.function_name = function_name
        BaseResource.__init__(self)
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        kwargs = {}
        for key in request.args:
            kwargs[key] = request.args[key][0]
        d = maybeDeferred(self.interfaceserver.createReservation, self.function_name, **kwargs)
        d.addCallback(self._successResponse)
        d.addErrback(self._errorResponse)
        d.addCallback(self._immediateResponse, request)
        return server.NOT_DONE_YET
