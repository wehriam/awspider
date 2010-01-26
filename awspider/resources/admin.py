from twisted.python.failure import Failure
from twisted.web import server
from .base import BaseResource


class AdminResource(BaseResource):
    
    isLeaf = True
    
    def __init__(self, adminserver):
        self.adminserver = adminserver
        BaseResource.__init__(self)
        
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        if len(request.postpath) > 0:
            if request.postpath[0] == "clear_http_cache":
                d = self.adminserver.clearHTTPCache()
                d.addCallback(self._successResponse)
                d.addErrback(self._errorResponse)
                d.addCallback(self._immediateResponse, request)
                return server.NOT_DONE_YET
        return self._errorResponse(Failure(exc_value=Exception("Unknown request."))) 

