from twisted.web import server
from .base import BaseResource
from twisted.python.failure import Failure

class InterfaceResource(BaseResource):
    
    isLeaf = True
    
    def __init__(self, interfaceserver):
        self.interfaceserver = interfaceserver
        BaseResource.__init__(self)
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        if len(request.postpath) > 0:
            if request.postpath[0] == "show_reservation":
                if "uuid" in request.args:
                    d = self.interfaceserver.showReservation(request.args["uuid"][0])
                    d.addCallback(self._successResponse)
                    d.addErrback(self._errorResponse)
                    d.addCallback(self._immediateResponse, request)
                    return server.NOT_DONE_YET
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required.")))
            elif request.postpath[0] == "delete_reservation":
                if "uuid" in request.args:
                    d = self.interfaceserver.deleteReservation(request.args["uuid"][0])
                    d.addCallback(self._successResponse)
                    d.addErrback(self._errorResponse)
                    d.addCallback(self._immediateResponse, request)
                    return server.NOT_DONE_YET
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required.")))

