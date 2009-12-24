from twisted.web import server
from .base import BaseResource
from twisted.python.failure import Failure
from twisted.internet.defer import DeferredList

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
                    deferreds = []
                    for uuid in request.args["uuid"]:
                        deferreds.append(self.interfaceserver.showReservation(uuid))
                    d = DeferredList(deferreds, consumeErrors=True)
                    d.addCallback(self._showReservationCallback, request.args["uuid"])
                    d.addCallback(self._successResponse)
                    d.addErrback(self._errorResponse)
                    d.addCallback(self._immediateResponse, request)
                    return server.NOT_DONE_YET
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required.")))
            elif request.postpath[0] == "execute_reservation":
                if "uuid" in request.args:
                    d = self.interfaceserver.executeReservation(request.args["uuid"][0])
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

    def _showReservationCallback(self, data, uuids):
        print data[0][1]
        response = {}
        for i in range(0, len(uuids)):
            if data[i][0] == True:
                response[uuids[i]] = data[i][1]
            else:
                response[uuids[i]] = {"error":str(data[i][1].value)}
        return response

