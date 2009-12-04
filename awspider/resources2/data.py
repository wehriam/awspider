from twisted.web import server
from .base import BaseResource


class DataResource(BaseResource):
    
    def __init__(self, dataserver):
        self.dataserver = dataserver
        BaseResource.__init__(self)
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        if "uuid" in request.args:
            get_deferreds = []
            for uuid in request.args["uuid"]:
                get_deferreds.append(self.dataserver.getData(uuid))
            d = DeferredList(get_deferreds, consumeErrors = True)
            d.addCallback(self._getCallback, request.args["uuid"])
            d.addCallback(self._successResponse)
            d.addErrback(self._errorResponse)
            d.addCallback(self._immediateResponse, request)
            return server.NOT_DONE_YET
        return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required."))) 

    def _getCallback(self, data, uuids):
        response = {}
        for i in range(0, len(uuids)):
            if data[i][0] == True:
                response[uuids[i]] = data[i][1]
            else:
                response[uuids[i]] = {"error":str(data[i][1].value)}
        return response

