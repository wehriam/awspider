from twisted.python.failure import Failure
from twisted.web.resource import Resource
from twisted.internet.defer import DeferredList
from twisted.web import server
import cStringIO, gzip
import cPickle
import traceback
import simplejson

class DataResource(Resource):
    
    isLeaf = True
    
    def __init__(self, dataserver):
        self.dataserver = dataserver
        Resource.__init__(self)
    
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
        else:
            return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required.")))
        return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required."))) 

    def _getCallback(self, data, uuids):
        response = {}
        for i in range(0, len(uuids)):
            if data[i][0] == True:
                response[uuids[i]] = data[i][1]
            else:
                response[uuids[i]] = {"error":str(data[i][1].value)}
        return response

    def _successResponse(self, data):
        return simplejson.dumps(data)

    def _errorResponse(self, error):
        reason = str(error.value)
        tb = traceback.format_exc(traceback.extract_tb(error.tb))
        return simplejson.dumps({"error":reason, "traceback":tb})

    def _immediateResponse(self, data, request):
        encoding = request.getHeader("accept-encoding")
        if encoding and "gzip" in encoding:
            zbuf = cStringIO.StringIO()
            zfile = gzip.GzipFile(None, 'wb', 9, zbuf)
            if isinstance(data, unicode):
                zfile.write(unicode(data).encode("utf-8"))
            elif isinstance(data, str):
                zfile.write(unicode(data, 'utf-8').encode("utf-8"))
            else:
                zfile.write(unicode(data).encode("utf-8"))
            zfile.close()
            request.setHeader("Content-encoding","gzip")
            request.write(zbuf.getvalue())
        else:
            request.write(data)
        request.finish()