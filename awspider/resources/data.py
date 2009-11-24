from twisted.web.resource import Resource
import simplejson
from twisted.internet.defer import DeferredList
from twisted.web import server
import traceback
from datetime import datetime
from twisted.python.failure import Failure
import cStringIO, gzip



class DataResource(Resource):
    
    isLeaf = True
    
    def __init__(self, spider):
        
        self.spider = spider
        Resource.__init__(self)
    
    def render(self, request):
        
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        
        if len(request.postpath) > 0:
            if request.postpath[0] == "server":
                return simplejson.dumps(self.spider.getServerData())
            elif request.postpath[0] == "exposed_function_details":
                return simplejson.dumps(self.spider.getExposedFunctionDetails())
            elif request.postpath[0] == "get":
                if "uuid" in request.args:
                    get_deferreds = []
                    for uuid in request.args["uuid"]:
                        get_deferreds.append(self.spider.get(uuid))
                    d = DeferredList(get_deferreds, consumeErrors = True)
                    d.addCallback(self._getCallback, request.args["uuid"])
                    d.addCallback(self._successResponse)
                    d.addErrback(self._errorResponse)
                    d.addCallback(self._immediateResponse, request)
                    return server.NOT_DONE_YET
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required.")))
            elif request.postpath[0] == "show_reservation":
                if "uuid" in request.args:
                    d = self.spider.showReservation(request.args["uuid"][0])
                    d.addCallback(self._successResponse)
                    d.addErrback(self._errorResponse)
                    d.addCallback(self._immediateResponse, request)
                    return server.NOT_DONE_YET
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required.")))
            elif request.postpath[0] == "delete_reservation":
                if "uuid" in request.args:
                    d = self.spider.deleteReservation(request.args["uuid"][0])
                    d.addCallback(self._successResponse)
                    d.addErrback(self._errorResponse)
                    d.addCallback(self._immediateResponse, request)
                    return server.NOT_DONE_YET
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required.")))
            elif request.postpath[0] == "delete_function_reservations": 
                if "function_name" in request.args:
                    d = self.spider.deleteFunctionReservations(request.args["function_name"][0])
                    d.addCallback(self._successResponse)
                    d.addErrback(self._errorResponse)
                    d.addCallback(self._immediateResponse, request)
                    return server.NOT_DONE_YET           
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter function_name is required."))) 
            elif request.postpath[0] == "execute_reservation":
                if "uuid" in request.args:
                    d = self.spider.queryByUUID(request.args["uuid"][0])
                    d.addCallback(self._successResponse)
                    d.addErrback(self._errorResponse)
                    d.addCallback(self._immediateResponse, request)
                    return server.NOT_DONE_YET
                else:
                    return self._errorResponse(Failure(exc_value=Exception("Parameter UUID is required.")))                                                            
    def _getCallback(self, data, uuids):
        response = {}
        for i in range(0, len(uuids)):
            if data[i][0] == True:
                response[ uuids[i] ] = data[i][1]
            else:
                response[ uuids[i] ] = {"error":str(data[i][1].value)}
                
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