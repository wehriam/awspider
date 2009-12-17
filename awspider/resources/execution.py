from twisted.web import server
import simplejson
from twisted.internet import reactor
from ..evaluateboolean import evaluateBoolean
import urlparse
from .base import BaseResource


class ExecutionResource(BaseResource):
    
    def __init__(self, executionserver):    
        self.executionserver = executionserver
        BaseResource.__init__(self)
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        if len(request.postpath) > 0:
            if request.postpath[0] == "coordinate":
                reactor.callLater(5, self.executionserver.coordinate)
                return simplejson.dumps(True)  
            elif request.postpath[0] == "server":
                return simplejson.dumps(self.executionserver.getServerData())
            elif request.postpath[0] == "getpage" and "url" in request.args:
                #print "Recieving peering request for %s" % request.args["url"][0]
                kwargs = {}
                if "method" in request.args:
                    kwargs["method"] = request.args["method"][0]     
                if "postdata" in request.args: 
                    kwargs["postdata"] = urlparse.parse_qs(request.args["postdata"][0])
                if "headers" in request.args: 
                    kwargs["headers"] = urlparse.parse_qs(request.args["headers"][0])  
                if "cookies" in request.args: 
                    kwargs["cookies"] = urlparse.parse_qs(request.args["cookies"][0])           
                if "agent" in request.args:
                    kwargs["agent"] = request.args["agent"][0]
                if "timeout" in request.args:
                    kwargs["timeout"] = int(request.args["timeout"][0])
                if "followRedirect" in request.args:
                    kwargs["followRedirect"] = evaluateBoolean(request.args["followRedirect"][0])
                if "url_hash" in request.args: 
                    kwargs["url_hash"] = request.args["url_hash"][0] 
                if "cache" in request.args: 
                    kwargs["cache"] = int(request.args["cache"][0])
                if "prioritize" in request.args: 
                    kwargs["prioritize"] = evaluateBoolean(request.args["prioritize"][0])
                d = self.executionserver.pg.getPage(request.args["url"][0], **kwargs)              
                d.addCallback(self._getpageCallback, request)         
                d.addErrback(self._errorResponse) 
                d.addCallback(self._immediateResponse, request)    
                return server.NOT_DONE_YET
        message = "No such resource."
        request.setResponseCode(404, message)
        self._immediateResponse(simplejson.dumps({"error":message}), request)
    
    def _getpageCallback(self, data, request):
        request.setResponseCode(data["status"], data["message"])
        for header in data["headers"]:
            request.setHeader(header, data["headers"][header][0])
        return self._immediateResponse(data["response"], request)