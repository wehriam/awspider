import urllib
import time
from twisted.internet.defer import Deferred
from twisted.internet import reactor
from twisted.web.client import HTTPClientFactory, _parse
import dateutil.parser
from .unicodeconverter import convertToUTF8


class RequestQueuer(object):

    def __init__(self, max_simultaneous_requests=50,
                 max_requests_per_host_per_second=1,
                 max_simultaneous_requests_per_host=5):
                 
        self.max_simul_reqs = int(max_simultaneous_requests)
        max_req_per_host_per_sec = float(max_requests_per_host_per_second)
        self.min_req_interval_per_host = 1.0 / max_req_per_host_per_sec
        self.max_simul_reqs_per_host = int(max_simultaneous_requests_per_host)
        self.pending_reqs = {}
        self.pending_last_req = {}
        self.active_reqs = {}
        self.min_req_interval_per_hosts = {}
        self.max_simul_reqs_per_hosts = {}

    def getPending(self):
        return sum([len(x) for x in self.pending_reqs.values()])

    pending = property(getPending)

    def getActive(self):
        return sum(self.active_reqs.values())

    active = property(getActive)

    def setHostMaxRequestsPerSecond(self, host, max_reqs_per_sec):
        if max_reqs_per_sec == 0:
            self.min_req_interval_per_hosts[host] = 0
        else:
            min_req_interval = 1.0 / float(max_reqs_per_sec)
            self.min_req_interval_per_hosts[host] = min_req_interval

    def setHostMaxSimultaneousRequests(self, host, max_simul_reqs):
        if max_simul_reqs == 0:
            self.max_simul_reqs_per_hosts[host] = self.max_simul_reqs
        else:
            self.max_simul_reqs_per_hosts[host] = max_simul_reqs

    def checkActive(self):
        while self.active < self.max_simul_reqs and self.pending > 0:
            in_loop_req_count = 0
            for host in self.pending_reqs:
                if len(self.pending_reqs[host]) > 0 and \
                time.time() - self.pending_last_req.get(host, 0) > self.min_req_interval_per_hosts.get(host, self.min_req_interval_per_host) and \
                self.active_reqs.get(host, 0) < self.max_simul_reqs_per_hosts.get(host, self.max_simul_reqs_per_host):
                    in_loop_req_count += 1
                    req = self.pending_reqs[host].pop(0)
                    d = self._getPage(req)
                    d.addCallback(self.reqComplete, req["deferred"], host)
                    d.addErrback(self.reqError, req["deferred"], host)
                    self.pending_last_req[host] = time.time()
                    self.active_reqs[host] = self.active_reqs.get(host, 0) + 1
            if in_loop_req_count == 0:
                reactor.callLater(.1, self.checkActive)
                return

    def reqComplete(self, response, deferred, host):
        self.active_reqs[host] -= 1
        if host in self.pending_reqs and len(self.pending_reqs[host]) == 0:
            del self.pending_reqs[host]
        self.checkActive()
        deferred.callback(response)
        return None

    def reqError(self, error, deferred, host):
        self.active_reqs[host] -= 1
        if host in self.pending_reqs and len(self.pending_reqs[host]) == 0:
            del self.pending_reqs[host]
        self.checkActive()
        deferred.errback(error)
        return None

    def getPage(self, url, last_modified=None, etag=None, method='GET', postdata=None, headers=None, agent="AWSpider", timeout=60, cookies=None, follow_redirect=1, prioritize=False):
        if isinstance(postdata, dict):
            for key in postdata:
                postdata[key] = convertToUTF8(postdata[key])
            postdata = urllib.urlencode(postdata)
        if headers is None:
            headers = {}
        if method.lower() == "post":
            headers["content-type"] = "application/x-www-form-urlencoded"
        if last_modified is not None:
            headers['If-Modified-Since'] = time.strftime("%a, %d %b %Y %T %z", dateutil.parser.parse(last_modified).timetuple())
        if etag is not None:
            headers["If-None-Match"] = etag
        req = {
            "url":convertToUTF8(url),
            "method":method,
            "postdata":postdata,
            "headers":headers,
            "agent":agent,
            "timeout":timeout,
            "cookies":cookies,
            "follow_redirect":follow_redirect,
            "deferred":Deferred()
        }
        host = _parse(req["url"])[1]
        if host not in self.pending_reqs:
            self.pending_reqs[host] = []
        if prioritize:
            self.pending_reqs[host].insert(0, req)
        else:
            self.pending_reqs[host].append(req)
        self.checkActive()
        return req["deferred"]

    def _getPage(self, req): 
        scheme, host, port = _parse(req['url'])[0:3]
        factory = HTTPClientFactory(
            req['url'],
            method=req['method'],
            postdata=req['postdata'],
            headers=req['headers'],
            agent=req['agent'],
            timeout=req['timeout'],
            cookies=req['cookies'],
            followRedirect=req['follow_redirect']
        )
        if scheme == 'https':
            from twisted.internet import ssl
            context_factory = ssl.ClientContextFactory()
            reactor.connectSSL(host, port, factory, context_factory, timeout=req['timeout'])
        else:
            reactor.connectTCP(host, port, factory, timeout=req['timeout'])
        factory.deferred.addCallback(self._getPageComplete, factory)
        factory.deferred.addErrback(self._getPageError, factory)
        return factory.deferred

    def _getPageComplete(self, response, factory):
        return {"response":response, "headers":factory.response_headers, "status":int(factory.status), "message":factory.message}

    def _getPageError(self, error, factory):
        if "response_headers" in factory.__dict__ and factory.response_headers is not None:
            error.value.headers = factory.response_headers
        return error