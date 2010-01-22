from twisted.trial import unittest
from twisted.internet.defer import Deferred

from awspider.requestqueuer import RequestQueuer

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

from miniwebserver import MiniWebServer

import twisted
twisted.internet.base.DelayedCall.debug = True

class RequestQueuerTestCase(unittest.TestCase):
    
    def setUp(self):
        self.deferred = Deferred()
        self.mini_web_server = MiniWebServer()
        self.rq = RequestQueuer(max_requests_per_host_per_second=3, max_simultaneous_requests_per_host=5)
        
    def tearDown(self):
        return self.mini_web_server.shutdown()

    def testRequestQueuerOnSuccess(self):  
        d = self.rq.getPage("http://127.0.0.1:8080/helloworld", timeout=5)
        return d

    def testRequestQueuerOnFailure(self): 
        d = self.rq.getPage("http://0.0.0.0:99", timeout=5)
        d.addErrback(self._getPageErrback)  
        return d      
    
    def testHostMaxRequestsPerSecond(self,):
        self.failUnlessEqual(
            self.rq.getHostMaxRequestsPerSecond("example.com"), 3)
        self.rq.setHostMaxRequestsPerSecond("example2.com", 7)
        self.failUnlessEqual(
            self.rq.getHostMaxRequestsPerSecond("example2.com"), 7)
            
    def testHostMaxSimultaneousRequests(self,):
        self.failUnlessEqual(
            self.rq.getHostMaxSimultaneousRequests("example.com"), 5)
        self.rq.setHostMaxSimultaneousRequests("example2.com", 11)
        self.failUnlessEqual(
            self.rq.getHostMaxSimultaneousRequests("example2.com"),
            11)
            
    def testActive(self):
        self.failUnlessEqual(isinstance(self.rq.getActive(), int), True)
            
    def testPending(self):
        self.failUnlessEqual(isinstance(self.rq.getPending(), int), True)

    def testActiveRequestsByHost(self):
        self.failUnlessEqual(isinstance(self.rq.getActiveRequestsByHost(), dict), True)

    def testPendingRequestsByHost(self):
        self.failUnlessEqual(isinstance(self.rq.getPendingRequestsByHost(), dict), True)

    def _getPageErrback(self, error):
        return True
        
