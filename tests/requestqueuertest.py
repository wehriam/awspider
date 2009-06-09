from twisted.trial import unittest
from twisted.internet.defer import Deferred

from awspider.pagegetter import RequestQueuer

import os
import sys
sys.path.append(os.path.join( os.path.dirname(__file__), "lib"))

from miniwebserver import MiniWebServer

import twisted
twisted.internet.base.DelayedCall.debug = True

class RequestQueuerTestCase(unittest.TestCase):
    
    def setUp(self):
        self.deferred = Deferred()
        self.mini_web_server = MiniWebServer()
        self.rq = RequestQueuer()
        
    def tearDown(self):
        return self.mini_web_server.shutdown()

    def testRequestQueuerOnSuccess(self):  
        d = self.rq.getPage("http://127.0.0.1:8080", timeout=5)
        return d

    def testRequestQueuerOnFailure(self): 
        d = self.rq.getPage("http://0.0.0.0:99", timeout=5)
        d.addErrback( self._getPageErrback )  
        return d      
    
    def testActive(self):
        self.failUnlessEqual( isinstance(self.rq.active, int), True )
            
    def testPending(self):
        self.failUnlessEqual( isinstance(self.rq.pending, int), True )
        
    def _getPageErrback( self, error ):
        return True
        
