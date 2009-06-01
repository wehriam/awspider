from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import Deferred

import os
import sys
sys.path.append(os.path.join( os.path.dirname(__file__), "lib"))

import twisted
twisted.internet.base.DelayedCall.debug = True

class SimpleTestCase(unittest.TestCase):
    
    deferred = Deferred()
    
    def setUp(self):
        pass
        
    def tearDown(self):
        pass

    def succeeded(self):
        self.deferred.callback( True )

    def failed(self, e ):
        self.deferred.errback( e )

    def testOneThing(self):
        self.timeout = reactor.callLater(5, self.succeeded )
        return self.deferred 
    
    def testSomeOtherThing(self):
        self.timeout = reactor.callLater(5, self.failed, Exception("This other thing didn't work.") )
        return self.deferred