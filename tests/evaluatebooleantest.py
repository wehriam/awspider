from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import Deferred

import os
import sys
sys.path.append(os.path.join( os.path.dirname(__file__), "lib"))

import twisted
twisted.internet.base.DelayedCall.debug = True

from awspider.evaluateboolean import evaluateBoolean

class EvaluateBooleanTestCase(unittest.TestCase):
    
    def testEvaluateBoolean( self ):
        self.failUnlessEqual( evaluateBoolean("TRUE"), True )
        self.failUnlessEqual( evaluateBoolean("true"), True )
        self.failUnlessEqual( evaluateBoolean("yes"), True )
        self.failUnlessEqual( evaluateBoolean(u"YES"), True )
        self.failUnlessEqual( evaluateBoolean(u"1"), True )
        self.failUnlessEqual( evaluateBoolean(True), True )
        self.failUnlessEqual( evaluateBoolean(1), True )
        self.failUnlessEqual( evaluateBoolean("FALSE"), False )
        self.failUnlessEqual( evaluateBoolean("false"), False )
        self.failUnlessEqual( evaluateBoolean("no"), False )
        self.failUnlessEqual( evaluateBoolean(u"NO"), False )
        self.failUnlessEqual( evaluateBoolean(u"0"), False )
        self.failUnlessEqual( evaluateBoolean(False), False )
        self.failUnlessEqual( evaluateBoolean(0), False )
        