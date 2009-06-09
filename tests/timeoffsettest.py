from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import Deferred

import os
import sys
sys.path.append(os.path.join( os.path.dirname(__file__), "lib"))

import twisted
twisted.internet.base.DelayedCall.debug = True

from awspider.timeoffset import getTimeOffset

import re

class TimeOffsetTestCase(unittest.TestCase):
    
    def testGetTimeOffset( self ):
        d = getTimeOffset()
        d.addCallback( self._getTimeOffsetCallback )
        return d
        
    def _getTimeOffsetCallback( self, offset ):
        self.failUnlessEqual( isinstance(offset, float), True )
        self.failUnlessEqual( offset != 0, True )