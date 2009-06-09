from twisted.trial import unittest
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet import reactor

from awspider.pagegetter import PageGetter

import os
import sys
sys.path.append(os.path.join( os.path.dirname(__file__), "lib"))

from miniwebserver import MiniWebServer

from awspider.aws import AmazonS3

import yaml
import hashlib

import twisted
twisted.internet.base.DelayedCall.debug = True

class PageGetterTestCase(unittest.TestCase):
    
    def setUp(self):
        self.mini_web_server = MiniWebServer()
        
        config_path = os.path.abspath( os.path.join( os.path.dirname(__file__), "config.yaml" ) )
        
        if not os.path.isfile( config_path ):
            self.raiseConfigException( config_path )
            
        config = yaml.load( open( config_path, 'r').read() )
        
        if not "aws_access_key_id" in config or "aws_secret_access_key" not in config:
            self.raiseConfigException( config_path )
        
        self.s3 = AmazonS3( config["aws_access_key_id"], config["aws_secret_access_key"])
        
        self.uuid = hashlib.sha256( config["aws_access_key_id"] + config["aws_secret_access_key"] + self.__class__.__name__ ).hexdigest()
        
        d = self.s3.putBucket( self.uuid )

        self.pg = PageGetter( self.s3, self.uuid )
        return d
        
    def tearDown(self):
        a = self.mini_web_server.shutdown()
        b = self.pg.clearCache()
        d = DeferredList([a,b])
        d.addCallback( self._tearDownCallback )
        return d
        
    def _tearDownCallback( self, data ):
        d = self.s3.deleteBucket( self.uuid )
        return d

    def testPageGetterOnSuccess(self):  
        d = self.pg.getPage("http://127.0.0.1:8080", timeout=5)
        return d
    
    def testPageGetterOnSuccessB(self):  
        d = self.pg.getPage("http://127.0.0.1:8080", timeout=5)
        return d
    
    def testPageGetterOnFailure(self): 
        d = self.pg.getPage("http://0.0.0.0:99", timeout=5)
        d.addErrback( self._getPageErrback )  
        return d 
    
    def _getPageErrback( self, error ):
        return True

    def testClearCache(self):
        d = self.pg.clearCache()
        return d

        
