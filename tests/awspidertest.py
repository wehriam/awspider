from twisted.trial import unittest
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet import reactor
from random import random
import os
import sys
sys.path.append(os.path.join( os.path.dirname(__file__), "lib"))

from miniwebserver import MiniWebServer

from awspider import AWSpider
from awspider.aws import AmazonS3, AmazonSDB

import yaml
import hashlib

from awspider.pagegetter import PageGetter

#import twisted
#twisted.internet.base.DelayedCall.debug = True

#class AWSpiderStartTestCase(unittest.TestCase):
#    
#    def setUp(self):
#        
#        config_path = os.path.abspath( os.path.join( os.path.dirname(__file__), "config.yaml" ) )
#        
#        if not os.path.isfile( config_path ):
#            self.raiseConfigException( config_path )
#            
#        config = yaml.load( open( config_path, 'r').read() )
#        
#        if not "aws_access_key_id" in config or "aws_secret_access_key" not in config:
#            self.raiseConfigException( config_path )
#            
#        self.uuid = hashlib.sha256( config["aws_access_key_id"] + config["aws_secret_access_key"] + self.__class__.__name__ ).hexdigest()
#        
#        self.aws_access_key_id = config["aws_access_key_id"]
#        self.aws_secret_access_key = config["aws_secret_access_key"]
#        self.aws_s3_cache_bucket = "%s_cache" % self.uuid
#        self.aws_s3_storage_bucket = "%s_storage" % self.uuid
#        self.aws_sdb_reservation_domain = "%s_reservation" % self.uuid
#        self.aws_sdb_coordination_domain = "%s_coordination" % self.uuid
#        
#        self.spider = AWSpider( 
#            aws_access_key_id = self.aws_access_key_id, 
#            aws_secret_access_key = self.aws_secret_access_key,
#            aws_s3_cache_bucket = self.aws_s3_cache_bucket, 
#            aws_s3_storage_bucket = self.aws_s3_storage_bucket, 
#            aws_sdb_reservation_domain = self.aws_sdb_reservation_domain, 
#            aws_sdb_coordination_domain = self.aws_sdb_coordination_domain, 
#            port = 5000
#        )
#    
#    def tearDown(self):
#        
#        self.s3 = AmazonS3(self.aws_access_key_id, self.aws_secret_access_key)
#        self.sdb = AmazonSDB(self.aws_access_key_id, self.aws_secret_access_key)
#        
#        deferreds = []        
#        deferreds.append(self.spider.pg.clearCache())
#        deferreds.append(self.spider.clearStorage())
#                
#        deferreds.append(self.sdb.deleteDomain(self.aws_sdb_reservation_domain))
#        deferreds.append(self.sdb.deleteDomain(self.aws_sdb_coordination_domain))
#        
#        d = DeferredList(deferreds)
#        d.addCallback( self._tearDownCallback )
#        return d
#
#    def _tearDownCallback( self, data ):
#        deferreds = []        
#        deferreds.append(self.s3.deleteBucket(self.aws_s3_cache_bucket))
#        deferreds.append(self.s3.deleteBucket(self.aws_s3_storage_bucket))        
#        d = DeferredList(deferreds)
#        return d
#        
#    def testStart( self ):
#        d = self.spider.start()
#        d.addCallback( self._startCallback )
#        return d 
#        
#    def _startCallback( self, data ):
#        d = self.spider.shutdown()
#        return d        

def foo():
    return True
    
class AWSpiderTestCase(unittest.TestCase):
    def setUp(self):
        
        self.mini_web_server = MiniWebServer()
        
        config_path = os.path.abspath( os.path.join( os.path.dirname(__file__), "config.yaml" ) )
        
        if not os.path.isfile( config_path ):
            self.raiseConfigException( config_path )
            
        config = yaml.load( open( config_path, 'r').read() )
        
        if not "aws_access_key_id" in config or "aws_secret_access_key" not in config:
            self.raiseConfigException( config_path )
            
        self.uuid = hashlib.sha256( config["aws_access_key_id"] + config["aws_secret_access_key"] + self.__class__.__name__ ).hexdigest()
        
        self.aws_access_key_id = config["aws_access_key_id"]
        self.aws_secret_access_key = config["aws_secret_access_key"]
        self.aws_s3_cache_bucket = "%s_cache" % self.uuid
        self.aws_s3_storage_bucket = "%s_storage" % self.uuid
        self.aws_sdb_reservation_domain = "%s_reservation" % self.uuid
        self.aws_sdb_coordination_domain = "%s_coordination" % self.uuid
        
        self.spider = AWSpider( 
            aws_access_key_id = self.aws_access_key_id, 
            aws_secret_access_key = self.aws_secret_access_key,
            aws_s3_cache_bucket = self.aws_s3_cache_bucket, 
            aws_s3_storage_bucket = self.aws_s3_storage_bucket, 
            aws_sdb_reservation_domain = self.aws_sdb_reservation_domain, 
            aws_sdb_coordination_domain = self.aws_sdb_coordination_domain, 
            port = 5000 )
        
        self.s3 = AmazonS3( config["aws_access_key_id"], config["aws_secret_access_key"])

        return self.spider.start()
        
    def tearDown(self):

        deferreds = []        
        deferreds.append(self.spider.shutdown())
        deferreds.append(self.mini_web_server.shutdown())
        d = DeferredList(deferreds)
        d.addCallback(self._tearDownCallback)
        return d 
    
    def _tearDownCallback(self, data):
        
        self.s3 = AmazonS3(self.aws_access_key_id, self.aws_secret_access_key)
        self.sdb = AmazonSDB(self.aws_access_key_id, self.aws_secret_access_key)
               
        deferreds = []        
        deferreds.append(self.spider.pg.clearCache())
        deferreds.append(self.spider.clearStorage())
                
        deferreds.append(self.sdb.deleteDomain(self.aws_sdb_reservation_domain))
        deferreds.append(self.sdb.deleteDomain(self.aws_sdb_coordination_domain))
        
        d = DeferredList(deferreds)
        d.addCallback( self._tearDownCallback2 )
        return d

    def _tearDownCallback2( self, data ):

        deferreds = []        
        deferreds.append(self.s3.deleteBucket(self.aws_s3_cache_bucket))
        deferreds.append(self.s3.deleteBucket(self.aws_s3_storage_bucket))        
        d = DeferredList(deferreds)
        return d
    
    def testPageGetter(self):  
        d = self.spider.getPage("http://127.0.0.1:8080", timeout=5)
        return d

    def testClearStorage(self):
        d = self.spider.clearStorage()
        return d
    
    def testGetServerData(self):
        server_data = self.spider.getServerData()
        self.failUnlessEqual( isinstance(server_data, dict), True )
                
    def testMakeCallable(self):  
        self.spider.makeCallable( foo )

