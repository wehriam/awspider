from twisted.internet.defer import Deferred, DeferredList
from twisted.trial import unittest
from awspider import AWSpider
from awspider.aws import AmazonS3, AmazonSDB
import yaml
import hashlib
import os
import glob
import urllib
import simplejson

import chardet

import difflib

def load_data( filename ):
    s = open( filename ).read()
    try:
        encoding = chardet.detect( s )['encoding']
        s = unicode(s, encoding )
        return s
    except:
        pass
        
    try:
        s = unicode(s)
        return s
    except:
        pass
    
    return u"Doh"
    
class EncodingTestCase(unittest.TestCase):
    def setUp(self):
        
        self.filenames = glob.glob( os.path.join(os.path.dirname(__file__), 'data', '**', '*.xml') )
        
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
        #self.aws_sdb_coordination_domain = "%s_coordination" % self.uuid

        self.spider = AWSpider( 
            aws_access_key_id = self.aws_access_key_id, 
            aws_secret_access_key = self.aws_secret_access_key,
            aws_s3_cache_bucket = self.aws_s3_cache_bucket, 
            aws_s3_storage_bucket = self.aws_s3_storage_bucket, 
            aws_sdb_reservation_domain = self.aws_sdb_reservation_domain, 
            #aws_sdb_coordination_domain = self.aws_sdb_coordination_domain, 
            port = 5000,
            log_level="debug" )

        self.s3 = AmazonS3( config["aws_access_key_id"], config["aws_secret_access_key"])
        self.spider.expose( load_data )
        self.spider.expose( load_data, interval=60*60*24, name="load_data_stored" )
        return self.spider.start()

    def tearDown(self):

        deferreds = []        
        deferreds.append(self.spider.shutdown())
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
        #deferreds.append(self.sdb.deleteDomain(self.aws_sdb_coordination_domain))

        d = DeferredList(deferreds)
        d.addCallback( self._tearDownCallback2 )
        return d

    def _tearDownCallback2( self, data ):

        deferreds = []        
        deferreds.append(self.s3.deleteBucket(self.aws_s3_cache_bucket))
        deferreds.append(self.s3.deleteBucket(self.aws_s3_storage_bucket))        
        d = DeferredList(deferreds)
        return d
    
#    def testImmediateReturn(self):
#        deferreds = []
#        for filename in self.filenames[0:5]:
#            d = self.spider.rq.getPage("http://127.0.0.1:5000/function/load_data", method="POST", postdata={"filename":filename})
#            d.addCallback( self._processRequestCallback, filename )
#            deferreds.append(d)
#        d = DeferredList(deferreds, consumeErrors=True )
#        d.addCallback(self._testImmediateReturnCallback)
#        d.addErrback( self._testImmediateReturnErrback )
#        return d
#
#    def _processRequestCallback(self, data, filename):
#        processed_data = simplejson.loads( data["response"] )
#        original_data = load_data( filename )
#        if processed_data == original_data:
#            #print "%s passed comparison test." % filename
#            return True
#        print "%s failed comparison test." % filename
#        return False
#
#    def _testImmediateReturnCallback(self, data):
#        for row in data:
#            if row[0] == False:
#                raise row[1]
#            else:
#                self.failUnlessEqual( row[1], True )
#
#    def _testImmediateReturnErrback(self, error):
#        return error
    
    def testStoredReturn(self):
        deferreds = []
        for filename in self.filenames:
            d = self.spider.rq.getPage("http://127.0.0.1:5000/function/load_data_stored", method="POST", postdata={"filename":filename})
            d.addCallback( self._processStoredRequestCallback, filename )
            deferreds.append(d)
        d = DeferredList(deferreds, consumeErrors=True )
        d.addCallback(self._testStoredReturnCallback)
        d.addErrback( self._testStoredReturnErrback )
        return d        
    
    def _processStoredRequestCallback(self, data, filename):
        processed_data = simplejson.loads( data["response"] )
        uuid = processed_data.keys()[0]
        d = self.spider.rq.getPage("http://127.0.0.1:5000/data/get?uuid=%s" % uuid)
        d.addCallback( self._processStoredRequestCallback2, filename )
        return d
        
    def _processStoredRequestCallback2(self, data, filename):
        processed_data = simplejson.loads( data["response"] )
        original_data = load_data( filename )
        if processed_data == original_data:
            #print "%s passed comparison test." % filename
            return True
        print "%s failed comparison test." % filename
        return False

    def _testStoredReturnCallback(self, data):
        for row in data:
            if row[0] == False:
                raise row[1]
            else:
                self.failUnlessEqual( row[1], True )
                
    def _testStoredReturnErrback(self, error):
        return error        