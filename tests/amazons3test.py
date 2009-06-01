import hashlib
import os
import sys
sys.path.append(os.path.join( os.path.dirname(__file__), "lib") )


from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import Deferred
import twisted
twisted.internet.base.DelayedCall.debug = True

import yaml

from awspider.aws import AmazonS3

class AmazonS3TestCase(unittest.TestCase):
    
    def setUp(self):
        
        config_path = os.path.abspath( os.path.join( os.path.dirname(__file__), "../../test/config.yaml" ) )
        
        if not os.path.isfile( config_path ):
            self.raiseConfigException( config_path )
            
        config = yaml.load( open( config_path, 'r').read() )
        
        if not "aws_access_key_id" in config or "aws_secret_access_key" not in config:
            self.raiseConfigException( config_path )
        
        self.s3 = AmazonS3( config["aws_access_key_id"], config["aws_secret_access_key"])
        
        self.uuid = hashlib.sha256( config["aws_access_key_id"] + config["aws_secret_access_key"] ).hexdigest()
        
    def raiseConfigException( self, filename ):
        raise Exception("Please create a YAML config file at %s with 'aws_access_key_id' and 'aws_secret_access_key'." % filename )
    
    def tearDown(self):
        pass

    def test_1_PutBucket(self):
        d = self.s3.putBucket( self.uuid )
        return d
    
    def test_2_GetBucket(self):
        d = self.s3.getBucket( self.uuid )
        return d
    
    def test_3_PutObject(self):
        d = self.s3.putObject( self.uuid, "test", "This is a test object.")
        return d

    def test_3a_PutObject_Unicode(self):
        d = self.s3.putObject( self.uuid, "test", u"This is a test unicode object.")
        return d

    def test_4_HeadObject(self):
        d = self.s3.headObject( self.uuid, "test")
        return d
    
    def test_5_GetObject(self):
        d = self.s3.getObject( self.uuid, "test")
        return d    

    def test_6_DeleteObject(self):
        d = self.s3.deleteObject( self.uuid, "test")
        return d
    
    def test_7_DeleteBucket(self):
        d = self.s3.deleteBucket( self.uuid )
        return d
        