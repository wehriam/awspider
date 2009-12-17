import os
import hashlib
from twisted.internet.defer import DeferredList, maybeDeferred
from twisted.trial import unittest
import yaml
from awspider.servers import InterfaceServer
from awspider.aws import AmazonS3, AmazonSDB

class InterfaceServerStartTestCase(unittest.TestCase):
    
    def setUp(self):
        config_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__), "config.yaml"))
        if not os.path.isfile(config_path):
            self.raiseConfigException(config_path)
        config = yaml.load(open(config_path, 'r').read())
        if not "aws_access_key_id" in config or "aws_secret_access_key" not in config:
            self.raiseConfigException(config_path)
        self.uuid = hashlib.sha256("%s%s%s" % (
            config["aws_access_key_id"],
            config["aws_secret_access_key"], 
            self.__class__.__name__)).hexdigest()
        self.aws_access_key_id = config["aws_access_key_id"]
        self.aws_secret_access_key = config["aws_secret_access_key"]
        self.aws_s3_http_cache_bucket = "%s_http_cache" % self.uuid
        self.aws_s3_storage_bucket = "%s_storage" % self.uuid
        self.aws_sdb_reservation_domain = "%s_reservation" % self.uuid
        self.aws_sdb_coordination_domain = "%s_coordination" % self.uuid
        self.interfaceserver = InterfaceServer( 
            aws_access_key_id = self.aws_access_key_id, 
            aws_secret_access_key = self.aws_secret_access_key,
            aws_s3_http_cache_bucket = self.aws_s3_http_cache_bucket,
            aws_s3_storage_bucket = self.aws_s3_storage_bucket, 
            aws_sdb_reservation_domain = self.aws_sdb_reservation_domain, 
            aws_sdb_coordination_domain = self.aws_sdb_coordination_domain)
    
    def tearDown(self):
        s3 = AmazonS3(self.aws_access_key_id, self.aws_secret_access_key)
        sdb = AmazonSDB(self.aws_access_key_id, self.aws_secret_access_key)
        deferreds = []        
        deferreds.append(s3.deleteBucket(self.aws_s3_http_cache_bucket)) 
        deferreds.append(s3.deleteBucket(self.aws_s3_storage_bucket)) 
        deferreds.append(sdb.deleteDomain(self.aws_sdb_reservation_domain)) 
        deferreds.append(sdb.deleteDomain(self.aws_sdb_coordination_domain))        
        d = DeferredList(deferreds)
        return d
        
    def testStart(self):
        d = self.interfaceserver.start()
        d.addCallback(self._startCallback)
        return d 
    
    def _startCallback(self, data):
        d = self.interfaceserver.shutdown()
        return d

class InterfaceTestCase(unittest.TestCase):

    def setUp(self):
        config_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__), "config.yaml"))
        if not os.path.isfile(config_path):
            self.raiseConfigException(config_path)
        config = yaml.load(open(config_path, 'r').read())
        if not "aws_access_key_id" in config or "aws_secret_access_key" not in config:
            self.raiseConfigException(config_path)
        self.uuid = hashlib.sha256("%s%s%s" % (
            config["aws_access_key_id"],
            config["aws_secret_access_key"], 
            self.__class__.__name__)).hexdigest()
        self.aws_access_key_id = config["aws_access_key_id"]
        self.aws_secret_access_key = config["aws_secret_access_key"]
        self.aws_s3_http_cache_bucket = "%s_http_cache" % self.uuid
        self.aws_s3_storage_bucket = "%s_storage" % self.uuid
        self.aws_sdb_reservation_domain = "%s_reservation" % self.uuid
        self.aws_sdb_coordination_domain = "%s_coordination" % self.uuid
        self.interfaceserver = InterfaceServer( 
            aws_access_key_id = self.aws_access_key_id, 
            aws_secret_access_key = self.aws_secret_access_key,
            aws_s3_http_cache_bucket = self.aws_s3_http_cache_bucket,
            aws_s3_storage_bucket = self.aws_s3_storage_bucket, 
            aws_sdb_reservation_domain = self.aws_sdb_reservation_domain, 
            aws_sdb_coordination_domain = self.aws_sdb_coordination_domain)
        return self.interfaceserver.start()
    
    def tearDown(self):
        deferreds = []  
        deferreds.append(self.interfaceserver.shutdown())
        d = DeferredList(deferreds)
        d.addCallback(self._tearDownCallback)
        return d
        
    def _tearDownCallback(self, data):
        s3 = AmazonS3(self.aws_access_key_id, self.aws_secret_access_key)
        sdb = AmazonSDB(self.aws_access_key_id, self.aws_secret_access_key)
        deferreds = []        
        deferreds.append(s3.deleteBucket(self.aws_s3_http_cache_bucket)) 
        deferreds.append(s3.deleteBucket(self.aws_s3_storage_bucket)) 
        deferreds.append(sdb.deleteDomain(self.aws_sdb_reservation_domain)) 
        deferreds.append(sdb.deleteDomain(self.aws_sdb_coordination_domain))       
        d = DeferredList(deferreds)
        return d

