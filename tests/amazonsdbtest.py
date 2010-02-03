import hashlib
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
from uuid import uuid4
import time
from twisted.internet.task import deferLater
from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
import twisted
twisted.internet.base.DelayedCall.debug = True

import yaml

from awspider.aws import AmazonSDB

class AmazonSDBTestCase(unittest.TestCase):
    
    def setUp(self):
        
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "config.yaml"))
        
        if not os.path.isfile(config_path):
            self.raiseConfigException(config_path)
            
        config = yaml.load(open(config_path, 'r').read())
        
        if not "aws_access_key_id" in config or "aws_secret_access_key" not in config:
            self.raiseConfigException(config_path)
        
        self.sdb = AmazonSDB(config["aws_access_key_id"], config["aws_secret_access_key"])
        
        self.uuid = hashlib.sha256(config["aws_access_key_id"] + config["aws_secret_access_key"] + self.__class__.__name__).hexdigest()
        
    def raiseConfigException(self, filename):
        raise Exception("Please create a YAML config file at %s with 'aws_access_key_id' and 'aws_secret_access_key'." % filename)
    
    def tearDown(self):
        pass

    def test_01_CreateDomain(self):
        d = self.sdb.createDomain(self.uuid)
        return d
        
    def test_02_ListDomains(self):
        d = self.sdb.listDomains()
        return d

    def test_03_DomainMetadata(self):
        d = self.sdb.domainMetadata(self.uuid)
        return d
    
    def test_04_PutAttributes(self):
        d = self.sdb.putAttributes(self.uuid, "test", {"a":[1,3], "b":2})
        return d
    
    def test_04a_BatchPutAttributes(self):
        attributes_by_item_name = {
            "test_a":{"a":1},
            "test_b":{"b":2},
            "test_c":{"c":[3,4], "d":5}}
        d = self.sdb.batchPutAttributes(self.uuid, attributes_by_item_name)
        d.addCallback(self._batchPutAttributesCallback)
        return d

    def _batchPutAttributesCallback(self, data):
        d = deferLater(reactor, 10, self._batchPutAttributesCallback2)
        return d
    
    def _batchPutAttributesCallback2(self):
        deferreds = []
        deferreds.append(self.sdb.getAttributes(self.uuid, "test_a"))
        deferreds.append(self.sdb.getAttributes(self.uuid, "test_b"))
        deferreds.append(self.sdb.getAttributes(self.uuid, "test_c"))
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._batchPutAttributesCallback3)
        return d
    
    def _batchPutAttributesCallback3(self, data):
        for row in data:
            if row[0] == False:
                raise row[1]
        if "a" not in data[0][1]:
            raise Exception("GetAttributes 1 (a) failed on BatchPutAttributes")
        if "1" not in data[0][1]["a"]:
            raise Exception("GetAttributes 1 (b) failed on BatchPutAttributes")
        if "b" not in data[1][1]:
            raise Exception("GetAttributes 2 (a) failed on BatchPutAttributes")
        if "2" not in data[1][1]["b"]:
            raise Exception("GetAttributes 2 (b) failed on BatchPutAttributes") 
        if "c" not in data[2][1]:
            raise Exception("GetAttributes 3 (a) failed on BatchPutAttributes")
        if "4" not in data[2][1]["c"] or "4" not in data[2][1]["c"]:
            raise Exception("GetAttributes 3 (b) failed on BatchPutAttributes")   
        if "5" not in data[2][1]["d"]:
            raise Exception("GetAttributes 3 (c) failed on BatchPutAttributes") 
            
    def test_04b_BatchPutAttributesMass(self): 
        attributes_by_item_name = {} 
        for i in range(1,25):
            attributes = {}
            for j in range(0,10):
                attributes[uuid4().hex] = uuid4().hex
            attributes_by_item_name[uuid4().hex] = attributes
        attributes_by_item_name["test_d"] = {"e":1} 
        d = self.sdb.batchPutAttributes(self.uuid, attributes_by_item_name)
        d.addCallback(self._batchPutAttributesMassCallback)
        return d
    
    def _batchPutAttributesMassCallback(self, data):
        d = deferLater(reactor, 10, self._batchPutAttributesMassCallback2)
        return d
        
    def _batchPutAttributesMassCallback2(self):
        d = self.sdb.getAttributes(self.uuid, "test_d")
        d.addCallback(self._batchPutAttributesMassCallback3)
        return d
    
    def _batchPutAttributesMassCallback3(self, data):
        if "e" not in data:
            raise Exception("GetAttributes failed on BatchPutAttributesMass")   
        if "1" not in data["e"]:
            raise Exception("GetAttributes failed on BatchPutAttributesMass")   
                                 
    def test_05_GetAttributes(self):
        d = self.sdb.getAttributes(self.uuid, "test")
        return d  
    
    def test_05a_GetAttributes_Individual(self):
        d = self.sdb.getAttributes(self.uuid, "test", "a")
        return d 
 
    def test_06_Select(self):
        d = self.sdb.select("SELECT * FROM `%s` WHERE `a`='1'" % self.uuid)
        return d
    
    def test_08_DeleteAttributes_NameAndValue(self):
        d = self.sdb.deleteAttributes(self.uuid, "test", {"a":1})
        return d

    def test_08a_DeleteAttributes_Name(self):
        d = self.sdb.deleteAttributes(self.uuid, "test", ["a"])
        return d    
    
    def test_09_Delete(self):
        d = self.sdb.delete(self.uuid, "test")
        return d
    
    def test_10_DeleteDomain(self):
        d = self.sdb.deleteDomain(self.uuid)
        return d
        
    def test_11_CheckAndCreateDomain(self):
        d = self.sdb.checkAndCreateDomain(self.uuid)
        d.addCallback(self._checkAndCreateDomainCallback)
        return d
        
    def _checkAndCreateDomainCallback(self, data):
        d = self.sdb.checkAndCreateDomain(self.uuid)
        d.addCallback(self._checkAndCreateDomainCallback2)
        return d
    
    def _checkAndCreateDomainCallback2(self, data):
        d = self.sdb.deleteDomain(self.uuid)
        return d
    
    def test_12_CopyDomain(self):
        d = self.sdb.createDomain(self.uuid)
        d.addCallback(self._copyDomainCallback)
        return d    
    
    def _copyDomainCallback(self, data):
        deferreds = []
        for letter in "abcdefghijklmnopqrstuvwxyz":
            d = self.sdb.putAttributes(self.uuid, letter, {letter:[letter]})
            deferreds.append(d)
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._copyDomainCallback2)
        return d
        
    def _copyDomainCallback2(self, data):
        for row in data:
            if row[0] == False:
                raise row[1]
        destination_domain = "%s_destination" % self.uuid
        d = self.sdb.copyDomain(self.uuid, destination_domain)
        d.addCallback(self._copyDomainCallback3, destination_domain)
        return d
    
    def _copyDomainCallback3(self, data, destination_domain):
        deferreds = []
        for letter in "abcdefghijklmnopqrstuvwxyz":
            d = self.sdb.getAttributes(destination_domain, letter)
            deferreds.append(d)
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._copyDomainCallback4, destination_domain)
        return d
        
    def _copyDomainCallback4(self, data, destination_domain):
        for row in data:
            if row[0] == False:
                raise row[1]
        for row in data:
            letter = row[1].keys()[0]
            if row[1][letter][0] != letter:
                raise Exception("CopyDomain did not properly execute.")
        deferreds = []
        deferreds.append(self.sdb.deleteDomain(self.uuid))
        deferreds.append(self.sdb.deleteDomain(destination_domain))
        d = DeferredList(deferreds)
        return d
        
