import hashlib
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))


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

    def test_05_GetAttributes(self):
        d = self.sdb.getAttributes(self.uuid, "test")
        return d  
    
    def test_05a_GetAttributes_Individual(self):
        d = self.sdb.getAttributes(self.uuid, "test", "a")
        return d 
 
    def test_06_Select(self):
        d = self.sdb.select("SELECT * FROM `%s` WHERE `a`='1'" % self.uuid)
        return d
    
    def test_07_Query(self):
        raise unittest.SkipTest("Not implemented.")
    
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
        