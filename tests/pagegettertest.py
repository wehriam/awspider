from twisted.trial import unittest
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet import reactor

from awspider.pagegetter import PageGetter, StaleContentException

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

from miniwebserver import MiniWebServer

from awspider.aws import AmazonS3

import yaml
import hashlib

import logging
import logging.handlers
LOGGER = logging.getLogger("main")

import twisted
twisted.internet.base.DelayedCall.debug = True

class PageGetterTestCase(unittest.TestCase):
    
    def setUp(self):
        self.mini_web_server = MiniWebServer()
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "config.yaml"))
        if not os.path.isfile(config_path):
            self.raiseConfigException(config_path)
        config = yaml.load(open(config_path, 'r').read())
        if not "aws_access_key_id" in config or "aws_secret_access_key" not in config:
            self.raiseConfigException(config_path)
        self.s3 = AmazonS3(
            config["aws_access_key_id"], 
            config["aws_secret_access_key"])        
        self.uuid = hashlib.sha256("".join([
            config["aws_access_key_id"],
            config["aws_secret_access_key"],
            self.__class__.__name__])).hexdigest()
        self.pg = PageGetter(self.s3, self.uuid)
        self.logging_handler = logging.StreamHandler()
        formatter = logging.Formatter("%(levelname)s: %(message)s %(pathname)s:%(lineno)d")
        self.logging_handler.setFormatter(formatter)
        LOGGER.addHandler(self.logging_handler)
        LOGGER.setLevel(logging.DEBUG)
        d = self.s3.putBucket(self.uuid)
        return d
        
    def tearDown(self):
        LOGGER.removeHandler(self.logging_handler)
        a = self.mini_web_server.shutdown()
        b = self.pg.clearCache()
        d = DeferredList([a, b])
        d.addCallback(self._tearDownCallback)
        return d
    
    def _tearDownCallback(self, data):
        d = self.s3.deleteBucket(self.uuid)
        return d

    def test_01_PageGetterOnSuccess(self):  
        d = self.pg.getPage(
            "http://127.0.0.1:8080/helloworld", 
            confirm_cache_write=True)
        return d
    
    def test_02_PageGetterOnFailure(self): 
        d = self.pg.getPage(
            "http://0.0.0.0:99", 
            timeout=5, 
            confirm_cache_write=True)
        d.addErrback(self._getPageErrback)  
        return d 
    
    def _getPageErrback(self, error):
        return True
    
    def test_04_ContentSHA1(self):  
        d = self.pg.getPage(
            "http://127.0.0.1:8080/helloworld", 
            confirm_cache_write=True)
        d.addCallback(self._contentSHA1Callback)
        return d
    
    def _contentSHA1Callback(self, data):
        if "content-sha1" in data:
            content_sha1 = data["content-sha1"]
            d = self.pg.getPage(
                "http://127.0.0.1:8080/helloworld", 
                content_sha1=content_sha1, 
                confirm_cache_write=True)
            d.addCallback(self._contentSHA1Callback2)
            d.addErrback(self._contentSHA1Errback)
            return d
        else:
            raise Exception("Data should have Content SHA1 signature.")
    
    def _contentSHA1Callback2(self, data):
        raise Exception("Pagegetter.getPage() should have raised StaleContentException")
    
    def _contentSHA1Errback(self, error):
        try:
            error.raiseException()
        except StaleContentException, e:
            return True
        except:
            return error

    def test_05_ContentSHA1Changed(self):  
        d = self.pg.getPage(
            "http://127.0.0.1:8080/random", 
            confirm_cache_write=True)
        d.addCallback(self._contentSHA1ChangedCallback)
        return d

    def _contentSHA1ChangedCallback(self, data):
        if "content-sha1" in data:
            content_sha1 = data["content-sha1"]
            d = self.pg.getPage(
                "http://127.0.0.1:8080/random", 
                content_sha1=content_sha1, 
                confirm_cache_write=True)
            d.addCallback(self._contentSHA1ChangedCallback2)
            d.addErrback(self._contentSHA1ChangedErrback)
            return d
        else:
            raise Exception("Data should have Content SHA1 signature.")

    def _contentSHA1ChangedCallback2(self, data):
        return True

    def _contentSHA1ChangedErrback(self, error):
        try:
            error.raiseException()
        except StaleContentException, e:
            raise Exception("Pagegetter.getPage() should not have raised StaleContentException")
        except:
            return error
            
    def test_06_ExpiresGetPage(self):
        d = self.pg.getPage(
            "http://127.0.0.1:8080/expires", 
            confirm_cache_write=True)
        d.addCallback(self._expiresGetPageCallback)
        return d
    
    def _expiresGetPageCallback(self, data):
        if "pagegetter-cache-hit" in data:
            # We shouldn't get a cache hit here.
            self.failUnlessEqual(data["pagegetter-cache-hit"], False)
            # But on the next try...
            d = self.pg.getPage(
                "http://127.0.0.1:8080/expires", 
                confirm_cache_write=True)
            d.addCallback(self._expiresGetPageCallback2)
            return d
        else:
            raise Exception("Data should have pagegetter-cache-hit flag.")
    
    def _expiresGetPageCallback2(self, data):
        if "pagegetter-cache-hit" in data:
            # We should get a cache hit here.
            self.failUnlessEqual(data["pagegetter-cache-hit"], True)
            if "content-sha1" in data:
                content_sha1 = data["content-sha1"]
                d = self.pg.getPage(
                    "http://127.0.0.1:8080/expires", 
                    content_sha1=content_sha1, 
                    confirm_cache_write=True)
                d.addCallback(self._expiresGetPageCallback3)
                d.addErrback(self._expiresGetPageErrback)
                return d
            else:
                raise Exception("Data should have content SHA1 signature.")
        else:
            raise Exception("Data should have pagegetter-cache-hit flag.")        
    
    def _expiresGetPageCallback3(self, data):
        raise Exception("Pagegetter.getPage() should have raised StaleContentException")
    
    def _expiresGetPageErrback(self, error):
        try:
            error.raiseException()
        except StaleContentException, e:
            return True
        except:
            return error
            
    def test_07_ClearCache(self):
        d = self.pg.clearCache()
        return d     
    
    def test_08