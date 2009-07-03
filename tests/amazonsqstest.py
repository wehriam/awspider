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

from awspider.aws import AmazonSQS

import time

class AmazonSQSTestCase(unittest.TestCase):
    
    def setUp(self):
        
        config_path = os.path.abspath( os.path.join( os.path.dirname(__file__), "config.yaml" ) )
        
        if not os.path.isfile( config_path ):
            self.raiseConfigException( config_path )
            
        config = yaml.load( open( config_path, 'r').read() )
        
        if not "aws_access_key_id" in config \
            or "aws_secret_access_key" not in config \
            or "aws_account_number" not in config:
            
            self.raiseConfigException( config_path )
        
        self.aws_account_number = config["aws_account_number"].replace("-","")
        
        self.sqs = AmazonSQS(config["aws_access_key_id"], config["aws_secret_access_key"])
        
        self.uuid = hashlib.sha256(config["aws_access_key_id"] + config["aws_secret_access_key"]  + self.__class__.__name__).hexdigest()
        
        d = self.sqs.createQueue(self.uuid)
        d.addCallback(self._setUpCallback)
        return d
    
    def _setUpCallback(self, queue_path):
        self.queue_path = queue_path

    def raiseConfigException(self, filename):
        raise Exception("Please create a YAML config file at %s with 'aws_access_key_id' and 'aws_secret_access_key'." % filename )
    
    def test_00_listQueues(self):
        d = self.sqs.listQueues()
        d.addCallback(self._genericCallback)
        d.addErrback(self._genericErrback)
        return d

    def test_01_createQueue(self):
        d = self.sqs.createQueue(self.uuid)
        d.addCallback(self._genericCallback)
        d.addErrback(self._genericErrback)
        return d
        
    def test_02_setQueueAttributes(self):
        self.failIfEqual(self.queue_path, None)
        d = self.sqs.setQueueAttributes(self.queue_path, visibility_timeout=30)
        d.addCallback(self._genericCallback)
        d.addErrback(self._genericErrback)
        return d
    
    def test_02a_setQueueAttributes(self):
        self.failIfEqual(self.queue_path, None)
        policy = {
            "Version": "2008-10-17",
            "Id":self.uuid,
            "Statement":{
                "Sid":"%s_1" % self.uuid,
                "Effect": "Allow",
                "Principal":{"AWS": "*"},
                "Action":"SQS:ReceiveMessage",
                "Resource":self.queue_path
            }
        }
        d = self.sqs.setQueueAttributes(self.queue_path, policy=policy)
        d.addCallback(self._genericCallback)
        d.addErrback(self._genericErrback)
        return d

    def test_03_getQueueAttributes(self):
        self.failIfEqual(self.queue_path, None)
        d = self.sqs.getQueueAttributes(self.queue_path)
        d.addCallback(self._genericCallback)
        d.addErrback(self._genericErrback)
        return d

    def test_04_addPermission(self):
        self.failIfEqual(self.queue_path, None)
        label = "%s_account_permission_test" % self.uuid
        aws_account_id = self.aws_account_number
        actions = "SendMessage"
        d = self.sqs.addPermission(self.queue_path, label, aws_account_id, actions)
        d.addCallback(self._genericCallback)
        d.addErrback(self._genericErrback)
        return d

    def test_05_removePermission(self):
        self.failIfEqual(self.queue_path, None)
        label = "%s_account_permission_test" % self.uuid
        d = self.sqs.removePermission(self.queue_path, label)
        d.addCallback(self._genericCallback)
        d.addErrback(self._genericErrback)
        return d
    
    def test_06_sendMessage(self):
        self.failIfEqual(self.queue_path, None)
        message = "This is a test message."
        d = self.sqs.sendMessage(self.queue_path, message)
        d.addCallback(self._sendMessageCallback)
        d.addErrback(self._genericErrback)
        return d    

    def _sendMessageCallback(self, message_id):
        d = self.sqs.receiveMessage(self.queue_path)
        d.addCallback(self._receiveMessageCallback)
        d.addErrback(self._genericErrback)
        return d
        
    def _receiveMessageCallback(self, messages):
        d = self.sqs.changeMessageVisibility(self.queue_path, messages[0]["receipt_handle"], 60)
        d.addCallback(self._changeMessageVisibilityCallback, messages)
        d.addErrback(self._genericErrback)
        return d        
        
    def _changeMessageVisibilityCallback(self, data, messages):
        d = self.sqs.deleteMessage(self.queue_path, messages[0]["receipt_handle"])
        d.addCallback(self._genericCallback)
        d.addErrback(self._genericErrback)
        return d        
        
    def test_99_deleteQueue(self):
        self.failIfEqual(self.queue_path, None)
        # Will return a successful response if queue doesn't exist.
        d = self.sqs.deleteQueue(self.queue_path + "_other")
        d.addCallback(self._genericCallback)
        d.addErrback(self._genericErrback)
        return d

    def _genericCallback(self, data):
        return data
        
    def _genericErrback(self, error):
        print error
        return error
        
        
