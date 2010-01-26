import hashlib
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))


from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import Deferred
import twisted
twisted.internet.base.DelayedCall.debug = True

import yaml

from awspider.aws import AmazonProductAdvertising

class AmazonProductAdvertisingTestCase(unittest.TestCase):
    
    def setUp(self):
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "config.yaml"))
        if not os.path.isfile(config_path):
            self.raiseConfigException(config_path)
        config = yaml.load(open(config_path, 'r').read())
        if not "aws_access_key_id" in config or "aws_secret_access_key" not in config:
            self.raiseConfigException(config_path)
        self.pa = AmazonProductAdvertising(config["aws_access_key_id"], config["aws_secret_access_key"])
        self.uuid = hashlib.sha256(config["aws_access_key_id"] + config["aws_secret_access_key"] + self.__class__.__name__).hexdigest()
        
    def raiseConfigException(self, filename):
        raise Exception("Please create a YAML config file at %s with 'aws_access_key_id' and 'aws_secret_access_key'." % filename)

    def test_01_itemSearch(self):
        
        d = self.pa.itemSearch(
            Keywords="Johnny Depp",
            ResponseGroup="Accessories,BrowseNodes,EditorialReview,ItemAttributes,ItemIds,Large,ListmaniaLists,Medium,MerchantItemAttributes,OfferFull,Offers,OfferSummary,Reviews,SearchBins,Similarities,Subjects,Tags,TagsSummary,Tracks,VariationMinimum,Variations,VariationSummary")
        d.addCallback(self._itemSearchCallback)
        return d
        
    def _itemSearchCallback(self, data):
        print data