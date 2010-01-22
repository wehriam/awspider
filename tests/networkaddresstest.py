from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import Deferred

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

import twisted
twisted.internet.base.DelayedCall.debug = True

from awspider.networkaddress import NetworkAddressGetter, getNetworkAddress

import re

class NetworkAddressTestCase(unittest.TestCase):
    
    def testGetNetworkAddress(self):
        d = getNetworkAddress()
        d.addCallback(self._testGetNetworkAddressCallback)
        return d
    
    def _testGetNetworkAddressCallback(self, result):
        if "public_ip" in result:
            self._checkIP(result["public_ip"])
        if "local_ip" in result:
            self._checkIP(result["local_ip"])
        if "public_ip" not in result and "local_ip" not in result:
            raise Exception("Could not find a local or public IP.")
    
    def testNetworkAddressGetter(self):
        n = NetworkAddressGetter()
        d = n()
        d.addCallback(self._testGetNetworkAddressCallback)
        return d
    
    def testGetPublicIP(self):
        n = NetworkAddressGetter()
        d = n.getPublicIP()
        d.addCallback(self._testGetNetworkAddressCallback)
        return d
        
    def testGetAmazonIPs(self):
        n = NetworkAddressGetter()
        d = n.getAmazonIPs()
        d.addCallback(self._testGetNetworkAddressCallback)
        return d
        
    def testGetIPPages(self):
        n = NetworkAddressGetter()
        d = n.getIPPages()
        d.addCallback(self._checkIP)
        return d
        
    def testGetDomaintools(self):
        n = NetworkAddressGetter()
        d = n.getDomaintools() 
        d.addCallback(self._checkIP)      
        return d
    
    def _checkIP(self, s):
        ip_address_regex = r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
        if len(re.findall(ip_address_regex, s)) != 1:
            raise Exception("Function returned invalid IP string.")
        return True
        
        