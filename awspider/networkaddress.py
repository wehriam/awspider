from twisted.internet.defer import DeferredList
from twisted.web.client import HTTPClientFactory, _parse
from twisted.internet import reactor

import xml.etree.cElementTree as ET

import logging
logger = logging.getLogger("main")

import random
import socket
            
def getPage( url, method='GET', postdata=None, headers=None, agent="AWSpider", timeout=60, cookies=None, followRedirect=1 ):
    
    scheme, host, port, path = _parse(url)

    factory = HTTPClientFactory(
        url, 
        method=method, 
        postdata=postdata,
        headers=headers, 
        agent=agent, 
        timeout=timeout, 
        cookies=cookies,
        followRedirect=followRedirect
    )

    if scheme == 'https':
        from twisted.internet import ssl
        contextFactory = ssl.ClientContextFactory()
        reactor.connectSSL(host, port, factory, contextFactory, timeout=timeout)
    else:
        reactor.connectTCP(host, port, factory, timeout=timeout)

    return factory.deferred

class NetworkAddressGetter():
    
    local_ip = None
    public_ip = None
    
    def __init__( self ):
        self.ip_functions = [self.getDomaintools, self.getIPPages]
        random.shuffle(self.ip_functions)
    
    def __call__( self ):
        d = self.getAmazonIPs()
        d.addCallback( self._getAmazonIPsCallback )
        d.addErrback( self.getPublicIP )
        return d
    
    def getAmazonIPs( self ):
        logger.debug( "Getting local IP from Amazon." )
        a = getPage( "http://169.254.169.254/2009-04-04/meta-data/local-ipv4", timeout=5 )

        logger.debug( "Getting public IP from Amazon." )
        b = getPage( "http://169.254.169.254/2009-04-04/meta-data/public-ipv4", timeout=5  )
        
        d = DeferredList([a,b], consumeErrors=True)
        return d
    
    def _getAmazonIPsCallback( self, data ):
        
        if data[0][0] == True:
            self.local_ip = data[0][1]
            logger.debug( "Got local IP %s from Amazon." % self.local_ip )
        else:
            logger.debug( "Could not get local IP from Amazon." )

        if data[1][0] == True:
            public_ip = data[1][1]
            logger.debug( "Got public IP %s from Amazon." % public_ip )
            
            response = {}
            if self.local_ip is not None:
                response["local_ip"] = self.local_ip
            response["public_ip"] = public_ip
            
            return response
            
        else:
            logger.debug( "Could not get public IP from Amazon." )
            raise Exception( "Could not get public IP from Amazon." )
            
        
    def getPublicIP( self, error=None ):
        
        if len(self.ip_functions) > 0:
            func = self.ip_functions.pop()
            d = func()
            d.addCallback( self._getPublicIPCallback )
            d.addErrback( self.getPublicIP )
            return d
        else:
            logger.error( "Unable to get public IP address. Check your network connection" )
            response = {}
            if self.local_ip is not None:
                response["local_ip"] = self.local_ip
            else:
                response["local_ip"] = socket.gethostbyname(socket.gethostname())
            return response
             
    def _getPublicIPCallback( self, public_ip ):
        response = {}
        response["public_ip"] = public_ip
        if self.local_ip is not None:
            response["local_ip"] = self.local_ip
        else:
            response["local_ip"] = socket.gethostbyname(socket.gethostname())
        return response

    def getIPPages(self):
        logger.debug( "Getting public IP from ippages.com." )
        d = getPage( "http://www.ippages.com/xml/", timeout=5 )
        d.addCallback( self._getIPPagesCallback )
        return d       
    
    def _getIPPagesCallback(self, data ):
        domaintools_xml = ET.XML( data )
        public_ip = domaintools_xml.find("ip").text
        logger.debug( "Got public IP %s from ippages.com." % public_ip ) 
        return public_ip
    
    def getDomaintools(self):
        logger.debug( "Getting public IP from domaintools.com." )
        d = getPage( "http://ip-address.domaintools.com/myip.xml", timeout=5 )
        d.addCallback( self._getDomaintoolsCallback )
        return d
        
    def _getDomaintoolsCallback(self, data):
        domaintools_xml = ET.XML( data )
        public_ip = domaintools_xml.find("ip_address").text
        logger.debug( "Got public IP %s from domaintools.com." % public_ip ) 
        return public_ip
            

def getNetworkAddress():
    n = NetworkAddressGetter()
    d = n()
    d.addCallback( _getNetworkAddressCallback )
    return d
    
def _getNetworkAddressCallback( data ):
    return data    
    

if __name__ == "__main__":

    import logging.handlers
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s: %(message)s %(pathname)s:%(lineno)d")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    reactor.callWhenRunning( getNetworkAddress )     
    reactor.run()