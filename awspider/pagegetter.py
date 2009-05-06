from twisted.internet import reactor
from twisted.internet import defer
from twisted.web.client import getPage, HTTPClientFactory, HTTPDownloader, _parse
from unicodeconverter import convertToUTF8, convertToUnicode
import cPickle
import sha
import time
import dateutil.parser
import datetime
import pytz

import logging
logger = logging.getLogger("jasper")

class RequestQueuer():

    def __init__(self, max_requests=50):
    
        self.max_requests = max_requests
            
        self.pending = []
        self.active = 0

    def checkActive(self):
        
        while self.active < self.max_requests:
            
            if len( self.pending ) > 0:
                
                req = self.pending.pop(0)
                
                self.active = self.active + 1

                d = self._getPage(  
                    req["url"], 
                    method=req["method"], 
                    postdata=req["postdata"],
                    headers=req["headers"], 
                    agent=req["agent"], 
                    timeout=req["timeout"], 
                    cookies=req["cookies"],
                    followRedirect=req["followRedirect"]
                )

                d.addCallback( self.requestComplete, req["deferred"] ).addErrback( self.requestError, req["deferred"] )

            else:
                break

    def requestComplete( self, response, deferred ):
        self.active = self.active - 1
        self.checkActive()
        deferred.callback( response )
        return None
    
    def requestError( self, error, deferred ):
        self.active = self.active - 1
        self.checkActive()
        deferred.errback( error )  
        return None
    
    def getPage(self, url, last_modified=None, etag=None, method='GET', postdata=None, headers=None, agent="Offbeat Guides", timeout=60, cookies=None, followRedirect=1, prioritize=False ):

        if headers is None:
            headers = {}

        if last_modified is not None:
            headers['If-Modified-Since'] = time.strftime( "%a, %d %b %Y %T %z", dateutil.parser.parse(last_modified).timetuple() )

        if etag is not None:
            headers["If-None-Match"] = etag

        deferred = defer.Deferred()
                
        url = convertToUTF8( url )
        
        req = { 
            "url":url, 
            "method":method, 
            "postdata":postdata,
            "headers":headers, 
            "agent":agent, 
            "timeout":timeout, 
            "cookies":cookies,
            "followRedirect":followRedirect,
            "deferred":deferred 
        }
                
        if prioritize:
            self.pending.insert( 0, req )
        else:
            self.pending.append( req )
            
        self.checkActive()

        return deferred

    def _getPage(self, url, method='GET', postdata=None, headers=None, agent="Offbeat Guides", timeout=60, cookies=None, followRedirect=1 ):

        if headers is None:
            headers = {}

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
            reactor.connectSSL(host, port, factory, contextFactory, timeout=60)
        else:
            reactor.connectTCP(host, port, factory, timeout=60)

        factory.deferred.addCallback( self._getPageComplete, factory )
        factory.deferred.addErrback( self._getPageError, factory )
        return factory.deferred

    def _getPageComplete( self, response, factory ):
        return {"response":response, "headers":factory.response_headers, "status":int(factory.status), "message":factory.message }

    def _getPageError( self, error, factory ):
        
        if "response_headers" in factory.__dict__ and factory.response_headers is not None:
            error.value.headers = factory.response_headers
        
        return error

class PageGetter:
    
    def __init__( self, s3, aws_s3_bucket, rq=None ):
        
        self.s3 = s3
        self.aws_s3_bucket = aws_s3_bucket
        
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
    
    def getPage(self, url, method='GET', postdata=None, headers=None, agent="AWSpider", timeout=60, cookies=None, followRedirect=1, hash_url=None, cache=0, metadata=None, prioritize=True ):

        cache=int(cache)
        
        if cache not in [-1,0,1]:
            raise Exception("Unknown caching mode.")

        url = convertToUTF8( url )
        if hash_url is not None:
            hash_url = convertToUTF8( hash_url )

        if metadata is None:
            metadata = {}

        if method != "GET":
            d = self.rq.getPage( url, method=method, postdata=postdata, headers=headers, agent=agent, timeout=timeout, cookies=cookies, followRedirect=followRedirect, prioritize=prioritize )
            return d       
        
        parameters_to_be_hashed = cPickle.dumps( [headers, agent, cookies] )
        
        if hash_url is None:
            metadata["url"] = url
            request_hash = sha.new( url + parameters_to_be_hashed ).hexdigest()
        else:
            metadata["url"] = hash_url
            request_hash = sha.new( hash_url + parameters_to_be_hashed ).hexdigest()
        
        if cache == -1:
            logger.debug( "Getting request %s for URL %s." % (request_hash, url) )
            d = self.rq.getPage( url, method="GET", postdata=postdata, headers=headers, agent=agent, timeout=timeout, cookies=cookies, followRedirect=followRedirect, prioritize=prioritize )
            d.addCallback( self._storeData, request_hash )
            return d
            
        if cache == 0:
            logger.debug( "Checking S3 Head object request %s for URL %s." % (request_hash, url) )
            d = self.s3.headObject( self.aws_s3_bucket, request_hash )
            d.addCallback( self._s3HeadObjectCallback, url, request_hash, postdata, headers, agent, timeout, cookies, followRedirect, prioritize )
            d.addErrback( self._s3HeadObjectErrback, url, request_hash, postdata, headers, agent, timeout, cookies, followRedirect, prioritize )        
            return d
        
        if cache == 1:
            logger.debug( "Getting S3 object request %s for URL %s." % (request_hash, url) )
            d = self.s3.getObject( self.aws_s3_bucket, request_hash )
            d.addCallback( self._s3GetObjectCallback, request_hash )
            d.addErrback( self._s3HeadObjectErrback, url, request_hash, postdata, headers, agent, timeout, cookies, followRedirect, prioritize )       
            return d             
        
    def _s3HeadObjectCallback( self, data, url, request_hash, postdata, headers, agent, timeout, cookies, followRedirect, prioritize ):
        
        logger.debug( "Got S3 Head object request %s for URL %s." % (request_hash, url) )
    
        try:
            if "cache-expires" in data["headers"]:
                expires = dateutil.parser.parse(data["headers"]["cache-expires"][0])
                now = datetime.datetime.now( pytz.timezone("UTC") )
                if expires < now:
                    logger.debug( "Stored data request %s for URL %s is not stale. Getting from S3." % (request_hash, url) )
                    d = self.s3.getObject( self.aws_s3_bucket, request_hash )
                    d.addCallback( self._s3GetObjectCallback, request_hash )
                    d.addErrback( self._s3GetObjectErrback, None, request_hash )
                    return d                
        except Exception, e:
            logger.error( str(e) )
            
        request_keywords = {
            "method":"GET", 
            "postdata":postdata, 
            "headers":headers, 
            "agent":agent, 
            "timeout":timeout, 
            "cookies":cookies, 
            "followRedirect":followRedirect, 
            "prioritize":prioritize
        }
        
        if "cache-etag" in data["headers"]:
            request_keywords["etag"] = data["headers"]["cache-etag"][0]
        
        if "cache-last-modified" in data["headers"]:
            request_keywords["last_modified"] = data["headers"]["cache-last-modified"][0]
        
        #print "Requesting page with etag, last_modified headers"
        logger.debug( "Requesting %s for URL %s etag and last-modified headers." % (request_hash, url) )
        d = self.rq.getPage( url, **request_keywords )
        d.addCallback( self._s3HeadObjectCallback2, request_hash, url )
        d.addErrback( self._s3HeadObjectErrback2, request_hash, url )
        return d
        
    def _s3HeadObjectCallback2( self, data, request_hash, url ):
        logger.debug( "Got request %s for URL %s." % (request_hash, url) )
        return data
    
    def _s3HeadObjectErrback2( self, error, request_hash, url ):
        
        if error.value.status == "304":
            #print "Page hasn't been modified"
            logger.debug( "Request %s for URL %s hasn't been modified since it was last downloaded. Getting data from S3." % (request_hash, url) )
            d = self.s3.getObject( self.aws_s3_bucket, request_hash )
            d.addCallback( self._s3GetObjectCallback, request_hash )
            d.addErrback( self._s3GetObjectErrback, error, request_hash )
            return d
        else:
            logger.error( "Unable to get request %s for URL %s.\n%s" % (request_hash, url, error) )
            return error
    
    def _s3GetObjectCallback( self, data, request_hash ):
        
        logger.debug( "Got request %s from S3." % (request_hash) )
        
        #print "Got S3 object"
        data["status"] = 304
        data["message"] = "Not Modified"
        if "cache-expires" in data["headers"]:
            data["headers"]["expires"] = data["headers"]["cache-expires"]
            del data["headers"]["cache-expires"]
        
        if "cache-etag" in data["headers"]:
            data["headers"]["etag"] = data["headers"]["cache-etag"]
            del data["headers"]["cache-etag"]

        if "cache-last-modified" in data["headers"]:
            data["headers"]["last-modified"] = data["headers"]["cache-last-modified"]
            del data["headers"]["cache-last-modified"]
        
        return data
    
    def _s3GetObjectErrback( self, error, page_error, request_hash ):
        
        logger.error( "Error getting data from S3 for request %s.\n%s\n%s" % (request_hash, page_error, error) )
        
        if page_error is None:
            return error
        else:
            return page_error
            
    def _s3HeadObjectErrback(self, error, url, request_hash, postdata, headers, agent, timeout, cookies, followRedirect, prioritize):
        # No header, let's get the object!
        
        logger.debug( "Unable to find header for request %s on S3, fetching from %s." % (request_hash, url) )
        
        d = self.rq.getPage( url, method="GET", postdata=postdata, headers=headers, agent=agent, timeout=timeout, cookies=cookies, followRedirect=followRedirect, prioritize=prioritize )
        d.addCallback( self._storeData, request_hash )
        return d
        
    def _storeData( self, data, request_hash ):
        
        logger.debug("Writing data for request %s to S3." % request_hash )

        headers = {}
        
        if "cache-control" in data["headers"]: 
            if "no-cache" in data["headers"]["cache-control"][0]:
                return data
        
        if "expires" in data["headers"]:
            headers["cache-expires"] = data["headers"]["expires"][0]

        if "etag" in data["headers"]:
            headers["cache-etag"] = data["headers"]["etag"][0]
            
        if "last-modified" in data["headers"]:
            headers["cache-last-modified"] = data["headers"]["last-modified"][0]

        if "content-type" in data["headers"]:
            contentType = data["headers"]["content-type"][0]
        
        self.s3.putObject( self.aws_s3_bucket, request_hash, data["response"], contentType=contentType, headers=headers )
        return data
        
        
