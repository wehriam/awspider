import cPickle
import sha

import dateutil.parser
import datetime

from .requestqueuer import RequestQueuer
from .unicodeconverter import convertToUTF8, convertToUnicode

import logging
logger = logging.getLogger("main")

# A UTC class.

class UTC(datetime.tzinfo):
    ZERO = datetime.timedelta(0)
    def utcoffset(self, dt):
        return self.ZERO
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return self.ZERO
        
utc = UTC()

class PageGetter:
    
    def __init__( self, s3, aws_s3_bucket, rq=None ):
        
        self.s3 = s3
        self.aws_s3_bucket = aws_s3_bucket
        
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
        
    def clearCache( self, result=None ):
        d = self.s3.emptyBucket( self.aws_s3_bucket )
        return d
        
    def getPage(self, url, method='GET', postdata=None, headers=None, agent="AWSpider", timeout=60, cookies=None, follow_redirect=1, hash_url=None, cache=0, prioritize=True ):

        cache=int(cache)
        
        if cache not in [-1,0,1]:
            raise Exception("Unknown caching mode.")

        url = convertToUTF8( url )
        if hash_url is not None:
            hash_url = convertToUTF8( hash_url )
                
        if method.lower() != "get":
            d = self.rq.getPage( url, method=method, postdata=postdata, headers=headers, agent=agent, timeout=timeout, cookies=cookies, follow_redirect=follow_redirect, prioritize=prioritize )
            return d       
        
        parameters_to_be_hashed = cPickle.dumps( [headers, agent, cookies] )
        
        if hash_url is None:
            request_hash = sha.new( url + parameters_to_be_hashed ).hexdigest()
        else:
            request_hash = sha.new( hash_url + parameters_to_be_hashed ).hexdigest()
        
        if cache == -1:
            logger.debug( "Getting request %s for URL %s." % (request_hash, url) )
            d = self.rq.getPage( url, method="GET", postdata=postdata, headers=headers, agent=agent, timeout=timeout, cookies=cookies, follow_redirect=follow_redirect, prioritize=prioritize )
            d.addCallback( self._storeData, request_hash )
            return d
            
        if cache == 0:
            logger.debug( "Checking S3 Head object request %s for URL %s." % (request_hash, url) )
            d = self.s3.headObject( self.aws_s3_bucket, request_hash )
            d.addCallback( self._s3HeadObjectCallback, url, request_hash, postdata, headers, agent, timeout, cookies, follow_redirect, prioritize )
            d.addErrback( self._s3HeadObjectErrback, url, request_hash, postdata, headers, agent, timeout, cookies, follow_redirect, prioritize )        
            return d
        
        if cache == 1:
            logger.debug( "Getting S3 object request %s for URL %s." % (request_hash, url) )
            d = self.s3.getObject( self.aws_s3_bucket, request_hash )
            d.addCallback( self._s3GetObjectCallback, request_hash )
            d.addErrback( self._s3HeadObjectErrback, url, request_hash, postdata, headers, agent, timeout, cookies, follow_redirect, prioritize )       
            return d             
        
    def _s3HeadObjectCallback( self, data, url, request_hash, postdata, headers, agent, timeout, cookies, follow_redirect, prioritize ):
        
        logger.debug( "Got S3 Head object request %s for URL %s." % (request_hash, url) )
    
        try:
            if "cache-expires" in data["headers"]:
                
                expires = dateutil.parser.parse(data["headers"]["cache-expires"][0])
                now = datetime.datetime.now( utc )
                                
                if expires > now:
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
            "follow_redirect":follow_redirect, 
            "prioritize":prioritize
        }
        
        if "cache-etag" in data["headers"]:
            request_keywords["etag"] = data["headers"]["cache-etag"][0]
        
        if "cache-last-modified" in data["headers"]:
            request_keywords["last_modified"] = data["headers"]["cache-last-modified"][0]
    
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
            
    def _s3HeadObjectErrback(self, error, url, request_hash, postdata, headers, agent, timeout, cookies, follow_redirect, prioritize):
        # No header, let's get the object!
        
        logger.debug( "Unable to find header for request %s on S3, fetching from %s." % (request_hash, url) )
        
        d = self.rq.getPage( url, method="GET", postdata=postdata, headers=headers, agent=agent, timeout=timeout, cookies=cookies, follow_redirect=follow_redirect, prioritize=prioritize )
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
            content_type = data["headers"]["content-type"][0]
        
        d = self.s3.putObject( self.aws_s3_bucket, request_hash, data["response"], content_type=content_type, headers=headers )
        d.addCallback( self._storeDataCallback, data )
        return d
        
    def _storeDataCallback( self, data, response_data ):
        return response_data
        
        
