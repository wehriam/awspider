import cPickle
import twisted.python.failure
import datetime
import dateutil.parser
import hashlib
import logging
import time
from .requestqueuer import RequestQueuer
from .unicodeconverter import convertToUTF8, convertToUnicode


class ReportedFailure(twisted.python.failure.Failure):
    pass


class StaleContentException(Exception):
    pass


# A UTC class.
class CoordinatedUniversalTime(datetime.tzinfo):
    
    ZERO = datetime.timedelta(0)
    
    def utcoffset(self, dt):
        return self.ZERO
        
    def tzname(self, dt):
        return "UTC"
        
    def dst(self, dt):
        return self.ZERO


UTC = CoordinatedUniversalTime()
LOGGER = logging.getLogger("main")


class PageGetter:
    
    def __init__(self, 
        s3, 
        aws_s3_http_cache_bucket,
        time_offset=0,
        rq=None):
        """
        Create an S3 based HTTP cache.

        **Arguments:**
         * *s3* -- S3 client object.
         * *aws_s3_http_cache_bucket* -- S3 bucket to use for the HTTP cache.

        **Keyword arguments:**
         * *rq* -- Request Queuer object. (Default ``None``)      

        """
        self.s3 = s3
        self.aws_s3_http_cache_bucket = aws_s3_http_cache_bucket
        self.time_offset = time_offset
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
    
    def clearCache(self):
        """
        Clear the S3 bucket containing the S3 cache.
        """
        d = self.s3.emptyBucket(self.aws_s3_http_cache_bucket)
        return d
        
    def getPage(self, 
            url, 
            method='GET', 
            postdata=None,
            headers=None, 
            agent="AWSpider", 
            timeout=60, 
            cookies=None, 
            follow_redirect=1, 
            prioritize=True,
            hash_url=None, 
            cache=0,
            content_sha1=None,
            confirm_cache_write=False):
        """
        Make a cached HTTP Request.

        **Arguments:**
         * *url* -- URL for the request.

        **Keyword arguments:**
         * *method* -- HTTP request method. (Default ``'GET'``)
         * *postdata* -- Dictionary of strings to post with the request. 
           (Default ``None``)
         * *headers* -- Dictionary of strings to send as request headers. 
           (Default ``None``)
         * *agent* -- User agent to send with request. (Default 
           ``'AWSpider'``)
         * *timeout* -- Request timeout, in seconds. (Default ``60``)
         * *cookies* -- Dictionary of strings to send as request cookies. 
           (Default ``None``).
         * *follow_redirect* -- Boolean switch to follow HTTP redirects. 
           (Default ``True``)
         * *prioritize* -- Move this request to the front of the request 
           queue. (Default ``False``)
         * *hash_url* -- URL string used to indicate a common resource.
           Example: "http://digg.com" and "http://www.digg.com" could both
           use hash_url, "http://digg.com" (Default ``None``)      
         * *cache* -- Cache mode. ``1``, immediately return contents of 
           cache if available. ``0``, check resource, return cache if not 
           stale. ``-1``, ignore cache. (Default ``0``)
         * *content_sha1* -- SHA-1 hash of content. If this matches the 
           hash of data returned by the resource, raises a 
           StaleContentException.  
         * *confirm_cache_write* -- Wait to confirm cache write before returning.       
        """       
        request_kwargs = {
            "method":method.upper(), 
            "postdata":postdata, 
            "headers":headers, 
            "agent":agent, 
            "timeout":timeout, 
            "cookies":cookies, 
            "follow_redirect":follow_redirect, 
            "prioritize":prioritize}
        cache=int(cache)
        if cache not in [-1,0,1]:
            raise Exception("Unknown caching mode.")
        if not isinstance(url, str):
            url = convertToUTF8(url)
        if hash_url is not None and not isinstance(hash_url, str):
            hash_url = convertToUTF8(hash_url)
        if request_kwargs["method"] != "GET":
            d = self.rq.getPage(url, **request_kwargs)
            return d
        # Create request_hash to serve as a cache key from
        # either the URL or user-provided hash_url.
        if hash_url is None:
            request_hash = hashlib.sha1(cPickle.dumps([
                url, 
                headers, 
                agent, 
                cookies])).hexdigest()
        else:
            request_hash = hashlib.sha1(cPickle.dumps([
                hash_url, 
                headers, 
                agent, 
                cookies])).hexdigest()
        if cache == -1:
            # Cache mode -1. Bypass cache entirely.
            LOGGER.debug("Getting request %s for URL %s." % (request_hash, url))
            d = self.rq.getPage(url, **request_kwargs)
            d.addCallback(
                self._storeData, 
                request_hash, 
                content_sha1, 
                confirm_cache_write)
            return d
        elif cache == 0:
            # Cache mode 0. Check cache, send cached headers, possibly use cached data.
            LOGGER.debug("Checking S3 Head object request %s for URL %s." % (request_hash, url))
            # Check if there is a cache entry, return headers.
            d = self.s3.headObject(self.aws_s3_http_cache_bucket, request_hash)
            d.addCallback(self._checkCacheHeaders, 
                request_hash,
                url,  
                request_kwargs,
                content_sha1,
                confirm_cache_write)
            d.addErrback(self._requestWithNoCacheHeaders, 
                request_hash, 
                url, 
                request_kwargs,
                content_sha1,
                confirm_cache_write)        
            return d
        elif cache == 1:
            # Cache mode 1. Use cache immediately, if possible.
            LOGGER.debug("Getting S3 object request %s for URL %s." % (request_hash, url))
            d = self.s3.getObject(self.aws_s3_http_cache_bucket, request_hash)
            d.addCallback(self._returnCachedData, request_hash, content_sha1)
            d.addErrback(self._requestWithNoCacheHeaders, 
                request_hash, 
                url, 
                request_kwargs,
                content_sha1,
                confirm_cache_write)       
            return d      
                  
    def _checkCacheHeaders(self, 
            data, 
            request_hash, 
            url, 
            request_kwargs,
            content_sha1,
            confirm_cache_write):
        LOGGER.debug("Got S3 Head object request %s for URL %s." % (request_hash, url))
        http_history = {}
        if "content-sha1" in data["headers"]:
            http_history["content-sha1"] = data["headers"]["content-sha1"][0]
        if "request-failures" in data["headers"]:
            http_history["request-failures"] = data["headers"]["request-failures"][0].split(",")
        if "content-changes" in data["headers"]:
            http_history["content-changes"] = data["headers"]["content-changes"][0].split(",")
        # If cached data is not stale, return it.
        if "cache-expires" in data["headers"]:
            try:
                expires = dateutil.parser.parse(data["headers"]["cache-expires"][0])
                now = datetime.datetime.now(UTC)
                if expires > now:
                    LOGGER.debug("Cached data %s for URL %s is not stale. Getting from S3." % (request_hash, url))
                    d = self.s3.getObject(self.aws_s3_http_cache_bucket, request_hash)
                    d.addCallback(self._returnCachedData, request_hash, content_sha1)
                    d.addErrback(
                        self._requestWithNoCacheHeaders, 
                        request_hash, 
                        url,
                        request_kwargs, 
                        content_sha1,
                        confirm_cache_write,
                        http_history=http_history)
                    return d
            except Exception, e:
                LOGGER.error(str(e))
        # At this point, cached data may or may not be stale.
        # If cached data has an etag header, include it in the request.
        if "cache-etag" in data["headers"]:
            request_kwargs["etag"] = data["headers"]["cache-etag"][0]
        # If cached data has a last-modified header, include it in the request.
        if "cache-last-modified" in data["headers"]:
            request_kwargs["last_modified"] = data["headers"]["cache-last-modified"][0]
        LOGGER.debug("Requesting %s for URL %s with etag and last-modified headers." % (request_hash, url))
        # Make the request. A callback means a 20x response. An errback 
        # could be a 30x response, indicating the cache is not stale.
        d = self.rq.getPage(url, **request_kwargs)
        d.addCallback(
            self._returnFreshData, 
            request_hash,
            url, 
            content_sha1,
            confirm_cache_write,
            http_history=http_history)
        d.addErrback(
            self._handleRequestWithCacheHeadersError, 
            request_hash, 
            url, 
            request_kwargs,
            content_sha1, 
            confirm_cache_write,
            data,
            http_history=http_history)
        return d
        
    def _returnFreshData(self, 
            data, 
            request_hash, 
            url, 
            content_sha1, 
            confirm_cache_write,
            http_history=None):
        LOGGER.debug("Got request %s for URL %s." % (request_hash, url))
        data["pagegetter-cache-hit"] = False
        return self._storeData(
            data, 
            request_hash, 
            content_sha1, 
            confirm_cache_write,
            http_history=http_history)

    def _requestWithNoCacheHeaders(self, 
            error, 
            request_hash, 
            url, 
            request_kwargs, 
            content_sha1,
            confirm_cache_write,
            http_history=None):
        if isinstance(error, ReportedFailure):
            return error
        # No header stored in the cache. Make the request.
        LOGGER.debug("Unable to find header for request %s on S3, fetching from %s." % (request_hash, url))
        d = self.rq.getPage(url, **request_kwargs)
        d.addCallback(
            self._returnFreshData, 
            request_hash, 
            url, 
            content_sha1,
            confirm_cache_write,
            http_history=http_history)
        d.addErrback(
            self._requestWithNoCacheHeadersErrback, 
            request_hash, 
            url, 
            content_sha1,
            confirm_cache_write,
            http_history=http_history)
        return d        
    
    def _requestWithNoCacheHeadersErrback(self, 
            error,     
            request_hash, 
            url, 
            content_sha1,
            confirm_cache_write,
            http_history=None):
        LOGGER.critical(error.value.__dict__)
        LOGGER.error("Unable to get request %s for URL %s.\n%s" % (request_hash, url, error))
        if http_history is None:
            http_history = {} 
        if "request-failures" not in http_history:
            http_history["request-failures"] = [str(int(self.time_offset + time.time()))]
        else:
            http_history["request-failures"].append(str(int(self.time_offset + time.time())))
        http_history["request-failures"] = http_history["request-failures"][-3:]
        LOGGER.debug("Writing data for failed request %s to S3." % request_hash)
        headers = {}
        headers["request-failures"] = ",".join(http_history["request-failures"])
        d = self.s3.putObject(
            self.aws_s3_http_cache_bucket, 
            request_hash, 
            "", 
            content_type="text/plain", 
            headers=headers)
        if confirm_cache_write:
            d.addCallback(self._requestWithNoCacheHeadersErrbackCallback, error)
            return d       
        return error
        
    def _requestWithNoCacheHeadersErrbackCallback(self, data, error):
        return error
    
    def _handleRequestWithCacheHeadersError(self, 
            error, 
            request_hash, 
            url, 
            request_kwargs, 
            content_sha1, 
            confirm_cache_write,
            data,
            http_history=None):
        if error.value.status == "304":
            LOGGER.debug("Request %s for URL %s hasn't been modified since it was last downloaded. Getting data from S3." % (request_hash, url))
            d = self.s3.getObject(self.aws_s3_http_cache_bucket, request_hash)
            d.addCallback(self._returnCachedData, request_hash, content_sha1)
            d.addErrback(
                self._requestWithNoCacheHeaders, 
                request_hash, 
                url, 
                request_kwargs, 
                content_sha1,
                confirm_cache_write,
                http_history=http_history)
            return d
        else:
            if http_history is None:
                http_history = {} 
            if "request-failures" not in http_history:
                http_history["request-failures"] = [str(int(self.time_offset + time.time()))]
            else:
                http_history["request-failures"].append(str(int(self.time_offset + time.time())))
            http_history["request-failures"] = http_history["request-failures"][-3:]
            LOGGER.debug("Writing data for failed request %s to S3." % request_hash)
            headers = {}
            for key in data["headers"]:
                headers[key] = data["headers"][key][0]
            headers["request-failures"] = ",".join(http_history["request-failures"])
            d = self.s3.putObject(
                self.aws_s3_http_cache_bucket, 
                request_hash, 
                data["response"], 
                content_type=data["headers"]["content-type"][0], 
                headers=headers)
            if confirm_cache_write:
                d.addCallback(self._handleRequestWithCacheHeadersErrorCallback, error)
                return d
            return ReportedFailure(error)
            
    def _handleRequestWithCacheHeadersErrorCallback(self, data, error):
        return ReportedFailure(error)
        
    def _returnCachedData(self, data, request_hash, content_sha1):
        LOGGER.debug("Got request %s from S3." % (request_hash))
        data["pagegetter-cache-hit"] = True
        data["status"] = 304
        data["message"] = "Not Modified"
        if "content-sha1" in data["headers"]:
            data["content-sha1"] = data["headers"]["content-sha1"][0]
            if content_sha1 == data["content-sha1"]:
                raise StaleContentException(content_sha1)
            del data["headers"]["content-sha1"]
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
            
    def _storeData(self, 
            data, 
            request_hash, 
            content_sha1, 
            confirm_cache_write,
            http_history=None):
        data["content-sha1"] = hashlib.sha1(data["response"]).hexdigest()
        if http_history is None:
            http_history = {} 
        if "content-sha1" not in http_history:
            http_history["content-sha1"] = data["content-sha1"]
        if "content-changes" not in http_history:
            http_history["content-changes"] = []
        if data["content-sha1"] != http_history["content-sha1"]:
            http_history["content-changes"].append(str(int(self.time_offset + time.time())))
        http_history["content-changes"] = http_history["content-changes"][-10:]
        LOGGER.debug("Writing data for request %s to S3." % request_hash)
        headers = {}
        headers["content-changes"] = ",".join(http_history["content-changes"])
        headers["content-sha1"] = data["content-sha1"]
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
        d = self.s3.putObject(
            self.aws_s3_http_cache_bucket, 
            request_hash, 
            data["response"], 
            content_type=content_type, 
            headers=headers)
        if content_sha1 == data["content-sha1"]:
            raise StaleContentException(content_sha1)
        if confirm_cache_write:
            d.addCallback(self._storeDataCallback, data)
            return d
        return data
        
    def _storeDataCallback(self, data, response_data):
        return response_data

