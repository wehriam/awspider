import cStringIO
import gzip as gzip_package
import base64
import hmac
import hashlib
import logging
import time
import urllib
import xml.etree.cElementTree as ET
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList, deferredGenerator, waitForDeferred
from ..unicodeconverter import convertToUTF8
from ..requestqueuer import RequestQueuer
from .lib import return_true, etree_to_dict


S3_NAMESPACE = "{http://s3.amazonaws.com/doc/2006-03-01/}"
LOGGER = logging.getLogger("main")

class AmazonS3:
    """
    Amazon Simple Storage Service API.
    """
   
    ACCEPTABLE_ERROR_CODES = [400, 403, 404, 409]
    host = "s3.amazonaws.com"
    reserved_headers = ["x-amz-id-2", "x-amz-request-id", "date", "last-modified", "etag", "content-type", "content-length", "server"]
    
    def __init__(self, aws_access_key_id, aws_secret_access_key, rq=None):
        """
        **Arguments:**
         * *aws_access_key_id* -- Amazon AWS access key ID
         * *aws_secret_access_key* -- Amazon AWS secret access key
       
        **Keyword arguments:**
         * *rq* -- Optional RequestQueuer object.
        """
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
        self.rq.setHostMaxRequestsPerSecond(self.host, 0)
        self.rq.setHostMaxSimultaneousRequests(self.host, 0)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
       
    def checkAndCreateBucket(self, bucket):
        """
        Check for a bucket's existence. If it does not exist, 
        create it.
       
        **Arguments:**
         * *bucket* -- Bucket name
        """
        d = self.getBucket(bucket)
        d.addErrback(self._checkAndCreateBucketErrback, bucket) 
        return d

    def _checkAndCreateBucketErrback(self, error, bucket):
        if int(error.value.status) == 404:    
            d = self.putBucket(bucket)
            d.addErrback(self._checkAndCreateBucketErrback2, bucket)
            return d
        raise Exception("Could not find or create bucket '%s'." % bucket_name)

    def _checkAndCreateBucketErrback2( self, error, bucket_name):
        raise Exception("Could not create bucket '%s'" % bucket_name)      
    
    def emptyBucket(self, bucket):
        """
        Delete all items in a bucket.
       
        **Arguments:**
         * *bucket* -- Bucket name
        """
        d = self.getBucket(bucket)
        d.addCallback(self._emptyBucketCallback, bucket)
        return d
       
    def _emptyBucketCallback(self, result, bucket): 
        xml = ET.XML(result["response"])
        key_nodes = xml.findall(".//%sKey" % S3_NAMESPACE)
        delete_deferreds = []
        for node in key_nodes:
            delete_deferreds.append(self.deleteObject(bucket, node.text))
        if len(delete_deferreds) == 0:
            return True
        d = DeferredList(delete_deferreds)
        if xml.find('.//%sIsTruncated' % S3_NAMESPACE).text == "false":
            d.addCallback(return_true)
        else:
            d.addCallback(self._emptyBucketCallbackRepeat, bucket)
        return d
   
    def _emptyBucketCallbackRepeat(self, data, bucket):
        return self.emptyBucket(bucket)
   
#    def listObjects(self, bucket, marker=None):
#        """
#        List information about the objects in the bucket.
#
#        **Arguments:**
#         * *bucket* -- Bucket name
#        """
#        bucket = convertToUTF8(bucket)
#        path = bucket
#        if marker is not None:
#            path = urllib.urlencode({"marker":marker})
#        headers = self._getAuthorization("GET", "", "", {}, "/" + path)
#        url = "http://%s/%s" % (self.host, path)
#        d = self.rq.getPage(url, method="GET", headers=headers)
#        d.addCallback(self._listObjectsCallback, bucket)
#        d.addErrback(self._genericErrback, url, method="GET", headers=headers)
#        return d
#    
#    @deferredGenerator
#    def _listObjectsCallback(self, result, bucket):
#        xml = ET.XML(result["response"])            
#        data = etree_to_dict(xml, namespace='{http://s3.amazonaws.com/doc/2006-03-01/}')
#        for obj in data["Contents"]:
#            for key in obj:
#                obj[key] = obj[key][0]
#            for key in obj["Owner"]:
#                obj["Owner"][key] = obj["Owner"][key][0]
#            yield obj
#        
#        if data["IsTruncated"][0] == "true":
#            d = self.listObjects(bucket, marker=obj["Key"])
#            wfd = waitForDeferred(d)
#            yield wfd
            
    def getBucket(self, bucket):
        """
        List information about the objects in the bucket.
       
        **Arguments:**
         * *bucket* -- Bucket name
        """
        bucket = convertToUTF8(bucket)
        headers = self._getAuthorization("GET", "", "", {}, "/" + bucket)
        url = "http://%s/%s" % (self.host, bucket)
        d = self.rq.getPage(url, method="GET", headers=headers)
        d.addErrback(self._genericErrback, url, method="GET", headers=headers)
        return d       
        
    def putBucket(self, bucket):
        """
        Creates a new bucket.
       
        **Arguments:**
         * *bucket* -- Bucket name
        """
        bucket = convertToUTF8(bucket)
        headers = {
            "Content-Length": 0
        }
        auth = self._getAuthorization("PUT", "", "", headers, "/" + bucket)
        headers.update(auth)
        url = "http://%s/%s" % (self.host, bucket)
        d = self.rq.getPage(url, method="PUT", headers=headers)
        d.addErrback(self._genericErrback, url, method="PUT", headers=headers)
        return d

    def deleteBucket(self, bucket):
        """
        Delete the bucket.
       
        **Arguments:**
         * *bucket* -- Bucket name
        """
        bucket = convertToUTF8(bucket)
        headers = {
            "Content-Length": 0
        }
        auth = self._getAuthorization("DELETE", "", "", headers, "/" + bucket)
        headers.update(auth)
        url = "http://%s/%s" % (self.host, bucket)
        d = self.rq.getPage(url, method="DELETE", headers=headers)
        d.addErrback(self._genericErrback, url, method="DELETE",    
                     headers=headers)
        return d

    def headObject(self, bucket, key):
        """
        Retrieve information about a specific object or object size, without
        actually fetching the object itself.
       
        **Arguments:**
         * *bucket* -- Bucket name
         * *key* -- Key name
        """
        bucket = convertToUTF8(bucket)
        key = convertToUTF8(key)
        path = "/" + bucket + "/" + key
        headers = self._getAuthorization("HEAD", "", "", {}, path)
        url = "http://%s/%s/%s" % (self.host, bucket, key)
        d = self.rq.getPage(url, method="HEAD", headers=headers)
        d.addCallback(self._getObjectCallback)
        d.addErrback(self._genericErrback, url, method="HEAD", headers=headers)
        return d

    def _decodeAmazonHeaders(self, headers):
        """
        Remove custom header prefix from header dictionary keys.
        """
        for key in headers.keys():
            if ("x-amz-meta-%s" % key.lower()) in self.reserved_headers:
                message = "Header %s is reserved for use by Amazon S3." % key
                LOGGER.critical(message)
                raise Exception(message)
        keys = headers.keys()
        values = headers.values()
        meta = "x-amz-meta-"
        return dict(zip([x.replace(meta,"") for x in keys], values))

    def getObject(self, bucket, key):
        """
        Returns object directly from Amazon S3 using a client/server 
        delivery mechanism.
       
        **Arguments:**
         * *bucket* -- Bucket name
         * *key* -- Key name
        """       
        bucket = convertToUTF8(bucket)
        key = convertToUTF8(key)
        path = "/" + bucket + "/" + key
        headers = self._getAuthorization("GET", "", "", {}, path)
        url = "http://%s/%s/%s" % (self.host, bucket, key)
        d = self.rq.getPage(url, method="GET", headers=headers)
        d.addCallback(self._getObjectCallback)
        d.addErrback(self._genericErrback, url, method="GET", headers=headers)
        return d   
       
    def _getObjectCallback(self, data):
        if "content-encoding" in data["headers"]:
            if "gzip" in data["headers"]["content-encoding"]:
                compressedstream = cStringIO.StringIO(data["response"])
                zfile = gzip_package.GzipFile(fileobj=compressedstream)
                data["response"] = zfile.read()
        data["headers"] = self._decodeAmazonHeaders(data["headers"])
        return data
   
    def _encodeAmazonHeaders(self, headers):
        """
        Prepend custom header prefix to header dictionary keys.
        """
        headers = dict([(x[0].lower(), x[1]) for x in headers.items()])
        for header in self.reserved_headers:
            if header in headers:
                del headers[header]
        keys = headers.keys()
        values = headers.values()
        #for key in keys:
        #    if key.lower() in self.reserved_headers:
        #        message = "Header %s is reserved for use by Amazon S3." % key
        #        LOGGER.error(message)
        #        raise Exception(message)
        meta = "x-amz-meta-"
        return dict(zip(["%s%s" % (meta, x) for x in keys], values))
       
    def putObject(self, bucket, key, data, content_type="text/html", 
                  public=True, headers=None, gzip=False):
        """
        Add an object to a bucket.
       
        **Arguments:**
         * *bucket* -- Bucket name
         * *key* -- Key name
         * *data* -- Data string
       
        **Keyword arguments:**
         * *content_type* -- Content type header (Default 'text/html')
         * *public* -- Boolean flag representing access (Default True)
         * *headers* -- Custom header dictionary (Default empty dictionary)
         * *gzip* -- Boolean flag to gzip data (Default False)
        """
        bucket = convertToUTF8(bucket)
        key = convertToUTF8(key)
        if not isinstance(data, str):
            data = convertToUTF8(data)
        if headers is None:
            headers = {}
        # Wrap user-defined headers in with the Amazon custom header prefix.
        headers = self._encodeAmazonHeaders(headers)
        if gzip:
            # Gzip that bastard!
            headers["content-encoding"] = "gzip"
            zbuf = cStringIO.StringIO()
            zfile = gzip_package.GzipFile(None, 'wb', 9, zbuf)
            zfile.write(data)
            zfile.close()
            data = zbuf.getvalue()
        content_md5 = base64.encodestring(hashlib.md5(data).digest()).strip()
        if public:
            headers['x-amz-acl'] = 'public-read'
        else:
            headers['x-amz-acl'] = 'private'
        headers.update({
            'Content-Length':len(data),
            'Content-Type':content_type,
            'Content-MD5':content_md5
        })
        path = "/" + bucket + "/" + key
        auth = self._getAuthorization("PUT", content_md5, content_type, 
                                      headers, path)
        headers.update(auth)
        url = "http://%s/%s/%s" % (self.host, bucket, key)
        d = self.rq.getPage(url, method="PUT", headers=headers, postdata=data)
        d.addErrback(self._genericErrback, url, method="PUT", headers=headers, 
                     postdata=data)
        return d

    def deleteObject(self, bucket, key):
        """
        Remove the specified object from Amazon S3.
       
        **Arguments:**
         * *bucket* -- Bucket name
         * *key* -- Key name
        """
        bucket = convertToUTF8(bucket)
        key = convertToUTF8(key)
        path = "/" + bucket + "/" + key
        headers = self._getAuthorization("DELETE", "", "", {}, path)
        url = "http://%s/%s/%s" % (self.host, bucket, key)
        d = self.rq.getPage(url, method="DELETE", headers=headers)
        d.addErrback(self._genericErrback, url, method="DELETE", 
                     headers=headers)
        return d
           
    def _genericErrback(self, error, url, method="GET", headers=None,
                        postdata=None, count=0):
        if headers is None:
            headers = {}
        if "status" in error.value.__dict__:
            # 204, empty response but otherwise OK, as in the case of a delete.
            # Move on, nothing to see here.
            if int(error.value.status) == 204:
                return {
                        "response":error.value.response,
                        "status":int(error.value.status),
                        "headers":error.value.response,
                        "message":error.value.headers
                    }
            # Something other than a 40x error. Something is wrong, but let's
            # try that again a few times.          
            elif int(error.value.status) not in self.ACCEPTABLE_ERROR_CODES \
                 and count < 3:
                d = self.rq.getPage(url,       
                                    method=method,
                                    headers=headers,
                                    postdata=postdata
                                    )
                                   
                d.addErrback(self._genericErrback,
                                url,
                                method=method,
                                headers=headers,
                                postdata=postdata,
                                count=count + 1
                                )
                return d
            # 404 or other normal error, pass it along.
            else:
                return error   
        else:
            return error
               
    def _canonicalize(self, headers):
        """
        Canonicalize headers for use with AWS Authorization.
       
        **Arguments:**
         * *headers* -- Key value pairs of headers
       
        **Returns:**
         * A string representation of the parameters.
        """
        keys = [k for k in headers.keys() if k.startswith("x-amz-")]
        keys.sort(key = str.lower)
        return "\n".join(["%s:%s" % (x, headers[x]) for x in keys])

    def _getAuthorization(self, method, content_hash, content_type, 
                          headers, resource):
        """
        Create authentication headers.
       
        **Arguments:**
         * *method* -- HTTP method of the request
         * *parameters* -- Key value pairs of parameters
       
        **Returns:**
         * A dictionary of authorization parameters
        """
        date = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())
        amazon_headers = self._canonicalize(headers)
        if len(amazon_headers):
            data = "%s\n%s\n%s\n%s\n%s\n%s" % (
                method,
                content_hash,
                content_type,
                date,
                amazon_headers,
                resource
           )
        else:
            data = "%s\n%s\n%s\n%s\n%s%s" % (
                method,
                content_hash,
                content_type,
                date,
                amazon_headers,
                resource
           )
        args = [self.aws_secret_access_key, data, hashlib.sha1]
        signature = base64.encodestring(hmac.new(*args).digest()).strip()
        authorization = "AWS %s:%s" % (self.aws_access_key_id, signature)
        return {'Authorization': authorization, 'Date':date, "Host":self.host}

