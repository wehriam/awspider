"""
Twisted API for Amazon's various web services.
"""

__all__ = [
    'AmazonSQS', 
    'AmazonSDB', 
    'AmazonS3', 
    'sdb_now', 
    'sdb_now_add', 
    'sdb_parse_time', 
    'sdb_latitude', 
    'sdb_longitude' 
]

import cStringIO
import gzip as gzip_package
import base64
import hmac
import urllib
import hashlib
from datetime import datetime, timedelta
import xml.etree.cElementTree as ET
import time

import simplejson
import dateutil.parser
from twisted.internet.defer import DeferredList
from twisted.web.client import _parse

from unicodeconverter import convertToUTF8
from pagegetter import RequestQueuer


def sdb_now(offset=0):
    """Return an 11 character, zero padded string with the current Unixtime.
    
    Keyword arguments:
    offset -- Offset in seconds. (Default 0)
    """
    return str(int(offset + time.time())).zfill(11)


def sdb_now_add(i, offset=0):
    """Return an 11 character, zero padded string with the current Unixtime
    plus an integer.
    
    Arguments:
    i -- Seconds to add to the current time.
    
    Keyword arguments:
    offset -- Offset in seconds. (Default 0)
    """
    return str(int(offset + time.time() + i)).zfill(11)


def sdb_parse_time(s, offset=0):
    """Parse a date string, then return an 11 character, zero padded
    string with the current Unixtime plus an integer.
    
    Arguments:
    s -- Date string
    
    Keyword arguments:
    offset -- Offset in seconds. (Default 0)
    """
    parsed_time = time.mktime(dateutil.parser.parse(s).timetuple())
    return str(int(offset + parsed_time)).zfill(11)


def sdb_latitude(latitude):
    """Return an 8 character, zero padded string version of the 
    latitude parameter.
    
    Arguments:
    latitude -- Latitude.
    """
    adjusted = (90 + float(latitude)) * 100000
    return str(int(adjusted)).zfill(8)


def sdb_longitude(longitude):
    """Return an 8 character, zero padded string version of the 
    longitude parameter.
    
    Arguments:
    longitude -- Longitude.
    """
    adjusted = (180 + float(longitude)) * 100000
    return str(int(adjusted)).zfill(8)


def _safe_quote(s):
    """AWS safe version of urllib.quote"""
    return urllib.quote(str(s), '-_.~')


def _safe_quote_tuple(x):
    """Convert a 2-tuple to a string for use with AWS"""
    return "%s=%s" % (_safe_quote(x[0]), _safe_quote(x[1]))

def _etree_to_dict(etree, namespace=None):
    children = etree.getchildren()
    if len(children) == 0:
        return etree.text
    children_dict = {}
    for element in children:
        tag = element.tag
        if namespace is not None:
            tag = tag.replace(namespace, "")
        if tag in children_dict:
            children_dict[tag].append(_etree_to_dict(element, namespace=namespace))
        else:
            children_dict[tag] = [_etree_to_dict(element, namespace=namespace)]
    return children_dict

S3_NAMESPACE = "{http://s3.amazonaws.com/doc/2006-03-01/}"
SDB_NAMESPACE = "{http://sdb.amazonaws.com/doc/2007-11-07/}"
SQS_NAMESPACE = "{http://queue.amazonaws.com/doc/2009-02-01/}"


class AmazonSQS:
    
    """
    Amazon Simple Queue Service API.
    """
    
    host = "queue.amazonaws.com"
    
    def __init__(self, aws_access_key_id, aws_secret_access_key, rq=None):
        """
        Arguments:
        aws_access_key_id -- Amazon AWS access key ID
        aws_secret_access_key -- Amazon AWS secret access key
        
        Keyword arguments:
        rq -- Optional RequestQueuer object.
        """
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
        # Don't rate limit requests to AWS.
        self.rq.setHostMaxRequestsPerSecond(self.host, 0)
        self.rq.setHostMaxSimultaneousRequests(self.host, 0)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key   

    def listQueues(self, name_prefix=None):
        parameters = {
            "Action":"ListQueues"
        }
        if name_prefix is not None:
            parameters["QueueNamePrefix"] = name_prefix
        d = self._request(parameters)
        d.addCallback(self._listQueuesCallback)
        return d

    def _listQueuesCallback(self, data):
        xml = ET.fromstring(data["response"])
        queue_urls = xml.findall(".//%sQueueUrl" % SQS_NAMESPACE)
        host_string = "https://%s" % self.host
        queue_urls = map(lambda x:x.text.replace(host_string, ""), queue_urls)
        return queue_urls

    def createQueue(self, name, visibility_timeout=None):
        name = convertToUTF8(name)
        parameters = {
            "Action":"CreateQueue",
            "QueueName":name,
        }
        if visibility_timeout is not None:
            parameters["DefaultVisibilityTimeout"] = visibility_timeout
        d = self._request(parameters)
        d.addCallback(self._createQueueCallback)
        d.addErrback(self._genericErrback)
        return d
        
    def _createQueueCallback(self, data):
        xml = ET.fromstring(data["response"])
        queue_url = xml.find(".//%sQueueUrl" % SQS_NAMESPACE).text
        return queue_url.replace("https://%s" % self.host, "")
        
    def deleteQueue(self, resource):
        parameters = {
            "Action":"DeleteQueue"
        }
        d = self._request(parameters, resource=resource, method="DELETE")
        d.addCallback(self._deleteQueueCallback)
        d.addErrback(self._genericErrback)
        return d

    def _deleteQueueCallback(self, data):
        return True
        
    def setQueueAttributes(self, resource, visibility_timeout=None, policy=None):
        parameters = {
            "Action":"SetQueueAttributes"
        }
        attributes = {}
        if policy is not None:
            attributes["Policy"] = simplejson.dumps(policy)
        if visibility_timeout is not None:
            attributes["VisibilityTimeout"] = visibility_timeout
        i = 1
        for name in attributes.keys():
            parameters["Attribute.%s.Name" % i] = name
            parameters["Attribute.%s.Value" % i] = attributes[name]
        d = self._request(parameters, resource=resource)
        d.addCallback(self._setQueueAttributesCallback)
        d.addErrback(self._genericErrback)
        return d
        
    def _setQueueAttributesCallback(self, data):
        return True
    
    def getQueueAttributes(self, resource, name=None):
        attributes = [
            "All", 
            "ApproximateNumberOfMessages", 
            "VisibilityTimeout", 
            "CreatedTimestamp", 
            "LastModifiedTimestamp", 
            "Policy"
        ]
        if name is not None:
            if name not in attributes:
                raise Exception("Unknown attribute name '%s'." % name)
        else:
            name = "All"
        parameters = {
            "Action":"GetQueueAttributes",
            "AttributeName":name
        }        
        d = self._request(parameters, resource=resource, method="DELETE")
        d.addCallback(self._getQueueAttributesCallback)
        d.addErrback(self._genericErrback)
        return d
            
    def _getQueueAttributesCallback(self, data):
        attributes = {}
        xml = ET.fromstring(data["response"])
        xml_attributes = xml.findall(".//%sAttribute" % SQS_NAMESPACE)
        for attribute in xml_attributes:
            name = attribute.find(".//%sName" % SQS_NAMESPACE).text
            value = attribute.find(".//%sValue" % SQS_NAMESPACE).text
            attributes[name] = value
        if "Policy" in attributes:
            attributes["Policy"] = simplejson.loads(attributes["Policy"])
        integer_attribute_names = [
            'CreatedTimestamp', 
            'ApproximateNumberOfMessages', 
            'LastModifiedTimestamp', 
            'VisibilityTimeout'
        ]
        for name in integer_attribute_names:
            if name in attributes:
                attributes[name] = int(attributes[name])
        return attributes
            
    def addPermission(self, resource, label, aws_account_ids, actions=None):
        if actions is None:
            actions = ["*"]
        if isinstance(actions, str) or isinstance(actions, unicode):
            actions = [str(actions)]
        if not isinstance(actions, list):
            raise Exception("Actions must be a string or list of strings.")
        if isinstance(aws_account_ids, str) or isinstance(aws_account_ids, unicode):
            aws_account_ids = [str(aws_account_ids)]
        if not isinstance(aws_account_ids, list):
            raise Exception("aws_account_ids must be a string or list of strings.")        
        action_options = [
            "*",
            "SendMessage",
            "ReceiveMessage",
            "DeleteMessage",
            "ChangeMessageVisibility",
            "GetQueueAttributes"
        ]
        for action in actions:
            if action not in action_options:
                raise Exception("Unknown action name '%s'." % action)
        if len(actions) == 0:
            actions.append("*")
        parameters = {
            "Action":"AddPermission",
            "Label":label
        }
        i = 1
        for name in actions:
            parameters["ActionName.%s" % i] = name
        i = 1
        for name in aws_account_ids:
            parameters["AWSAccountId.%s" % i] = name
        d = self._request(parameters, resource=resource)
        d.addCallback(self._addPermissionCallback)
        d.addErrback(self._genericErrback)
        return d  
        
    def _addPermissionCallback(self, data):
        return True

    def removePermission(self, resource, label):
        parameters = {
            "Action":"RemovePermission",
            "Label":label
        }
        d = self._request(parameters, resource=resource)
        d.addCallback(self._removePermissionCallback)
        d.addErrback(self._genericErrback)
        return d  

    def _removePermissionCallback(self, data):
        return True

    def sendMessage(self, resource, message):
        parameters = {
            "Action":"SendMessage",
            "MessageBody":message
        }
        d = self._request(parameters, resource=resource)
        return d  
        
#   
#    def changeMessageVisibility(self):
#        parameters = {
#            "Action":"ChangeMessageVisibility"
#        }
#        d = self._request(parameters)
#        return d    
#        
#  
#        
#    def deleteMessage(self):
#        parameters = {
#            "Action":"DeleteMessage"
#        }
#        d = self._request(parameters)
#        return d    
#        

#    def receiveMessage(self):
#        parameters = {
#            "Action":"ReceiveMessage"
#        }
#        d = self._request(parameters)
#        return d    
#        

#        

#          
       
    def _genericErrback(self, error):
        if hasattr(error, "value"):
            if hasattr(error.value, "response"):
                xml = ET.fromstring(error.value.response)
                message = xml.find(".//%sMessage" % SQS_NAMESPACE).text
                raise Exception(message)
        return error        
        
    def _canonicalize(self, parameters):
        """
        Canonicalize parameters for use with AWS Authorization.
        
        Arguments:
        parameters -- Key value pairs of parameters
        
        Returns:
        A safe-quoted string representation of the parameters.
        """
        parameters = parameters.items()
        # Alphebetize key-value pairs.
        parameters.sort(lambda x,y:cmp(x[0],y[0]))
        # Safe-quote and combine parameters into a string
        return "&".join(map(_safe_quote_tuple, parameters))

    def _request(self, parameters, method="GET", resource="/"):
        """
        Add authentication parameters and make request to Amazon.
        
        Arguments:
        parameters -- Key value pairs of parameters
        """

        parameters = self._getAuthorization(method, parameters, resource=resource)
            
        query_string = urllib.urlencode(parameters)
 
        url = "https://%s%s?%s" % (self.host, resource, query_string)
        
        print url
        
        d = self.rq.getPage(url, method=method)
        return d

    def _getAuthorization(self, method, parameters, resource="/"):
        """
        Create authentication parameters.
        
        Arguments:
        method -- HTTP method of the request
        parameters -- Key value pairs of parameters
        
        Returns:
        A dictionary of authorization parameters
        """
        expires = datetime.utcnow() + timedelta(30)
        signature_parameters = { 
            "AWSAccessKeyId":self.aws_access_key_id, 
            "SignatureVersion":"2", 
            "SignatureMethod":"HmacSHA256", 
            'Expires':"%s+00:00" % expires.isoformat()[0:19],
            "AWSAccessKeyId":self.aws_access_key_id, 
            "Version":"2009-02-01"
        }
        signature_parameters.update(parameters)
        qs = self._canonicalize(signature_parameters)
        string_to_sign = "%(method)s\n%(host)s\n%(resource)s\n%(qs)s" % {
            "method":method,
            "host":self.host.lower(),
            "resource":resource,
            "qs":qs,
        }
        args = [self.aws_secret_access_key, string_to_sign, hashlib.sha256]
        signature = base64.encodestring(hmac.new(*args).digest()).strip()
        signature_parameters.update({'Signature':signature})
        return signature_parameters

class AmazonS3:
    
    """
    Amazon Simple Storage Service API.
    """
    
    ACCEPTABLE_ERROR_CODES = [400, 403, 404, 409]
    host = "s3.amazonaws.com"
    
    def __init__(self, aws_access_key_id, aws_secret_access_key, rq=None):
        """
        Arguments:
        aws_access_key_id -- Amazon AWS access key ID
        aws_secret_access_key -- Amazon AWS secret access key
        
        Keyword arguments:
        rq -- Optional RequestQueuer object.
        """
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
        self.rq.setHostMaxRequestsPerSecond(self.host, 0)
        self.rq.setHostMaxSimultaneousRequests(self.host, 0)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        
    def emptyBucket(self, bucket):
        """
        Delete all items in a bucket.
        
        Arguments:
        bucket -- Bucket name
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
            d.addCallback(self._emptyBucketCallback2)
        else:
            d.addCallback(self._emptyBucketCallbackRepeat, bucket)
        return d
    
    def _emptyBucketCallbackRepeat(self, data, bucket):
        return self.emptyBucket(bucket)
    
    def _emptyBucketCallback2(self, result):
        return True
    
    def getBucket(self, bucket):
        """
        List information about the bucket.
        
        Arguments:
        bucket -- Bucket name
        """
        bucket = convertToUTF8(bucket)
        headers = self._getAuthorization("GET", "", "", {}, "/" + bucket)
        url = "http://%s/%s" % (self.host, bucket)
        d = self.rq.getPage(url, method="GET", headers=headers)
        d.addErrback(self._genericErrback, url, method="GET", headers=headers)
        return d        

    def putBucket(self, bucket):
        """
        Create a bucket.
        
        Arguments:
        bucket -- Bucket name
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
        Delete a bucket.
        
        Arguments:
        bucket -- Bucket name
        """
        bucket = convertToUTF8(bucket)
        headers = {
            "Content-Length": 0
        }
        auth = self._getAuthorization("DELETE", "", "", headers, "/" + bucket)
        headers.update(auth)
        url = "http://%s/%s" % (self.host, bucket)
        d = self.rq.getPage(url, method="DELETE", headers=headers)
        d.addErrback(self._genericErrback, url, method="DELETE", headers=headers)
        return d

    def headObject(self, bucket, key):
        """
        Get an object's headers.
        
        Arguments:
        bucket -- Bucket name
        key -- Key name
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
        keys = headers.keys()
        values = headers.values()
        meta = "x-amz-meta-"
        return dict(zip(map(lambda x:x.replace(meta,""), keys), values))

    def getObject(self, bucket, key):
        """
        Get an object.
        
        Arguments:
        bucket -- Bucket name
        key -- Key name
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
        keys = headers.keys()
        values = headers.values()
        meta = "x-amz-meta-"
        return dict(zip(map(lambda x:"%s%s" % (meta, x), keys), values))
        
    def putObject(self, bucket, key, data, content_type="text/html", public=True, headers=None, gzip=False):
        """
        Put an object.
        
        Arguments:
        bucket -- Bucket name
        key -- Key name
        data -- Data string
        
        Keyword Arguments:
        content_type = Content type header (Default 'text/html')
        public = Boolean flag representing access (Default True)
        headers = Custom header dictionary (Default empty dictionary)
        gzip -- Boolean flag to gzip data (Default False)
        """
        data = convertToUTF8(data) 
        bucket = convertToUTF8(bucket)
        key = convertToUTF8(key)
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
        auth = self._getAuthorization("PUT", content_md5, content_type, headers, path)
        headers.update(auth)
        url = "http://%s/%s/%s" % (self.host, bucket, key)
        d = self.rq.getPage(url, method="PUT", headers=headers, postdata=data)
        d.addErrback(self._genericErrback, url, method="PUT", headers=headers, postdata=data)
        return d

    def deleteObject(self, bucket, key):
        """
        Delete an object.
        
        Arguments:
        bucket -- Bucket name
        key -- Key name
        """
        bucket = convertToUTF8(bucket)
        key = convertToUTF8(key)
        path = "/" + bucket + "/" + key
        headers = self._getAuthorization("DELETE", "", "", {}, path)
        url = "http://%s/%s/%s" % (self.host, bucket, key)
        d = self.rq.getPage(url, method="DELETE", headers=headers)
        d.addErrback(self._genericErrback, url, method="DELETE", headers=headers)
        return d
            
    def _genericErrback(self, error, url, method="GET", headers=None, postdata=None, count=0):
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
            elif int(error.value.status) not in self.ACCEPTABLE_ERROR_CODES and count < 3:
                d = self.rq.getPage(url,        
                                    method=method, 
                                    headers=headers, 
                                    postdata=postdata
                                    )
                                    
                d.addErrback(   self._genericErrback, 
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
        
        Arguments:
        headers -- Key value pairs of headers
        
        Returns:
        A string representation of the parameters.
        """
        keys = [k for k in headers.keys() if k.startswith("x-amz-")]
        keys.sort(key = str.lower)
        return "\n".join(map(lambda key:key + ":" + headers[key], keys))

    def _getAuthorization(self, method, content_hash, content_type, headers, resource):
        """
        Create authentication headers.
        
        Arguments:
        method -- HTTP method of the request
        parameters -- Key value pairs of parameters
        
        Returns:
        A dictionary of authorization parameters
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
 
class AmazonSDB:
    
    """
    Amazon Simple Database API.
    """
    
    host = "sdb.amazonaws.com"
    box_usage = 0.0
    
    def __init__(self, aws_access_key_id, aws_secret_access_key, rq=None):
        """
        Arguments:
        aws_access_key_id -- Amazon AWS access key ID
        aws_secret_access_key -- Amazon AWS secret access key
        
        Keyword arguments:
        rq -- Optional RequestQueuer object.
        """
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.rq.setHostMaxRequestsPerSecond(self.host, 0)
        self.rq.setHostMaxSimultaneousRequests(self.host, 0)

    def createDomain(self, domain):
        """
        Create a SimpleDB domain.
        
        Arguments:
        domain -- Domain name
        """
        parameters = {}
        parameters["Action"] = "CreateDomain"
        parameters["DomainName"] = domain
        d = self._request(parameters)
        d.addCallback(self._createDomainCallback)
        return d

    def _createDomainCallback(self, data):
        xml = ET.fromstring(data["response"])
        self.box_usage += float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        return True
    
    def deleteDomain(self, domain):
        """
        Delete a SimpleDB domain.
        
        Arguments:
        domain -- Domain name
        """
        parameters = {}
        parameters["Action"] = "DeleteDomain"
        parameters["DomainName"] = domain
        d = self._request(parameters)
        d.addCallback(self._deleteDomainCallback)
        return d

    def _deleteDomainCallback(self, data):
        xml = ET.fromstring(data["response"])
        self.box_usage += float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        return True    

    def listDomains(self):
        """
        List SimpleDB domains associated with an account.
        """
        return self._listDomains()
    
    def _listDomains(self, next_token=None, previous_results=None):
        parameters = {}
        parameters["Action"] = "ListDomains"
        if next_token is not None:
            parameters["NextToken"] = next_token
        d = self._request(parameters)
        d.addCallback(self._listDomainsCallback, previous_results=previous_results)
        return d

    def _listDomainsCallback(self, data, previous_results=None):
        xml = ET.fromstring(data["response"])
        self.box_usage += float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        xml_response = _etree_to_dict(xml, namespace=SDB_NAMESPACE)
        if "DomainName" in xml_response["ListDomainsResult"][0]:
            results = xml_response["ListDomainsResult"][0]["DomainName"]
        else:
            results = []
        if previous_results is not None:
            results.extend(previous_results)
        if "NextToken" in xml_response["ListDomainsResult"]:
            next_token = xml_response["ListDomainsResult"][0]["NextToken"][0]
            return self._listDomains(next_token=next_token, previous_results=results)
        return results

    def domainMetadata(self, domain):
        """
        Return meta-information about a domain.
        
        Arguments:
        domain -- Domain name
        """
        parameters = {}
        parameters["Action"] = "DomainMetadata"
        parameters["DomainName"] = domain
        d = self._request(parameters)
        d.addCallback(self._domainMetadataCallback)
        return d
        
    def _domainMetadataCallback(self, data):
        xml = ET.fromstring(data["response"])
        self.box_usage += float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        xml_response = _etree_to_dict(xml, namespace=SDB_NAMESPACE)
        return xml_response["DomainMetadataResult"][0]
    
    def putAttributes(self, domain, item_name, attributes, replace=None):
        """
        Put attributes into domain at item_name.
        
        Arguments:
        domain -- Domain name
        item_name -- Item name
        attributes -- Dictionary of attributes
        
        Keyword Arguments:
        replace -- List of attributes that should be overwritten (Default empty list)
        """
        if replace is None:
            replace = []
        if not isinstance(replace, list):
            raise Exception("Replace argument must be a list.")
        if not isinstance(attributes, dict):
            raise Exception("Attributes argument must be a dictionary.")
        parameters = {}
        parameters["Action"] = "PutAttributes"
        parameters["DomainName"] = domain
        parameters["ItemName"] = item_name
        attributes_list = []
        for attribute in attributes.items():
            # If the attribute is a list, split into multiple attributes.
            if isinstance(attribute[1], list):
                for value in attribute[1]:
                    attributes_list.append((attribute[0], value))
            else:
                attributes_list.append(attribute)
        i = 0
        for attribute in attributes_list:
            parameters["Attribute.%s.Name" % i] = attribute[0]
            parameters["Attribute.%s.Value" % i] = attribute[1]
            if attribute[0] in replace:
                parameters["Attribute.%s.Replace" % i] = "true"
            i += 1
        d = self._request(parameters)
        d.addCallback(self._putAttributesCallback)
        return d
        
    def _putAttributesCallback(self, data):
        xml = ET.fromstring(data["response"])
        self.box_usage += float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        return True
    
    def getAttributes(self, domain, item_name, attribute_name=None):
        """
        Get one or all attributes from domain at item_name.
        
        Arguments:
        domain -- Domain name
        item_name -- Item name
        
        Keyword Arguments:
        attribute_name -- Name of specific attribute to get (Default None)
        """
        parameters = {}
        parameters["Action"] = "GetAttributes"
        parameters["DomainName"] = domain
        parameters["ItemName"] = item_name
        if attribute_name is not None:
            parameters["AttributeName"] = attribute_name
        d = self._request(parameters)
        d.addCallback(self._getAttributesCallback)
        return d
        
    def _getAttributesCallback(self, data):
        xml = ET.fromstring(data["response"])
        self.box_usage += float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        xml_response = _etree_to_dict(xml, namespace=SDB_NAMESPACE)
        attributes = {}
        if xml_response["GetAttributesResult"][0] is None:
            raise Exception("Item does not exist.")
        for attribute in xml_response["GetAttributesResult"][0]['Attribute']:
            if attribute["Name"][0] not in attributes:
                attributes[attribute["Name"][0]] = []
            attributes[attribute["Name"][0]].extend(attribute["Value"])
        return attributes

    def delete(self, domain, item_name):
        """
        Delete all attributes from domain at item_name.
        
        Arguments:
        domain -- Domain name
        item_name -- Item name
        """
        return self.deleteAttributes(domain, item_name)

    def deleteAttributes(self, domain, item_name, attributes=None):
        """
        Delete one or all attributes from domain at item_name.
        
        Arguments:
        domain -- Domain name
        item_name -- Item name
        
        Keyword Arguments:
        attributes -- List of attribute names, or dictionary of 
        attribute name / value pairs. (Default empty dict)
        """        
        if attributes is None:
            attributes = {}
        if not isinstance(attributes, dict) and not isinstance(attributes, list):
            raise Exception("Attributes parameter must be a dictionary or a list.")
        parameters = {}
        parameters["Action"] = "DeleteAttributes"
        parameters["DomainName"] = domain
        parameters["ItemName"] = item_name
        if isinstance(attributes, dict):
            i = 0
            for key in attributes:
                parameters["Attribute.%s.Name" % i] = key
                parameters["Attribute.%s.Value" % i] = attributes[key]
                i += 1
        if isinstance(attributes, list):
            i = 0
            for key in attributes:
                parameters["Attribute.%s.Name" % i] = key
                i += 1
        d = self._request(parameters)
        d.addCallback(self._deleteAttributesCallback)
        return d

    def _deleteAttributesCallback(self, data):
        xml = ET.fromstring(data["response"])
        self.box_usage += float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        return True
    
    def select(self, select_expression):
        """
        Run a select query
        
        Arguments:
        select_expression -- Select expression
        """    
        return self._select(select_expression)
        
    def _select(self, select_expression, next_token=None, previous_results=None):
        parameters = {}
        parameters["Action"] = "Select"
        parameters["SelectExpression"] = select_expression
        if next_token is not None:
            parameters["NextToken"] = next_token
        d = self._request(parameters)
        d.addCallback(self._selectCallback, select_expression=select_expression, previous_results=previous_results)
        return d

    def _selectCallback(self, data, select_expression=None, previous_results=None):
        if previous_results is not None:
            results = previous_results
        else:
            results = {}
        xml = ET.fromstring(data["response"])
        self.box_usage += float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        next_token_element = xml.find(".//%sNextToken" % SDB_NAMESPACE)
        if next_token_element is not None:
            next_token = next_token_element.text
        else:
            next_token = None
        items = xml.findall(".//%sItem" % SDB_NAMESPACE)
        results = {}
        for item in items:
            name = item.find("%sName" % SDB_NAMESPACE).text
            attributes = item.findall("%sAttribute" % SDB_NAMESPACE)
            attribute_dict = {}
            for attribute in attributes:
                name = attribute.find("%sName" % SDB_NAMESPACE).text
                value = attribute.find("%sValue" % SDB_NAMESPACE).text
                if name in attribute_dict:   
                    attribute_dict[name].append(value)
                else:
                    attribute_dict[name] = [value]
            results[name] = attribute_dict
        if next_token is not None:
            return self._select(select_expression, next_token=next_token, previous_results=results)
        return results        

    def _request(self, parameters):
        """
        Add authentication parameters and make request to Amazon.
        
        Arguments:
        parameters -- Key value pairs of parameters
        """
        parameters = self._getAuthorization("GET", parameters)
        query_string = urllib.urlencode(parameters)
        url = "https://%s/?%s" % (self.host, query_string)
        d = self.rq.getPage(url, method="GET")
        return d
          
    def _canonicalize(self, parameters):
        """
        Canonicalize parameters for use with AWS Authorization.
        
        Arguments:
        parameters -- Key value pairs of parameters
        
        Returns:
        A safe-quoted string representation of the parameters.
        """
        parameters = parameters.items()
        parameters.sort(lambda x,y:cmp(x[0],y[0]))
        return "&".join(map(_safe_quote_tuple, parameters))
        
    def _getAuthorization(self, method, parameters):
        """
        Create authentication parameters.
        
        Arguments:
        method -- HTTP method of the request
        parameters -- Key value pairs of parameters
        
        Returns:
        A dictionary of authorization parameters
        """
        signature_parameters = { 
            "AWSAccessKeyId":self.aws_access_key_id, 
            "SignatureVersion":"2", 
            "SignatureMethod":"HmacSHA256", 
            'Timestamp':datetime.utcnow().isoformat()[0:19]+"+00:00", 
            "AWSAccessKeyId":self.aws_access_key_id, 
            "Version":"2007-11-07" 
        }
        signature_parameters.update(parameters)
        qs = self._canonicalize(signature_parameters)
        string_to_sign = "%(method)s\n%(host)s\n%(resource)s\n%(qs)s" % {
            "method":method,
            "host":self.host.lower(),
            "resource":"/",
            "qs":qs,
        }
        args = [self.aws_secret_access_key, string_to_sign, hashlib.sha256]
        signature = base64.encodestring(hmac.new(*args).digest()).strip()
        signature_parameters.update({'Signature': signature})
        return signature_parameters












