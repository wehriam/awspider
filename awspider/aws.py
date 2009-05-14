
from unicodeconverter import convertToUTF8
from pagegetter import RequestQueuer
from time import strftime, gmtime
from datetime import datetime
import base64, hmac, sha, md5, urllib

import hashlib

import urllib

import xml.etree.cElementTree as ET

import time

import cStringIO
import gzip as gzip_package

import dateutil.parser

def sdb_now(offset=0):
    return str(int(offset + time.time())).zfill(11)

def sdb_now_add( i, offset=0 ):
    return str(int(offset + time.time() + i) ).zfill(11)

def sdb_parse_time(s, offset=0): # Parse a string into a SDB formatted timestamp
    return str(int(offset + time.mktime(dateutil.parser.parse( s ).timetuple()))).zfill(11)
    

    
def sdb_latitude( latitude ):
    adjusted = ( 90 + float( latitude ) ) * 100000
    return str( int( adjusted ) ).zfill(8)

def sdb_longitude( longitude ):
    adjusted = ( 180 + float( longitude) ) * 100000
    return str( int( adjusted ) ).zfill(8)

class AmazonS3:
    
    ACCEPTABLE_ERROR_CODES = [400, 403, 404, 409]
    
    def __init__(self, aws_access_key_id, aws_secret_access_key, rq=None ):
        
        # We're using a centralized Request Queuer when possible so as to not overwhelm the system.
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
        
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
    

    
    def getBucket( self, bucket ):
        bucket = convertToUTF8( bucket )
        host = "%s.s3.amazonaws.com" % bucket
        headers = {
            "Host": host
        }
        headers.update( self._getAuthorizationHeaders("GET", "", "", headers, "/" + bucket + "/") )
        url = "http://%s/" % host
        d = self.rq.getPage(url, method="GET", headers=headers )
        
        # Errback will try again a few times if it gets unusual (ie 503) errors.
        d.addErrback( self._genericErrback, url, method="GET", headers=headers )
        return d          

    def putBucket( self, bucket ):
        bucket = convertToUTF8( bucket )
        host = "%s.s3.amazonaws.com" % bucket
        headers = {
            "Host": host,
            "Content-Length": 0
        }
        headers.update( self._getAuthorizationHeaders("PUT", "", "", headers, "/" + bucket + "/") )
        url = "http://%s/" % host
        d = self.rq.getPage( url, method="PUT", headers=headers )
        
        # Errback will try again a few times if it gets unusual (ie 503) errors.
        d.addErrback( self._genericErrback, url, method="PUT", headers=headers )
        return d

    def deleteBucket( self, bucket ):
        bucket = convertToUTF8( bucket )
        host = "%s.s3.amazonaws.com" % bucket
        headers = {
            "Host": host,
            "Content-Length": 0
        }
        headers.update( self._getAuthorizationHeaders("DELETE", "", "", headers, "/" + bucket + "/") )
        url = "http://%s/" % host
        d = self.rq.getPage( url, method="DELETE", headers=headers )

        # Errback will try again a few times if it gets unusual (ie 503) errors.
        d.addErrback( self._genericErrback, url, method="DELETE", headers=headers )
        return d

    def headObject( self, bucket, key ):

        bucket = convertToUTF8( bucket )
        key = convertToUTF8( key )

        host = "%s.s3.amazonaws.com" % bucket
        headers = {
            "Host": host
        }
        headers.update( self._getAuthorizationHeaders("HEAD", "", "", headers, "/" + bucket + "/" + key ) )
        url = "http://%s/%s" % (host, key)
        d = self.rq.getPage(url, method="HEAD", headers=headers )

        # Errback will try again a few times if it gets unusual (ie 503) errors.
        d.addCallback( self._getObjectCallback )
        d.addErrback( self._genericErrback, url, method="HEAD", headers=headers )
        return d    

    def _headObjectCallback( self, data ):
        data["headers"] = dict( zip( map(lambda x:x.replace("x-amz-meta-",""), data["headers"].keys() ), data["headers"].values() ) )
        return data    

    def getObject( self, bucket, key ):
        
        bucket = convertToUTF8( bucket )
        key = convertToUTF8( key )
        
        host = "%s.s3.amazonaws.com" % bucket
        headers = {
            "Host": host
        }
        headers.update( self._getAuthorizationHeaders("GET", "", "", headers, "/" + bucket + "/" + key ) )
        url = "http://%s/%s" % (host, key)
        d = self.rq.getPage(url, method="GET", headers=headers )

        # Errback will try again a few times if it gets unusual (ie 503) errors.
        d.addCallback( self._getObjectCallback )
        d.addErrback( self._genericErrback, url, method="GET", headers=headers )
        return d    

    def _getObjectCallback( self, data ):
        
        if "content-encoding" in data["headers"]:
            if "gzip" in data["headers"]["content-encoding"]:
                compressedstream = cStringIO.StringIO( data["response"] ) 
                zfile = gzip_package.GzipFile( fileobj=compressedstream )
                data["response"] = zfile.read()
        
        data["headers"] = dict( zip( map(lambda x:x.replace("x-amz-meta-",""), data["headers"].keys() ), data["headers"].values() ) )
        return data
    
    def putObject( self, bucket, key, data, contentType=None, public=True, headers=None, gzip=False ):
        
        if contentType is None:
            contentType = "text/html"
        
        data = convertToUTF8( data ) 
        bucket = convertToUTF8( bucket )
        key = convertToUTF8( key )
        
        host = "%s.s3.amazonaws.com" % bucket
        
        if headers is None:
            headers = {}
        
        # Wrap any user-defined headers in with the Amazon custom header prefix.
        headers = dict( zip( map(lambda x:"x-amz-meta-" + x, headers.keys() ), headers.values() ) )
        
        if gzip:
            # Gzip that bastard!
            headers["content-encoding"] = "gzip"
            zbuf = cStringIO.StringIO()
            zfile = gzip_package.GzipFile(None, 'wb', 9, zbuf)
            zfile.write( data )
            zfile.close()
            data =  zbuf.getvalue()
        
        content_md5 = base64.encodestring( md5.new( data ).digest() ).strip()
        
        headers['x-amz-acl'] = 'public-read'
        
        #authorization_headers = self._getAuthorizationHeaders( 'PUT', content_md5, contentType, headers, "/" + bucket + "/" + key )
        generated_headers = { 'Content-Length': len( data ), 'Content-Type': contentType, 'Content-MD5': content_md5 }
        
        #headers.update( authorization_headers )
        headers.update( generated_headers )
        
        headers.update( self._getAuthorizationHeaders("PUT", content_md5, contentType, headers, "/" + bucket + "/" + key ) )
        
        url = "http://%s.s3.amazonaws.com/%s" % ( bucket, key )

        d = self.rq.getPage( url, method="PUT", headers=headers, postdata=data )

        d.addErrback( self._genericErrback, url, method="PUT", headers=headers, postdata=data )
        return d

    def deleteObject( self, bucket, key ):

        bucket = convertToUTF8( bucket )
        key = convertToUTF8( key )

        host = "%s.s3.amazonaws.com" % bucket
        headers = {
            "Host": host
        }
        headers.update( self._getAuthorizationHeaders("DELETE", "", "", headers, "/" + bucket + "/" + key ) )
        url = "http://%s/%s" % (host, key)
        d = self.rq.getPage(url, method="DELETE", headers=headers )

        # Errback will try again a few times if it gets unusual (ie 503) errors.
        d.addErrback( self._genericErrback, url, method="DELETE", headers=headers )
        return d
            
    def _genericErrback( self, error, url, method="GET", headers=None, postdata=None, count=0 ):
        
        if headers is None:
            headers = {}
        
        if "status" in error.value.__dict__:
            # 204, empty response but otherwise OK, as in the case of a delete.
            # Move on, nothing to see here.
            if int(error.value.status) == 204:
                return { "response":error.value.response, "status":int(error.value.status), "headers":error.value.response, "message":error.value.headers }

            # Something other than a 40x error. Something is wrong, but let's try
            # that again a few times.           
            elif int(error.value.status) not in self.ACCEPTABLE_ERROR_CODES and count < 3:

                d = self.rq.getPage( url, method=method, headers=headers, postdata=postdata )
                d.addErrback( self._genericErrback, url, method=method, headers=headers, postdata=postdata, count=count + 1 )
                return d

            # 404 or other normal error, pass it along.
            else:
                return error    
        else:
            return error
                
    def _canonalizeAmazonHeaders( self, headers ):
        keys = [k for k in headers.keys() if k.startswith("x-amz-")]
        keys.sort( key = str.lower)
        return "\n".join( map( lambda key: key + ":" + headers.get( key ), keys ) )

    def _getAuthorizationHeaders( self, method, contentHash, contentType, headers, resource ):
        date = strftime( u"%a, %d %b %Y %H:%M:%S +0000", gmtime() )
        amazon_headers = self._canonalizeAmazonHeaders( headers )
        if len( amazon_headers ):
            data = "%s\n%s\n%s\n%s\n%s\n%s" % ( method, contentHash, contentType, date, amazon_headers, resource )
        else:
            data = "%s\n%s\n%s\n%s\n%s%s" % ( method, contentHash, contentType, date, amazon_headers, resource )            
        authorization = "AWS %s:%s" % ( self.aws_access_key_id, base64.encodestring( hmac.new( self.aws_secret_access_key, data, sha ).digest() ).strip() )
        return { 'Authorization': authorization, 'Date': date }


 
 
class AmazonSDB:
    
    ACCEPTABLE_ERROR_CODES = []
    
    def __init__(self, aws_access_key_id, aws_secret_access_key, rq=None ):
        
        # We're using a centralized Request Queuer when possible so as to not overwhelm the system.
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
        
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.host = "sdb.amazonaws.com"
        self.box_usage = 0.0
        
    def request( self, parameters ):
        parameters = self._getAuthorization("GET", parameters )
        query_string = urllib.urlencode(parameters)
        url = "https://%s/?%s" % (self.host, query_string)
        d = self.rq.getPage(url, method="GET" )
        return d

    def etree_to_dict( self, etree ):
        children = etree.getchildren()
        if len(children) == 0:
            return etree.text
        children_dict = {}
        for element in children:
            tag = element.tag.replace("{http://sdb.amazonaws.com/doc/2007-11-07/}", "")
            if tag in children_dict:
                children_dict[ tag ].append( self.etree_to_dict( element ) )
            else:
                children_dict[ tag ] = [ self.etree_to_dict( element ) ]
        return children_dict

    def createDomain( self, domain ):
        parameters = {}
        parameters["Action"] = "CreateDomain"
        parameters["DomainName"] = domain
        d = self.request(parameters)
        d.addCallback( self._createDomainCallback )
        return d

    def _createDomainCallback( self, data ):
        
        xml = ET.fromstring(data["response"])
        self.box_usage += float( xml.find(".//{http://sdb.amazonaws.com/doc/2007-11-07/}BoxUsage").text )
        
        return True
    
    def deleteDomain( self, domain ):
        parameters = {}
        parameters["Action"] = "DeleteDomain"
        parameters["DomainName"] = domain
        d = self.request(parameters)
        d.addCallback( self._deleteDomainCallback )
        return d

    def _deleteDomainCallback( self, data ):
        
        xml = ET.fromstring(data["response"])
        self.box_usage += float( xml.find(".//{http://sdb.amazonaws.com/doc/2007-11-07/}BoxUsage").text )
        
        return True    

    def listDomains( self, next_token=None, previous_results=None ):
        parameters = {}
        parameters["Action"] = "ListDomains"
        if next_token is not None:
            parameters["NextToken"] = next_token
        d = self.request(parameters)
        d.addCallback( self._listDomainsCallback, previous_results=previous_results )
        return d

    def _listDomainsCallback( self, data, previous_results=None ):
        
        
        xml = ET.fromstring(data["response"])
        self.box_usage += float( xml.find(".//{http://sdb.amazonaws.com/doc/2007-11-07/}BoxUsage").text )
        
        xml_response = self.etree_to_dict( xml )

        if "DomainName" in xml_response["ListDomainsResult"][0]:
            results = xml_response["ListDomainsResult"][0]["DomainName"]
        else:
            results = []
        
        if previous_results is not None:
            results.extend( previous_results )
        
        if "NextToken" in xml_response["ListDomainsResult"]:
            return self.listDomains( next_token=xml_response["ListDomainsResult"][0]["NextToken"][0], previous_results=results )
        
        return results

    def domainMetadata( self, domain ):
        parameters = {}
        parameters["Action"] = "DomainMetadata"
        parameters["DomainName"] = domain
        d = self.request(parameters)
        d.addCallback( self._domainMetadataCallback )
        return d
        
    def _domainMetadataCallback( self, data):
        
        xml = ET.fromstring(data["response"])
        self.box_usage += float( xml.find(".//{http://sdb.amazonaws.com/doc/2007-11-07/}BoxUsage").text )
        
        xml_response = self.etree_to_dict( xml )
        return xml_response["DomainMetadataResult"][0]
    
    def putAttributes(self, domain, item_name, attributes, replace=None ):
        
        if replace is None:
            replace = []

        if not isinstance(replace, list):
            raise Exception("Replace parameter must be a list.")
        
        if not isinstance(attributes, dict):
            raise Exception("Attributes parameter must be a dictionary.")
        
        parameters = {}
        parameters["Action"] = "PutAttributes"
        parameters["DomainName"] = domain
        parameters["ItemName"] = item_name
        
        attributes_list = []
        for attribute in attributes.items():
            if isinstance(attribute[1], list):
                for value in attribute[1]:
                    attributes_list.append( (attribute[0], value) )
            else:
                attributes_list.append( attribute )
        
        i = 0
        for attribute in attributes_list:
            parameters["Attribute.%s.Name" % i] = attribute[0]
            parameters["Attribute.%s.Value" % i] = attribute[1]
            if attribute[0] in replace:
                parameters["Attribute.%s.Replace" % i] = "true"
            i += 1
        
        d = self.request(parameters)
        d.addCallback( self._putAttributesCallback )
        return d
        
    def _putAttributesCallback( self, data ):
        
        xml = ET.fromstring(data["response"])
        self.box_usage += float( xml.find(".//{http://sdb.amazonaws.com/doc/2007-11-07/}BoxUsage").text )
        
        return True
    
    def getAttributes( self, domain, item_name, attribute_name=None ):
        parameters = {}
        parameters["Action"] = "GetAttributes"
        parameters["DomainName"] = domain
        parameters["ItemName"] = item_name
        if attribute_name is not None:
            parameters["AttributeName"] = attribute_name
        d = self.request(parameters)
        d.addCallback( self._getAttributesCallback )
        return d
        
    def _getAttributesCallback( self, data ):
        print data
        xml = ET.fromstring(data["response"])
        self.box_usage += float( xml.find(".//{http://sdb.amazonaws.com/doc/2007-11-07/}BoxUsage").text )
        
        xml_response = self.etree_to_dict( xml )
        attributes = {}

        if xml_response["GetAttributesResult"][0] is None:
            raise Exception("Item does not exist.")
            
        for attribute in xml_response["GetAttributesResult"][0]['Attribute']:
            if attribute["Name"][0] not in attributes:
                attributes[ attribute["Name"][0] ] = []
            attributes[ attribute["Name"][0] ].extend( attribute["Value"] )
        return attributes

    def delete( self, domain, item_name ):
        return self.deleteAttributes(domain, item_name )

    def deleteAttributes(self, domain, item_name, attributes=None ):
        
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
                parameters["Attribute.%s.Value" % i] = attributes[ key ]
                i += 1
                
        if isinstance(attributes, list):
            i = 0
            for key in attributes:
                parameters["Attribute.%s.Name" % i] = key
                i += 1
        
        d = self.request(parameters)
        d.addCallback( self._deleteAttributesCallback )
        return d

    def _deleteAttributesCallback( self, data ):
        
        xml = ET.fromstring(data["response"])
        self.box_usage += float( xml.find(".//{http://sdb.amazonaws.com/doc/2007-11-07/}BoxUsage").text )
        
        return True
    
    def select( self, select_expression, next_token=None, previous_results=None ):
        parameters = {}
        parameters["Action"] = "Select"
        parameters["SelectExpression"] = select_expression
        if next_token is not None:
            parameters["NextToken"] = next_token
        d = self.request(parameters)
        d.addCallback( self._selectCallback, select_expression=select_expression, previous_results=previous_results )
        return d

    def _selectCallback( self, data, select_expression=None, previous_results=None ):
        
        if previous_results is not None:
            results = previous_results
        else:
            results = {}
            
        xml = ET.fromstring(data["response"])
        self.box_usage += float( xml.find(".//{http://sdb.amazonaws.com/doc/2007-11-07/}BoxUsage").text )
                
        next_token_element = xml.find(".//{http://sdb.amazonaws.com/doc/2007-11-07/}NextToken")
        
        if next_token_element is not None:
            next_token = next_token_element.text
        else:
            next_token = None
            
        items = xml.findall(".//{http://sdb.amazonaws.com/doc/2007-11-07/}Item")
        results = {}
        for item in items:
            name = item.find("{http://sdb.amazonaws.com/doc/2007-11-07/}Name").text
            attributes = item.findall("{http://sdb.amazonaws.com/doc/2007-11-07/}Attribute")
            attribute_dict = {}
            for attribute in attributes:
                attribute_name = attribute.find("{http://sdb.amazonaws.com/doc/2007-11-07/}Name").text
                attribute_value = attribute.find("{http://sdb.amazonaws.com/doc/2007-11-07/}Value").text
                if attribute_name in attribute_dict:   
                    attribute_dict[ attribute_name ].append( attribute_value )
                else:
                    attribute_dict[ attribute_name ] = [ attribute_value ]
            results[ name ] = attribute_dict
        
        if next_token is not None:
            return self.select( select_expression, next_token=next_token, previous_results=results )

        return results        
    
    def safe_quote( self, s ):
        return urllib.quote( str(s), '-_.~' )
        
    def _canonicalize( self, parameters ):
        parameters = parameters.items()
        parameters.sort( lambda x,y:cmp(x[0],y[0]) )
        return "&".join( map( lambda x:"%s=%s" % ( self.safe_quote(x[0]), self.safe_quote(x[1]) ), parameters) )
        
    def _getAuthorization( self, method, parameters ):
        signature_parameters = { "AWSAccessKeyId":self.aws_access_key_id, "SignatureVersion":"2", "SignatureMethod":"HmacSHA256", 'Timestamp':datetime.utcnow().isoformat()[0:19]+"+00:00", "AWSAccessKeyId":self.aws_access_key_id, "Version":"2007-11-07" }
        signature_parameters.update( parameters )
        qs = self._canonicalize( signature_parameters )
        string_to_sign = "%(method)s\n%(host)s\n%(resource)s\n%(qs)s" % {
            "method":method,
            "host":self.host.lower(),
            "resource":"/",
            "qs":qs,
        }
          
        signature = base64.encodestring( hmac.new( self.aws_secret_access_key, string_to_sign, hashlib.sha256 ).digest() ).strip()
        signature_parameters.update( {'Signature': signature})
        return signature_parameters












