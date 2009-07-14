import base64
import hmac
import hashlib
import urllib
import xml.etree.cElementTree as ET
from datetime import datetime
import time
import dateutil.parser
from ..pagegetter import RequestQueuer
from .lib import etree_to_dict, safe_quote_tuple


SDB_NAMESPACE = "{http://sdb.amazonaws.com/doc/2009-04-15/}"


def sdb_now(offset=0):
    """Return an 11 character, zero padded string with the current Unixtime.
    
    **Keyword arguments:**
     * *offset* -- Offset in seconds. (Default 0)
    """
    return str(int(offset + time.time())).zfill(11)


def sdb_now_add(seconds, offset=0):
    """Return an 11 character, zero padded string with the current Unixtime
    plus an integer.
   
    **Arguments:**
     * *seconds* -- Seconds to add to the current time.
   
    **Keyword arguments:**
     * *offset* -- Offset in seconds. (Default 0)
    """
    return str(int(offset + time.time() + seconds)).zfill(11)


def sdb_parse_time(date_string, offset=0):
    """Parse a date string, then return an 11 character, zero padded
    string with the current Unixtime plus an integer.
   
    **Arguments:**
     * *date_string* -- Date string
   
    **Keyword arguments:**
     * *offset* -- Offset in seconds. (Default 0)
    """
    parsed_time = time.mktime(dateutil.parser.parse(date_string).timetuple())
    return str(int(offset + parsed_time)).zfill(11)


def sdb_latitude(latitude):
    """Return an 8 character, zero padded string version of the
    latitude parameter.
   
    **Arguments:**
     * *latitude* -- Latitude.
    """
    adjusted = (90 + float(latitude)) * 100000
    return str(int(adjusted)).zfill(8)


def sdb_longitude(longitude):
    """Return an 8 character, zero padded string version of the
    longitude parameter.
   
    **Arguments:**
     * *longitude* -- Longitude.
    """
    adjusted = (180 + float(longitude)) * 100000
    return str(int(adjusted)).zfill(8)

class AmazonSDB:
   
    """
    Amazon Simple Database API.
    """
   
    host = "sdb.amazonaws.com"
    box_usage = 0.0
   
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
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.rq.setHostMaxRequestsPerSecond(self.host, 0)
        self.rq.setHostMaxSimultaneousRequests(self.host, 0)

    def createDomain(self, domain):
        """
        Create a SimpleDB domain.
       
        **Arguments:**
         * *domain* -- Domain name
        """
        parameters = {
            "Action":"CreateDomain",
            "DomainName":domain
        }
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
       
        **Arguments:**
         * *domain* -- Domain name
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
        d.addCallback(self._listDomainsCallback, 
                      previous_results=previous_results)
        return d

    def _listDomainsCallback(self, data, previous_results=None):
        xml = ET.fromstring(data["response"])
        self.box_usage += float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        xml_response = etree_to_dict(xml, namespace=SDB_NAMESPACE)
        if "DomainName" in xml_response["ListDomainsResult"][0]:
            results = xml_response["ListDomainsResult"][0]["DomainName"]
        else:
            results = []
        if previous_results is not None:
            results.extend(previous_results)
        if "NextToken" in xml_response["ListDomainsResult"]:
            next_token = xml_response["ListDomainsResult"][0]["NextToken"][0]
            return self._listDomains(next_token=next_token,
                                     previous_results=results)
        return results

    def domainMetadata(self, domain):
        """
        Return meta-information about a domain.
       
        **Arguments:**
         * *domain* -- Domain name
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
        xml_response = etree_to_dict(xml, namespace=SDB_NAMESPACE)
        return xml_response["DomainMetadataResult"][0]
   
    def putAttributes(self, domain, item_name, attributes, replace=None):
        """
        Put attributes into domain at item_name.
       
        **Arguments:**
         * *domain* -- Domain name
         * *item_name* -- Item name
         * *attributes* -- Dictionary of attributes
       
        **Keyword arguments:**
         * *replace* -- List of attributes that should be overwritten
           (Default empty list)
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
       
        **Arguments:**
         * *domain* -- Domain name
         * *item_name* -- Item name
       
        **Keyword arguments:**
         * *attribute_name* -- Name of specific attribute to get (Default None)
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
        xml_response = etree_to_dict(xml, namespace=SDB_NAMESPACE)
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
       
        **Arguments:**
         * *domain* -- Domain name
         * *item_name* -- Item name
        """
        return self.deleteAttributes(domain, item_name)

    def deleteAttributes(self, domain, item_name, attributes=None):
        """
        Delete one or all attributes from domain at item_name.
       
        **Arguments:**
         * *domain* -- Domain name
         * *item_name* -- Item name
       
        **Keyword arguments:**
         * *attributes* -- List of attribute names, or dictionary of
           attribute name / value pairs. (Default empty dict)
        """       
        if attributes is None:
            attributes = {}
        if not isinstance(attributes, dict) and \
           not isinstance(attributes, list):
            message = "Attributes parameter must be a dictionary or a list."
            raise Exception(message)
        parameters = {}
        parameters["Action"] = "DeleteAttributes"
        parameters["DomainName"] = domain
        parameters["ItemName"] = item_name
        if isinstance(attributes, dict):
            attr_count = 1
            for key in attributes:
                parameters["Attribute.%s.Name" % attr_count] = key
                parameters["Attribute.%s.Value" % attr_count] = attributes[key]
                attr_count += 1
        if isinstance(attributes, list):
            attr_count = 0
            for key in attributes:
                parameters["Attribute.%s.Name" % attr_count] = key
                attr_count += 1
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
       
        **Arguments:**
         * *select_expression* -- Select expression
        """   
        return self._select(select_expression)
       
    def _select(self, select_expression, next_token=None, 
                previous_results=None):
        parameters = {}
        parameters["Action"] = "Select"
        parameters["SelectExpression"] = select_expression
        if next_token is not None:
            parameters["NextToken"] = next_token
        d = self._request(parameters)
        d.addCallback(self._selectCallback, 
                      select_expression=select_expression,
                      previous_results=previous_results)
        return d

    def _selectCallback(self, data, select_expression=None, 
                        previous_results=None):
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
            key = item.find("./%sName" % SDB_NAMESPACE).text
            attributes = item.findall("%sAttribute" % SDB_NAMESPACE)
            attribute_dict = {}
            for attribute in attributes:
                attr_name = attribute.find("./%sName" % SDB_NAMESPACE).text
                attr_value = attribute.find("./%sValue" % SDB_NAMESPACE).text
                if attr_name in attribute_dict:  
                    attribute_dict[attr_name].append(attr_value)
                else:
                    attribute_dict[attr_name] = [attr_value]
            results[key] = attribute_dict
        if next_token is not None:
            return self._select(select_expression, next_token=next_token,
                                previous_results=results)
        return results       

    def _request(self, parameters):
        """
        Add authentication parameters and make request to Amazon.
       
        **Arguments:**
         * *parameters* -- Key value pairs of parameters
        """
        parameters = self._getAuthorization("GET", parameters)
        query_string = urllib.urlencode(parameters)
        url = "https://%s/?%s" % (self.host, query_string)
        d = self.rq.getPage(url, method="GET")
        return d
         
    def _canonicalize(self, parameters):
        """
        Canonicalize parameters for use with AWS Authorization.
       
        **Arguments:**
         * *parameters* -- Key value pairs of parameters
       
        **Returns:**
         * A safe-quoted string representation of the parameters.
        """
        parameters = parameters.items()
        parameters.sort(lambda x, y:cmp(x[0], y[0]))
        return "&".join([safe_quote_tuple(x) for x in parameters])
       
    def _getAuthorization(self, method, parameters):
        """
        Create authentication parameters.
       
        **Arguments:**
         * *method* -- HTTP method of the request
         * *parameters* -- Key value pairs of parameters
       
        **Returns:**
         * A dictionary of authorization parameters
        """
        signature_parameters = {
            "AWSAccessKeyId":self.aws_access_key_id,
            "SignatureVersion":"2",
            "SignatureMethod":"HmacSHA256",
            'Timestamp':datetime.utcnow().isoformat()[0:19]+"+00:00",
            "AWSAccessKeyId":self.aws_access_key_id,
            "Version":"2009-04-15"
        }
        signature_parameters.update(parameters)
        query_string = self._canonicalize(signature_parameters)
        string_to_sign = "%(method)s\n%(host)s\n%(resource)s\n%(qs)s" % {
            "method":method,
            "host":self.host.lower(),
            "resource":"/",
            "qs":query_string,
        }
        args = [self.aws_secret_access_key, string_to_sign, hashlib.sha256]
        signature = base64.encodestring(hmac.new(*args).digest()).strip()
        signature_parameters.update({'Signature': signature})
        return signature_parameters
