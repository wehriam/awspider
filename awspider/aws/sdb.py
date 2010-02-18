import base64
import hmac
import hashlib
import urllib
import xml.etree.cElementTree as ET
from datetime import datetime
import time
import dateutil.parser
import logging
from twisted.internet.defer import DeferredList
from ..requestqueuer import RequestQueuer
from .lib import etree_to_dict, safe_quote_tuple


LOGGER = logging.getLogger("main")


SDB_NAMESPACE = "{http://sdb.amazonaws.com/doc/2009-04-15/}"

def base10toN(num,n):
    """Change a  to a base-n number.
    Up to base-36 is supported without special notation."""
    num_rep={10:'a',
         11:'b',
         12:'c',
         13:'d',
         14:'e',
         15:'f',
         16:'g',
         17:'h',
         18:'i',
         19:'j',
         20:'k',
         21:'l',
         22:'m',
         23:'n',
         24:'o',
         25:'p',
         26:'q',
         27:'r',
         28:'s',
         29:'t',
         30:'u',
         31:'v',
         32:'w',
         33:'x',
         34:'y',
         35:'z'}
    new_num_string=''
    current=num
    while current!=0:
        remainder=current%n
        if 36>remainder>9:
            remainder_string=num_rep[remainder]
        elif remainder>=36:
            remainder_string='('+str(remainder)+')'
        else:
            remainder_string=str(remainder)
        new_num_string=remainder_string+new_num_string
        current=current/n
    return new_num_string


def base10to36(i):
    return base10toN(i, 36)
        

def base36to10(s):
    return int(s, 36)


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

    def copyDomain(self, source_domain, destination_domain):
        """
        Copy all elements of a source domain to a destination domain.
       
        **Arguments:**
         * *source_domain* -- Source domain name
         * *destination_domain* -- Destination domain name
        """
        d = self.checkAndCreateDomain(destination_domain)
        d.addCallback(self._copyDomainCallback, source_domain, 
            destination_domain)
        return d
    
    def _copyDomainCallback(self, data, source_domain, destination_domain):
        return self._copyDomainCallback2(source_domain, destination_domain)
    
    def _copyDomainCallback2(self, source_domain, destination_domain, 
        next_token=None, total_box_usage=0):
        parameters = {}
        parameters["Action"] = "Select"
        parameters["SelectExpression"] = "SELECT * FROM `%s`" % source_domain
        if next_token is not None:
            parameters["NextToken"] = next_token
        d = self._request(parameters)
        d.addCallback(self._copyDomainCallback3, 
                      source_domain=source_domain,
                      destination_domain=destination_domain,
                      total_box_usage=total_box_usage)
        d.addErrback(self._genericErrback)
        return d

    def _copyDomainCallback3(self, data, source_domain, destination_domain,
        total_box_usage=0):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        total_box_usage += box_usage
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
        deferreds = []
        for key in results:
            d = self.putAttributes(destination_domain, key, results[key])
            d.addErrback(self._copyPutAttributesErrback, destination_domain, key, results[key])
            deferreds.append(d)
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._copyDomainCallback4, source_domain,
            destination_domain, next_token=next_token, total_box_usage=total_box_usage)
        return d
        
    def _copyDomainCallback4(self, data, source_domain, destination_domain,
        next_token=None, total_box_usage=0):
        for row in data:
            if row[0] == False:
                raise row[1]
        if next_token is not None:
            return self._copyDomainCallback2(
                source_domain=source_domain,
                destination_domain=destination_domain,
                next_token=next_token,
                total_box_usage=total_box_usage)
        LOGGER.debug("""CopyDomain:\n%s -> %s\nBox usage: %s""" % (
            source_domain,
            destination_domain,
            total_box_usage))
        return True
    
    def _copyPutAttributesErrback(self, error, destination_domain, key, attributes, count=0):
        if count < 3:
            d = self.putAttributes(destination_domain, key, attributes)
            d.addErrback(self._copyPutAttributesErrback, destination_domain, key, attributes, count=count + 1)
            return d
        return error
            
    def checkAndCreateDomain(self, domain): 
        """
        Check for a SimpleDB domain's existence. If it does not exist, 
        create it.
       
        **Arguments:**
         * *domain* -- Domain name
        """   
        d = self.domainMetadata(domain)
        d.addErrback(self._checkAndCreateDomainErrback, domain)  
        return d

    def _checkAndCreateDomainErrback(self, error, domain):
        if hasattr(error, "value") and hasattr(error.value, "status"):
            if int(error.value.status) == 400:  
                d = self.createDomain(domain)
                d.addErrback(self._checkAndCreateDomainErrback2, domain)
                return d
        message = "Could not find or create domain '%s'." % domain
        raise Exception(message)

    def _checkAndCreateDomainErrback2(self, error, domain):
        message =  "Could not create domain '%s'" % domain
        raise Exception(message)

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
        d.addCallback(self._createDomainCallback, domain)
        d.addErrback(self._genericErrback)   
        return d

    def _createDomainCallback(self, data, domain):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        LOGGER.debug("Created SimpleDB domain '%s'. Box usage: %s" % (domain,
            box_usage))
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
        d.addCallback(self._deleteDomainCallback, domain)
        d.addErrback(self._genericErrback)
        return d

    def _deleteDomainCallback(self, data, domain):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        LOGGER.debug("Deleted SimpleDB domain '%s'. Box usage: %s" % (domain, 
            box_usage))
        return True   

    def listDomains(self):
        """
        List SimpleDB domains associated with an account.
        """
        return self._listDomains()
   
    def _listDomains(self, 
                     next_token=None, 
                     previous_results=None, 
                     total_box_usage=0):
        parameters = {}
        parameters["Action"] = "ListDomains"
        if next_token is not None:
            parameters["NextToken"] = next_token
        d = self._request(parameters)
        d.addCallback(self._listDomainsCallback, 
                      previous_results=previous_results,
                      total_box_usage=total_box_usage)
        d.addErrback(self._genericErrback)   
        return d

    def _listDomainsCallback(self, 
                             data, 
                             previous_results=None, 
                             total_box_usage=0):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        total_box_usage += box_usage
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
                                     previous_results=results,
                                     total_box_usage=total_box_usage)
        LOGGER.debug("Listed domains. Box usage: %s" % total_box_usage)
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
        d.addCallback(self._domainMetadataCallback, domain)
        d.addErrback(self._genericErrback)   
        return d
       
    def _domainMetadataCallback(self, data, domain):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        LOGGER.debug("Got SimpleDB domain '%s' metadata. Box usage: %s" % (
            domain,
            box_usage))
        xml_response = etree_to_dict(xml, namespace=SDB_NAMESPACE)
        return xml_response["DomainMetadataResult"][0]
   
    def batchPutAttributes(self, domain, attributes_by_item_name, 
            replace_by_item_name=None):
        """
        Batch put attributes into domain.
       
        **Arguments:**
         * *domain* -- Domain name
         * *attributes_by_item_name* -- Dictionary of dictionaries. First 
           level keys are the item name, value is dictionary of key/value 
           pairs. Example: ``{"item_name":{"attribute_name":"value"}}``
         
         **Keyword arguments:**
         * *replace_by_item_name* -- Dictionary of lists. First level keys
           are the item names, value is a list of of attributes that should
           be overwritten. ``{"item_name":["attribute_name"]}`` (Default 
           empty dictionary)
        """   
        if replace_by_item_name is None:
            replace_by_item_name = {}
        if len(attributes_by_item_name) > 25:
            raise Exception("Too many items in batchPutAttributes. Up to 25 items per call allowed.")
        for item_name in replace_by_item_name:            
            if not isinstance(replace_by_item_name[item_name], list):
                raise Exception("Replace argument '%s' must be a list." % item_name)
        for item_name in attributes_by_item_name:
            if not isinstance(attributes_by_item_name[item_name], dict):
                raise Exception("Attributes argument '%s' must be a dictionary." % item_name)
        parameters = {}
        parameters["Action"] = "BatchPutAttributes"
        parameters["DomainName"] = domain
        i = 0
        for item_name in attributes_by_item_name:
            parameters["Item.%s.ItemName" % i] = item_name
            attributes_list = []
            for attribute in attributes_by_item_name[item_name].items():
                # If the attribute is a list, split into multiple attributes.
                if isinstance(attribute[1], list):
                    for value in attribute[1]:
                        attributes_list.append((attribute[0], value))
                else:
                    attributes_list.append(attribute)
            j = 0
            for attribute in attributes_list:
                parameters["Item.%s.Attribute.%s.Name" % (i,j)] = attribute[0]
                parameters["Item.%s.Attribute.%s.Value" % (i,j)] = attribute[1]
                if item_name in replace_by_item_name:
                    if attribute[0] in replace_by_item_name[item_name]:
                        parameters["Item.%s.Attribute.%s.Replace" % (i,j)] = "true"
                j += 1
            i += 1
        d = self._request(parameters)
        d.addCallback(
            self._batchPutAttributesCallback, 
            domain, 
            attributes_by_item_name)
        d.addErrback(self._genericErrback)
        return d
    
    def _batchPutAttributesCallback(self, 
            data, 
            domain, 
            attributes_by_item_name):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        LOGGER.debug("""Batch put attributes %s in SimpleDB domain '%s'. Box usage: %s""" % (
            attributes_by_item_name,
            domain,
            box_usage))
        return True
        
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
        d.addCallback(self._putAttributesCallback, domain, item_name, attributes)
        d.addErrback(self._genericErrback)
        return d
       
    def _putAttributesCallback(self, data, domain, item_name, attributes):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        LOGGER.debug("""Put attributes %s on '%s' in SimpleDB domain '%s'. Box usage: %s""" % (
            attributes,
            item_name,
            domain,
            box_usage))
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
        d.addCallback(self._getAttributesCallback, domain, item_name)
        d.addErrback(self._genericErrback)
        return d
       
    def _getAttributesCallback(self, data, domain, item_name):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        LOGGER.debug("""Got attributes from '%s' in SimpleDB domain '%s'. Box usage: %s""" % (
            item_name,
            domain,
            box_usage))
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
        d.addCallback(self._deleteAttributesCallback, domain, item_name)
        d.addErrback(self._genericErrback)   
        return d

    def _deleteAttributesCallback(self, data, domain, item_name):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        LOGGER.debug("""Deleted attributes from '%s' in SimpleDB domain '%s'. Box usage: %s""" % (
            item_name,
            domain,
            box_usage))
        return True
   
    def select(self, select_expression, max_results=0):
        """
        Run a select query
       
        **Arguments:**
         * *select_expression* -- Select expression
        """  
        if "count(" in select_expression.lower():
            return self._selectCount(select_expression)
        return self._select(select_expression, max_results=max_results)
      
    def _selectCount(self, select_expression, next_token=None, 
            previous_count=0,
            total_box_usage=0):
        parameters = {}
        parameters["Action"] = "Select"
        parameters["SelectExpression"] = select_expression
        if next_token is not None:
            parameters["NextToken"] = next_token
        d = self._request(parameters)
        d.addCallback(self._selectCountCallback, 
                      select_expression=select_expression,
                      previous_count=previous_count,
                      total_box_usage=total_box_usage)
        d.addErrback(self._genericErrback)   
        return d

    def _selectCountCallback(self, data, select_expression=None, 
            previous_count=0,
            total_box_usage=0):
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        total_box_usage += box_usage
        next_token_element = xml.find(".//%sNextToken" % SDB_NAMESPACE)
        if next_token_element is not None:
            next_token = next_token_element.text
        else:
            next_token = None
        count = previous_count + int(xml.find(".//%sValue" % SDB_NAMESPACE).text)
        if next_token is not None:
            return self._selectCount(select_expression, next_token=next_token,
                previous_count=count,
                total_box_usage=total_box_usage)
        LOGGER.debug("""Select:\n'%s'\nBox usage: %s""" % (
            select_expression,
            total_box_usage))
        return count
        
    def _select(self, select_expression, next_token=None, 
            previous_results=None,
            total_box_usage=0,
            max_results=0):
        parameters = {}
        parameters["Action"] = "Select"
        parameters["SelectExpression"] = select_expression
        if next_token is not None:
            parameters["NextToken"] = next_token
        d = self._request(parameters)
        d.addCallback(self._selectCallback, 
                      select_expression=select_expression,
                      previous_results=previous_results,
                      total_box_usage=total_box_usage,
                      max_results=max_results)
        d.addErrback(self._genericErrback)   
        return d

    def _selectCallback(self, data, select_expression=None, 
            previous_results=None,
            total_box_usage=0,
            max_results=0):
        if previous_results is not None:
            results = previous_results
        else:
            results = {}
        xml = ET.fromstring(data["response"])
        box_usage = float(xml.find(".//%sBoxUsage" % SDB_NAMESPACE).text)
        self.box_usage += box_usage
        total_box_usage += box_usage
        next_token_element = xml.find(".//%sNextToken" % SDB_NAMESPACE)
        if next_token_element is not None:
            next_token = next_token_element.text
        else:
            next_token = None
        items = xml.findall(".//%sItem" % SDB_NAMESPACE)
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
            if max_results == 0 or len(results) < max_results:
                return self._select(select_expression, next_token=next_token,
                                    previous_results=results,
                                    total_box_usage=total_box_usage,
                                    max_results=max_results)
        LOGGER.debug("""Select:\n'%s'\nBox usage: %s""" % (
            select_expression,
            total_box_usage))
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
        if len(url) > 4096:
            del parameters['Signature']
            parameters = self._getAuthorization("POST", parameters)
            query_string = urllib.urlencode(parameters)       
            url = "https://%s" % (self.host)
            d = self.rq.getPage(url, method="POST", postdata=query_string)
            return d
        else:
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

    def _genericErrback(self, error):
        if hasattr(error, "value"):
            if hasattr(error.value, "response"):
                xml = ET.XML(error.value.response)
                try:
                    LOGGER.debug(xml.find(".//Message").text)
                except Exception, e:
                    pass
        return error
        