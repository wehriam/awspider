import base64
import hmac
import hashlib
import urllib
import xml.etree.cElementTree as ET
from datetime import datetime, timedelta
import simplejson
from ..unicodeconverter import convertToUTF8
from ..requestqueuer import RequestQueuer
from .lib import return_true, safe_quote_tuple


SQS_NAMESPACE = "{http://queue.amazonaws.com/doc/2009-02-01/}"


class AmazonSQS:
   
    """
    Amazon Simple Queue Service API.
    """
   
    host = "queue.amazonaws.com"
   
    def __init__(self, aws_access_key_id, aws_secret_access_key, rq=None):
        """
        **Arguments:**
         * *aws_access_key_id* -- Amazon AWS access key ID string
         * *aws_secret_access_key* -- Amazon AWS secret access key string
       
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

    def listQueues(self, name_prefix=None):
        """
        Return a list of your queues. The maximum number of queues
        that can be returned is 1000. If you specify a value for
        the optional name_prefix parameter, only queues with
        a name beginning with the specified value are returned.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QueryListQueues.html
       
        **Keyword arguments:**
         * *name_prefix* -- String to use for filtering the list results.
           Only those queues whose name begins with the specified
           string are returned. (Default None)

        ****Returns:****
         * Deferred of a list of queue paths that can be used as the
           'resource' argument for other class methods.
        """
        parameters = {
            "Action":"ListQueues"}
        if name_prefix is not None:
            parameters["QueueNamePrefix"] = name_prefix
        d = self._request(parameters)
        d.addCallback(self._listQueuesCallback)
        return d

    def _listQueuesCallback(self, data):
        xml = ET.fromstring(data["response"])
        queue_urls = xml.findall(".//%sQueueUrl" % SQS_NAMESPACE)
        host_string = "https://%s" % self.host
        queue_paths = [x.text.replace(host_string, "") for x in queue_urls]
        return queue_paths

    def createQueue(self, name, visibility_timeout=None):
        """
        Create a new queue.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QueryCreateQueue.html
       
        **Arguments:**
         * *name* -- The name to use for the queue created.
       
        **Keyword arguments:**
         * *visibility_timeout* -- The visibility timeout (in seconds) to use
           for this queue. (Default None)

        **Returns:**
         * Deferred of a queue path string that can be used as the 'resource' argument
           for other class methods.
        """
        name = convertToUTF8(name)
        parameters = {
            "Action":"CreateQueue",
            "QueueName":name}
        if visibility_timeout is not None:
            parameters["DefaultVisibilityTimeout"] = visibility_timeout
        d = self._request(parameters)
        d.addCallback(self._createQueueCallback)
        d.addErrback(self._genericErrback)
        return d
       
    def _createQueueCallback(self, data):
        xml = ET.fromstring(data["response"])
        queue_url = xml.find(".//%sQueueUrl" % SQS_NAMESPACE).text
        queue_path = queue_url.replace("https://%s" % self.host, "")
        return queue_path
       
    def deleteQueue(self, resource):
        """
        Delete the queue specified by resource, regardless of whether the
        queue is empty. If the specified queue does not exist, returns
        a successful response.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QueryDeleteQueue.html
       
        **Arguments:**
         * *resource* -- The path of the queue.

        **Returns:**
         * Deferred of boolean True.
        """
        parameters = {"Action":"DeleteQueue"}
        d = self._request(parameters, resource=resource, method="DELETE")
        d.addCallback(return_true)
        d.addErrback(self._genericErrback)
        return d
       
    def setQueueAttributes(self, resource, visibility_timeout=None,
                           policy=None):
        """
        Set attributes of a queue.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QuerySetQueueAttributes.html
       
        **Keyword arguments:**
         * *visibility_timeout* -- The visibility timeout (in seconds) to use
           for this queue. (Default None)
         * *policy* -- Python object representing a valid policy. (Default None)
           See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?AccessPolicyLanguage_Concepts.html
       
        **Arguments:**
         * *resource* -- The path of the queue.

        **Returns:**
         * Deferred of boolean True.
        """
        parameters = {"Action":"SetQueueAttributes"}
        attributes = {}
        if policy is not None:
            attributes["Policy"] = simplejson.dumps(policy)
        if visibility_timeout is not None:
            attributes["VisibilityTimeout"] = visibility_timeout
        attr_count = 1
        for name in attributes:
            parameters["Attribute.%s.Name" % attr_count] = name
            parameters["Attribute.%s.Value" % attr_count] = attributes[name]
            attr_count += 1
        d = self._request(parameters, resource=resource)
        d.addCallback(return_true)
        d.addErrback(self._genericErrback)
        return d
   
    def getQueueAttributes(self, resource, name=None):
        """
        Get attributes of a queue.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QueryGetQueueAttributes.html
       
        **Keyword arguments:**
         * *name* -- The attribute you want to get. (Default All.) Valid 
           values are ``(All | ApproximateNumberOfMessages | VisibilityTimeout
           | CreatedTimestamp | LastModifiedTimestamp | Policy)``
       
        **Arguments:**
         * *resource* -- The path of the queue.

        **Returns:**
         * Deferred of a dictionary of the requested attributes.
        """
        attributes = [
            "All",
            "ApproximateNumberOfMessages",
            "VisibilityTimeout",
            "CreatedTimestamp",
            "LastModifiedTimestamp",
            "Policy"]
        if name is not None:
            if name not in attributes:
                raise Exception("Unknown attribute name '%s'." % name)
        else:
            name = "All"
        parameters = {
            "Action":"GetQueueAttributes",
            "AttributeName":name}       
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
            'VisibilityTimeout']
        for name in integer_attribute_names:
            if name in attributes:
                attributes[name] = int(attributes[name])
        return attributes
           
    def addPermission(self, resource, label, aws_account_ids, actions=None):
        """
        Add a permission to a queue for a specific principal. This allows
        for sharing access to the queue.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QueryAddPermission.html
       
        **Arguments:**
         * *resource* -- The path of the queue.
         * *label* -- The unique identification string of the permission you're
           setting.
         * *aws_account_ids* -- A string or list of strings of (a) valid 
           12-digit AWS account number(s), with or without hyphens.
       
        **Keyword arguments:**
         * *actions* -- A string or list of strings of actions you want to
           allow for the specified principal. Default '*'. Valid values are (* |
           SendMessage | ReceiveMessage | DeleteMessage | ChangeMessageVisibility
           | GetQueueAttributes)
       
        **Returns:**
         * Deferred of boolean True.
        """
        if actions is None:
            actions = ["*"]
        if isinstance(actions, str) or isinstance(actions, unicode):
            actions = [str(actions)]
        if not isinstance(actions, list):
            raise Exception("Actions must be a string or list of strings.")
            
        if isinstance(aws_account_ids, str) or \
           isinstance(aws_account_ids, unicode):
            aws_account_ids = [str(aws_account_ids)]
        if not isinstance(aws_account_ids, list):
            message = "aws_account_ids must be a string or list of strings."
            raise Exception(message)   
        aws_account_ids = [x.replace("-", "") for x in aws_account_ids]   
        action_options = [
            "*",
            "SendMessage",
            "ReceiveMessage",
            "DeleteMessage",
            "ChangeMessageVisibility",
            "GetQueueAttributes"]
        for action in actions:
            if action not in action_options:
                raise Exception("Unknown action name '%s'." % action)
        if len(actions) == 0:
            actions.append("*")
        parameters = {
            "Action":"AddPermission",
            "Label":label}
        action_count = 1
        for name in actions:
            parameters["ActionName.%s" % action_count] = name
            action_count += 1
        aws_account_id_count = 1
        for name in aws_account_ids:
            parameters["AWSAccountId.%s" % aws_account_id_count] = name
            aws_account_id_count += 1
        d = self._request(parameters, resource=resource)
        d.addCallback(return_true)
        d.addErrback(self._genericErrback)
        return d 

    def removePermission(self, resource, label):
        """
        Revokes any permissions in the queue policy that matches the
        label parameter.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QueryRemovePermission.html
       
        **Arguments:**
         * *resource -- The path of the queue.
         * *label -- The identfication of the permission you want to remove.
       
        **Returns:**
         * Deferred of boolean True.
        """
        parameters = {
            "Action":"RemovePermission",
            "Label":label}
        d = self._request(parameters, resource=resource)
        d.addCallback(return_true)
        d.addErrback(self._genericErrback)
        return d 

    def sendMessage(self, resource, message):
        """
        Deliver a message to the specified queue. The maximum allowed message
        size is 8 KB.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QuerySendMessage.html
       
        **Arguments:**
         * *resource* -- The path of the queue.
         * *message* -- The message to send. Characters must be in: ( #x9 | #xA |
           #xD | [#x20 to #xD7FF] | [#xE000 to #xFFFD] | [#x10000 to #x10FFFF] )
           See: http://www.w3.org/TR/REC-xml/#charsets
       
        **Returns:**
         * Deferred of a string with a Message ID.
        """
        parameters = {
            "Action":"SendMessage",
            "MessageBody":message}
        d = self._request(parameters, resource=resource)
        d.addCallback(self._sendMessageCallback)
        d.addErrback(self._genericErrback)
        return d 
   
    def _sendMessageCallback(self, data):
        xml = ET.fromstring(data["response"])
        return xml.find(".//%sMessageId" % SQS_NAMESPACE).text

    def receiveMessage(self, resource, visibility_timeout=None,
                       max_number_of_messages=1, get_sender_id=False,
                       get_sent_timestamp=False):
        """
        Retrieve one or more messages from the specified queue.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QueryReceiveMessage.html
       
        **Arguments:**
         * *resource* -- The path of the queue.
       
        **Keyword arguments:**
         * *visibility_timeout* -- The duration (in seconds) that the received
           messages are hidden from subsequent retrieve requests after being
           retrieved by a ReceiveMessage request. (Default None)
         * *max_number_of_messages* -- Maximum number of messages to return. 1-10.
           (Default 1)
         * *get_sender_id* -- Retrieve the sender id attribute. (Default False)
         * *get_sent_timestamp* -- Retrieve the timestamp attribute. (Default False)
       
        **Returns:**
         * Deferred of dictionary with attributes 'md5', 'id', 'receipt_handle',
           'body'. Optionally 'sender_id' and 'sent_timestamp'.
        """
        parameters = {
            "Action":"ReceiveMessage"}
        if visibility_timeout is not None:
            parameters["VisibilityTimeout"] = int(visibility_timeout)
        if int(max_number_of_messages) > 1:
            parameters["MaxNumberOfMessages"] = int(max_number_of_messages)
        attr_count = 1
        if get_sender_id == True:
            parameters["AttributeName.%s" % attr_count] = "SenderId"
            attr_count += 1
        if get_sent_timestamp == True:
            parameters["AttributeName.%s" % attr_count] = "SentTimestamp"
            attr_count += 1
        d = self._request(parameters, resource=resource)
        d.addCallback(self._receiveMessageCallback)
        d.addErrback(self._genericErrback)
        return d
       
    def _receiveMessageCallback(self, data):
        messages = []
        xml = ET.fromstring(data["response"])
        message_elements = xml.findall(".//%sMessage" % SQS_NAMESPACE)
        for element in message_elements:
            message = {}
            attributes = element.findall(".//%sAttributes" % SQS_NAMESPACE)
            for attribute in attributes:
                name = attribute.find(".//%sName" % SQS_NAMESPACE).text
                value = attribute.find(".//%sValue" % SQS_NAMESPACE).text
                if name == "SenderId":
                    message['sender_id'] = value
                elif name == "SentTimestamp":
                    message['sent_timestamp'] = value
            message["md5"] = element.find(".//%sMD5OfBody" % SQS_NAMESPACE).text
            message["id"] = element.find(".//%sMessageId" % SQS_NAMESPACE).text
            message["receipt_handle"] = element.find(".//%sReceiptHandle" % SQS_NAMESPACE).text
            message["body"] = element.find(".//%sBody" % SQS_NAMESPACE).text
            messages.append(message)
        return messages

    def deleteMessage(self, resource, receipt_handle):
        """
        Delete the specified message from the specified queue.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QueryDeleteMessage.html
       
        **Arguments:**
         * *resource* -- The path of the queue.
         * *receipt_handle* -- The receipt handle associated with the message
           you want to delete.
       
        **Returns:**
         * Deferred of boolean True.
        """
        parameters = {
            "Action":"DeleteMessage",
            "ReceiptHandle":receipt_handle}
        d = self._request(parameters, resource=resource)
        d.addCallback(return_true)
        d.addErrback(self._genericErrback)
        return d
   
    def changeMessageVisibility(self, resource, receipt_handle, 
                                visibility_timeout):
        """
        Change the visibility timeout of a specified message in a queue to
        a new value.
        
        See: http://docs.amazonwebservices.com/AWSSimpleQueueService/2009-02-01/SQSDeveloperGuide/index.html?Query_QueryChangeMessageVisibility.html
       
        **Arguments:**
         * *resource* -- The path of the queue.
         * *receipt_handle* -- The receipt handle associated with the message
           you want to delete.
         * *visibility_timeout* -- The new value for the message's visibility timeout (in seconds).
       
        **Returns:**
         * Deferred of boolean True.
        """
        parameters = {
            "Action":"ChangeMessageVisibility",
            "ReceiptHandle":receipt_handle,
            "VisibilityTimeout":int(visibility_timeout)}
        d = self._request(parameters, resource=resource)
        d.addCallback(return_true)
        d.addErrback(self._genericErrback)
        return d
      
    def _genericErrback(self, error):
        """
        Use server specified error when possible.
       
        **Arguments:**
         * *error* -- Twisted error
       
        **Returns:**
         * Twisted error
        """
        if hasattr(error, "value"):
            if hasattr(error.value, "response"):
                xml = ET.fromstring(error.value.response)
                message = xml.find(".//%sMessage" % SQS_NAMESPACE).text
                raise Exception(message)
        return error       
       
    def _canonicalize(self, parameters):
        """
        Canonicalize parameters for use with AWS Authorization.
       
        **Arguments:**
         * *parameters* -- Key value pairs of parameters
       
        **Returns:**
         * A safe-quoted string representation of the parameters.
        """
        parameters = parameters.items()
        # Alphebetize key-value pairs.
        parameters.sort(lambda x, y:cmp(x[0], y[0]))
        # Safe-quote and combine parameters into a string
        return "&".join([safe_quote_tuple(x) for x in parameters])

    def _request(self, parameters, method="GET", resource="/"):
        """
        Add authentication parameters and make request to Amazon.
       
        **Arguments:**
         * *parameters* -- Key value pairs of parameters
       
        **Keyword arguments:**
         * *method* -- HTTP request method
         * *resource* -- Requested server resource
       
        **Returns:**
         * Deferred of server response.
        """

        parameters = self._getAuthorization(method, parameters, 
                                            resource=resource)
           
        query_string = urllib.urlencode(parameters)

        url = "https://%s%s?%s" % (self.host, resource, query_string)
       
        #print url
       
        d = self.rq.getPage(url, method=method)
        return d

    def _getAuthorization(self, method, parameters, resource="/"):
        """
        Create authentication parameters.
       
        **Arguments:**
         * *method* -- HTTP method of the request
         * *parameters* -- Key value pairs of parameters
       
        **Returns:**
         * A dictionary of authorization parameters
        """
        expires = datetime.utcnow() + timedelta(30)
        signature_parameters = {
            "AWSAccessKeyId":self.aws_access_key_id,
            "SignatureVersion":"2",
            "SignatureMethod":"HmacSHA256",
            'Expires':"%s+00:00" % expires.isoformat()[0:19],
            "AWSAccessKeyId":self.aws_access_key_id,
            "Version":"2009-02-01"}
        signature_parameters.update(parameters)
        query_string = self._canonicalize(signature_parameters)
        string_to_sign = "%(method)s\n%(host)s\n%(resource)s\n%(qs)s" % {
            "method":method,
            "host":self.host.lower(),
            "resource":resource,
            "qs":query_string}
        args = [self.aws_secret_access_key, string_to_sign, hashlib.sha256]
        signature = base64.encodestring(hmac.new(*args).digest()).strip()
        signature_parameters.update({'Signature':signature})
        return signature_parameters
