import base64
import hmac
import hashlib
import urllib
from datetime import datetime
import xml.etree.cElementTree as ET
from .lib import safe_quote_tuple, etree_to_dict
from ..requestqueuer import RequestQueuer

PA_NAMESPACE = "{http://webservices.amazon.com/AWSECommerceService/2009-10-01}"

class AmazonProductAdvertising:
    """
    Amazon Product Advertising API.
    """
    host = "ecs.amazonaws.com"
    
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
    
    def itemSearch(self, **kwargs):
        if "MerchantId" not in kwargs:
            kwargs["MerchantId"] = "All"
        if "SearchIndex" not in kwargs:
            kwargs["SearchIndex"] = "All"
        parameters = kwargs
        parameters["Operation"] = "ItemSearch"
        d = self._request(parameters)
        d.addCallback(self._itemSearchCallback)
        return d
        
    def _itemSearchCallback(self, data):
        xml = ET.fromstring(data["response"])
        errors = xml.findall(".//%sError" % PA_NAMESPACE)
        if len(errors) > 0:
            message = errors[0].find("./%sMessage" % PA_NAMESPACE).text
            raise Exception(message)
        items = xml.findall(".//%sItem" % PA_NAMESPACE)
        results = []
        for item in items:
            results.append(self._processItem(item))
        return results
    
    def _processItem(self, item_etree):
        item = {}
        item["asin"] = item_etree.find("./%sASIN" % PA_NAMESPACE).text
        item["detailpageurl"] = item_etree.find("./%sDetailPageURL" % PA_NAMESPACE).text
        # Attributes
        attributes = item_etree.find("./%sItemAttributes" % PA_NAMESPACE).getchildren()
        for element in attributes:
            tag = element.tag.replace(PA_NAMESPACE, "")
            item[tag.lower()] = element.text
        # Links
        links = item_etree.findall(".//%sItemLink" % PA_NAMESPACE)
        if len(links) > 0:
            link_list = []
            for link in links:
                link_list.append({
                    "description":link.find("./%sDescription" % PA_NAMESPACE).text,
                    "url":link.find("./%sURL" % PA_NAMESPACE).text})
            item["links"] = link_list
        # Sales Rank
        sales_rank = item_etree.find(".//%sSalesRank" % PA_NAMESPACE)
        if sales_rank is not None:
            item["sales_rank"] = sales_rank.text
        # Images
        item.update(self._processImageSet(item_etree))
        image_sets = item_etree.findall(".//%sImageSet" % PA_NAMESPACE)
        if len(image_sets) > 0:
            item["image_sets"] = {}
            for image_set in image_sets:
                item["image_sets"][image_set.attrib["Category"]] = self._processImageSet(image_set)
        # Subjects
        subjects = item_etree.findall(".//%sSubject" % PA_NAMESPACE)
        if len(subjects) > 0:
            item["subjects"] = []
            for subject in subjects:
                item["subjects"].append(subject.text)
        # Offer summary
        offer_summary = item_etree.find(".//%sOfferSummary" % PA_NAMESPACE)
        item["offer_summary"] = etree_to_dict(
            offer_summary, 
            namespace=PA_NAMESPACE, 
            tag_list=False, 
            convert_camelcase=True)
        # Offers 
        total_offers = item_etree.find(".//%sTotalOffers" % PA_NAMESPACE)
        if total_offers is not None:
            item["total_offers"] = total_offers.text
        total_offer_pages = item_etree.find(".//%sTotalOfferPages" % PA_NAMESPACE)
        if total_offer_pages is not None:
            item["total_offer_pages"] = total_offer_pages.text
        offers = item_etree.findall(".//%sOffer" % PA_NAMESPACE)
        if len(offers) > 0:
            item["offers"] = []
            for offer in offers:
                item["offers"].append(etree_to_dict(
                    offer, 
                    namespace=PA_NAMESPACE, 
                    tag_list=False, 
                    convert_camelcase=True))
        # Reviews 
        average_rating = item_etree.find(".//%sAverageRating" % PA_NAMESPACE)
        if average_rating is not None:
            item["average_rating"] = average_rating.text
        total_reviews = item_etree.find(".//%sTotalReviews" % PA_NAMESPACE)
        if total_reviews is not None:
            item["total_reviews"] = total_reviews.text
        total_review_pages = item_etree.find(".//%sTotalReviewPages" % PA_NAMESPACE)
        if total_review_pages is not None:
            item["total_review_pages"] = total_review_pages.text        
        reviews = item_etree.findall(".//%sReview" % PA_NAMESPACE)
        if len(reviews) > 0:
            item["reviews"] = []
            for review in reviews:
                item["reviews"].append(etree_to_dict(
                    review, 
                    namespace=PA_NAMESPACE, 
                    tag_list=False, 
                    convert_camelcase=True))
        # Editorial reviews 
        editorial_reviews = item_etree.findall(".//%sEditorialReview" % PA_NAMESPACE)
        if len(editorial_reviews) > 0:
            item["editorial_reviews"] = []
            for review in editorial_reviews:
                item["editorial_reviews"].append(etree_to_dict(
                    review, 
                    namespace=PA_NAMESPACE, 
                    tag_list=False, 
                    convert_camelcase=True))
        # Similar products 
        similar_products = item_etree.findall(".//%sSimilarProduct" % PA_NAMESPACE)
        if len(similar_products) > 0:
            item["similar_products"] = []
            for product in similar_products:
                item["similar_products"].append(etree_to_dict(
                    product, 
                    namespace=PA_NAMESPACE, 
                    tag_list=False, 
                    convert_camelcase=True))       
        # Tags
        if item_etree.find(".//%sTags" % PA_NAMESPACE) is not None:
            item["tag_information"] = {}
            distinct_tags = item_etree.find(".//%sDistinctTags" % PA_NAMESPACE)
            if distinct_tags is not None:
                item["tag_information"]["distinct_tags"] = distinct_tags.text
            distinct_items = item_etree.find(".//%sDistinctItems" % PA_NAMESPACE)
            if distinct_items is not None:
                item["tag_information"]["distinct_items"] = distinct_items.text
            distinct_users = item_etree.find(".//%sDistinctUsers" % PA_NAMESPACE)
            if distinct_users is not None:
                item["tag_information"]["distinct_users"] = distinct_users.text
            total_usages = item_etree.find(".//%sTotalUsages" % PA_NAMESPACE)
            if total_usages is not None:
                item["tag_information"]["total_usages"] = total_usages.text
            first_tagging = item_etree.find(".//%sFirstTagging" % PA_NAMESPACE)
            if first_tagging is not None:
                item["tag_information"]["first_tagging"] = etree_to_dict(
                    first_tagging, 
                    namespace=PA_NAMESPACE, 
                    tag_list=False, 
                    convert_camelcase=True)
            last_tagging = item_etree.find(".//%sLastTagging" % PA_NAMESPACE)
            if last_tagging is not None:
                item["tag_information"]["last_tagging"] = etree_to_dict(
                    last_tagging, 
                    namespace=PA_NAMESPACE, 
                    tag_list=False, 
                    convert_camelcase=True)
        tags = item_etree.findall(".//%sTag" % PA_NAMESPACE)
        if len(tags) > 0:
            item["tags"] = []
            for tag in tags:
                item["tags"].append(etree_to_dict(
                    tag, 
                    namespace=PA_NAMESPACE, 
                    tag_list=False, 
                    convert_camelcase=True))
        # BrowseNodes
        browse_nodes = item_etree.find(".//%sBrowseNodes" % PA_NAMESPACE)
        if browse_nodes is not None:
            item["browse_nodes"] = []
            for browse_node in browse_nodes.getchildren():
                item["browse_nodes"].append(etree_to_dict(
                    browse_node, 
                    namespace=PA_NAMESPACE, 
                    tag_list=False, 
                    convert_camelcase=True))
        # Lists
        listmania_lists = item_etree.findall(".//%sListmaniaList" % PA_NAMESPACE)
        if len(listmania_lists) > 0:
            item["listmania_lists"] = []
            for listmania_list in listmania_lists:
                item["listmania_lists"].append(etree_to_dict(
                    listmania_list, 
                    namespace=PA_NAMESPACE, 
                    tag_list=False, 
                    convert_camelcase=True))        
        return item
    
    def _processImageSet(self, item_etree):
        item = {}
        swatch_image = item_etree.find("./%sSwatchImage" % PA_NAMESPACE)
        if swatch_image is not None:
            item["swatch_image"] = swatch_image.find("./%sURL" % PA_NAMESPACE).text
        small_image = item_etree.find("./%sSmallImage" % PA_NAMESPACE)
        if small_image is not None:
            item["small_image"] = small_image.find("./%sURL" % PA_NAMESPACE).text  
        thumbnail_image = item_etree.find("./%sThumbnailImage" % PA_NAMESPACE)
        if thumbnail_image is not None:
            item["thumbnail_image"] = thumbnail_image.find("./%sURL" % PA_NAMESPACE).text
        tiny_image = item_etree.find("./%sTinyImage" % PA_NAMESPACE)
        if tiny_image is not None:
            item["tiny_image"] = tiny_image.find("./%sURL" % PA_NAMESPACE).text
        medium_image = item_etree.find("./%sMediumImage" % PA_NAMESPACE)
        if medium_image is not None:
            item["medium_image"] = medium_image.find("./%sURL" % PA_NAMESPACE).text
        large_image = item_etree.find("./%sLargeImage" % PA_NAMESPACE)
        if large_image is not None:
            item["large_image"] = large_image.find("./%sURL" % PA_NAMESPACE).text
        return item
        
    def _request(self, parameters):
        """
        Add authentication parameters and make request to Amazon.

        **Arguments:**
         * *parameters* -- Key value pairs of parameters
        """
        parameters["Service"] = "AWSECommerceService"
        parameters = self._getAuthorization("GET", parameters)
        query_string = urllib.urlencode(parameters)
        url = "https://%s/onca/xml?%s" % (self.host, query_string)
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
            'Timestamp':datetime.utcnow().isoformat()[0:19]+"+00:00",
            "AWSAccessKeyId":self.aws_access_key_id,
            "Version":"2009-10-01"
        }
        signature_parameters.update(parameters)
        query_string = self._canonicalize(signature_parameters)
        string_to_sign = "%(method)s\n%(host)s\n%(resource)s\n%(qs)s" % {
            "method":method,
            "host":self.host.lower(),
            "resource":"/onca/xml",
            "qs":query_string,
        }
        args = [self.aws_secret_access_key, string_to_sign, hashlib.sha256]
        signature = base64.encodestring(hmac.new(*args).digest()).strip()
        signature_parameters.update({'Signature': signature})
        return signature_parameters
