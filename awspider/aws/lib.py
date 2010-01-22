import urllib
import re

__all__ = ["safe_quote_tuple", "etree_to_dict", "return_true"]

camelcase_to_underscore = lambda s: re.sub('(((?<=[a-z])[A-Z])|([A-Z](?![A-Z]|$)))', '_\\1', s).lower().strip('_')

def safe_quote_tuple(tuple_):
    """Convert a 2-tuple to a string for use with AWS"""
    key = urllib.quote(str(tuple_[0]), '-_.~')
    value = urllib.quote(str(tuple_[1]), '-_.~')
    return "%s=%s" % (key, value)


def etree_to_dict(etree, namespace=None, tag_list=True, convert_camelcase=False):
    """
    Convert an etree to a dict.
   
    **Keyword arguments:**
     * *namespace* -- XML Namespace to be removed from tag names (Default None)
    """
    children = etree.getchildren()
    if len(children) == 0:
        return etree.text
    children_dict = {}
    for element in children:
        tag = element.tag
        if namespace is not None:
            tag = tag.replace(namespace, "")
        if convert_camelcase:
            tag = camelcase_to_underscore(tag)
        element_dict = etree_to_dict(element, namespace=namespace, tag_list=tag_list, convert_camelcase=convert_camelcase)
        if tag in children_dict:
            if not isinstance(children_dict[tag], list):
                children_dict[tag] = [children_dict[tag]]
            children_dict[tag].append(element_dict)
        else:
            if tag_list:    
                children_dict[tag] = [element_dict]
            else:
                children_dict[tag] = element_dict
    return children_dict


def return_true(data):
    return True
