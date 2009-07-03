def _safe_quote(unquoted_string):
    """AWS safe version of urllib.quote"""
    return urllib.quote(str(unquoted_string), '-_.~')


def _safe_quote_tuple(tuple_):
    """Convert a 2-tuple to a string for use with AWS"""
    return "%s=%s" % (_safe_quote(tuple_[0]), _safe_quote(tuple_[1]))


def _etree_to_dict(etree, namespace=None):
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
        element_dict = _etree_to_dict(element, namespace=namespace)
        if tag in children_dict:
            children_dict[tag].append(element_dict)
        else:
            children_dict[tag] = [element_dict]
    return children_dict


def _return_true(data):
    return True
