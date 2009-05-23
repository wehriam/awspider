from unicodeconverter import convertToUnicode

def evaluateBoolean( b ):
    
    if isinstance(b, bool):
        return b
    
    b = convertToUnicode(b)
    
    if b.lower() == u"false":
        return False
    elif b.lower() == u"true":
        return True
    elif b.lower() == u"no":
        return False
    elif b.lower() == u"yes":
        return True
    else:
        try:
            return bool( int(b) )
        except:
            return False
            