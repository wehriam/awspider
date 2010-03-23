import inspect
import types 


__all__ = ["aliases", "expose", "make_callable", "AWSpiderPlugin"]
EXPOSED_FUNCTIONS = {}
CALLABLE_FUNCTIONS = {}
MEMOIZED_FUNCTIONS = {}
FUNCTION_ALIASES = {}

def aliases(*args):
    def decorator(f):
        FUNCTION_ALIASES[id(f)] = args
        return f
    return decorator
    
def expose(func=None, interval=0, name=None, memoize=False):
    if func is not None:
        EXPOSED_FUNCTIONS[id(func)] = {"interval":interval, "name":name}
        return func
    def decorator(f):
        EXPOSED_FUNCTIONS[id(f)] = {"interval":interval, "name":name}
        return f
    return decorator


def make_callable(func=None, interval=0, name=None, memoize=False):
    if func is not None:
        CALLABLE_FUNCTIONS[id(func)] = {"interval":interval, "name":name}
        return func
    def decorator(f):
        CALLABLE_FUNCTIONS[id(f)] = {"interval":interval, "name":name}
        return f
    return decorator


class AWSpiderPlugin(object):
    
    def __init__(self, spider):
        self.spider = spider
        check_method = lambda x:isinstance(x[1], types.MethodType)
        instance_methods = filter(check_method, inspect.getmembers(self))
        for instance_method in instance_methods:
            instance_id = id(instance_method[1].__func__)
            if instance_id in EXPOSED_FUNCTIONS:
                self.spider.expose(
                    instance_method[1],
                    interval=EXPOSED_FUNCTIONS[instance_id]["interval"],
                    name=EXPOSED_FUNCTIONS[instance_id]["name"])
                if instance_id in FUNCTION_ALIASES:
                    for name in FUNCTION_ALIASES[instance_id]:
                        self.spider.expose(
                            instance_method[1],
                            interval=EXPOSED_FUNCTIONS[instance_id]["interval"],
                            name=name)                        
            if instance_id in CALLABLE_FUNCTIONS:
                self.spider.expose(
                    instance_method[1],
                    interval=CALLABLE_FUNCTIONS[instance_id]["interval"],
                    name=CALLABLE_FUNCTIONS[instance_id]["name"])
                if instance_id in FUNCTION_ALIASES:
                    for name in CALLABLE_FUNCTIONS[instance_id]:
                        self.spider.expose(
                            instance_method[1],
                            interval=CALLABLE_FUNCTIONS[instance_id]["interval"],
                            name=name)
                            
    def setReservationFastCache(self, uuid, data):
        return self.spider.setReservationFastCache(uuid, data)
   
    def setReservationCache(self, uuid, data):
        return self.spider.setReservationCache(uuid, data)
    
    def getPage(self, *args, **kwargs):
        return self.spider.getPage(*args, **kwargs)
        