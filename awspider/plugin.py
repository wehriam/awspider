import inspect
import types 

__all__ = ["expose", "make_callable", "AWSpiderPlugin"]

EXPOSED_FUNCTIONS = {}
CALLABLE_FUNCTIONS = {}
MEMOIZED_FUNCTIONS = {}

    
def expose(func=None, interval=0, name=None, memoize=False):
    if func is not None:
        if memoize:
            MEMOIZED_FUNCTIONS[id(f)] = True
        EXPOSED_FUNCTIONS[id(func)] = {"interval":interval, "name":name}
        return func
    def decorator(f):
        if memoize:
            MEMOIZED_FUNCTIONS[id(f)] = True
        EXPOSED_FUNCTIONS[id(f)] = {"interval":interval, "name":name}
        return f
    return decorator


def make_callable(func=None, interval=0, name=None, memoize=False):
    if func is not None:
        if memoize:
            MEMOIZED_FUNCTIONS[id(func)] = True
        CALLABLE_FUNCTIONS[id(func)] = {"interval":interval, "name":name}
        return func
    def decorator(f):
        if memoize:
            MEMOIZED_FUNCTIONS[id(f)] = True
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
            if instance_id in CALLABLE_FUNCTIONS:
                self.spider.expose(
                    instance_method[1],
                    interval=CALLABLE_FUNCTIONS[instance_id]["interval"],
                    name=CALLABLE_FUNCTIONS[instance_id]["name"])
            if instance_id in MEMOIZED_FUNCTIONS:
                self.spider.memoize(instance_method[1])
                
    def getPage(self, *args, **kwargs):
        return self.spider.getPage(*args, **kwargs)