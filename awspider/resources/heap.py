from .base import BaseResource

class HeapResource(BaseResource):
    
    def __init__(self, heapserver):    
        self.heapserver = heapserver
        BaseResource.__init__(self)
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        return "{}"