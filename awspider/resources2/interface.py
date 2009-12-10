from twisted.web import server
from .base import BaseResource


class InterfaceResource(BaseResource):
    
    
    
    def __init__(self, interfaceserver):
        self.interfaceserver = interfaceserver
        BaseResource.__init__(self)
    



