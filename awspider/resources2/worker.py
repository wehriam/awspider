from .base import BaseResource
import simplejson

class WorkerResource(BaseResource):
    isLeaf = True
    
    def __init__(self, workerserver):    
        self.workerserver = workerserver
        BaseResource.__init__(self)
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        data = {'completed': self.workerserver.jobs_complete,
                'queued': len(self.workerserver.job_queue),
                'active': len(self.workerserver.active_jobs),
               }
        return simplejson.dumps(data)