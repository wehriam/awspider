from twisted.internet.defer import Deferred, DeferredList
from twisted.web import server
from twisted.internet import reactor
from .base import BaseServer, LOGGER
from ..resources import DataResource

class DataServer(BaseServer):
    
    def __init__(self,
                 aws_access_key_id, 
                 aws_secret_access_key,
                 aws_s3_storage_bucket,
                 aws_sdb_reservation_domain,
                 port=5002,
                 log_file='dataserver.log',
                 log_directory=None,
                 log_level="debug",
                 name=None,
                 max_simultaneous_requests=50): 
        if name == None:
            name = "AWSpider Data Server UUID: %s" % self.uuid
        resource = DataResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        BaseServer.__init__(
            self,
            aws_access_key_id, 
            aws_secret_access_key,
            aws_s3_storage_bucket=aws_s3_storage_bucket,
            aws_sdb_reservation_domain=aws_sdb_reservation_domain,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level,
            name=name,
            max_simultaneous_requests=max_simultaneous_requests,
            port=port)
                
    def clearStorage(self):
        return self.s3.emptyBucket(self.aws_s3_storage_bucket)

    def getData(self, uuid):
        LOGGER.debug("Getting %s from S3." % uuid)
        d = self.s3.getObject(self.aws_s3_storage_bucket, uuid)
        d.addCallback(self._getCallback, uuid)
        d.addErrback(self._getErrback, uuid)
        return d

    def _getCallback(self, data, uuid):
        LOGGER.debug("Got %s from S3." % (uuid)) 
        return cPickle.loads(data["response"])

    def _getErrback(self, error, uuid):
        LOGGER.error("Could not get %s from S3.\n%s" % (uuid, error)) 
        return error
    
    def shutdown(self):
        deferreds = []
        LOGGER.debug("%s stopping on main HTTP interface." % self.name)
        d = self.site_port.stopListening()
        if isinstance(d, Deferred):
            deferreds.append(d)
        if len(deferreds) > 0:
            d = DeferredList(deferreds)
            d.addCallback(self._shutdownCallback)
            return d
        else:
            return self._shutdownCallback(None)

    def _shutdownCallback(self, data):
        return BaseServer.shutdown(self)