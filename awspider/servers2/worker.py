from .base import BaseServer, LOGGER
from ..resources2 import WorkerResource
from ..networkaddress import getNetworkAddress
from ..amqp import amqp as AMQP
from twisted.internet import reactor
from twisted.web import server
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred
import pprint

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class WorkerServer(BaseServer):
    
    public_ip = None
    local_ip = None
    network_information = {}
    job_queue = []
    job_count = 0
    simultaneous_jobs = 50
    
    def __init__(self,
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_s3_http_cache_bucket=None, 
            aws_s3_storage_bucket=None,
            amqp_host=None,
            amqp_username=None,
            amqp_password=None,
            amqp_vhost=None,
            amqp_queue=None,
            amqp_exchange=None,
            amqp_port=5672,
            max_simultaneous_requests=100,
            max_requests_per_host_per_second=0,
            max_simultaneous_requests_per_host=0,
            port=5005, 
            log_file='workerserver.log',
            log_directory=None,
            log_level="debug"):
        self.network_information["port"] = port
        # HTTP interface
        resource = WorkerResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Create AMQP Connection
        self.amqp_username = amqp_username
        self.amqp_password = amqp_password
        self.amqp_queue = amqp_queue
        self.amqp_exchange = amqp_exchange
        self.amqp = AMQP.createClient(amqp_host, amqp_vhost, amqp_port)
        BaseServer.__init__(
            self,
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key, 
            aws_s3_http_cache_bucket=aws_s3_http_cache_bucket, 
            aws_s3_storage_bucket=aws_s3_storage_bucket,
            max_simultaneous_requests=max_simultaneous_requests,
            max_requests_per_host_per_second=max_requests_per_host_per_second,
            max_simultaneous_requests_per_host=max_simultaneous_requests_per_host,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level)
        
    def start(self):
        reactor.callWhenRunning(self._start)
        return self.start_deferred
        
    def _start(self):
        deferreds = []
        deferreds.append(self.getNetworkAddress())
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._startCallback)
        
    def _startCallback(self, data):
        for row in data:
            if row[0] == False:
                d = self.shutdown()
                d.addCallback(self._startHandleError, row[1])
                return d
        d = BaseServer.start(self)   
        self.amqp.addCallback(self._startCallback2)
        
    def _startCallback2(self, conn):
        LOGGER.info('Connecting to broker.')
        auth = conn.authenticate(self.amqp_username, self.amqp_password)
        LOGGER.info("Authenticated. Ready to recieve messages")
        auth.addCallback(self._startCallback3)
        auth.addErrback(self._errCallback3)
        
    def _errCallback3(self, error):
        LOGGER.error('%s' % error.printBriefTraceback)
    
    def _startCallback3(self, conn):
        LOGGER.debug('foo')
        # chan = conn.channel(1)
        # chan.channel_open()
        # chan.queue_bind(queue=self.amqp_queue, exchange=self.amqp_exchange)
        # chan.basic_consume(queue=self.amqp_queue, no_ack=True, consumer_tag="awspider_consumer")
        # queue = conn.queue("awspider_consumer")
        # while True:
        #     msg = queue.get()
        #     uuid = UUID(bytes=msg.content.body)
        #     LOGGER.info('Recieved UUID %s from Queue' % uuid)
            
    def getNetworkAddress(self):
        d = getNetworkAddress()
        d.addCallback(self._getNetworkAddressCallback)
        d.addErrback(self._getNetworkAddressErrback)
        return d
        
    def _getNetworkAddressCallback(self, data):   
        if "public_ip" in data:
            self.public_ip = data["public_ip"]
            self.network_information["public_ip"] = self.public_ip
        if "local_ip" in data:
            self.local_ip = data["local_ip"]  
            self.network_information["local_ip"] = self.local_ip

    def _getNetworkAddressErrback(self, error):
        message = "Could not get network address." 
        LOGGER.error(message)
        raise Exception(message)