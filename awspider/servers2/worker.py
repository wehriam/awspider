from .base import BaseServer, LOGGER
from ..resources2 import WorkerResource
from ..networkaddress import getNetworkAddress
from ..amqp import amqp as AMQP
from ..resources import InterfaceResource, ExposedResource
from MySQLdb.cursors import DictCursor
from twisted.internet import reactor, protocol, task
from twisted.enterprise import adbapi
from twisted.web import server
from twisted.protocols.memcache import MemCacheProtocol, DEFAULT_PORT
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred, inlineCallbacks
from twisted.internet.threads import deferToThread
from uuid import UUID, uuid4
import pprint
import simplejson

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class WorkerServer(BaseServer):
    
    public_ip = None
    local_ip = None
    network_information = {}
    simultaneous_jobs = 20
    jobs_complete = 0
    job_queue = []
    job_queue_a = job_queue.append
    jobsloop = None
    pending_dequeue = False
    
    def __init__(self,
            aws_access_key_id,
            aws_secret_access_key,
            aws_s3_http_cache_bucket=None,
            aws_s3_storage_bucket=None,
            mysql_username=None,
            mysql_password=None,
            mysql_host=None,
            mysql_database=None,
            amqp_host=None,
            amqp_username=None,
            amqp_password=None,
            amqp_vhost=None,
            amqp_queue=None,
            amqp_exchange=None,
            memcached_host=None,
            service_mapping=None,
            service_args_mapping=None,
            amqp_port=5672,
            amqp_prefetch_count=1000,
            mysql_port=3306,
            memcached_port=11211,
            max_simultaneous_requests=100,
            max_requests_per_host_per_second=0,
            max_simultaneous_requests_per_host=0,
            port=5005,
            log_file='workerserver.log',
            log_directory=None,
            log_level="debug"):
        self.network_information["port"] = port
        # Create MySQL connection.
        self.mysql = adbapi.ConnectionPool(
            "MySQLdb",
            db=mysql_database,
            port=mysql_port,
            user=mysql_username,
            passwd=mysql_password,
            host=mysql_host,
            cp_reconnect=True,
            cursorclass=DictCursor)
        # Create Memcached client
        self.memcached_host = memcached_host
        self.memcached_port = memcached_port
        self.memc_ClientCreator = protocol.ClientCreator(
            reactor, MemCacheProtocol)
        # Resource Mappings
        self.service_mapping = service_mapping
        self.service_args_mapping = service_args_mapping
        # HTTP interface
        resource = WorkerResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Create AMQP Connection
        # AMQP connection parameters
        self.amqp_host = amqp_host
        self.amqp_vhost = amqp_vhost
        self.amqp_port = amqp_port
        self.amqp_username = amqp_username
        self.amqp_password = amqp_password
        self.amqp_queue = amqp_queue
        self.amqp_exchange = amqp_exchange
        self.amqp_prefetch_count = amqp_prefetch_count
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
    
    @inlineCallbacks
    def _start(self):
        yield self.getNetworkAddress()
        # Create memcached client
        self.memc = yield self.memc_ClientCreator.connectTCP(self.memcached_host, self.memcached_port)
        LOGGER.info('Connecting to broker.')
        self.conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_vhost,
            self.amqp_port)
        self.auth = yield self.conn.authenticate(
            self.amqp_username,
            self.amqp_password)
        self.chan = yield self.conn.channel(1)
        yield self.chan.channel_open()
        yield self.chan.basic_qos(prefetch_count=self.amqp_prefetch_count)
        # Create Queue
        yield self.chan.queue_declare(
            queue=self.amqp_queue,
            durable=False,
            exclusive=False,
            auto_delete=False)
        yield self.chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        yield self.chan.basic_consume(queue=self.amqp_queue,
            no_ack=False,
            consumer_tag="awspider_consumer")
        self.queue = yield self.conn.queue("awspider_consumer")
        yield BaseServer.start(self)
        self.jobsloop = task.LoopingCall(self.executeJobs)
        self.jobsloop.start(1)
        LOGGER.info('Starting dequeueing thread...')
        deferToThread(self.dequeue)
    
    @inlineCallbacks
    def shutdown(self):
        LOGGER.debug("Closing connection")
        try:
            self.jobsloop.cancel()
        except:
            pass
        # Shut things down
        LOGGER.info('Closing broker connection')
        yield self.chan.channel_close()
        chan0 = yield self.conn.channel(0)
        yield chan0.connection_close()
        #close mysql connection
        #close memcache connection
        
    def dequeue(self):
        LOGGER.debug('Pending Deuque: %s / Completed Jobs: %d / Queued Jobs: %d / Active Jobs: %d' % (self.pending_dequeue, self.jobs_complete, len(self.job_queue), len(self.active_jobs)))
        if len(self.job_queue) <= self.amqp_prefetch_count and not self.pending_dequeue:
            self.pending_dequeue = True
            LOGGER.debug('Fetching from queue')
            d = self.queue.get()
            d.addCallback(self._dequeueCallback)
            d.addErrback(self._dequeueErrback)
        else:
            reactor.callLater(1, self.dequeue)
            
    def _dequeueErrback(self, error):
        LOGGER.error('Dequeue Error: %s' % error)
        self.pending_dequeue = False
        reactor.callLater(0, self.dequeue)
        
    def _dequeueCallback(self, msg):
        LOGGER.debug('fetched msg from queue: %s' % repr(msg))
        # Get the hex version of the UUID from byte string we were sent
        uuid = UUID(bytes=msg.content.body).hex
        d = self.getJob(uuid, msg.delivery_tag)
        d.addCallback(self._dequeueCallback2, msg)
        d.addErrback(self._dequeueErrback, 'Dequeue Callback')
    
    def _dequeueCallback2(self, job, msg):
        # Load custom function.
        if job['function_name'] in self.functions:
            job['exposed_function'] = self.functions[job['function_name']]
            LOGGER.debug('Pulled job off of AMQP queue')
            job['kwargs'] = self.mapKwargs(job)
            job['delivery_tag'] = msg.delivery_tag
            self.job_queue_a(job)
        else:
            LOGGER.error("Could not find function %s." % job['function_name'])
        self.pending_dequeue = False
        reactor.callLater(0, self.dequeue)
        
    def executeJobs(self):
        while len(self.job_queue) > 0 and len(self.active_jobs) < self.simultaneous_jobs:
            job = self.job_queue.pop(0)
            exposed_function = job["exposed_function"]
            kwargs = job["kwargs"]
            function_name = job["function_name"]
            if job.has_key('uuid'):
                uuid = job["uuid"]
            else:
                # assign a temp uuid
                uuid = UUID(bytes=msg.content.body).hex
            self.chan.basic_ack(delivery_tag=job['delivery_tag'])
            d = self.callExposedFunction(
                exposed_function["function"], 
                kwargs, 
                function_name, 
                uuid=uuid)
            d.addCallback(self._executeJobCallback, job)
            d.addErrback(self.workerErrback, 'Execute Jobs')
        
    def _executeJobCallback(self, data, job):
        self.jobs_complete += 1
        LOGGER.debug('Completed Jobs: %d / Queued Jobs: %d / Active Jobs: %d' % (self.jobs_complete, len(self.job_queue), len(self.active_jobs)))
        
    def workerErrback(self, error, function_name='Worker'):
        LOGGER.error('%s Error: %s' % (function_name, str(error)))
        self.pending_dequeue = False
    
    def getJob(self, uuid, delivery_tag):
        d = self.memc.get(uuid)
        d.addCallback(self._getJobCallback, uuid, delivery_tag)
        d.addErrback(self.workerErrback, 'Get Job')
        return d
    
    def _getJobCallback(self, account, uuid, delivery_tag):
        job = account[1]
        if not job:
            LOGGER.debug('Could not find uuid in memcached: %s' % uuid)
            sql = "SELECT account_id, type FROM spider_service WHERE uuid = '%s'" % uuid
            d = self.mysql.runQuery(sql)
            d.addCallback(self.getAccountMySQL, uuid, delivery_tag)
            d.addErrback(self.workerErrback, 'Get Job Callback')
            return d
        else:
            LOGGER.debug('Found uuid in memcached: %s' % uuid)
            return simplejson.loads(job)
    
    def getAccountMySQL(self, spider_info, uuid, delivery_tag):
        if spider_info:
            account_type = spider_info[0]['type'].split('/')[0]
            sql = "SELECT * FROM content_%saccount WHERE account_id = %d" % (account_type, spider_info[0]['account_id'])
            d = self.mysql.runQuery(sql)
            d.addCallback(self.createJob, spider_info, uuid)
            d.addErrback(self.workerErrback, 'Get MySQL Account')
            return d
        LOGGER.debug('No spider_info given for uuid %s' % uuid)
        self.chan.basic_ack(delivery_tag=delivery_tag)
        return None
    
    def createJob(self, account_info, spider_info, uuid):
        job = {}
        account = account_info[0]
        function_name = spider_info[0]['type']
        if self.service_mapping and self.service_mapping.has_key(function_name):
            LOGGER.debug('Remapping resource %s to %s' % (function_name, self.service_mapping[function_name]))
            function_name = self.service_mapping[function_name]
        job['function_name'] = function_name
        job['uuid'] = uuid
        job['account'] = account
        # Save account info in memcached for up to 7 days
        d = self.memc.set(uuid, simplejson.dumps(job), 60*60*24*7)
        d.addCallback(self._createJobCallback, job)
        d.addErrback(self.workerErrback, 'Create Job')
        return d
    
    def _createJobCallback(self, memc, job):
        return job
    
    # TODO: map before putting into memcache
    def mapKwargs(self, job):
        kwargs = {}
        service_name = job['function_name'].split('/')[0]
        # remap some fields that differ from the plugin and the database
        if service_name in self.service_args_mapping:
            for key in self.service_args_mapping[service_name]:
                if key in job['account']:
                    job['account'][self.service_args_mapping[service_name][key]] = job['account'][key]
        # apply job fields to req and optional kwargs
        for arg in job['exposed_function']['required_arguments']:
            if arg in job['account']:
                kwargs[arg] = job['account'][arg]
        for arg in job['exposed_function']['optional_arguments']:
            if arg in job['account']:
                kwargs[arg] = job['account'][arg]
        LOGGER.debug('Function: %s\nKWARGS: %s' %(job['function_name'], repr(kwargs)))
        return kwargs
        
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