from .base import BaseServer, LOGGER
from ..resources2 import WorkerResource
from ..networkaddress import getNetworkAddress
from ..amqp import amqp as AMQP
from MySQLdb.cursors import DictCursor
from twisted.internet import reactor, protocol
from twisted.enterprise import adbapi
from twisted.web import server
from twisted.protocols.memcache import MemCacheProtocol, DEFAULT_PORT
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred, inlineCallbacks
from twisted.internet.threads import deferToThread
from uuid import UUID
import pprint
import cPickle as pickle

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class WorkerServer(BaseServer):
    
    public_ip = None
    local_ip = None
    network_information = {}
    job_queue = []
    job_count = 0
    simultaneous_jobs = 50
    doSomethingCallLater = None
    
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
            resource_mapping=None,
            amqp_port=5672,
            amqp_prefetch_size=0,
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
        self.resource_mapping = resource_mapping
        LOGGER.debug('Resources: %s' % repr(resource_mapping))
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
        self.amqp_prefetch_size = amqp_prefetch_size
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
        yield self.chan.basic_qos(prefetch_size=self.amqp_prefetch_size)
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
            no_ack=True,
            consumer_tag="awspider_consumer")
        self.queue = yield self.conn.queue("awspider_consumer")
        yield BaseServer.start(self)
        self.dequeue()
    
    @inlineCallbacks
    def shutdown(self):
        try:
            self.doSomethingCallLater.cancel()
        except:
            pass
        LOGGER.debug("Closting connection")
        yield None # You've got to yield something.
        # Closing the connection
    
    def dequeue(self):
        deferToThread(self._dequeue)
    
    def _dequeue(self):
        msgs = self.queue.get()
        d = self.queue.get()
        d.addCallback(self._dequeue2)
        d.addErrback(self._dequeueErr)
    
    def _dequeue2(self, msg):
        # Get the hex version of the UUID from byte string we were sent
        uuid = UUID(bytes=msg.content.body).hex
        d = self.getAccount(uuid)
        d.addCallback(self._dequeue3)
        d.addErrback(self._dequeueErr)
    
    def _dequeue3(self, account):
        self.job_queue.append(account.copy())
        self.job_count += 1
        self.dequeueCallLater = reactor.callLater(1, self.dequeue)
    
    def _dequeueErr(self, error):
        LOGGER.error(error)
        raise
    
    def getAccount(self, uuid):
        d = self.memc.get(uuid)
        d.addCallback(self._getAccount, uuid)
        d.addErrback(self._dequeueErr)
        return d
    
    def _getAccount(self, account, uuid):
        account = account[1]
        if not account:
            LOGGER.debug('Could not find uuid in memcached: %s' % uuid)
            sql = "SELECT account_id, type FROM spider_service WHERE uuid = '%s'" % uuid
            d = self.mysql.runQuery(sql)
            d.addCallback(self._getAccountMySQL, uuid)
            d.addErrback(self._dequeueErr)
            return d
        else:
            LOGGER.debug('Found uuid in memcached: %s' % uuid)
            return pickle.loads(account)
    
    def _getAccountMySQL(self, spider_info, uuid):
        account_type = spider_info[0]['type'].split('/')[0]
        sql = "SELECT * FROM content_%saccount WHERE account_id = %d" % (account_type, spider_info[0]['account_id'])
        d = self.mysql.runQuery(sql)
        d.addCallback(self._getAccountMySQL2, spider_info, uuid)
        d.addErrback(self._dequeueErr)
        return d
    
    def _getAccountMySQL2(self, account_info, spider_info, uuid):
        account = account_info[0]
        type = spider_info[0]['type']
        if self.resource_mapping and self.resource_mapping.has_key(type):
            LOGGER.info('Remapping resource %s to %s' % (type, self.resource_mapping[type]))
            account['type'] = self.resource_mapping[type]
        account['type'] = type
        account['uuid'] = uuid
        # Save account info in memcached for up to 7 days
        d = self.memc.set(uuid, pickle.dumps(account), 60*60*24*7)
        d.addCallback(self._getAccountMySQL3, account)
        d.addErrback(self._dequeueErr)
        return d
    
    def _getAccountMySQL3(self, memc, account):
        return account
    
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