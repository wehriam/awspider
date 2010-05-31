from uuid import UUID, uuid4
import time
import random
import logging
import logging.handlers
from heapq import heappush, heappop
from twisted.internet import reactor, task
from twisted.web import server
from twisted.enterprise import adbapi
from MySQLdb.cursors import DictCursor
from twisted.internet.defer import Deferred, inlineCallbacks, DeferredList
from twisted.internet import task
from twisted.internet.threads import deferToThread
from txamqp.content import Content
from .base import BaseServer, LOGGER
from ..resources2 import SchedulerResource
from ..amqp import amqp as AMQP
from ..resources import ExposedResource
from twisted.web.resource import Resource


class SchedulerServer(BaseServer):
    exposed_functions = []
    exposed_function_resources = {}
    name = "AWSpider Schedule Server UUID: %s" % str(uuid4())
    heap = []
    enqueueCallLater = None
    statusloop = None
    amqp_queue_size = 0
    
    def __init__(self,
            mysql_username,
            mysql_password,
            mysql_host,
            mysql_database,
            amqp_host,
            amqp_username,
            amqp_password,
            amqp_vhost,
            amqp_queue,
            amqp_exchange,
            amqp_port=5672,
            mysql_port=3306,
            port=5004, 
            service_mapping=None,
            log_file='schedulerserver.log',
            log_directory=None,
            log_level="debug"):
        self.function_resource = Resource()
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
        # Resource Mappings
        self.service_mapping = service_mapping
        # AMQP connection parameters
        self.amqp_host = amqp_host
        self.amqp_vhost = amqp_vhost
        self.amqp_port = amqp_port
        self.amqp_username = amqp_username
        self.amqp_password = amqp_password
        self.amqp_queue = amqp_queue
        self.amqp_exchange = amqp_exchange
        # HTTP interface
        resource = SchedulerResource(self)
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging, etc
        BaseServer.__init__(
            self,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level) 
        function_name = BaseServer.makeCallable(
            self, 
            self.remoteAddToHeap, 
            interval=0, 
            name=None, 
            expose=True)
        er = ExposedResource(self, function_name)
        function_name_parts = function_name.split("/")
        if len(function_name_parts) > 1:
            if function_name_parts[0] in self.exposed_function_resources:
                r = self.exposed_function_resources[function_name_parts[0]]
            else:
                r = Resource()
                self.exposed_function_resources[function_name_parts[0]] = r
            self.function_resource.putChild(function_name_parts[0], r)
            r.putChild(function_name_parts[1], er)
        else:
            self.function_resource.putChild(function_name_parts[0], er)
        LOGGER.info("Function %s is now available via the HTTP interface." % function_name)
    
    def start(self):
        reactor.callWhenRunning(self._start)
        return self.start_deferred
    
    @inlineCallbacks   
    def _start(self):
        # Load in names of functions supported by plugins
        self.function_names = self.functions.keys()
        LOGGER.info('Connecting to broker.')
        self.conn = yield AMQP.createClient(
            self.amqp_host, 
            self.amqp_vhost, 
            self.amqp_port)
        yield self.conn.authenticate(self.amqp_username, self.amqp_password)
        self.chan = yield self.conn.channel(1)
        yield self.chan.channel_open()
        # Create Queue
        yield self.chan.queue_declare(
            queue=self.amqp_queue, 
            durable=False, 
            exclusive=False, 
            auto_delete=False)
        # Create Exchange
        yield self.chan.exchange_declare(
            exchange=self.amqp_exchange, 
            type="fanout", 
            durable=False, 
            auto_delete=False)
        yield self.chan.queue_bind(
            queue=self.amqp_queue, 
            exchange=self.amqp_exchange)
        # Build heap from data in MySQL
        yield self._loadFromMySQL()
        self.statusloop = task.LoopingCall(self.queueStatusCheck)
        self.statusloop.start(60)
        
    def _loadFromMySQL(self, start=0):
        # Select the entire spider_service DB, 10k rows at at time.
        sql = "SELECT uuid, type FROM spider_service ORDER BY id LIMIT %s, 10000" % start
        LOGGER.debug(sql)
        d = self.mysql.runQuery(sql)
        d.addCallback(self._loadFromMySQLCallback, start)
        d.addErrback(self._loadFromMySQLErrback)
        return d
        
    def _loadFromMySQLCallback(self, data, start):
        # Add rows to heap. The second argument is interval, would be 
        # based on the plugin's interval setting, random for now.
        for row in data:
            self.addToHeap(row["uuid"], row["type"])
        # Load next chunk.
        if len(data) >= 10000:
           return self._loadFromMySQL(start=start + 10000)
        # Done loading, start queuing
        self.enqueue()
        d = BaseServer.start(self)   
        return d
    
    def _loadFromMySQLErrback(self, error):
        return error
    
    @inlineCallbacks
    def shutdown(self):
        LOGGER.debug("Closting connection")
        try:
            self.enqueueCallLater.cancel()
        except:
            pass
        # Shut things down
        LOGGER.info('Closing broker connection')
        yield self.chan.channel_close()
        chan0 = yield self.conn.channel(0)
        yield chan0.connection_close()
        LOGGER.info('Closing MYSQL Connnection Pool')
        yield self.mysql.close()
    
    # def enqueue(self):
    #     # Defer this to a thread so we don't block on the web interface.
    #     deferToThread(self._enqueue)

    @inlineCallbacks
    def queueStatusCheck(self):
        yield self.chan.queue_bind(
            queue=self.amqp_queue, 
            exchange=self.amqp_exchange)
        queue_status = yield self.chan.queue_declare(
            queue=self.amqp_queue,
            passive=True)
        self.amqp_queue_size = queue_status.fields[1]
        LOGGER.debug('AMQP queue size: %d' % self.amqp_queue_size)
        
    def enqueue(self):
        now = int(time.time())
        # Compare the heap min timestamp with now().
        # If it's time for the item to be queued, pop it, update the 
        # timestamp and add it back to the heap for the next go round.
        queue_items = []
        if self.amqp_queue_size < 100000:
            queue_items_a = queue_items.append
            LOGGER.debug("%s:%s" % (self.heap[0][0], now))
            while self.heap[0][0] < now and len(queue_items) < 1000:
                job = heappop(self.heap)
                queue_items_a(job[1][0])
                new_job = (now + job[1][1], job[1])
                heappush(self.heap, new_job)
        else:
            LOGGER.critical('AMQP queue is at or beyond max limit (%d/100000)' % self.amqp_queue_size)
        # add items to amqp
        if queue_items:
            LOGGER.info('Found %d new uuids, adding them to the queue' % len(queue_items))
            msgs = [Content(uuid) for uuid in queue_items]
            deferreds = [self.chan.basic_publish(exchange=self.amqp_exchange, content=msg) for msg in msgs]
            d = DeferredList(deferreds, consumeErrors=True)
            d.addCallbacks(self._addToQueueComplete, self._addToQueueErr)
        else:
            self.enqueueCallLater = reactor.callLater(1, self.enqueue)
        
    def _addToQueueComplete(self, data):
        LOGGER.info('Completed adding items into the queue...')
        self.enqueueCallLater = reactor.callLater(2, self.enqueue)
        
    def _addToQueueErr(self, error):
        LOGGER.error(error.printBriefTraceback)
        raise
            
    def remoteAddToHeap(self, uuid, type):
        LOGGER.debug('remoteAddToHeap: uuid=%s, type=%s' % (uuid, type))
        pass
        
    def createReservation(self, function_name, **kwargs):
        LOGGER.debug('%s Called' % function_name)
        if function_name == 'schedulerserver/remoteaddtoheap':
            LOGGER.debug('kwargs: %s' % repr(kwargs))
            if set(('uuid', 'type')).issubset(set(kwargs)):
                LOGGER.debug('\tUUID: %s\n\tType: %s' % (kwargs['uuid'], kwargs['type']))
                if kwargs['uuid']:
                    self.addToHeap(kwargs['uuid'], kwargs['type'])
                return {}
            else:
                return {'error': 'invalid parameters passed: required parameters are uuid and type'}
        return
        
    def addToHeap(self, uuid, type):
        # lookup if type is in the service_mapping, if it is
        # then rewrite type to the proper resource
        if self.service_mapping and self.service_mapping.has_key(type):
            LOGGER.info('Remapping resource %s to %s' % (type, self.service_mapping[type]))
            type = self.service_mapping[type]
        try:
            # Make sure the uuid is in bytes
            uuid = UUID(uuid).bytes
        except ValueError:
            LOGGER.error('Cound not turn UUID into byes using string %s' % uuid)
            return
        if self.functions.has_key(type) and self.functions[type].has_key('interval'):
            interval = int(self.functions[type]['interval'])
        else:
            LOGGER.error('Could not find interval for type %s' % type)
            return
        enqueue_time = int(time.time() + interval)
        # Add a UUID to the heap.
        LOGGER.debug('Adding %s to heap with enqueue_time %s and interval of %s' % (UUID(bytes=uuid).hex, enqueue_time, interval))
        heappush(self.heap, (enqueue_time, (uuid, interval)))