from uuid import UUID
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


class SchedulerServer(BaseServer):
    
    heap = []
    enqueueCallLater = None
    
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
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging, etc
        BaseServer.__init__(
            self,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level) 
    
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
        #if len(data) >= 10000:
        #    return self._loadFromMySQL(start=start + 10000)
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
    
    def enqueue(self):
        # Defer this to a thread so we don't block on the web interface.
        deferToThread(self._enqueue)
        
    def _enqueue(self):
        now = int(time.time())
        # Compare the heap min timestamp with now().
        # If it's time for the item to be queued, pop it, update the 
        # timestamp and add it back to the heap for the next go round.
        queue_items = []
        queue_items_a = queue_items.append
        LOGGER.debug("%s:%s" % (self.heap[0][0], now))
        while self.heap[0][0] < now:
            job = heappop(self.heap)
            queue_items_a(job[1][0])
            new_job = (now + job[1][1], job[1])
            heappush(self.heap, new_job)
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
        self.enqueueCallLater = reactor.callLater(1, self.enqueue)
        
    def _addToQueueErr(self, error):
        LOGGER.error(error.printBriefTraceback)
        raise
        
    def addToHeap(self, uuid, type):
        # lookup if type is in the service_mapping, if it is
        # then rewrite type to the proper resource
        if self.service_mapping and self.service_mapping.has_key(type):
            LOGGER.info('Remapping resource %s to %s' % (type, self.service_mapping[type]))
            type = self.service_mapping[type]
        uuid = UUID(uuid).bytes
        interval = 10 #int(self.functions[type]['interval'])
        try:
            type = self.function_names.index(type)
        except:
            LOGGER.error("Unsupported function type: %s" % type)
            return
        enqueue_time = int(time.time() + interval)
        # Add a UUID to the heap.
        heappush(self.heap, (enqueue_time, (uuid, interval)))