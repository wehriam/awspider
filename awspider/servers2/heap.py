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
from twisted.internet.defer import Deferred
from twisted.internet import task
from twisted.internet.threads import deferToThread
from txamqp.content import Content
from .base import BaseServer, LOGGER
from ..resources import HeapResource
from ..amqp import amqp as AMQP

class HeapServer(BaseServer):
    
    heap = []
    
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
            log_file='heapserver.log',
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
        # Create AMQP Connection
        self.amqp_username = amqp_username
        self.amqp_password = amqp_password
        self.amqp_queue = amqp_queue
        self.amqp_exchange = amqp_exchange
        self.amqp = AMQP.createClient(amqp_host, amqp_vhost, amqp_port)
        # HTTP interface
        resource = HeapResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging
        BaseServer.__init__(self)
        
    def start(self):
        self.function_names = self.functions.keys()
        reactor.callWhenRunning(self._start)
        return self.start_deferred
        
    def _start(self, start=0):
        # Select the entire spider_service DB, 10k rows at at time.
        sql = "SELECT uuid, type FROM spider_service ORDER BY id LIMIT %s, 10000" % start
        LOGGER.debug(sql)
        d = self.mysql.runQuery(sql)
        d.addCallback(self._startCallback, start)
        d.addErrback(self._startErrback)
        return d
        
    def _startCallback(self, data, start):
        # Add rows to heap. The second argument is interval, would be 
        # based on the plugin's interval setting, random for now.
        for row in data:
            self.addToHeap(row["uuid"], row["type"])
        # Load next chunk.
        if len(data) >= 10000:
            return self._start(start=start + 10000)
        # Done loading, start queuing
        self.enqueue()
        d = BaseServer.start(self)   
        return d
    
    def _startErrback(self, error):
        return error
    
    def shutdown(self):
        pass
    
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
        while self.heap[0][0] < now:
            job = heappop(self.heap)
            queue_items_a(job[1])
            new_job = (now + job[1][1], job[1])
            heappush(self.heap, new_job)
        
        # add items to the queue
        self.addToQueue(queue_items)
        
        # Check again in a second.
        reactor.callLater(1, self.enqueue)
        
    def addToQueue(self, uuids):
        LOGGER.info("Connecting to broker")
        yield conn.authenticate(self.amqp_username, self.amqp_password)
        chan = yield conn.channel(1)
        yield chan.channel_open()
        # Create queue
        yield chan.queue_declare(queue=self.amqp_queue, durable=False, exclusive=False, auto_delete=False)
        yield chan.exchange_declare(exchange=self.amqp_exchange, type="fanout", durable=False, auto_delete=False)
        yield chan.queue_bind(queue=self.amqp_queue, exchange=self.amqp_exchange)
        def send_messages():
            def message_iterator():
                for uuid in uuids:
                    msg = Content(uuid)
                    msg["delivery mode"] = 2
                    chan.basic_publish(exchange=self.amqp_exchange, content=msg)
                    yield None
            return task.coiterate(message_iterator())
        yield send_messages()
        # Shut things down
        LOGGER.info('Closing broker connection')
        yield chan.channel_close()
        chan0 = yield conn.channel(0)
        yield chan0.connection_close()
            
    def addToHeap(self, uuid, type):
        uuid = UUID(uuid).bytes
        interval = 10 #int(self.functions[type]['interval'])
        try:
            type = self.function_names.index(type)
        except:
            print type
            return
        enqueue_time = int(time.time() + interval)
        # Add a UUID to the heap.
        heappush(self.heap, (enqueue_time, (uuid, interval)))