from uuid import UUID
import time
import random
import logging
import logging.handlers
from heapq import heappush, heappop
from twisted.internet import reactor
from twisted.web import server
from twisted.enterprise import adbapi
from MySQLdb.cursors import DictCursor
from twisted.internet.defer import Deferred
from twisted.internet import task
from twisted.internet.threads import deferToThread
from .base import BaseServer, LOGGER
from ..resources import HeapResource

class HeapServer(BaseServer):
    
    network_information = {}
    heap = []
    
    def __init__(self,
            mysql_username,
            mysql_password,
            mysql_host,
            mysql_database,
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
        # HTTP interface
        resource = HeapResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging
        BaseServer.__init__(self)
        
    def start(self):
        self.function_names = self.functions.keys()
        reactor.callWhenRunning(self._start)
        # Setup shutdown trigger.
        self.shutdown_trigger_id = reactor.addSystemEventTrigger(
            'before', 
            'shutdown', 
            self.shutdown)
        return self.start_deferred
        
    def _start(self, start=0):
        # Select the entire spider_service DB, 10k rows at at time.
        sql = "SELECT uuid, type, account_id FROM spider_service ORDER BY id LIMIT %s, 10000" % start
        LOGGER.debug(sql)
        d = self.mysql.runQuery(sql)
        d.addCallback(self._startCallback, start)
        d.addErrback(self._startErrback)
        return d
        
    def _startCallback(self, data, start):
        # Add rows to heap. The second argument is interval, would be 
        # based on the plugin's interval setting, random for now.
        for row in data:
            self.addToHeap(row["uuid"], row["type"], row["account_id"])
        # Load next chunk.
        #if len(data) >= 10000:
        #    return self._start(start=start + 10000)
        # Done loading!
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
        LOGGER.debug("Enqueing")
        # Compare the heap min timestamp with now().
        # If it's time for the item to be queued, pop it, update the 
        # timestamp and add it back to the heap for the next go round.
        while self.heap[0][0] < now:
            job = heappop(self.heap)
            self.addToQueue(job[1])
            new_job = (now + job[1][3], job[1])
            heappush(self.heap, new_job)
        # Check again in a second.
        reactor.callLater(1, self.enqueue)
        
    def addToQueue(self, job):
        job = (
            UUID(int=job[0]).hex, # UUID as 32 character hex
            self.function_names[job[1]], # Full type string
            job[2]) # Account ID
        #print job
        # Presumably we'd add to RabbitMQ here.
        pass
    
    def addToHeap(self, uuid, type, account_id):
        print (uuid, type, account_id, int(self.functions[type]['interval']))
        uuid = UUID(uuid).int
        interval = int(self.functions[type]['interval'])
        type = self.function_names.index(type)
        account_id = int(account_id)
        enqueue_time = int(time.time() + interval)
        # Add a UUID to the heap.
        heappush(self.heap, (enqueue_time, (uuid, type, account_id, interval)))
