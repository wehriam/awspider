from uuid import uuid4
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
from .base import LOGGER
from ..resources import HeapResource

class HeapServer():
    
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
        # Deferred that fires after the DB is synced. 
        # Returned by HeapServer::start()
        self.start_deferred = Deferred()
        # HTTP interface
        resource = HeapResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging
        self._setupLogging(log_file, log_directory, log_level)
        
    def start(self):
        reactor.callWhenRunning(self._start)
        # Setup shutdown trigger.
        self.shutdown_trigger_id = reactor.addSystemEventTrigger(
            'before', 
            'shutdown', 
            self.shutdown)
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
            self.addToHeap(row["uuid"], random.randint(10,100))
        # Load next chunk.
        if len(data) >= 10000:
            return self._start(start=start + 10000)
        # Done loading!
        self.start_deferred.callback(True)
        self.enqueue()
    
    def _startErrback(self, error):
        return error
    
    def shutdown(self):
        pass
    
    def enqueue(self):
        # Defer this to a thread so we don't block on the web interface.
        deferToThread(self._enqueue)
        
    def _enqueue(self):
        now = time.time()
        LOGGER.debug("Enqueing")
        # Compare the heap min timestamp with now().
        # If it's time for the item to be queued, pop it, update the 
        # timestamp and add it back to the heap for the next go round.
        while self.heap[0][0] < now:
            job = heappop(self.heap)
            self.addToQueue(job)
            new_job = (now + job[1][1], job[1])
            heappush(self.heap, new_job)
        # Check again in a second.
        reactor.callLater(1, self.enqueue)
        
    def addToQueue(self, job):
        # Presumably we'd add to RabbitMQ here.
        pass
    
    def addToHeap(self, uuid, interval):
        # Add a UUID to the heap.
        heappush(self.heap, (time.time() + interval, (uuid, interval)))

    def _setupLogging(self, log_file, log_directory, log_level):
        if log_directory is None:
            self.logging_handler = logging.StreamHandler()
        else:
            self.logging_handler = logging.handlers.TimedRotatingFileHandler(
                os.path.join(log_directory, log_file), 
                when='D', 
                interval=1)
        log_format = "%(levelname)s: %(message)s %(pathname)s:%(lineno)d"
        self.logging_handler.setFormatter(logging.Formatter(log_format))
        LOGGER.addHandler(self.logging_handler)
        log_level = log_level.lower()
        log_levels = {
            "debug":logging.DEBUG, 
            "info":logging.INFO, 
            "warning":logging.WARNING, 
            "error":logging.ERROR,
            "critical":logging.CRITICAL
        }
        if log_level in log_levels:
            LOGGER.setLevel(log_levels[log_level])
        else:
            LOGGER.setLevel(logging.DEBUG)     
            def start(self):
                reactor.callWhenRunning(self._start)
                return self.start_deferred