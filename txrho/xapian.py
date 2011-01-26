import thread
import time
import xappy

from twisted.internet import reactor, threads
from twisted.python import threadpool


class ConnectionHandler(object):
    def __init__(self, pool):
        self._pool = pool
        self._connection = None

    def connect(self):
        if self._connection is None:
            self._connection = xappy.SearchConnection(self._pool.index_path)
            self._last_open = time.time()
        else:
            curr_time = time.time()
            open_time = curr_time - self._last_open
            if open_time > self._pool.refresh_interval:
                self._connection.reopen()
                self._last_time = curr_time

        return self._connection

    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None


class SearchPool(object):

    running = False
    min = 1
    max = 3
    refresh_interval = 60

    def __init__(self, index_path, **kwargs):
        self.index_path = index_path
        self.min = kwargs.get('min', self.min)
        self.max = kwargs.get('max', self.max)
        self.refresh_interval = kwargs.get('refresh_interval', self.refresh_interval)

        self._conn_handlers = {}
        self.threadID = thread.get_ident
        self.threadpool = threadpool.ThreadPool(self.min, self.max)

        self.startID = reactor.callWhenRunning(self._start)

    def _start(self):
        self.startID = None
        return self.start()

    def start(self):
        """Start the connection pool.
        """
        if not self.running:
            self.threadpool.start()
            self.shutdownID = reactor.addSystemEventTrigger("during",
                                                            "shutdown",
                                                            self.finalClose)
            self.running = True

    def connect(self):
        """
        Returns current database connection.
        Should be called within a thread.
        """
        tid = self.threadID()
        handler = self._conn_handlers.get(tid)
        if handler is None:
            handler = ConnectionHandler(self)
            self._conn_handlers[tid] = handler
        return handler.connect()

    def close(self):
        """Close all pool connections and shutdown the pool"""
        if self.shutdownID:
            reactor.removeSystemEventTrigger(self.shutdownID)
            self.shutdownID = None
        if self.startID:
            reactor.removeSystemEventTrigger(self.startID)
            self.startID = None
        self.finalClose()

    def finalClose(self):
        """This should be only called by shutdown trigger"""
        self.shutdownID = None
        self.threadpool.stop()
        self.running = False
        for handler in self._conn_handlers.itervalues():
            handler.close()
        self._conn_handlers.clear()

    def runWithConnection(self, func, *args, **kwargs):
        """
        Executes a function with database connection and return the result.
        """
        return threads.deferToThreadPool(reactor, self.threadpool,
                                         self._runWithConnection,
                                         func, *args, **kwargs)

    def _runWithConnection(self, func, *args, **kwargs):
        conn = self.connect()
        result = func(conn, *args, **kwargs)
        return result
