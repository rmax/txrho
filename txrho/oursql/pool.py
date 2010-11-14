from time import time

from twisted.enterprise import adbapi

from .cursor import RowDictCursor
from .util import named_params


class ConnectionPool(adbapi.ConnectionPool):
    """Connection pool with default RowDictCursor and idle ping capability.

    Query operations supports qmark style.

    dbpool = ConnectionPool()
    d = dbpool.query("select * from foo where id > :old", old=5)
    d.addCallback(doStuff)
    """

    def __init__(self, **options):
        # TODO: getstate not include these new attrs
        # max time idle per connection before attempt idle ping
        self.max_idle_time = options.pop('max_idle_time', 7*3600)
        # perform idle ping by default
        self.conn_autoping = options.get('autoping', False)
        # store last time connect per connection
        self.conn_last_use_time = {}

        defaults = dict(
            default_cursor=RowDictCursor,
            init_command='SET time_zone="+0:00"',
            use_unicode=True,
            charset='utf8',
        )
        defaults.update(options)
        adbapi.ConnectionPool.__init__(self, "oursql", **defaults)

    def connect(self):
        """Returns database connection.

        Performs ping if connection has reached max idle time.
        """
        conn = adbapi.ConnectionPool.connect(self)
        # use hasattr to workaround getstate/setstate
        if hasattr(self, 'conn_autoping') and not self.conn_autoping:
            self.idle_ping(conn)
        return conn

    def disconnect(self, conn):
        # use hasattr to workaround getstate/setstate
        if conn is not None and hasattr(self, 'conn_last_use_time'):
            if conn in self.conn_last_use_time:
                del self.conn_last_use_time[conn]
        return adbapi.ConnectionPool.disconnect(self, conn)

    def idle_ping(self, conn):
        """Perform ping if connection reached max idle time"""
        last_use_time = self.conn_last_use_time.get(conn, 0)
        curr_time = time()
        idle_time = curr_time - last_use_time
        # always set last use time
        self.conn_last_use_time[conn] = curr_time

        if idle_time > self.max_idle_time:
            # ping for alive connection or reconnect silently
            conn.ping()

    #
    # shortcuts
    #
    def query(self, sql, *args, **kwargs):
        """Performs query and returns rows.

        def printRows(rows):
            for row in rows:
                print row.title

        d = dbpool.query("select * from articles")
        d.addCallback(printRows)
        """
        sql, params = self._sql_params(sql, *args, **kwargs)
        return self.runQuery(sql, params)

    def get(self, sql, *args, **kwargs):
        """Performs query and returns one row.

        def printRow(row):
            pritn row.title

        d = dbpool.get("select * from articles where id=?", 1)
        d.addCallback(printRow)
        """
        sql, params = self._sql_params(sql, *args, **kwargs)
        return self.runInteraction(self._get, sql, params)

    def _get(self, trans, sql, params):
        trans.execute(sql, params)
        # TODO: check multiple rows returned
        return trans.fetchone()

    def execute(self, sql, *args, **kwargs):
        """Executes query and returns rows affected or last row id (insert).

        def printId(id):
            print "Row id:", id

        d = dbpool.execute("insert into articles(title) values (?)", title)
        d.addCallback(printId)

        def cbUpdate(_):
            print "article updated"

        d = dbpool.update("update articles set title=? where id=?", *params)
        d.addCallback(cbUpdate)
        """
        sql, params = self._sql_params(sql, *args, **kwargs)
        return self.runInteraction(self._execute, sql, params)

    def _execute(self, trans, sql, params):
        trans.execute(sql, params)
        return trans.lastrowid

    def _sql_params(self, sql, *args, **kwargs):
        """Converts arguments into sql, params"""
        if args and kwargs:
            raise TypeError('mix params and named params not supported')

        if kwargs:
            return named_params(sql, kwargs)
        else:
            return sql, args


