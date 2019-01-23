import mysql.connector as mariadb
from mysql.connector.errors import OperationalError


class MariaDBCurs:
    """
    This class is intended as a wrapper to any cursor to make it work with
    context manager and our code.
    """

    def __init__(self, conn):
        self.conn = conn
        self.curs = None

    def __enter__(self):
        self.curs = self.conn.cursor()
        return self.curs

    def __exit__(self, *args):
        self.close()

    def execute(self, operation, params=(), multi=False):
        """
        This was added because the method pandas.read_sql requires it. The
        latter is used in some of our scripts and it's given the connection
        object returned by the class MysqlConnector.
        The signature is based on that of the method execute of the cursor
        implemented by mysql.connector.connect().cursor().
        :param operation: This is the SQL statement
        :param params: Some params for the SQL statement
        :param multi: no idea
        :return: the result of the curs.execute() operation
        """
        self.curs = self.conn.cursor()
        return self.curs.execute(operation, params=params, multi=multi)

    @property
    def description(self):
        """
        This was added because the method pandas.read_sql requires it. The
        latter is used in some of our scripts and it's given the connection
        object returned by the class MysqlConnector
        :return: The description attribute of the cursor object
        """
        return self.curs.description

    def fetchall(self):
        """
        This was added because the method pandas.read_sql requires it. The
        latter is used in some of our scripts and it's given the connection
        object returned by the class MysqlConnector
        :return: The result of the fetchall() method
        """
        return self.curs.fetchall()

    def close(self):
        """
        This was added because the method pandas.read_sql requires it. The
        latter is used in some of our scripts and it's given the connection
        object returned by the class MysqlConnector
        :return: the results of the close method
        """
        return self.curs.close()


class MysqlConnector:
    """
    This class is intended as a wrapper to any connector we use. It allows to
    return the right cursor class, i.e. MariaDBCurs
    """
    # Requested because we try to catch this error in Db
    OperationalError = OperationalError

    def __init__(self):
        self.conn = None
        self.mdb_curs = None
        self.first_run = True

    def connect(self, *args, **kwargs):
        self.conn = mariadb.connect(*args, **kwargs)
        self.conn.autocommit = True
        self.mdb_curs = MariaDBCurs(self.conn)
        return self

    def cursor(self):
        return self.mdb_curs

    def close(self):
        self.conn.close()
        self.conn = None
        return self.conn

    def get_rows(self, n):
        if not self.conn.unread_result:
            return None

        return self.conn.get_rows(n)[0]

    def commit(self):
        return self.conn.commit()

    def rollback(self):
        return self.conn.rollback()

