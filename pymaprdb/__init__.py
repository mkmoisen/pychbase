class Connection(object):
    def __init__(self, cldbs):
        self.cldbs = cldbs
        self._connection = pymaprdb._connection(cldbs)
        self._client = pymaprdb._client(self._connection)

    def table(self, table_name):
        return Table(self)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def close(self):
        pass


class Table(object):
    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name

    def row(self, row_key):
        pass

    def put(self, row_key, data):
        pass

    def scan(self, start=None, stop=None):
        pass

    def close(self):
        self.connection.close()


    def batch(self):
        return Batch(self)


class Batch(object):
    def __init__(self, table):
        self.table = table

