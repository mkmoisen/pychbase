from pymaprdb import _connection, _table

# TODO It would be cool to see if ld_library_path is set correctly?

class Connection(object):
    def __init__(self, cldbs):
        self.cldbs = cldbs
        self._connection = _connection(cldbs)
        self._connection.open()

    def table(self, table_name):
        return Table(self, table_name)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def create_table(self, table_name, column_families):
        pass

    def close(self):
        pass


class Table(object):
    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name
        self._table = _table(connection, table_name)

    def row(self, row_key):
        return self._table.row(row_key)

    def put(self, row_key, data):
        return self._table.put(row_key, data)

    def delete(self, row_key):
        return self._table.delete(row_key)

    def scan(self, start='', stop=''):
        # Should start and stop default to None ?
        # TODO Add filters
        for k, v in self._table.scan(start, stop):
            yield k, v

    def close(self):
        self.connection.close()

    def batch(self):
        return Batch(self)


class Batch(object):
    def __init__(self, table, batch_size=None):
        self.table = table
        self._actions = []
        self._batch_size = batch_size

    def put(self, row_key, data):
        self._actions.append(('put', row_key, data))
        self._check_send()

    def delete(self, row_key):
        self._actions.append(('delete', row_key))
        self._check_send()

    def _check_send(self):
        if self._batch_size and len(self._actions) > self._batch_size:
            self.send()

    def send(self):
        self.table._table.batch(self._actions)
        self._actions = []

