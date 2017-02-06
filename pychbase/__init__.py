from pychbase._pychbase import _connection, _table, HBaseError

# TODO It would be cool to see if ld_library_path is set correctly?

class Connection(object):
    def __init__(self, zookeepers=None):
        if zookeepers is None:
            zookeepers = self._extract_zookeepers()

        self.zookeepers = zookeepers
        self._connection = _connection(zookeepers)
        self._connection.open()

    @staticmethod
    def _extract_zookeepers():
        try:
            with open('/opt/mapr/conf/mapr-clusters.conf') as f:
                return Connection._extract_mapr_cldbs(f)
        except IOError:
            raise ValueError("No zookeeper provided and could not be found in /opt/mapr/conf/mapr-clusters.conf")

        # TODO add these for Cloudera and Horton works

    @staticmethod
    def _extract_mapr_cldbs(f):
        first_line = f.readline()
        statements = first_line.split()
        return ','.join(zookeeper for zookeeper in statements if ':' in zookeeper)

    def table(self, table_name):
        return Table(self, table_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def create_table(self, table_name, column_families):
        self._connection.create_table(table_name, column_families)

    def delete_table(self, table_name):
        self._connection.delete_table(table_name)

    def close(self):
        self._connection.close()


class Table(object):
    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name
        self._table = _table(connection._connection, table_name)

    def row(self, row):
        return self._table.row(row)

    def put(self, row, data):
        return self._table.put(row, data)

    def delete(self, row):
        return self._table.delete(row)

    def scan(self, start='', stop=''):
        # Should start and stop default to None ?
        # TODO Add filters
        for k, v in self._table.scan(start, stop):
            yield k, v

    def delete_prefix(self, rowkey_prefix):
        delete_count = 0
        batch = self.batch()
        for row_key, obj in self.scan(rowkey_prefix, rowkey_prefix + '~'):
            batch.delete(row_key)
            delete_count += 1

        errors = batch.send()
        # Should I raise an exception if there are errors?

        return delete_count

    def close(self):
        self.connection.close()

    def batch(self, batch_size=None):
        return Batch(self, batch_size)


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
        errors, results = self.table._table.batch(self._actions)
        self._actions = []
        return errors

