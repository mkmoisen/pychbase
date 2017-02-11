from pychbase._pychbase import _connection, _table, HBaseError

# TODO It would be cool to see if ld_library_path is set correctly?


class Connection(object):
    # TODO HappyBase API for __init__
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

    def table(self, table_name, use_prefix):
        # Todo use_prefix is not used
        return Table(self, table_name)

    def create_table(self, name, families):
        self._connection.create_table(name, families)

    def delete_table(self, table_name, disable=False):
        # TODO I'm deleting it even if it's not disabled
        self._connection.delete_table(table_name)

    def open(self):
        pass

    def close(self):
        self._connection.close()

    def __del__(self):
        self.close()

    def tables(self):
        raise NotImplementedError

    def enable_table(self):
        raise NotImplementedError

    def disable_table(self):
        raise NotImplementedError

    def is_table_enabled(self, name):
        raise NotImplementedError

    def compact_table(self, name, major=False):
        raise NotImplementedError




class Table(object):
    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name
        self._table = _table(connection._connection, table_name)

    def row(self, row, columns=None, timestamp=None, include_timestamp=False):
        # TODO columns
        return self._table.row(row, columns, timestamp, include_timestamp)

    def rows(self, rows, columns=None, timestamp=None, include_timestamp=False):
        # TODO add test
        return [self.row(row, columns, timestamp, include_timestamp) for row in rows]

    def put(self, row, data, timestamp=None, wal=True):
        # TODO columns
        return self._table.put(row, data, timestamp, wal)

    def delete(self, row, columns=None, timestamp=None, wal=True):
        # TODO columns
        return self._table.delete(row, columns, timestamp, wal)

    def scan(self, start=None, stop=None, row_prefix=None, columns=None, filter=None, timstamp=None,
             include_timestamp=None, batch_size=1000, scan_batching=None, limit=None, sorted_columns=False,
             reverse=False):
        # TODO columns
        # TODO filter
        # TODO timestamp
        # TODO include_timestamp
        # TODO Think about how to do batch_size/scan_batching
        if row_prefix is None:
            if start is None:
                start = ''
            if stop is None:
                stop = ''
        else:
            if start or stop:
                raise TypeError("Do not use start/stop in conjunction with row_prefix")
            start = row_prefix
            stop = row_prefix + '~'
        for k, v in self._table.scan(start, stop):
            yield k, v

    def delete_prefix(self, rowkey_prefix, *args, **kwargs):
        # TODO would this be faster if I moved it to C?
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

    def batch(self, timestamp=None, batch_size=None, transaction=False, wal=True):
        return Batch(self, timestamp, batch_size)

    def families(self):
        # I see no way to do this with libhbase
        raise NotImplementedError

    def regions(self):
        # I see no way to do this with libhbase
        raise NotImplementedError

    def cells(self, row, column, versions=None, timestamp=None, include_timestamp=False):
        raise NotImplementedError

    def counter_get(self, row, column):
        raise NotImplementedError

    def counter_set(self, row, column, value=0):
        raise NotImplementedError

    def counter_inc(self, row, column, value=1):
        raise NotImplementedError

    def counter_dec(self, row, column, value=1):
        raise NotImplementedError




class Batch(object):
    def __init__(self, table, timestamp=None, batch_size=None, *args, **kwargs):
        self.table = table
        self._actions = []
        self._batch_size = batch_size

    def put(self, row, data, *args, **kwargs):
        self._actions.append(('put', row, data))
        self._check_send()

    def delete(self, row, *args, **kwargs):
        self._actions.append(('delete', row))
        self._check_send()

    def _check_send(self):
        if self._batch_size and len(self._actions) > self._batch_size:
            self.send()

    def send(self):
        errors, results = self.table._table.batch(self._actions)
        self._actions = []
        return errors

