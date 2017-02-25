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

    def table(self, table_name, use_prefix=True):
        # Todo use_prefix is not used
        return Table(self, table_name)

    def create_table(self, name, families):
        self._connection.create_table(name, families)

    def delete_table(self, name, disable=False):
        if not disable:
            if self.is_table_enabled(name):
                raise ValueError("Table must be disabled before deleting")
        self._connection.delete_table(name)

    def open(self):
        pass

    def close(self):
        self._connection.close()

    def __del__(self):
        self.close()

    def tables(self):
        raise NotImplementedError

    def enable_table(self, name):
        return self._connection.enable_table(name)

    def disable_table(self, name):
        return self._connection.disable_table(name)

    def is_table_enabled(self, name):
        # How do we do name space here?
        return self._connection.is_table_enabled(name)

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

    @staticmethod
    def _start_stop_from_row_prefix(start, stop, row_prefix):
        if row_prefix is None:
            if start is None:
                start = ''
            if stop is None:
                stop = ''
        else:
            if start or stop:
                # HappyBase raises a TypeError instead of a ValueError
                raise TypeError("Do not use start/stop in conjunction with row_prefix")
            start = row_prefix
            stop = row_prefix + '~'

        return start, stop

    def scan(self, start=None, stop=None, row_prefix=None, columns=None, filter=None, timestamp=None,
             include_timestamp=None, batch_size=1000, scan_batching=None, limit=None, sorted_columns=False,
             reverse=False, only_rowkeys=False):

        # TODO Think about how to do scan_batching
        # TODO sorted columns
        # TODO reverse

        start, stop = Table._start_stop_from_row_prefix(start, stop, row_prefix)

        # If only_rowkeys is True, result will just be a rowkey str
        # otherwise, result is (key, data) tuple
        for result in self._table.scan(start=start, stop=stop, columns=columns, filter=filter, timestamp=timestamp,
                                       include_timestamp=include_timestamp, only_rowkeys=only_rowkeys,
                                       batch_size=batch_size, limit=limit):
            yield result

    def count(self, start=None, stop=None, row_prefix=None, filter=None, timestamp=None, batch_size=1000):
        start, stop = Table._start_stop_from_row_prefix(start, stop,row_prefix)
        # TODO C should inject the is_count, not Python
        return self._table.count(start=start, stop=stop, columns=None, filter=filter, timestamp=timestamp,
                                 only_rowkeys=True, batch_size=batch_size, is_count=True)

    def delete_prefix(self, rowkey_prefix, filter=None, timestamp=None, batch_size=1000):
        # TODO would this be faster if I moved it to C?
        # TODO test with filter
        # TODO test with timestamp
        delete_count = 0
        batch = self.batch()
        for row_key in self.scan(row_prefix=rowkey_prefix, filter=filter, timestamp=timestamp,
                                 batch_size=batch_size, only_rowkeys=True):
            batch.delete(row_key)
            delete_count += 1

        errors = batch.send()
        # Should I raise an exception if there are errors?
        return delete_count

    def close(self):
        self.connection.close()

    def batch(self, timestamp=None, batch_size=None, transaction=False, wal=True):
        return Batch(self, timestamp, batch_size, transaction, wal)

    def families(self):
        # I see no way to do this with libhbase
        raise NotImplementedError

    def regions(self):
        # I see no way to do this with libhbase
        raise NotImplementedError

    def cells(self, row, column, versions=None, timestamp=None, include_timestamp=False):
        raise NotImplementedError

    def counter_get(self, row, column):
        # Don't see anything in libhbase for this
        raise NotImplementedError

    def counter_set(self, row, column, value=0):
        # Don't see anything in libhbase for this
        raise NotImplementedError

    def counter_inc(self, row, column, value=1):
        # mutations.h hb_increment_create
        # mutations.h hb_increment_add_column
        raise NotImplementedError

    def counter_dec(self, row, column, value=1):
        # mutations.h hb_increment_add_column
        raise NotImplementedError


class Batch(object):
    def __init__(self, table, timestamp=None, batch_size=None, transaction=False, wal=True):
        self.table = table
        self._actions = []
        self._batch_size = batch_size
        self._timestamp = timestamp
        self._transaction = transaction
        self._wal = wal

    def put(self, row, data, wal=None, **kwargs):
        # TODO With libhbase I could probably include a timestamp to the `put` method
        is_wal = self._wal
        if wal is not None:
            is_wal = wal

        if 'timestamp' in kwargs:
            timestamp = kwargs['timestamp']
        else:
            timestamp = self._timestamp

        if timestamp is not None:
            if not isinstance(timestamp, int):
                raise TypeError("timestamp must be int")

        self._actions.append(('put', row, data, timestamp, is_wal)) # Probably insert timestamp and wal here
        self._check_send()

    def delete(self, row, columns=None, wal=None, **kwargs):
        is_wal = self._wal
        if wal is not None:
            is_wal = wal

        if 'timestamp' in kwargs:
            timestamp = kwargs['timestamp']
        else:
            timestamp = self._timestamp

        if timestamp is not None:
            if not isinstance(timestamp, int):
                raise TypeError("timestamp must be int")

        self._actions.append(('delete', row, columns, timestamp, is_wal)) # Probably insert timestamp and wal here
        self._check_send()

    def _check_send(self):
        if self._batch_size and len(self._actions) > self._batch_size:
            self.send()

    def send(self):
        errors, results = self.table._table.batch(self._actions)
        self._actions = []
        return errors

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # TODO transaction
        self.send()

