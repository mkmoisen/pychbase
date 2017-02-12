import unittest
from pychbase._pychbase import _connection, _table
from pychbase import Connection, Table, Batch
from StringIO import StringIO
from config import ZOOKEEPERS, TABLE_NAME
from datetime import datetime

class TestCConnection(unittest.TestCase):
    def test_bad_cldbs(self):
        connection = _connection('abc')
        self.assertFalse(connection.is_open())
        self.assertRaises(ValueError, connection.open)
        self.assertFalse(connection.is_open())
        connection.close()

    def test_good_cldbs(self):
        connection = _connection(ZOOKEEPERS)
        self.assertFalse(connection.is_open())
        connection.open()
        self.assertTrue(connection.is_open())
        connection.close()
        self.assertFalse(connection.is_open())
        connection.close()

    def test_enable_table(self):
        connection = _connection(ZOOKEEPERS)
        self.assertRaises(ValueError, connection.is_table_enabled, TABLE_NAME)
        self.assertRaises(ValueError, connection.enable_table, TABLE_NAME)
        self.assertRaises(ValueError, connection.disable_table, TABLE_NAME)
        connection.create_table(TABLE_NAME, {'f': {}})
        self.assertEquals(connection.is_table_enabled(TABLE_NAME), True)
        connection.disable_table(TABLE_NAME)
        self.assertEquals(connection.is_table_enabled(TABLE_NAME), False)
        # This wont throw an error in MapR, maybe it would in Cloudera
        connection.disable_table(TABLE_NAME)
        connection.enable_table(TABLE_NAME)
        self.assertEquals(connection.is_table_enabled(TABLE_NAME), True)
        connection.delete_table(TABLE_NAME)
        connection.close()



class TestCConnectionManageTable(unittest.TestCase):
    def setUp(self):
        connection = _connection(ZOOKEEPERS)
        try:
            connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        connection.close()

    def tearDown(self):
        connection = _connection(ZOOKEEPERS)
        try:
            connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        connection.close()

    def test_good(self):
        connection = _connection(ZOOKEEPERS)
        connection.create_table(TABLE_NAME, {'f': {}})
        connection.delete_table(TABLE_NAME)

    def test_already_created(self):
        connection = _connection(ZOOKEEPERS)
        connection.create_table(TABLE_NAME, {'f': {}})
        self.assertRaises(ValueError, connection.create_table, TABLE_NAME, {'f': {}})
        connection.delete_table(TABLE_NAME)

    def test_already_deleted(self):
        connection = _connection(ZOOKEEPERS)
        connection.create_table(TABLE_NAME, {'f': {}})
        connection.delete_table(TABLE_NAME)
        self.assertRaises(ValueError, connection.delete_table, TABLE_NAME)
#
    def test_large_qualifier(self):
        connection = _connection(ZOOKEEPERS)
        connection.create_table(TABLE_NAME, {''.join(['a' for _ in range(1000)]): {}})
        connection.delete_table(TABLE_NAME)

    def test_too_large_qualifier(self):
        connection = _connection(ZOOKEEPERS)
        self.assertRaises(ValueError, connection.create_table, TABLE_NAME, {''.join(['a' for _ in range(10000)]): {}})
        # Verify that table was not fake-created mapr bug
        self.assertRaises(ValueError, connection.delete_table, TABLE_NAME)

    def test_really_big_table_name(self):
        ## I think MapR C API seg faults with a tablename > 10000
        connection = _connection(ZOOKEEPERS)
        self.assertRaises(ValueError, connection.create_table, TABLE_NAME + ''.join(['a' for _ in range(10000)]), {'f': {}})
        self.assertRaises(ValueError, connection.delete_table, TABLE_NAME + ''.join(['a' for _ in range(10000)]))
#
    def test_pretty_big_table_name(self):
        ## I think MapR C API does not seg faults with a tablename ~ 1000
        connection = _connection(ZOOKEEPERS)
        self.assertRaises(ValueError, connection.create_table, TABLE_NAME + ''.join(['a' for _ in range(1000)]), {'f': {}})
        self.assertRaises(ValueError, connection.delete_table, TABLE_NAME + ''.join(['a' for _ in range(1000)]))

    def test_delete_really_big_table_name(self):
        connection = _connection(ZOOKEEPERS)
        self.assertRaises(ValueError, connection.delete_table, TABLE_NAME + ''.join(['a' for _ in range(10000)]))

    def test_delete_pretty_big_table_name(self):
        connection = _connection(ZOOKEEPERS)
        self.assertRaises(ValueError, connection.delete_table, TABLE_NAME + ''.join(['a' for _ in range(1000)]))

    def test_max_versions_happy(self):
        connection = _connection(ZOOKEEPERS)
        connection.create_table(TABLE_NAME, {'f': {
            'max_versions': 1,
            'min_versions': 1,
            'time_to_live': 0,
            'in_memory': 0,
        }})
        connection.delete_table(TABLE_NAME)

    def test_attrs_bad_key(self):
        connection = _connection(ZOOKEEPERS)
        cfs = {'f': {
            'max_versions': 1,
            'min_versions': 1,
            'time_to_live': 0,
            'not a valid key': 0,
        }}
        self.assertRaises(ValueError, connection.create_table, TABLE_NAME, cfs)

    def test_attrs_bad_value(self):
        connection = _connection(ZOOKEEPERS)
        cfs = {'f': {
            'max_versions': 1,
            'min_versions': 1,
            'time_to_live': 0,
            'in_memory': 'not an int',
        }}
        self.assertRaises(TypeError, connection.create_table, TABLE_NAME, cfs)

    def test_bad_key(self):
        connection = _connection(ZOOKEEPERS)
        cfs = {1: {
            'max_versions': 1,
            'min_versions': 1,
            'time_to_live': 0,
            'in_memory': 0,
        }}
        self.assertRaises(TypeError, connection.create_table, TABLE_NAME, cfs)

    def test_bad_value(self):
        connection = _connection(ZOOKEEPERS)
        cfs = {'f': "not a dict"}
        self.assertRaises(TypeError, connection.create_table, TABLE_NAME, cfs)

    def test_accept_unicode(self):
        connection = _connection(ZOOKEEPERS)
        connection.create_table(TABLE_NAME, {u'f': {u'max_versions': 1}})

    def test_special_symbols_in_table_name(self):
        raise NotImplementedError






class TestCTableInit(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)
        self.connection.create_table(TABLE_NAME, {'f': {}})

    def tearDown(self):
        try:
            self.connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        self.connection.close()

    def test_bad_table(self):
        self.connection.open()
        self.connection.delete_table(TABLE_NAME)
        self.assertRaises(ValueError, _table, self.connection, TABLE_NAME)
        self.connection.close()  # This segfaulted if I set self->connection before raising the exception for some reason

    def test_unopened_connection(self):
        self.connection = _connection(ZOOKEEPERS)
        table = _table(self.connection, TABLE_NAME)
        self.connection.close()


class TestCTableRow(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.table.put("foo", {"f:bar": "baz"})

    def tearDown(self):
        try:
            self.connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        self.connection.close()

    def test_happy(self):
        row = self.table.row('foo')
        self.assertEquals(row, {'f:bar': "baz"})

    def test_read_only_table_name(self):
        self.assertRaises(TypeError, setattr, self.table, 'table_name', 'foo')

    def test_timestamp_type(self):
        row = self.table.row('foo', None, None)
        row = self.table.row('foo', None, 1)
        self.assertRaises(TypeError, self.table.row, 'foo', None, 'invalid')

    def test_include_timestamp_type(self):
        row = self.table.row('foo', None, None, None)
        row = self.table.row('foo', None, None, True)
        row = self.table.row('foo', None, None, False)
        self.assertRaises(TypeError, self.table.row, 'foo', None, None, 'invalid')


class TestCTableRowColumns(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)


    def tearDown(self):
        try:
            self.connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        self.connection.close()

    def test_happy(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        row = self.table.row('foo')
        self.assertEquals(row, {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        row = self.table.row('foo', ('f',))
        self.assertEquals(row, {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        row = self.table.row('foo', ('f:',))
        self.assertEquals(row, {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})

        row = self.table.row('foo', ('f:a',))
        self.assertEquals(row, {"f:a": "foo"})

        row = self.table.row('foo', ('f:a', 'f:ab'))
        self.assertEquals(row, {"f:a": "foo", 'f:ab': 'bar'})

        row = self.table.row('foo', ('f:a', 'f:ab', 'f:abc'))
        self.assertEquals(row, {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})

        row = self.table.row('foo', ('f:nope',))
        self.assertEquals(row, {})

        # Hm, should I return an empty string if 'f:nope' doesn't exist?
        row = self.table.row('foo', ('f:a', 'f:nope'))
        self.assertEquals(row, {"f:a": "foo"})

    def test_type(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.assertRaises(TypeError, self.table.row, 'foo', 'bar')
        self.assertRaises(TypeError, self.table.row, 'foo', {'set', 'should', 'fail'})
        self.assertRaises(TypeError, self.table.row, 'foo', {'dict': 'should', 'also': 'fail'})

    def test_bad_column_family(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.assertRaises(ValueError, self.table.row, 'foo', ('b',))



class TestCTablePut(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)

    def tearDown(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.close()

    def test_happy(self):
        self.table.put("foo", {"f:bar": "baz"})
        row = self.table.row('foo')
        for _ in range(100):
            # Loop to check for buffer overflow error
            self.assertEquals(row, {'f:bar': "baz"})

    def test_invalid_key(self):
        self.assertRaises(TypeError, self.table.put, "foo", {10: "baz"})

    def test_invalid_value(self):
        self.assertRaises(TypeError, self.table.put, "foo", {"bar": 10})

    def test_empty_put(self):
        self.assertRaises(ValueError, self.table.put, 'foo', {})

    def test_bad_column_family_no_colon(self):
        #All keys in the put dict must contain a colon separating the family from the qualifier
        self.assertRaises(ValueError, self.table.put, 'foo', {'bar': 'baz'})

    def test_bad_colon_no_family(self):
        self.assertRaises(ValueError, self.table.put, 'foo', {":bar": "baz", 'invalid:foo': 'bar'})
        row = self.table.row('foo')
        self.assertEquals(row, {})

    def test_bad_colon_no_qualifier(self):
        # LOL Apparently this is totaly fine
        self.table.put('foo', {"f:": "baz"})
        row = self.table.row('foo')
        self.assertEquals(row, {"f:": "baz"})

    def test_invalid_column_family(self):
        self.assertRaises(ValueError, self.table.put, 'foo', {"f:bar": "baz", 'invalid:foo': 'bar'})
        row = self.table.row('foo')
        self.assertEquals(row, {})

    def test_set(self):
        self.assertRaises(TypeError, self.table.put, 'foo', {"f:bar", "baz"})
        row = self.table.row('foo')
        self.assertEquals(row, {})

    def test_empty_value(self):
        self.table.put("foo", {"f:bar": ""})
        row = self.table.row('foo')
        self.assertEquals(row, {'f:bar': ""})

    def test_unicode(self):
        self.table.put(u"foo", {u"f:bar": u"baz"})
        row = self.table.row('foo')
        self.assertEquals(row, {'f:bar': "baz"})

    def test_big_value(self):
        ## Greater than 1024
        #raise NotImplementedError
        self.table.put('foo', {'f:bar': ''.join(['a' for _ in range(10000)])})
        row = self.table.row('foo')
        self.assertEquals(row, {'f:bar': ''.join(['a' for _ in range(10000)])})

    def test_big_qualifier(self):
        ## Greater than 1024
        self.table.put('foo', {'f:' + ''.join(['a' for _ in range(10000)]): 'baz'})
        row = self.table.row('foo')
        self.assertEquals(row, {'f:' + ''.join(['a' for _ in range(10000)]): 'baz'})


    def test_big_row_key(self):
        ## Greater than 1024
        self.table.put(''.join(['a' for _ in range(10000)]), {'f:bar': 'baz'})
        row = self.table.row(''.join(['a' for _ in range(10000)]))
        self.assertEquals(row, {'f:bar': 'baz'})

    def test_big_column_family(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.create_table(TABLE_NAME, {''.join(['a' for _ in range(1000)]): {}})
        self.table.put('foo', {''.join(['a' for _ in range(1000)]) + ':bar': 'baz'})
        row = self.table.row('foo')
        self.assertEquals(row, {''.join(['a' for _ in range(1000)]) + ':bar': 'baz'})

    def test_type_timestamp(self):
        self.table.put("foo", {'f:foo': 'bar'}, None)
        self.assertEquals(self.table.row("foo"), {'f:foo': 'bar'})

    def test_type_timestamp_1(self):
        self.table.put("foo", {'f:foo': 'bar'}, 10)
        self.assertEquals(self.table.row("foo"), {'f:foo': 'bar'})

    def test_type_timestamp_2(self):
        self.assertRaises(TypeError, self.table.put, 'foo', {'f:foo': 'bar'}, 'invalid')

    def test_type_is_wall(self):
        self.table.put("foo", {"f:foo": "bar"}, None, None)
        self.table.put("foo", {"f:foo": "bar"}, None, True)
        self.table.put("foo", {"f:foo": "bar"}, None, False)
        self.assertRaises(TypeError, self.table.put, "foo",  {"f:foo": "bar"}, None, 'invalid')


class TestCTableTimestamp(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)
        try:
            self.connection.create_table(TABLE_NAME, {'f': {}})
        except ValueError:
            pass
        self.table = _table(self.connection, TABLE_NAME)

    def tearDown(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.close()

    def test_happy(self):
        self.table.put('foo', {'f:foo': 'bar'}, 10)
        row = self.table.row('foo', None, None, True)
        self.assertEquals(row, {'f:foo': ('bar', 10)})

    def test_happy_version(self):
        self.table.put('foo', {'f:foo': 'bar'}, 10)
        row = self.table.row('foo', None, 10, True)
        self.assertEquals(row, {'f:foo': ('bar', 10)})

        row = self.table.row('foo', None, 11, True)
        self.assertEquals(row, {'f:foo': ('bar', 10)})

        row = self.table.row('foo', None, 9, True)
        self.assertEquals(row, {})

    def test_happy_update(self):
        self.table.put('foo', {'f:foo': 'bar'}, 5)
        self.table.put('foo', {'f:foo': 'bar'}, 10)
        row = self.table.row('foo', None, None, True)
        self.assertEquals(row, {'f:foo': ('bar', 10)})

        row = self.table.row('foo', None, 5, True)
        self.assertEquals(row, {'f:foo': ('bar', 5)})

    def test_row_version_too_low(self):
        self.table.put('foo', {'f:foo': 'bar'}, 10)
        row = self.table.row('foo', None, 5)
        self.assertEquals(row, {})

    def test_happy_delete(self):
        self.table.put('foo', {'f:foo': 'bar'}, 10)
        self.table.delete('foo', None, 10)
        row = self.table.row('foo')
        self.assertEquals(row, {})

    def test_happy_delete_update_1(self):
        self.table.put('foo', {'f:foo': 'foo'}, 5)
        self.table.put('foo', {'f:foo': 'bar'}, 10)
        # This deletes everything up to the timestamp
        self.table.delete('foo', None, 10)
        row = self.table.row('foo', None, None, True)
        self.assertEquals(row, {})

        row = self.table.row('foo', None, 5)
        self.assertEquals(row, {})

    def test_happy_delete_update_2(self):
        self.table.put('foo', {'f:foo': 'foo'}, 5)
        self.table.put('foo', {'f:foo': 'bar'}, 10)
        self.table.delete('foo', None, 5)
        row = self.table.row('foo', None, None, True)
        self.assertEquals(row, {'f:foo': ('bar', 10)})

        row = self.table.row('foo', None, 5, True)
        self.assertEquals(row, {})

    def test_happy_delete_update_3(self):
        self.table.put('foo', {'f:foo': 'foo'}, 5)
        self.table.put('foo', {'f:foo': 'bar'}, 10)
        self.table.put('foo', {'f:foo': 'baz'}, 15)
        self.table.delete('foo', None, 10)

        row = self.table.row('foo', None, 5, True)
        self.assertEquals(row, {})

        row = self.table.row('foo', None, 10, True)
        self.assertEquals(row, {})

        row = self.table.row('foo', None, None, True)
        self.assertEquals(row, {'f:foo': ('baz', 15)})



    def test_scan_happy(self):
        for i in range(0, 10):
            self.table.put('foo%i' % i, {'f:foo': 'foo'}, 5)
            self.table.put('foo%i' % i, {'f:foo': 'bar'}, 10)

        for row, data in self.table.scan('', '', None, None, 5):
            self.assertEquals(data, {'f:foo': 'foo'})

        for row, data in self.table.scan('', '', None, None, 10):
            self.assertEquals(data, {'f:foo': 'bar'})

    def test_scan_to_low(self):
        for i in range(0, 10):
            self.table.put('foo%i' % i, {'f:foo': 'foo'}, 5)

        i = 0
        for row, data in self.table.scan('', '', None, None, 4):
            i += 1

        self.assertEquals(i, 0)

    def test_batch_put_happy(self):
        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 5, None) for i in range(10)])

        i = 0
        for row, data in self.table.scan('','', None, None, None, True):
            self.assertEquals(row, 'foo%i' % i)
            self.assertEquals(data, {'f:foo': ('foo%i' % i, 5)})
            i += 1

        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 10, None) for i in range(10)])

        i = 0
        for row, data in self.table.scan('', '', None, None, None, True):
            self.assertEquals(row, 'foo%i' % i)
            self.assertEquals(data, {'f:foo': ('foo%i' % i, 10)})
            i += 1

    def test_batch_delete_happy(self):
        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 5, None) for i in range(10)])
        self.table.batch([('delete', 'foo%i' % i, None, 5, None) for i in range(10)])
        i = 0
        for row, data in self.table.scan():
            i += 1

        self.assertEquals(i, 0)

    def test_batch_delete_happy_1(self):
        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 5, None) for i in range(10)])
        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 10, None) for i in range(10)])
        self.table.batch([('delete', 'foo%i' % i, None, 5, None) for i in range(10)])

        i = 0
        for row, data in self.table.scan('', '', None, None, 5, True):
            i += 1

        self.assertEquals(i, 0)

        for row, data in self.table.scan('', '', None, None, 10, True):
            i += 1

        self.assertEquals(i, 10)

    def test_batch_delete_happy_2(self):
        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 5, None) for i in range(10)])
        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 10, None) for i in range(10)])
        self.table.batch([('delete', 'foo%i' % i, None, 10, None) for i in range(10)])

        i = 0
        for row, data in self.table.scan('', '', None, None, 10, True):
            i += 1

        self.assertEquals(i, 0)

        for row, data in self.table.scan('', '', None, None, 5, True):
            i += 1

        self.assertEquals(i, 0)

    def test_batch_delete_happy_3(self):
        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 5, None) for i in range(10)])
        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 10, None) for i in range(10)])
        self.table.batch([('put', 'foo%i' % i, {'f:foo': 'foo%i' % i}, 15, None) for i in range(10)])
        self.table.batch([('delete', 'foo%i' % i, None, 10, None) for i in range(10)])

        i = 0
        for row, data in self.table.scan('', '', None, None, 5, True):
            i += 1

        self.assertEquals(i, 0)

        i = 0
        for row, data in self.table.scan('', '', None, None, 10, True):
            i += 1

        self.assertEquals(i, 0)

        for row, data in self.table.scan('', '', None, None, 15, True):
            i += 1

        self.assertEquals(i, 10)










class TestCTablePutNull(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)

    def tearDown(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.close()

    def test_input_with_null_as_final(self):
        self.table.put("foo", {"f:bar\\0": "baz\\0"})
        row = self.table.row('foo')
        self.assertEquals(row, {"f:bar\\0": "baz\\0"})

        self.table.put("bar", {"f:bar\0": "baz\0"})
        row = self.table.row('bar')
        self.assertEquals(row, {"f:bar": "baz"})

    def test_input_with_null_in_middle(self):
        self.table.put("foo", {"f:bar\\0baz": "baz\\0foo"})
        row = self.table.row('foo')
        self.assertEquals(row, {"f:bar\\0baz": "baz\\0foo"})

        self.table.put("bar", {"f:bar\0baz": "baz\0foo"})
        row = self.table.row('bar')
        self.assertEquals(row, {"f:bar": "baz"})

    def test_input_with_null_as_final_rowkey(self):
        self.table.put("bar\\0", {"f:bar\\0baz": "baz\\0foo"})
        row = self.table.row('bar\\0')
        self.assertEquals(row, {"f:bar\\0baz": "baz\\0foo"})

        self.assertRaises(TypeError, self.table.put, "bar\0", {"f:foo": "bar"})

    def test_input_with_null_in_middle_rowkey(self):
        self.table.put("bar\\0foo", {"f:bar\\0baz": "baz\\0foo"})
        row = self.table.row('bar\\0foo')
        self.assertEquals(row, {"f:bar\\0baz": "baz\\0foo"})

        self.assertRaises(TypeError, self.table.put, "bar\0foo", {"f:foo": "bar"})

    def test_input_with_xnull_as_final(self):
        self.table.put("foo", {"f:bar\\x00": "baz\\x00"})
        row = self.table.row('foo')
        self.assertEquals(row, {"f:bar\\x00": "baz\\x00"})

        self.table.put("bar", {"f:bar\x00": "baz\x00"})
        row = self.table.row('bar')
        self.assertEquals(row, {"f:bar": "baz"})

    def test_input_with_xnull_in_middle(self):
        self.table.put("foo", {"f:bar\\x00baz": "baz\\x00foo"})
        row = self.table.row('foo')
        self.assertEquals(row, {"f:bar\\x00baz": "baz\\x00foo"})

        self.table.put("bar", {"f:bar\x00baz": "baz\x00foo"})
        row = self.table.row('bar')
        self.assertEquals(row, {"f:bar": "baz"})

    def test_input_with_xnull_rowkey(self):
        self.table.put("foo\\x00", {"f:bar\\x00": "baz\\x00"})
        row = self.table.row('foo\\x00')
        self.assertEquals(row, {"f:bar\\x00": "baz\\x00"})

        self.assertRaises(TypeError, self.table.put, "bar\x00", {"f:foo": "bar"})




class TestCTablePutSplit(unittest.TestCase):
    #Purpose of this is to test the C split function
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)

    def tearDown(self):
        try:
            self.connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        self.connection.close()

    def test_first(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.table.put("a", {"f:{cq}".format(cq='f' * i): str(i) for i in range(1, 1000)})
        row = self.table.row("a")
        self.assertEquals(row, {"f:{cq}".format(cq='f' * i): str(i) for i in range(1, 1000)})

    def test_second(self):
        self.connection.create_table(TABLE_NAME, {'ff': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.table.put("a", {"ff:{cq}".format(cq='f' * i): str(i) for i in range(1, 1000)})
        row = self.table.row("a")
        self.assertEquals(row, {"ff:{cq}".format(cq='f' * i): str(i) for i in range(1, 1000)})

    def test_third(self):
        self.connection.create_table(TABLE_NAME, {'fff': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.table.put("a", {"fff:{cq}".format(cq='f' * i): str(i) for i in range(1, 1000)})
        row = self.table.row("a")
        self.assertEquals(row, {"fff:{cq}".format(cq='f' * i): str(i) for i in range(1, 1000)})


# START HERE

class TestCTableDelete(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)

    def tearDown(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.close()

    def test_happy(self):
        self.table.put("foo", {"f:bar": "baz"})
        row = self.table.row('foo')
        self.assertEquals(row, {'f:bar': "baz"})
        self.table.delete('foo')
        row = self.table.row('foo')
        self.assertEquals(row, {})

        for i in range(100):
            self.table.delete('foo')

    def test_type_rowkey(self):
        self.assertRaises(TypeError, self.table.delete, 1)
        self.assertRaises(TypeError, self.table.delete, {'foo': 'bar'})

    def test_type_timestamp(self):
        self.table.put("foo", {"f:bar": "baz"}, 10)
        self.table.delete("foo", None, None)
        self.assertEquals(self.table.row("foo"), {})

        self.table.put("foo", {"f:bar": "baz"}, 10)
        self.table.delete("foo", None, 10)
        self.assertEquals(self.table.row("foo"), {})

        self.table.put("foo", {"f:bar": "baz"}, 10)
        self.assertRaises(TypeError, self.table.delete, "foo", None, "invalid")

    def test_type_is_wall(self):
        self.table.put("foo", {"f:bar": "baz"})
        self.table.delete("foo", None, None, None)
        self.assertEquals(self.table.row("foo"), {})

        self.table.put("foo", {"f:bar": "baz"})
        self.table.delete("foo", None, None, True)
        self.assertEquals(self.table.row("foo"), {})

        self.table.put("foo", {"f:bar": "baz"})
        self.table.delete("foo", None, None, False)
        self.assertEquals(self.table.row("foo"), {})

        self.assertRaises(TypeError, self.table.delete, 'foo', None, None, 'invalid')


    def test_empty_row_key(self):
        self.assertRaises(ValueError, self.table.delete, '')


class TestCTableDeleteColumns(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)


    def tearDown(self):
        try:
            self.connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        self.connection.close()

    def test_happy(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        row = self.table.row('foo')
        self.assertEquals(row, {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})

        self.table.delete('foo', ('f',))
        row = self.table.row('foo')
        self.assertEquals(row, {})

        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        self.table.delete('foo', ('f:',))
        row = self.table.row('foo')
        self.assertEquals(row, {})

        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        self.table.delete('foo', ('f:a',))
        row = self.table.row('foo')
        self.assertEquals(row, {'f:ab': 'bar', 'f:abc': 'baz'})

        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        self.table.delete('foo', ('f:a', 'f:ab'))
        row = self.table.row('foo')
        self.assertEquals(row, {'f:abc': 'baz'})

        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        self.table.delete('foo', ('f:a', 'f:ab', 'f:abc'))
        row = self.table.row('foo')
        self.assertEquals(row, {})

        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        self.table.delete('foo', ('f:nope',))
        row = self.table.row('foo')
        self.assertEquals(row, {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})

        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'})
        self.table.delete('foo', ('f:a', 'f:nope'))
        row = self.table.row('foo')
        self.assertEquals(row, {'f:ab': 'bar', 'f:abc': 'baz'})

    def test_batch_delete_columns_happy(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        for i in range(10):
            self.table.put('foo%i' % i, {'f:a': 'foo%i' % i, 'f:ab': 'bar%i' % i, 'f:abc': 'baz%i' % i})

        self.table.batch([('delete', 'foo%i' % i, ('f',)) for i in range(10)])
        i = 0
        for row, data in self.table.scan():
            i += 1
        self.assertEquals(i, 0)

        for i in range(10):
            self.table.put('foo%i' % i, {'f:a': 'foo%i' % i, 'f:ab': 'bar%i' % i, 'f:abc': 'baz%i' % i})

        self.table.batch([('delete', 'foo%i' % i, ('f:',)) for i in range(10)])
        i = 0
        for row, data in self.table.scan():
            i += 1
        self.assertEquals(i, 0)

        for i in range(10):
            self.table.put('foo%i' % i, {'f:a': 'foo%i' % i, 'f:ab': 'bar%i' % i, 'f:abc': 'baz%i' % i})

        self.table.batch([('delete', 'foo%i' % i, ('f:a',)) for i in range(10)])

        i = 0
        for row, data in self.table.scan():
            self.assertEquals(data, {'f:ab': 'bar%i' % i, 'f:abc': 'baz%i' % i})
            i += 1
        self.assertEquals(i, 10)


    def test_batch_delete_columns_timestamp(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        for i in range(10):
            self.table.put('foo%i' % i, {'f:a': 'foo%i' % i, 'f:ab': 'foo%i' % i, 'f:abc': 'foo%i' % i}, 5)

        for i in range(10):
            self.table.put('foo%i' % i, {'f:a': 'bar%i' % i, 'f:ab': 'bar%i' % i, 'f:abc': 'bar%i' % i}, 10)

        for i in range(10):
            self.table.put('foo%i' % i, {'f:a': 'baz%i' % i, 'f:ab': 'baz%i' % i, 'f:abc': 'baz%i' % i}, 15)

        self.table.batch([('delete', 'foo%i' % i, ('f:a',), 10) for i in range(10)])

        i = 0
        for row, data in self.table.scan('', '', None, None, 10, True):
            self.assertEquals(data, {'f:ab': ('bar%i' % i, 10), 'f:abc': ('bar%i' % i, 10)})
            i += 1

        self.assertEquals(i, 10)

        i = 0
        for row, data in self.table.scan('', '', None, None, 15, True):
            self.assertEquals(data, {'f:a': ('baz%i' % i, 15), 'f:ab': ('baz%i' % i, 15), 'f:abc': ('baz%i' % i, 15)})
            i += 1
        self.assertEquals(i, 10)


    def test_no_timestamp(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'}, 5)
        self.table.put("foo", {"f:a": "FOO", 'f:ab': 'BAR', 'f:abc': 'BAZ'}, 10)

        self.table.delete('foo', ('f',))
        row = self.table.row('foo')
        self.assertEquals(row, {})

        row = self.table.row('foo', None, 10)
        self.assertEquals(row, {})

        row = self.table.row('foo', None, 5)
        self.assertEquals(row, {})

    def test_with_timestamp(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'}, 5)
        self.table.put("foo", {"f:a": "FOO", 'f:ab': 'BAR', 'f:abc': 'BAZ'}, 10)
        self.table.delete('foo', ('f',), 10)

        row = self.table.row('foo')
        self.assertEquals(row, {})

        row = self.table.row('foo', None, 10)
        self.assertEquals(row, {})

        row = self.table.row('foo', None, 5)
        self.assertEquals(row, {})

    def test_with_timestamp_1(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.table.put("foo", {"f:a": "foo", 'f:ab': 'bar', 'f:abc': 'baz'}, 5)
        self.table.put("foo", {"f:a": "FOO", 'f:ab': 'BAR', 'f:abc': 'BAZ'}, 10)
        self.table.delete('foo', ('f',), 5)

        row = self.table.row('foo')
        self.assertEquals(row,  {"f:a": "FOO", 'f:ab': 'BAR', 'f:abc': 'BAZ'})

        row = self.table.row('foo', None, 10)
        self.assertEquals(row,  {"f:a": "FOO", 'f:ab': 'BAR', 'f:abc': 'BAZ'})

        row = self.table.row('foo', None, 5)
        self.assertEquals(row, {})


    def test_type(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.assertRaises(TypeError, self.table.delete, 'foo', 'bar')
        self.assertRaises(TypeError, self.table.delete, 'foo', {'set', 'should', 'fail'})
        self.assertRaises(TypeError, self.table.delete, 'foo', {'dict': 'should', 'also': 'fail'})

    def test_bad_column_family(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.assertRaises(ValueError, self.table.delete, 'foo', ('b',))

class TestCTableScanStartStop(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        for i in range(1, 10):
            self.table.put("foo{i}".format(i=i), {"f:bar{i}".format(i=i): 'baz{i}'.format(i=i)})

        for i in range(1, 10):
            self.table.put("aaa{i}".format(i=i), {"f:aaa{i}".format(i=i): 'aaa{i}'.format(i=i)})

        for i in range(1, 10):
            self.table.put("zzz{i}".format(i=i), {"f:zzz{i}".format(i=i): 'zzz{i}'.format(i=i)})

    def tearDown(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.close()

    def test_happy(self):
        i = 0
        for row_key, obj in self.table.scan():
            i += 1
            if i <= 9:
                self.assertEquals(row_key, "aaa{i}".format(i=i))
                self.assertEquals(obj, {"f:aaa{i}".format(i=i): 'aaa{i}'.format(i=i)})
            elif i <= 18:
                self.assertEquals(row_key, "foo{i}".format(i= i - 9))
                self.assertEquals(obj, {"f:bar{i}".format(i=i-9): 'baz{i}'.format(i=i-9)})
            else:
                self.assertEquals(row_key, "zzz{i}".format(i=i-18))
                self.assertEquals(obj, {"f:zzz{i}".format(i=i-18): 'zzz{i}'.format(i=i-18)})

        self.assertEquals(i, 27)

    def test_happy_start(self):
        i = 0
        for row_key, obj in self.table.scan('zzz'):
            i += 1
            self.assertEquals(row_key, "zzz{i}".format(i=i))
            self.assertEquals(obj, {"f:zzz{i}".format(i=i): 'zzz{i}'.format(i=i)})

        self.assertEquals(i, 9)

    def test_happy_stop(self):
        i = 0
        for row_key, obj in self.table.scan('', 'aaa9~'):
            i += 1
            self.assertEquals(row_key, "aaa{i}".format(i=i))
            self.assertEquals(obj, {"f:aaa{i}".format(i=i): 'aaa{i}'.format(i=i)})

        self.assertEquals(i, 9)

    def test_happy_start_stop(self):
        i = 0
        for row_key, obj in self.table.scan('foo1', 'foo9~'):
            i += 1
            self.assertEquals(row_key, "foo{i}".format(i=i))
            self.assertEquals(obj, {"f:bar{i}".format(i=i): 'baz{i}'.format(i=i)})

        self.assertEquals(i, 9)

    def test_no_rows(self):
        i = 0
        for row_key, obj in self.table.scan('fake', 'fake~'):
            i += 1

        self.assertEquals(i, 0)


class TestCTableScan(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)
        try:
            self.connection.create_table(TABLE_NAME, {'f': {}})
        except ValueError:
            pass
        self.table = _table(self.connection, TABLE_NAME)

    def tearDown(self):
        try:
            self.connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        self.connection.close()

    def test_timestamp_type(self):
        for i in range(0, 10):
            self.table.put('foo%i' % i, {'f:foo': 'foo'}, 5)

        for row, data in self.table.scan('', '', None, None, None):
            self.assertEquals(data, {'f:foo': 'foo'})

        for row, data in self.table.scan('', '', None, None, 5):
            self.assertEquals(data, {'f:foo': 'foo'})

        self.assertRaises(TypeError, self.table.scan, '', '', None, None, 'invalid')

    def test_include_timestamp_type(self):
        for i in range(0, 10):
            self.table.put('foo%i' % i, {'f:foo': 'foo'}, 5)

        for row, data in self.table.scan('', '', None, None, 5, None):
            self.assertEquals(data, {'f:foo': 'foo'})

        for row, data in self.table.scan('', '', None, None, 5, False):
            self.assertEquals(data, {'f:foo': 'foo'})

        for row, data in self.table.scan('', '', None, None, 5, True):
            self.assertEquals(data, {'f:foo': ('foo', 5)})

        self.assertRaises(TypeError, self.table.scan, '', '', None, None, 5, 'invalid')


class TestCTableScanColumns(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)

    def tearDown(self):
        try:
            self.connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        self.connection.close()

    def test_scan_columns_happy(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        for i in range(0, 10):
            self.table.put('foo%i' % i, {'f:a': 'foo%i' % i, 'f:ab': 'bar%i' % i, 'f:abc': 'baz%i' % i})

        i = 0
        for row, data in self.table.scan('', '', ('f',)):
            self.assertEquals(data, {'f:a': 'foo%i' % i, 'f:ab': 'bar%i' % i, 'f:abc': 'baz%i' % i})
            i += 1
        self.assertEquals(i, 10)

        i = 0
        for row, data in self.table.scan('', '', ('f:',)):
            self.assertEquals(data, {'f:a': 'foo%i' % i, 'f:ab': 'bar%i' % i, 'f:abc': 'baz%i' % i})
            i += 1
        self.assertEquals(i, 10)

        i = 0
        for row, data in self.table.scan('', '', ('f:a',)):
            self.assertEquals(data, {'f:a': 'foo%i' % i})
            i += 1
        self.assertEquals(i, 10)

        i = 0
        for row, data in self.table.scan('', '', ('f:a', 'f:ab')):
            self.assertEquals(data, {'f:a': 'foo%i' % i, 'f:ab': 'bar%i' % i})
            i += 1
        self.assertEquals(i, 10)

        i = 0
        for row, data in self.table.scan('', '', ('f:a', 'f:ab', 'f:abc',)):
            self.assertEquals(data, {'f:a': 'foo%i' % i, 'f:ab': 'bar%i' % i, 'f:abc': 'baz%i' % i})
            i += 1
        self.assertEquals(i, 10)

        i = 0
        for row, data in self.table.scan('', '', ('f:nope',)):
            self.assertEquals(data, {})
            i += 1
        self.assertEquals(i, 0)

        i = 0
        for row, data in self.table.scan('', '', ('f:a', 'f:nope')):
            self.assertEquals(data, {'f:a': 'foo%i' % i})
            i += 1
        self.assertEquals(i, 10)

    def test_columns_type(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.assertRaises(TypeError, self.table.scan, '', '', 'invalid')
        self.assertRaises(TypeError, self.table.scan, '', '', {'no', 'sets'})
        self.assertRaises(TypeError, self.table.scan, '', '', {'no': 'dicts'})

    def test_bad_column_family(self):
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)
        self.assertRaises(ValueError, self.table.scan, '', '', ('bad',))
        self.assertRaises(ValueError, self.table.scan, '', '', ('bad:',))
        self.assertRaises(ValueError, self.table.scan, '', '', ('bad:bad',))
        self.assertRaises(ValueError, self.table.scan, '', '', ('f:good', 'bad:bad'))






# TODO Need to add more columns to tests
class TestCTableBatch(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(ZOOKEEPERS)
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)

    def tearDown(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.close()

    def test_happy(self):
        self.table.batch([('put', 'foo{}'.format(i), {"f:bar{i}".format(i=i): 'baz{i}'.format(i=i)}) for i in range(1, 1001)])
        rows = sorted(self.table.scan(), key=lambda x: int(x[0][3:]))
        i = 1
        for row_key, obj in rows:
            self.assertEquals(row_key, "foo{i}".format(i=i))
            self.assertEquals(obj, {"f:bar{i}".format(i=i): 'baz{i}'.format(i=i)})
            i += 1

        self.assertEquals(i, 1001)

        self.table.batch([('delete', 'foo{}'.format(i)) for i in range(1, 1001)])

        i = 0
        for row_key, obj in self.table.scan():
            i += 1

        self.assertEquals(i, 0)

    def test_happy_big_column(self):
        self.table.batch([('put', 'foo%i' % i, {"f:bar%o" % o: "baz%o" % o for o in range(100)}) for i in range(1, 1001)])

    def test_mixed_errors_put(self):
        actions = [
            ('put', 'a', {'f:foo': 'bar'}),
            ('put', 'b', {'f': 'bar'}),
            ('put', 'c', {'f:': 'bar'}), # This is legal - not have the new put
            ('put', 'd', {':foo': 'bar'}),
            ('put', 'e', {'invalid:foo': 'bar'}),
            ('put', 'f', 'invalid data type'),
            ('put', 'g', {'f:foo', 'bar'}),
            (1, 'h', {'f:foo': 'bar'}),
            ('put', 2, {'f:foo': 'bar'}),
            ('put', 'j', 3),
            'not a tuple',
            ('invalid', 'k', {'f:foo': 'bar'}),
            ('put', 'z', {'f:foo': 'bar'}),
        ]
        errors, results = self.table.batch(actions)
        self.assertEquals(errors, len(actions) - 3)
        # TODO scan for the good rows
        i = 0
        for row_key, obj in self.table.scan():
            i += 1

        self.assertEquals(i, 3)

    def test_mixed_errors_delete(self):
        actions = [
            ('delete', 'a'),
            ('delete', 1),
            ('delete', {"foo":"bar"}),
            (1, 'a'),
            ('invalid', 'b'),
            'not a tuple'
        ]
        errors, results = self.table.batch(actions)
        self.assertEquals(errors, len(actions) - 1)

    def test_empty_actions(self):
        errors, results = self.table.batch([])
        self.assertEquals(errors, 0)

    def test_put_timestamp_type(self):
        errors, results = self.table.batch([('put', 'a', {'f:foo': 'bar'}, None)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('put', 'a', {'f:foo': 'bar'}, 5)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('put', 'a', {'f:foo': 'bar'}, 'invalid')])
        self.assertEquals(errors, 1)

    def test_delete_timestamp_type(self):
        errors, results = self.table.batch([('delete', 'a', None, None)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('delete', 'a', None, 5)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('delete', 'a', None, 'invalid')])
        self.assertEquals(errors, 1)

    def test_put_is_wal_type(self):
        errors, results = self.table.batch([('put', 'a', {'f:foo': 'bar'}, None, None)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('put', 'a', {'f:foo': 'bar'}, None, True)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('put', 'a', {'f:foo': 'bar'}, None, False)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('put', 'a', {'f:foo': 'bar'}, None, 'invalid')])
        self.assertEquals(errors, 1)

    def test_delete_is_wal_type(self):
        errors, results = self.table.batch([('delete', 'a', None, None, None)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('delete', 'a', None, None, True)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('delete', 'a', None, None, False)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('delete', 'a', None, None, 'invalid')])
        self.assertEquals(errors, 1)

    def test_delete_columns_type(self):
        errors, results = self.table.batch([('delete', 'a', None)])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('delete', 'a', ('f',))])
        self.assertEquals(errors, 0)

        errors, results = self.table.batch([('delete', 'a', 'invalid')])
        self.assertEquals(errors, 1)

        errors, results = self.table.batch([('delete', 'a', {'no', 'sets'})])
        self.assertEquals(errors, 1)

        errors, results = self.table.batch([('delete', 'a', {'no': 'dicts'})])
        self.assertEquals(errors, 1)





class TestPython(unittest.TestCase):
    def setUp(self):
        # TODO Configure this for non-mapr users
        self.connection = Connection()
        try:
            self.connection.create_table(TABLE_NAME, {'f': {}})
        except ValueError:
            pass
        self.table = self.connection.table(TABLE_NAME)

    def tearDown(self):
        try:
            self.connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        self.connection.close()

    def test_rows(self):
        for i in range(5):
            self.table.put("foo%i" % i, {'f:foo': 'bar%i' % i})

        rows = self.table.rows(['foo%i' % i for i in range(1, 4)])
        self.assertEquals(rows, [{'f:foo': 'bar%i' % i} for i in range(1, 4)])

class TestPythonHappy(unittest.TestCase):
    def setUp(self):
        connection = Connection()
        try:
            connection.delete_table(TABLE_NAME)
        except ValueError:
            pass

        connection.close()

    def tearDown(self):
        connection = Connection()
        try:
            connection.delete_table(TABLE_NAME)
        except ValueError:
            pass

        connection.close()

    def test_extract_zookeeper(self):
        mapr_clusters_conf = StringIO("hadoopDev secure=true myhost:7222 myhost2:7222 myhost3:7222\n")
        cldbs = Connection._extract_mapr_cldbs(mapr_clusters_conf)
        self.assertEquals(cldbs, 'myhost:7222,myhost2:7222,myhost3:7222')

        mapr_clusters_conf = StringIO("Mapr5.1 hadoopDev secure=true myhost:7222 myhost2:7222 myhost3:7222\n")
        cldbs = Connection._extract_mapr_cldbs(mapr_clusters_conf)
        self.assertEquals(cldbs, 'myhost:7222,myhost2:7222,myhost3:7222')

    def test_happy(self):
        connection = Connection()
        connection.create_table(TABLE_NAME, {'f': {}})
        table = connection.table(TABLE_NAME)
        for i in range(0, 10):
            table.put("a{}".format(i), {"f:foo{}".format(i): "bar{}".format(i)})

        self.assertEquals(table.row("a0"), {"f:foo0": "bar0"})
        self.assertEquals(table.row("a4"), {"f:foo4": "bar4"})
        self.assertEquals(table.row("a9"), {"f:foo9": "bar9"})

        i = 0
        for row_key, obj in table.scan():
            self.assertEquals(row_key, 'a{}'.format(i))
            self.assertEquals(obj, {"f:foo{}".format(i): "bar{}".format(i)})
            i += 1

        self.assertEquals(i, 10)

        i = 1
        for row_key, obj in table.scan(start="a1"):
            self.assertEquals(row_key, 'a{}'.format(i))
            self.assertEquals(obj, {"f:foo{}".format(i): "bar{}".format(i)})
            i += 1

        self.assertEquals(i, 10)

        i = 0
        for row_key, obj in table.scan(stop="a8~"):
            self.assertEquals(row_key, 'a{}'.format(i))
            self.assertEquals(obj, {"f:foo{}".format(i): "bar{}".format(i)})
            i += 1

        self.assertEquals(i, 9)

        i = 1
        for row_key, obj in table.scan(start="a1", stop="a8~"):
            self.assertEquals(row_key, 'a{}'.format(i))
            self.assertEquals(obj, {"f:foo{}".format(i): "bar{}".format(i)})
            i += 1

        self.assertEquals(i, 9)

        table.delete("a0")
        table.delete("a9")
        self.assertEquals(table.row("a0"), {})
        self.assertEquals(table.row("a9"), {})

        batch = table.batch()
        for i in range(1000):
            batch.put("test{}".format(i), {"f:foo{}".format(i): "bar{}".format(i)})

        i = 0
        for row_key, obj in table.scan(start='test', stop='test~'):
            i += 1

        self.assertEquals(i, 0)

        errors = batch.send()
        self.assertEquals(errors, 0)

        self.assertEquals(len(batch._actions), 0)

        i = 0
        for row_key, obj in table.scan(start='test', stop='test~'):
            i += 1

        self.assertEquals(i, 1000)

        table.delete_prefix("test")

        i = 0
        for row_key, obj in table.scan(start='test', stop='test~'):
            i += 1

        self.assertEquals(i, 0)


class TestPythonRowPrefix(unittest.TestCase):
    def setUp(self):
        # TODO Configure this for non-mapr users
        self.connection = Connection()
        try:
            self.connection.create_table(TABLE_NAME, {'f': {}})
        except ValueError:
            pass
        self.table = self.connection.table(TABLE_NAME)
        batch = self.table.batch()
        batch.put('a', {'f:foo': 'foo'})
        for i in range(10):
            batch.put('b%i' % i, {'f:foo': 'bar'})
        batch.put('c', {'f:foo': 'baz'})
        batch.send()

    def tearDown(self):
        try:
            self.connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        self.connection.close()

    def test_row_prefix_happy(self):
        i = 0
        for row, data in self.table.scan(row_prefix='b'):
            self.assertEquals(row, 'b%i' % i)
            self.assertEquals(data, {'f:foo': 'bar'})
            i += 1

        self.assertEquals(i, 10)

    def test_start_happy(self):
        i = 0
        for row, data in self.table.scan(start='b'):
            i += 1

        self.assertEquals(row, 'c')
        self.assertEquals(data, {'f:foo': 'baz'})
        self.assertEquals(i, 11)

    def test_stop_happy(self):
        i = 0
        for row, data in self.table.scan(stop='b9~'):
            i += 1

        self.assertEquals(row, 'b9')
        self.assertEquals(data, {'f:foo': 'bar'})
        self.assertEquals(i, 11)

    def test_start_stop_happy(self):
        i = 2
        for row, data in self.table.scan(start='b2', stop='b5~'):
            self.assertEquals(row, 'b%i' % i)
            self.assertEquals(data, {'f:foo': 'bar'})
            i += 1

        self.assertEquals(i, 6)

    def test_mix(self):
        self.assertRaises(TypeError, self.table.scan(start='foo', stop='bar', row_prefix='foobar'))
        self.assertRaises(TypeError, self.table.scan(start='foo', row_prefix='foobar'))
        self.assertRaises(TypeError, self.table.scan(stop='bar', row_prefix='foobar'))


if __name__ == '__main__':
    s = datetime.now()
    unittest.main()
    e = datetime.now()
    print("Tests took %s" % (e - s))
