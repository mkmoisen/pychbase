import unittest
from pychbase._pychbase import _connection, _table
from pychbase import Connection, Table, Batch
from StringIO import StringIO
from config import ZOOKEEPERS, TABLE_NAME


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

    def test_type(self):
        self.assertRaises(TypeError, self.table.delete, 1)
        self.assertRaises(TypeError, self.table.delete, {'foo':'bar'})

    def test_empty_row_key(self):
        self.assertRaises(ValueError, self.table.delete, '')


class TestCTableScanHappy(unittest.TestCase):
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


class TestPython(unittest.TestCase):
    def setUp(self):
        pass

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


if __name__ == '__main__':
    unittest.main()
