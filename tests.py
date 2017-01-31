import unittest
#from pymaprdb import Connection, Table, Batch
from spam import _connection, _table

# TODO lol I reimported _connection and _table once and it resulted in a segmentation fault?

CLDBS = "hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222"

TABLE_NAME = '/app/SubscriptionBillingPlatform/testpymaprdb'


class TestCConnection(unittest.TestCase):
    def test_bad_cldbs(self):
        connection = _connection('abc')
        self.assertFalse(connection.is_open())
        self.assertRaises(ValueError, connection.open)
        self.assertFalse(connection.is_open())
        connection.close()

    def test_good_cldbs(self):
        connection = _connection(CLDBS)
        self.assertFalse(connection.is_open())
        connection.open()
        self.assertTrue(connection.is_open())
        connection.close()
        self.assertFalse(connection.is_open())
        connection.close()


class TestCConnectionManageTable(unittest.TestCase):
    def setUp(self):
        connection = _connection(CLDBS)
        try:
            connection.delete_table(TABLE_NAME)
        except ValueError:
            pass
        connection.close()

    def test_good(self):
        connection = _connection(CLDBS)
        connection.create_table(TABLE_NAME, {'f': {}})
        connection.delete_table(TABLE_NAME)

    def test_already_created(self):
        connection = _connection(CLDBS)
        connection.create_table(TABLE_NAME, {'f': {}})
        self.assertRaises(ValueError, connection.create_table, TABLE_NAME, {'f': {}})
        connection.delete_table(TABLE_NAME)

    def test_already_deleted(self):
        connection = _connection(CLDBS)
        connection.create_table(TABLE_NAME, {'f': {}})
        connection.delete_table(TABLE_NAME)
        self.assertRaises(ValueError, connection.delete_table, TABLE_NAME)



class TestCTableInit(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(CLDBS)
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
        self.connection = _connection(CLDBS)
        table = _table(self.connection, TABLE_NAME)
        self.connection.close()



class TestCTableRow(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(CLDBS)
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
        self.connection = _connection(CLDBS)
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)

    def tearDown(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.close()

    def test_happy(self):
        self.table.put("foo", {"f:bar": "baz"})
        row = self.table.row('foo')
        self.assertEquals(row, {'f:bar': "baz"})

    def test_empty_put(self):
        self.assertRaises(ValueError, self.table.put, 'foo', {})

    def test_bad_column_family_no_colon(self):
        """All keys in the put dict must contain a colon separating the family from the qualifier"""
        self.assertRaises(ValueError, self.table.put, 'foo', {'bar': 'baz'})

    def test_invalid_column_family(self):
        self.assertRaises(ValueError, self.table.put, 'foo', {"f:bar": "baz", 'invalid:foo': 'bar'})
        row = self.table.row('foo')
        self.assertEquals(row, {})




class TestCTableDelete(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(CLDBS)
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

    def test_empty_row_key(self):
        self.assertRaises(ValueError, self.table.delete, '')


class TestCTableScanHappy(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(CLDBS)
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
            print "WTF???", row_key, obj
            i += 1

        self.assertEquals(i, 0)



class TestCTableBatch(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(CLDBS)
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

    def test_mixed_errors(self):
        raise NotImplementedError


if __name__ == '__main__':
    unittest.main()

"""

import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.batch([('put', 'hello{}'.format(i), {'Name:bar':'bar{}'.format(i)}) for i in range(100000)])
#table.scan()


import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.batch([('delete', 'hello{}'.format(i), {'Name:bar':'bar{}'.format(i)}) for i in range(100000)])


from spam import _connection, _table

# TODO lol I reimported _connection and _table once and it resulted in a segmentation fault?

CLDBS = "hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222"

TABLE_NAME = '/app/SubscriptionBillingPlatform/testpymaprdb'

class TestCTableRow(unittest.TestCase):
def setUp(self):
connection = _connection(CLDBS)
connection.create_table(TABLE_NAME, {'f': {}})
table = _table(connection, TABLE_NAME)
table.put("foo", {"f:bar": "baz"})

def tearDown(self):
try:
    connection.delete_table(TABLE_NAME)
except ValueError:
    pass
self.connection.close()

def test_happy(self):
row = table.row('foo')
self.assertEquals(row, {'f:bar': "baz"})

# TODO Add test for empty put body
"""


"""
from spam import _connection, _table
CLDBS = "hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222"
TABLE_NAME = '/app/SubscriptionBillingPlatform/testpymaprdb'
connection = _connection(CLDBS)
connection.create_table(TABLE_NAME, {'f': {}})
table = _table(connection, TABLE_NAME)
table.put("lol", {})


for i in range(1, 10):
    table.put("foo{i}".format(i=i), {"f:bar{i}".format(i=i): 'baz{i}'.format(i=i)})

for i in range(1, 10):
    table.put("aaa{i}".format(i=i), {"f:aaa{i}".format(i=i): 'aaa{i}'.format(i=i)})

for i in range(1, 10):
    table.put("zzz{i}".format(i=i), {"f:zzz{i}".format(i=i): 'zzz{i}'.format(i=i)})

"""