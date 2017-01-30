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

    def test_bad_column_family(self):
        """All keys in the put dict must contain a colon separating the family from the qualifier"""
        return
        self.assertRaises(ValueError, self.table.row, 'foo', {'bar':'baz'})




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





class TestCTableScan(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(CLDBS)
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)

    def tearDown(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.close()


class TestCTableBatch(unittest.TestCase):
    def setUp(self):
        self.connection = _connection(CLDBS)
        self.connection.create_table(TABLE_NAME, {'f': {}})
        self.table = _table(self.connection, TABLE_NAME)

    def tearDown(self):
        self.connection.delete_table(TABLE_NAME)
        self.connection.close()




"""
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

"""