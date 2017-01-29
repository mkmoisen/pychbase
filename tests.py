import unittest
#from pymaprdb import Connection, Table, Batch
from spam import _connection, _table

# TODO lol I reimported _connection and _table once and it resulted in a segmentation fault?

CLDBS = "hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222"

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


class TestCTableInit(unittest.TestCase):
    def test_bad_table(self):
        connection = _connection(CLDBS)
        connection.open()
        self.assertRaises(ValueError, _table.connection, '/app/SubscriptionBillingPlatform/alksdjfklasdfjkl')
        connection.close()

    def test_unopened_connection(self):
        connection = _connection(CLDBS)
        table = _table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
        connection.close()

class TestCTableRow(unittest.TestCase):
    def test_closed_connection(self):
        connection = _connection(CLDBS)
        connection.open()
        table = _table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
        connection.close()
        table.row('hello')

    def test_read_only_table_name(self):
        connection = _connection(CLDBS)
        connection.open()
        table = _table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
        self.assertRaises(TypeError, setattr, table, 'table_name', 'foo')





from spam import _connection, _table
CLDBS = "hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222"
connection = _connection(CLDBS)
connection.open()
table = _table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
connection.close()
table.row('hello')