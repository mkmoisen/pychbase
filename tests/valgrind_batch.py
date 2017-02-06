from datetime import datetime
from _pychbase import _connection, _table
CLDBS = "hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222"
TABLE_NAME = '/app/SubscriptionBillingPlatform/testpymaprdb'
connection = _connection(CLDBS)
try:
    connection.delete_table(TABLE_NAME)
except ValueError:
    pass
connection.create_table(TABLE_NAME, {'f': {}})
table = _table(connection, TABLE_NAME)
lol = [('put', 'hello{}'.format(i), {'f:bar':'bar{}'.format(i)}) for i in range(100000)]
table.batch(lol, True)

lol = [('put', 'hello{}'.format(i), {'f:bar':'bar{}'.format(i)}) for i in range(100000)]
table.batch(lol, True)

lol = [('put', 'hello{}'.format(i), {'f:bar':'bar{}'.format(i)}) for i in range(100000)]
table.batch(lol, True)

connection.close()
