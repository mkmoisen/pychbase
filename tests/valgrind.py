from spam import _connection, _table

CLDBS = "hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222"
TABLE_NAME = '/app/SubscriptionBillingPlatform/testpymaprdb'

def setUp():
    connection = _connection(CLDBS)
    try:
        connection.create_table(TABLE_NAME, {'f': {}})
    except ValueError:
        pass
    table = _table(connection, TABLE_NAME)
    return connection, table


def tearDown(connection, table):
    connection.delete_table(TABLE_NAME)
    connection.close()


def main():
    connection, table = setUp()
    for i in range(1, 10000):
        table.put("foo{i}".format(i=i), {"f:bar{i}".format(i=i): 'baz{i}'.format(i=i)})
    tearDown(connection, table)

if __name__ == '__main__':
    main()