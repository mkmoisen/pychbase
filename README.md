# pychbase
This is a Python C wrapper for HBase using the [libhbase C API](https://github.com/mapr/libhbase).

Currently in beta, `pychbase` is tested on Python 2.7 and MapR 5.1.

# LD_LIBRARY_PATH

To compile as well as import `pychbase`, your `LD_LIBRARY_PATH` must have the directory with libjvm.so on it, normally in either:

 * $JAVA_HOME/jre/lib/amd64/server
 * $JAVA_HOME/lib/amd64/server

If you are using this with MapR, you must also have `/opt/mapr/lib` on your `LD_LIBRARY_PATH`

E.g,

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server:/opt/mapr/lib

# Installation on a MapR environment

    virtualenv pychbase
    cd pychbase
    source bin/activate
    pip install nose
    git clone https://github.com/mkmoisen/pychbase.git
    cd pychbase
    python setup.py install

# Installation on a non-MapR environment

The current `setup.py` script assumes a MapR environment. If you are not on MapR, you'll have to remove the mentions of MapR in the setup.py file, notably these strings:

    '/opt/mapr/include'
    'MapRClient'
    '/opt/mapr/lib'

However, you will need to hunt down the location of the `hbase.h` file that has hopefully been included with your distribution.
I haven't yet had a chance to download Cloudera or Horton works and test it out.

Once you find the directory for `hbase.h`, replace the `/opt/mapr/include` in `setup.py` with that directory

# Run the tests

Currently you will have to open `tests.py` and replace the `ZOOKEEPERS` and `TABLE_NAME` constants with those in your environment. Afterwords, just run

    python tests.py

or

    nosetests -v

# Usage

I have attempted to mimic the great HappyBase API somewhat.

Imports:

    from pychbase import Connection, Table, Batch

To create a connection:

    # On MapR, you don't need to specify the CLDBS/zookeepers:
    connection = Connection()

    # On non-MapR environments, you'll need to specify the zookeepers:
    connection = Connection('zookeeper-one:722,zookeeper-two:7222,zookeeper-three:7222')

To create and delete tables:

    # Create a table named 'test_table' with a single column family named 'f' with no additional attributes
    connection.create_table('test_table', {'f': {}})
    connection.delete_table('test_table')

To get a table to operate one:

    table = connection.table('test_table')

To put, delete, and get from a table:

    table.put('rowkey1', {'f:foo': 'bar'})
    obj = table.row('rowkey1')
    assert obj == {'f:foo': 'bar'}
    table.delete('rowkey1')
    obj = table.row('rowkey1')
    assert obj == {}

To scan:

    # Full table scan:
    for row_key, obj in table.scan():
        pass

    # Scan with a start and stop:
    for row_key, obj in table.scan('foo', 'bar'):
        pass

To batch put:

    batch = table.batch()
    objs = [
        ('foo', 'a', 'b'),
        ('foo1', 'a1', 'b1'),
        ('foo2', 'a2', 'b2'),
    ]
    for obj in objs:
        batch.put(obj[0], {'f:foo': obj[1], 'f:bar': obj[2]})

    batch.send()

To batch delete:

    batch = table.batch()
    row_keys = ['foo', 'foo1', 'foo2']
    for row_key in row_keys:
        batch.delete(row_key)

    batch.delete()

# License
MIT