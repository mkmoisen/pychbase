# pychbase
This is a Python C wrapper for MapRDB and HBase using the [libhbase C API](https://github.com/mapr/libhbase).

`pychbase` is modeled after the HappyBase API, but it does not use `thrift`, and is ideal for MapRDB.

`pychbase` is tested on Python 2.7 and MapR 5.1.

# LD_LIBRARY_PATH

To compile as well as import `pychbase`, your `LD_LIBRARY_PATH` must have the directory with `libjvm.so` on it, normally in either:

 * $JAVA_HOME/lib/amd64/server
 * $JAVA_HOME/jre/lib/amd64/server

If you are using this with MapR, you must also have `/opt/mapr/lib` on your `LD_LIBRARY_PATH`

E.g,

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib/amd64/server:/opt/mapr/lib

# Installation

## Installation on a MapR environment

Normally, the only environment variable to worry about on a MapR environment is $PYCHBASE_LIBJVM_DIR

    export PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/jre-1.7.0/lib/amd64/server
    virtualenv pychbase
    cd pychbase
    source bin/activate
    pip install pychbase

    # Or build it from source
    git clone https://github.com/mkmoisen/pychbase.git
    cd pychbase
    python setup.py install

## Installation on a Non-MapR environment

Please see the end of the readme for Cloudera intallation notes.

# Run the tests

The `config.py` file in the `tests` directory has two constants, `ZOOKEEPER` and `TABLE_NAME`, that probably won't work if you run the tests without modification

Create a `tests/local_config.py` file like the following:

    ZOOKEEPERS = 'localhost:7222'
    TABLE_NAME = 'testpychbase'

To run the tests, make sure to be in the `tests` directory, or else you will face import problems:

    cd tests
    python tests.py

Currently `nosetests` will not work without facing an import issue.

# Usage

I have attempted to mimic the great HappyBase API as closely as possible.

Make sure to set the LD_LIBRARY_PATH environment variable:

    # MapR
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server:/opt/mapr/lib

    # Non MapR
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/jvm/jre-1.7.0/lib/amd64/server::/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/lib/native
    export HBASE_LIB_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/lib/
    # I've only gotten it to work on CDH4. If you are on CDH5 you'll need to mess around with the classpath some more

Imports:

    from pychbase import Connection, Table, Batch

To create a connection:

    # On MapR, you don't need to specify the CLDBS/zookeepers:
    connection = Connection()

    # On Non-MapR environments, you'll need to specify the zookeepers:
    connection = Connection('zookeeper-one:2181",zookeeper-two:2181",zookeeper-three:2181"')

To create and delete tables:

    # Create a table named 'test_table' with a single column family named 'f' with no additional attributes
    connection.create_table('test_table', {'f': {}})
    connection.delete_table('test_table')

To get a table to operate on:

    table = connection.table('test_table')

To put, delete, and get from a table:

    # Put a row with the current timestamp
    table.put('rowkey1', {'f:foo': 'bar', 'f:hai': 'bai'})
    data = table.row('rowkey1')
    assert data == {'f:foo': 'bar', 'f:hai': 'bai'}

    # Get a row and only include a single column
    data = table.row('rowkey1', columns=('f:foo',))
    assert data == {'f:foo': 'bar'}

    # Delete the row
    table.delete('rowkey1')
    data = table.row('rowkey1')
    assert data == {}

    # Put a row with a given timestamp
    table.put('rowkey1', {'f:foo': 'bar'}, timestamp=1000)
    table.put('rowkey1', {'f:foo': 'BAR'}, timestamp=10000)

    # Get a row with a given timestamp and include its timestamp
    data = table.row('rowkey1', timestamp=1000, include_timestamp=True)
    assert data == {'f:foo': ('bar', 1000)}

To scan:

    # Full table scan:
    for row, data in table.scan():
        pass

    # Scan with a start and stop:
    for row, data in table.scan('foo', 'bar'):
        pass

    # Scan with a row prefix:
    for row, data in table.scan(row_prefix='baz'): # E.g., start='baz', stop='baz~'
        pass

    # Scan with a filter: # Check out tests.py on how to use all of the filters
    for row, data in table.scan(filter="SingleColumnValueFilter('f', 'foo', =, 'binary:foo')":
        pass

    # Scan a table but return only row keys, no rows:
    table.put('foo', {'f:foo': 'foo'}
    table.put('foo1', {'f:foo': 'foo'}
    table.put('foo2', {'f:foo': 'foo'}

    rows = list(table.scan(only_rowkeys=True))
    assert rows == ['foo', 'foo1', 'foo2']

To count the number of rows in a table:

    # Full table count:
    count = table.count()

    # Count all rows with a start and stop
    count = table.count('foo', 'bar')

    # Count all rows whose row key starts with a row prefix:
    count = table.count(row_prefix='baz') # E.g., start='baz', stop='baz~'

    # Count all rows with a filter:
    count = table.count(filter="SingleColumnValueFilter('f', 'foo', =, 'binary:foo')")

To batch put:

    batch = table.batch()
    datas = [
        ('foo', 'a', 'b'),
        ('foo1', 'a1', 'b1'),
        ('foo2', 'a2', 'b2'),
    ]

    for data in datas:
        batch.put(data[0], {'f:foo': data[1], 'f:bar': data[2]})

    errors = batch.send()

    assert errors = 0

To batch delete:

    batch = table.batch()
    rows = ['foo', 'foo1', 'foo2']
    for row in rows:
        batch.delete(row)

    errors = batch.send()

    assert errors = 0

Note that `batch.send()` returns the number of errors that occurred, if any. It is up to the client to ignore this or raise an exception.

    batch = table.batch()
    batch.put('foo', {'f:foo', 'bar'})
    batch.put('foo', 'invalid')

    errors = batch.send()
    assert errors == 1


An additional helper method is `table.delete_prefix(row_prefix)`, which deletes all rows containing starting with the prefix.

    table.put('foo', {'f:foo', 'foo'}
    table.put('foo1', {'f:foo', 'foo'}
    table.put('foo2', {'f:foo', 'foo'}
    table.put('foo3', {'f:foo', 'foo'}
    table.put('bar', {'f:bar', 'bar'}

    number_deleted = table.delete_prefix('foo')
    assert number_deleted == 3

    assert table.count() == 2

Note that attempting to batch/put unescaped null terminators will result in them being stripped.
Attempting to use a row key with an unescaped null terminator will raise a TypeException.
It is the users duty to escap null terminators before attempting to batch/put data.

    table.put('foo', {'f:foo\0bar': 'baz\0bak'})
    data = table.row('foo')
    assert data == {'f:foo': 'baz'}

    table.put('bar', {'f:foo\\0bar': 'baz\00bak'})
    data = table.row('foo')
    assert data == {'f:foo\\0bar': 'baz\00bak'}

# Happybase compatibility

One goal of this library is to maintain compatibility with the APIs in HappyBase.

Check out __init__.py to understand which features of HappyBase I have not yet implemented.

In the future, I will force print warnings to stderr in the event a user uses an unimplemented feature.

# Non-MapR Installation and Environment Variables Guide

I have not tested `pychbase` heavily on Cloudera. I couldn't get it working on CDH5 due to a classpath issue with `libhbase`, and
and while I was able to get it up and running with CDH4, some of the tests are failing. It seems to me that `libhbase` is not fully compatible outside of MapR.

    export PYCHBASE_IS_MAPR=FALSE
    export PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/jre-1.7.0/lib/amd64/server
    export PYCHBASE_INCLUDE_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/include
    export PYCHBASE_LIBRARY_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/lib/native
    virtualenv pychbase
    cd pychbase
    source bin/activate
    pip install pychbase

    # Or build it from source
    git clone https://github.com/mkmoisen/pychbase.git
    cd pychbase
    python setup.py install

Please note that the following environment variables must be set in order to install `pychbase` correctly:

 * PYCHBASE_IS_MAPR
 * PYCHBASE_LIBJVM_DIR
 * PYCHBASE_INCLUDE_DIR
 * PYCHBASE_LIBRARY_DIR

## PYCHBASE_IS_MAPR

This defaults to `TRUE`. IF you are using Cloudera/etc, make sure to:

    export PYCHBASE_IS_MAPR=FALSE

## PYCHBASE_LIBJVM_DIR

This is the directory that houses the `libjvm.so` file. Normally it is in either:

 * $JAVA_HOME/lib/amd64/server
 * $JAVA_HOME/jre/lib/amd64/server

If `PYCHBASE_LIBJVM_DIR` is not set, the installer will check if `JAVA_HOME` has been set, and then try each of the above directories.
If `JAVA_HOME` is not set, it will attempt to default to `/usr/lib/jvm/jre-1.7.0/`.

Example:

    export PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/jre-1.7.0/lib/amd64/server

## PYCHBASE_INCLUDE_DIR

This houses the `/hbase/hbase.h` and other `libhbase` C header files.

If `PYCHBASE_IS_MAPR` is true, this defaults to `/opt/mapr/include`.

For Non-MapR environments, this must be set or the installation will fail.

Example on Cloudera:

    export PYCHBASE_INCLUDE_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/include

## PYCHBASE_LIBRARY_DIR

This houses either the `libMapRClient.so` file on MapR environments, or the `libhbase.so` file on Non-MapR environments.

If `PYCHBASE_IS_MAPR` is true, this defaults to `/opt/mapr/lib`.

For Non-MapR environments, this must be set or the installation will fail.

Example on Cloudera:

    export PYCHBASE_LIBRARY_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/lib/native

# Author
[Matthew Moisen](https://matthewmoisen.com)

# License
MIT