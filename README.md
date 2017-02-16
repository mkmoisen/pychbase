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

# Installation

Please note that the following environment variables must be set in order to install `pychbase` correctly:

 * PYCHBASE_IS_MAPR
 * PYCHBASE_LIBJVM_DIR
 * PYCHBASE_INCLUDE_DIR
 * PYCHBASE_LIBRARY_DIR

## PYCHBASE_IS_MAPR

This defaults to TRUE. IF you are using Cloudera/etc, make sure to:

    export PYCHBASE_IS_MAPR=FALSE

## PYCHBASE_LIBJVM_DIR

This is the directory that houses the `libjvm.so` file. Normally it is in either:

 * $JAVA_HOME/lib/amd64/server
 * $JAVA_HOME/jre/lib/amd64/server

If `PYCHBASE_LIBJVM_DIR` is not set, the installer will check if `JAVA_HOME` has been set, and then try each of the above directories.
If `JAVA_HOME` is not set, it will attempt to default to `/usr/lib/jvm/jre-1.7.0/`

Example:

    export PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/jre-1.7.0/lib/amd64/server

## PYCHBASE_INCLUDE_DIR

This houses the `/hbase/hbase.h` and other `libhbase` C header files.

If `PYCHBASE_IS_MAPR` is true, this defaults to /opt/mapr/include.

For Non-MapR environments, this must be set or the installation will fail.

Example on Cloudera:

    export PYCHBASE_INCLUDE_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/include

## PYCHBASE_LIBRARY_DIR

This houses either the `libMapRClient.so` file on MapR environemnts, or the libhbase.so file on non-MapR environments.

If `PYCHBASE_IS_MAPR` is true, this defaults to /opt/mapr/lib.

For Non-MapR environments, this must be set or the installation will fail.

Example on Cloudera:

    export PYCHBASE_LIBRARY_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/lib/native

# Installation on a MapR environment

Normally, the only environment variable to worry about on a MapR environment is $PYCHBASE_LIBJVM_DIR

## Installation through Pip

    export PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/jre-1.7.0/lib/amd64/server
    virtualenv pychbase
    cd pychbase
    source bin/activate
    pip install pychbase

## Building from source

    export PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/jre-1.7.0/lib/amd64/server
    virtualenv pychbase
    cd pychbase
    source bin/activate
    git clone https://github.com/mkmoisen/pychbase.git
    cd pychbase
    python setup.py install

# Installation on a non-MapR environment

For non-MapR environments you have to worry about all the environment variables.

## Installation through Pip

    export PYCHBASE_IS_MAPR=FALSE
    export PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/jre-1.7.0/lib/amd64/server
    export PYCHBASE_INCLUDE_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/include
    export PYCHBASE_LIBRARY_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/lib/native
    virtualenv pychbase
    cd pychbase
    source bin/activate
    pip install pychbase

## Building from source

    export PYCHBASE_IS_MAPR=FALSE
    export PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/jre-1.7.0/lib/amd64/server
    export PYCHBASE_INCLUDE_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/include
    export PYCHBASE_LIBRARY_DIR=/home/matthew/libhbase/target/libhbase-1.0-SNAPSHOT/lib/native
    virtualenv pychbase
    cd pychbase
    source bin/activate
    git clone https://github.com/mkmoisen/pychbase.git
    cd pychbase
    python setup.py install

# Run the tests

The `config.py` file in the `tests` directory has two constants, `ZOOKEEPER` and `TABLE_NAME`, that probably won't work if you run the tests.

Create a `tests/local_config.py` file like the following:

    ZOOKEEPERS = 'localhost:7222'
    TABLE_NAME = 'testpychbase'

To run the tests, make sure to be in the `tests` directory, or else you will face import problems:

    cd tests
    python tests.py

Currently `nosetests` will not work without facing an import issue.

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

Note that attempting to batch/put unescaped null terminators will result in them being stripped.
Attempting to use a row key with an unescaped null terminator will raise a TypeException.
It is the users duty to escap null terminators before attempting to batch/put data.

    table.put('foo', {'f:foo\0bar': 'baz\0bak'})
    obj = table.row('foo')
    assert obj == {'f:foo': 'baz'}

    table.put('bar', {'f:foo\\0bar': 'baz\00bak'})
    obj = table.row('foo')
    assert obj == {'f:foo\\0bar': 'baz\00bak'}

# Happybase compatibility

One goal of this library is to maintain compatibility with the APIs in HappyBase.

Check out __init__.py to understand which features of HappyBase I have not yet implemented.

In the future, I will force print warnings to stderr in the event a user uses an unimplemented feature.

# License
MIT