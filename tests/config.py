ZOOKEEPERS = 'localhost:7222'
TABLE_NAME = 'testpychbase'

try:
    from local_config import *
except ImportError:
    pass