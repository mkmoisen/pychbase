from distutils.core import setup, Extension

module1 = Extension('pychbase',
                    sources=['pychbase.cc'],
                    include_dirs=['/opt/mapr/include'],
                    libraries=['MapRClient','jvm'],
                    library_dirs=['/opt/mapr/lib','/usr/lib/jvm/jre-1.7.0/lib/amd64/server/'])


setup (name='pychbase',
       version='1.0',
       description='A Python wrapper for the libhbase C API to HBase',
       ext_modules=[module1])
