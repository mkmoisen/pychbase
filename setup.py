from distutils.core import setup, Extension
from os import path
from codecs import open

module1 = Extension('_pychbase',
                    sources=['pychbase.cc'],
                    include_dirs=['/opt/mapr/include'],
                    libraries=['MapRClient','jvm'],
                    library_dirs=['/opt/mapr/lib','/usr/lib/jvm/jre-1.7.0/lib/amd64/server/'])


here = path.abspath(path.dirname(__file__))

try:
    with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
        long_description = f.read()
except IOError:
    long_description = 'A Python wrapper for the libhbase C API to HBase'

setup(name='pychbase',
      version='0.1',
      description='A Python wrapper for the libhbase C API to HBase',
      long_description=long_description,
      url='https://github.com/mkmoisen/pychbase',
      download_url='https://github.com/mkmoisen/pychbase/tarball/0.1',
      author='Matthew Moisen',
      author_email='mmoisen@cisco.com',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: Implementation :: CPython',
          'Natural Language :: English',
          'Operating System :: POSIX :: Linux',
          'Topic :: Database',
      ],
      keywords='hbase libhbase',

      ext_modules=[module1],
      packages=['pychbase'])
