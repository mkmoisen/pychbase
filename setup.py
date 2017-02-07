from distutils.core import setup, Extension
from os import path
from codecs import open

here = path.abspath(path.dirname(__file__))

long_description = 'A Python wrapper for the libhbase C API to HBase'
try:
    import requests
    def read_md(file_name):
        try:
            with open(file_name, 'r') as f:
                contents = f.read()
        except IOError as ex:
            return long_description
        try:
            r = requests.post(url='http://c.docverter.com/convert',
                              data={'to': 'rst', 'from': 'markdown'},
                              files={'input_files[]': contents})
            if not r.ok:
                raise Exception(r.text)
            return r.text
        except Exception as ex:
            print ex
            return open(file_name, 'r').read()
except ImportError:
    def read_md(file_name):
        try:
            with open(file_name, 'r') as f:
                print("requests module not available-- cannot convert MD to RST")
                return f.read()
        except IOError:
            return long_description



module1 = Extension('pychbase._pychbase',
                    sources=['pychbase.cc'],
                    include_dirs=['/opt/mapr/include'],
                    libraries=['MapRClient','jvm'],
                    library_dirs=['/opt/mapr/lib','/usr/lib/jvm/jre-1.7.0/lib/amd64/server/'])

setup(name='pychbase',
      version='0.1.5',
      description=long_description,
      long_description=read_md('README.md'),
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
