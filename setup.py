from distutils.core import setup, Extension
from os import path
from codecs import open
import os
import sys


is_fatal = False
'/usr/lib/jvm/jre-1.7.0/lib/amd64/server/'
# Both MapR and non-MapR environments require the libjvm.so file
libjvm_dir = os.environ.get('PYCHBASE_LIBJVM_DIR', None)
if libjvm_dir is None:
    sys.stderr.write('WARNING: $PYCHBASE_LIBJVM_DIR not set, trying $JAVA_HOME...\n')
    java_home = os.environ.get('JAVA_HOME', None)
    if java_home is None:
        sys.stderr.write("WARNING: $JAVA_HOME not set, trying '/usr/lib/jvm/jre-1.7.0/'\n")
        java_home = '/usr/lib/jvm/jre-1.7.0/'

    if os.path.isdir(java_home):
        if os.path.isdir(os.path.join(java_home, 'lib', 'amd64', 'server')):
            sys.stderr.write('WARNING: Located %s\n' % os.path.join(java_home, 'lib', 'amd64', 'server'))
            libjvm_dir = os.path.join(java_home, 'lib', 'amd64', 'server')
        elif os.path.isdir(os.path.join(java_home, 'jre', 'lib', 'amd64', 'server')):
            sys.stderr.write('WARNING: Located %s\n' % os.path.join(java_home, 'jre', 'lib', 'amd64', 'server'))
            libjvm_dir = os.path.join(java_home, 'jre', 'lib', 'amd64', 'server')
        else:
            sys.stderr.write("ERROR: Could not detect the directory of libjvm.so from $JAVA_HOME\n")
            is_fatal = True
    else:
        is_fatal = True
else:
    if not os.path.isdir(libjvm_dir):
        sys.stderr.write("ERROR: libjvm directory does not exist '%s' \n" % libjvm_dir)
        is_fatal = True

is_mapr = os.environ.get('PYCHBASE_IS_MAPR', None)
if is_mapr is None:
    sys.stderr.write('WARNING: $PYCHBASE_IS_MAPR not set, defaulting to True. This will not work on Non-MapR environments. \n\tPlease export $PYCHBASE_IS_MAPR=FALSE if this is on Cloudera/etc\n')
    is_mapr = 'TRUE'
else:
    if is_mapr.upper() == 'TRUE':
        is_mapr = True
    elif is_mapr.upper() == 'FALSE':
        is_mapr = False
    else:
        sys.stderr.write("WARNING: $PYCHBASE_IS_MAPR should be 'TRUE' or 'FALSE', not '%s', I am defaulting to TRUE\n" % is_mapr)



libraries = ['jvm']
library_dirs = [libjvm_dir]
if is_mapr:
    libraries.append('MapRClient')
    include_dir = os.environ.get('PYCHBASE_INCLUDE_DIR', None)
    if include_dir is None:
        sys.stderr.write("WARNING: $PYCHBASE_INCLUDE_DIR not set. I am defaulting to '/opt/mapr/include'\n")
        include_dir = '/opt/mapr/include'
    library_dir = os.environ.get('PYCHBASE_LIBRARY_DIR', None)
    if library_dir is None:
        sys.stderr.write("WARNING: $PYCHBASE_LIBRARY_DIR not set. I am defaulting to '/opt/mapr/lib'\n")
        library_dir = '/opt/mapr/lib'

    if not os.path.isdir(include_dir):
        sys.stderr.write("ERROR: $PYCHASE_INCLUDE_DIR '%s' does not exist.\n" % include_dir)
        is_fatal = True

    if not os.path.isdir(library_dir):
        sys.stderr.write("ERROR: $PYCHBASE_LIBRARY_DIR '%s' does not exist.\n" % library_dir)
        is_fatal = True
else:
    libraries.append('hbase')
    include_dir = os.environ.get('PYCHBASE_INCLUDE_DIR', None)
    if include_dir is None:
        sys.stderr.write("ERROR: Non-MapR environments must set the $PYCHBASE_INCLUDE_DIR environment variable.\n")
        is_fatal = True
    else:
        if not os.path.isdir(include_dir):
            sys.stderr.write("ERROR: $PYCHBASE_INCLUDE_DIR '%s' does not exist.\n" % include_dir)
            is_fatal = True

    library_dir = os.environ.get('PYCHBASE_LIBRARY_DIR', None)
    if library_dir is None:
        sys.stderr.write("ERROR: Non-MapR environments must set the $PYCHBASE_LIBRARY_DIR environment variable.\n")
        is_fatal = True
    else:
        if not os.path.isdir(library_dir):
            sys.stderr.write("ERROR: $PYCHBASE_LIBRARY_DIR does '%s' does not exist.\n" % library_dir)
            is_fatal = True

if is_fatal:
    sys.stderr.write('ERROR: Failed to install pychbase due to environment variables being set incorrectly. \n\tPlease read the warning/error messages above and consult the readme\n')
    raise ValueError("Failed to install. Please check the readme")

include_dirs = [include_dir]
library_dirs.append(library_dir)

print("include_dirs is %s" % include_dirs)
print("library_dirs is %s" % library_dirs)
print("libraries is %s" % libraries)

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
                    include_dirs=include_dirs,
                    libraries=libraries,
                    library_dirs=library_dirs)

setup(name='pychbase',
      version='0.1.6',
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
