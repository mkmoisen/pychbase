python setup.py install
if [ $? -eq 0 ]
then
        cd tests
        python tests.py
fi