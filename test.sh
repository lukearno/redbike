rm coverage.xml
rm .coverage
PYTHONWARNINGS=d nosetests -xs --with-xcover --cover-package redbike tests/
if [ $? -eq 0 ]; then
  echo "\nBehaviors verified:\n"
  grep '#B: ' tests/test_*.py | sed 's/^[ \t]*//;s/[ \t]*$//'
  echo
fi
flake8 redbike tests benchmark
if [ $? -eq 0 ]; then
  echo "\nStyle checks passed.\n"
fi
