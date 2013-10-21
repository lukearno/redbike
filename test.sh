rm coverage.xml
rm .coverage
PYTHONWARNINGS=d nosetests -xs --with-xcover --cover-package redbike tests/
flake8 redbike tests benchmark
