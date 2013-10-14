rm coverage.xml
rm .coverage
nosetests -xs --with-xcover --cover-package redbike tests/
flake8 redbike tests benchmark
