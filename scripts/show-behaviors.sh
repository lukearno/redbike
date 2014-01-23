echo "\nBehaviors verified:\n"
grep '#B: ' tests/test_*.py | sed 's/^[ \t]*//;s/[ \t]*$//'
echo
