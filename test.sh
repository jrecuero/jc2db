#!/bin/bash

rm -rf _test_/
py.test -vv test/ --cov-report html --cov=.
rm -rf _test_/
