#!/bin/bash

CURRENT_DIR=`pwd`
VENV_PATH=${HOME}/virtualenv
P3_PATH=${VENV_PATH}/python3/bin/
PYTEST=py.test

export PYTHONPATH=$PYTHONPATH:.

rm -rf _test_/
${P3_PATH}/${PYTEST} -vv test/ --cov-report html --cov=.
rm -rf _test_/
